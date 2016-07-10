#ifndef __EM_RADIX_HEAP_HPP_INCLUDED
#define __EM_RADIX_HEAP_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <queue>
#include <vector>
#include <limits>
#include <type_traits>
#include <algorithm>
#include <mutex>
#include <thread>
#include <condition_variable>

#include "utils.hpp"


template<typename ValueType>
class ram_queue {
  public:
    typedef ValueType value_type;
    typedef ram_queue<value_type> ram_queue_type;

  private:
    value_type *m_data;
    std::uint64_t m_beg;
    std::uint64_t m_end;
    std::uint64_t m_size;
    std::uint64_t m_max_size;

  public:
    ram_queue(std::uint64_t max_size) {
      set_empty();
      m_max_size = max_size;
      m_data = (value_type *)malloc(max_size * sizeof(value_type));
    }

    ~ram_queue() {
      free(m_data);
    }

    inline bool empty() const {
      return m_size == 0;
    }

    inline bool full() const {
      return m_size == m_max_size;
    }

    inline std::uint64_t size() const {
      return m_size;
    }

    void set_empty() {
      m_beg = 0;
      m_end = 0;
      m_size = 0;
    }

    void write_to_file(std::FILE *f) const {
      std::uint64_t beg = m_beg;
      std::uint64_t cursize = m_size;
      while (cursize > 0) {
        std::uint64_t towrite = std::min(m_max_size - beg, cursize);
        utils::write_to_file(m_data + beg, towrite, f);
        cursize -= towrite;
        beg += towrite;
        if (beg == m_max_size)
          beg = 0;
      }
    }

    void read_from_file(std::FILE *f) {
      set_empty();
      utils::read_from_file(m_data, m_max_size, f);
      m_size = m_max_size;
    }

    // Swap items from the queue with trailing items in q.
    void swap_refill(ram_queue_type *q) {
      std::uint64_t end = m_end;
      for (std::uint64_t i = 0; i < m_size; ++i) {
        if (end > 0) --end;
        else end = m_max_size - 1;
        if (q->m_end > 0) --q->m_end;
        else q->m_end = q->m_max_size - 1;
        std::swap(m_data[end], q->m_data[q->m_end]);
      }
      q->m_beg = q->m_end;
    }

    // Refill the current queue with items from q.
    void refill(ram_queue_type *q) {
      while (m_size < m_max_size && q->m_size > 0) {
        std::uint64_t can_move = m_max_size - std::max(m_size, m_end);
        std::uint64_t q_can_move = std::min(q->m_max_size - q->m_beg, q->m_size);
        std::uint64_t tomove = std::min(can_move, q_can_move);
        std::copy(q->m_data + q->m_beg, q->m_data + q->m_beg + tomove, m_data + m_end);
        m_size += tomove;
        m_end += tomove;
        if (m_end == m_max_size)
          m_end = 0;
        q->m_size -= tomove;
        q->m_beg += tomove;
        if (q->m_beg == q->m_max_size)
          q->m_beg = 0;
      }
    }

    inline std::uint64_t size_in_bytes() const {
      return m_size * sizeof(value_type);
    }

    void push(value_type x) {
      m_data[m_end++] = x;
      if (m_end == m_max_size)
        m_end = 0;
      ++m_size;
    }

    value_type &front() {
      return m_data[m_beg];
    }

    void pop() {
      --m_size;
      ++m_beg;
      if (m_beg == m_max_size)
        m_beg = 0;
    }
};

template<typename ValueType, typename RadixHeapType>
class em_queue {
  public:
    typedef ValueType value_type;

  private:
    typedef ram_queue<value_type> ram_queue_type;
    typedef RadixHeapType radix_heap_type;

    radix_heap_type *m_radix_heap;
    std::queue<ram_queue_type*> m_full_ram_queues;
    ram_queue_type *m_head_ram_queue;
    ram_queue_type *m_tail_ram_queue;
    std::uint64_t m_items_per_ram_queue;

    std::FILE *m_file;
    std::string m_filename;
    std::uint64_t m_file_size;
    std::uint64_t m_file_head;

    std::uint64_t m_size;
    std::uint64_t m_io_volume;

  public:
    inline bool full_ram_queue_available() const {
      return !m_full_ram_queues.empty();
    }

    inline ram_queue_type *flush_front_ram_queue() {
      ram_queue_type *q = m_full_ram_queues.front();
      m_full_ram_queues.pop();
      m_io_volume += q->size_in_bytes();
      m_file_size += q->size();
      ram_queue_type *ret = m_radix_heap->issue_write_request(q, m_file);
      return ret;
    }

  public:
    em_queue(std::uint64_t items_per_ram_queue, std::string filename, radix_heap_type *radix_heap) {
      m_radix_heap = radix_heap;
      m_size = 0;
      m_io_volume = 0;
      m_items_per_ram_queue = items_per_ram_queue;

      // Initialize file.
      m_filename = filename;
      m_file = utils::file_open(m_filename, "a+");
      m_file_size = 0;
      m_file_head = 0;

      // Initialize buffers.
      m_head_ram_queue = NULL;
      m_tail_ram_queue = NULL;
    }

    // Invariant: m_tail_ram_queue is never empty and never full.
    // (full queques are immediatelly added to m_full_ram_queues
    // and empty queues are returned back to empty ram queue
    // collection). m_head_ram_queue can be empty or full.
    // The returned boolean value indicates whether as an effect
    // of this push operation, the size of m_full_ram_queues of
    // this queue became non-empty -- this is used by radix heap
    // to update the pointed used to find the em_queue with the
    // largest ID that satisfies full_ram_queue_available() == true.
    inline bool push(value_type value) {
      bool ret = false;
      if (m_tail_ram_queue == NULL)
        m_tail_ram_queue = m_radix_heap->get_empty_ram_queue();

      m_tail_ram_queue->push(value);
      if (m_tail_ram_queue->full()) {
        if (m_head_ram_queue == m_tail_ram_queue) {
          m_tail_ram_queue = NULL;
        } else if (m_file_head == m_file_size && m_full_ram_queues.empty() &&
            m_head_ram_queue != NULL && !m_head_ram_queue->full()) {
          // Try to bypass I/O and make room in the tail ram queue
          // by moving elements directly into head ram queue.
          if (2 * m_head_ram_queue->size() <= m_items_per_ram_queue) {
            m_head_ram_queue->swap_refill(m_tail_ram_queue);
            std::swap(m_head_ram_queue, m_tail_ram_queue);
            if (m_tail_ram_queue->empty()) {
              m_radix_heap->add_empty_ram_queue(m_tail_ram_queue);
              m_tail_ram_queue = NULL;
            }
          } else m_head_ram_queue->refill(m_tail_ram_queue);
        } else {
          m_full_ram_queues.push(m_tail_ram_queue);
          m_tail_ram_queue = NULL;

          // Note: the instuction below is correct, because
          // this is the only place, where we push something
          // into m_full_ram_queues. It is possible that
          // in the meantime m_full_ram_queues becomes
          // empty and that is why when executing
          // get_empty_ram_queue, the radix_heap still
          // makes a call to full_ram_queue_available
          if (m_full_ram_queues.size() == 1)
            ret = true;
        }
      }

      ++m_size;
      return ret;
    }

    inline value_type& front() {
      if (m_head_ram_queue == NULL) {
        // Check where to look for the next item.
        if (m_file_head == m_file_size) {
          // The next item is in RAM.
          if (!m_full_ram_queues.empty()) {
            m_head_ram_queue = m_full_ram_queues.front();
            m_full_ram_queues.pop();
          } else std::swap(m_head_ram_queue, m_tail_ram_queue);
        } else {
          // The next item is on disk.
          // NOTE: it is crucial that fseek occurs after the call
          // to get_empty_ram_queue(), since in extreme case, that
          // call could modify the fseek pointer of m_file (in case
          // the empty ram queue was obtained by flushing one of the
          // full ram queues corresponding to this em_queue).
          m_head_ram_queue = m_radix_heap->get_empty_ram_queue();
          m_radix_heap->issue_read_request(m_head_ram_queue, m_file_head * sizeof(value_type), m_file);
          m_io_volume += m_head_ram_queue->size_in_bytes();
          m_file_head += m_head_ram_queue->size();
        }
      } else if (m_head_ram_queue->empty()) {
        if (m_file_head == m_file_size) {
          // The next item is in RAM.
          m_radix_heap->add_empty_ram_queue(m_head_ram_queue);
          m_head_ram_queue = NULL;
          if (!m_full_ram_queues.empty()) {
            m_head_ram_queue = m_full_ram_queues.front();
            m_full_ram_queues.pop();
          } else std::swap(m_head_ram_queue, m_tail_ram_queue);
        } else {
          // The next item is on disk.
          m_radix_heap->issue_read_request(m_head_ram_queue, m_file_head * sizeof(value_type), m_file);
          m_io_volume += m_head_ram_queue->size_in_bytes();
          m_file_head += m_head_ram_queue->size();
        }
      }

      return m_head_ram_queue->front();
    }

    inline void pop() {
      (void) front();
      --m_size;
      m_head_ram_queue->pop();
    }

    inline bool empty() const {
      return m_size == 0;
    }

    inline std::uint64_t size() const {
      return m_size;
    }

    inline std::uint64_t io_volume() const {
      return m_io_volume;
    }

    inline std::uint64_t get_items_per_ram_queue() const {
      return m_items_per_ram_queue;
    }

    void reset_file() {
      std::fclose(m_file);
      utils::file_delete(m_filename);
      m_file = utils::file_open(m_filename, "a+");
      m_file_size = 0;
      m_file_head = 0;
    }

    void reset_buffers() {
      if (m_tail_ram_queue == m_head_ram_queue)
        m_tail_ram_queue = NULL;
      if (m_head_ram_queue != NULL) {
        m_head_ram_queue->set_empty();
        m_radix_heap->add_empty_ram_queue(m_head_ram_queue);
        m_head_ram_queue = NULL;
      }
      if (m_tail_ram_queue != NULL) {
        m_tail_ram_queue->set_empty();
        m_radix_heap->add_empty_ram_queue(m_tail_ram_queue);
        m_tail_ram_queue = NULL;
      }
      while (!m_full_ram_queues.empty()) {
        ram_queue_type *q = m_full_ram_queues.front();
        m_full_ram_queues.pop();
        q->set_empty();
        m_radix_heap->add_empty_ram_queue(q);
      }
    }

    ~em_queue() {
      // Close and delete the file.
      std::fclose(m_file);
      if (utils::file_exists(m_filename))
        utils::file_delete(m_filename);
      reset_buffers();
    }
};

template<typename KeyType, typename ValueType>
class em_radix_heap {
  static_assert(sizeof(KeyType) <= 8,
      "em_radix_heap: sizeof(KeyType) > 8!");
  static_assert(std::is_unsigned<KeyType>::value,
      "em_radix_heap: KeyType not unsigned!");

  public:
    typedef KeyType key_type;
    typedef ValueType value_type;
    typedef em_radix_heap<key_type, value_type> radix_heap_type;

  private:
    template<typename S, typename T>
    struct packed_pair {
      packed_pair() {}
      packed_pair(S &f, T &s) {
        first = f;
        second = s;
      }

      S first;
      T second;
    } __attribute__((packed));

  private:
    typedef packed_pair<key_type, value_type> pair_type;
    typedef ram_queue<pair_type> ram_queue_type;
    typedef em_queue<pair_type, radix_heap_type> em_queue_type;

  private:
    static const std::uint64_t k_opt_single_queue_size_bytes = (1L << 20);
    static const std::uint64_t k_io_queues = 8;
    enum e_request_type { write_request, read_request };

    template<typename queue_type>
    struct io_request {
      io_request(queue_type *queue, std::FILE *file,
          e_request_type type, std::uint64_t pos = 0) {
        m_queue = queue;
        m_file = file;
        m_type = type;
        m_pos = pos;
      }

      queue_type *m_queue;
      std::FILE *m_file;
      e_request_type m_type;
      std::uint64_t m_pos;
    };

    template<typename req_type>
    struct request_queue {
      request_queue()
        : m_no_more_requests(false) {}

      req_type get() {
        req_type ret = m_requests.front();
        m_requests.pop();
        return ret;
      }

      void add(req_type &request) {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_requests.push(request);
      }

      inline bool empty() const {
        return m_requests.empty();
      }

      std::queue<req_type> m_requests;
      std::condition_variable m_cv;
      std::mutex m_mutex;
      bool m_no_more_requests;
    };

    template<typename queue_type>
    struct ram_queue_collection {
      ram_queue_collection(std::uint64_t n_queues, std::uint64_t items_per_queue) {
        for (std::uint64_t i = 0; i < n_queues; ++i)
          m_queues.push_back(new queue_type(items_per_queue));
      }

      ~ram_queue_collection() {
        for (std::uint64_t i = 0; i < m_queues.size(); ++i)
          delete m_queues[i];
      }

      queue_type* get() {
        queue_type *ret = m_queues.back();
        m_queues.pop_back();
        return ret;
      }

      void add(queue_type *q) {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_queues.push_back(q);
      }

      inline bool empty() const {
        return m_queues.empty();
      }

      std::vector<queue_type*> m_queues;
      std::condition_variable m_cv;
      std::mutex m_mutex;
    };

  private:
    template<typename S, typename T>
    static void async_io_thread_code(em_radix_heap<S, T> *caller) {
      typedef packed_pair<S, T> pair_type;
      typedef ram_queue<pair_type> ram_queue_type;
      typedef io_request<ram_queue_type> req_type;
      while (true) {
        // Wait for request or until 'no more requests' flag is set.
        std::unique_lock<std::mutex> lk(caller->m_io_request_queue.m_mutex);
        while (caller->m_io_request_queue.empty() &&
            !(caller->m_io_request_queue.m_no_more_requests))
          caller->m_io_request_queue.m_cv.wait(lk);

        if (caller->m_io_request_queue.empty() &&
            caller->m_io_request_queue.m_no_more_requests) {
          // No more requests -- exit.
          lk.unlock();
          break;
        }

        // Extract the request from the collection.
        req_type request = caller->m_io_request_queue.get();
        lk.unlock();

        // Process the request.
        if (request.m_type == write_request) {
          request.m_queue->write_to_file(request.m_file);
          request.m_queue->set_empty();

          // Add the (now empty) queue to the collection
          // of empty queues and notify the waiting thread.
          caller->m_empty_io_queues->add(request.m_queue);
          caller->m_empty_io_queues->m_cv.notify_one();
        } else {
          std::fseek(request.m_file, request.m_pos, SEEK_SET);
          request.m_queue->read_from_file(request.m_file);

          // Let the waiting main thread know that
          // the read request is now completed.
          std::unique_lock<std::mutex> lk2(caller->m_read_request_mutex);
          caller->m_read_io_request_complete = true;
          lk2.unlock();
          caller->m_read_request_cv.notify_one();
        }
      }
    }

    typedef ram_queue_collection<ram_queue_type> ram_queue_collection_type;
    typedef io_request<ram_queue_type> request_type;
    typedef request_queue<request_type> request_queue_type;

    ram_queue_collection_type *m_empty_io_queues;
    request_queue_type m_io_request_queue;
    std::condition_variable m_read_request_cv;
    std::mutex m_read_request_mutex;
    bool m_read_io_request_complete;
    std::thread *m_io_thread;

    std::uint64_t m_size;
    std::uint64_t m_key_lower_bound;
    std::uint64_t m_cur_bottom_level_queue_ptr;
    std::uint64_t m_min_compare_ptr;
    std::uint64_t m_get_empty_ram_queue_ptr;
    std::uint64_t m_em_queue_count;
    std::uint64_t m_bottom_level_radix;

    // Lookup tables used to compute bucket ID.
    std::vector<std::uint64_t> m_bin_len_to_level_id;
    std::vector<std::uint64_t> m_level_mask;
    std::vector<std::uint64_t> m_sum_of_radix_logs;
    std::vector<std::uint64_t> m_sum_of_radixes;

    em_queue_type **m_queues;
    std::vector<std::uint64_t> m_queue_min;
    std::vector<ram_queue_type*> m_empty_ram_queues;

  public:
    em_radix_heap(std::vector<std::uint64_t> radix_logs, std::string filename, std::uint64_t ram_use) {
      std::uint64_t em_queue_count = (radix_logs.size() - 1);
      for (std::uint64_t i = 0; i < radix_logs.size(); ++i)
        em_queue_count += (1UL << radix_logs[i]);
      std::uint64_t required_ram_queues_count = em_queue_count + 1;

      // Decide on the size of a single ram queue and their number.
      if ((required_ram_queues_count + k_io_queues) * k_opt_single_queue_size_bytes <= ram_use) {
        // Best case, we can allocate at least the required
        // number of ram queues of optimal size.
        std::uint64_t ram_for_nonio_ram_queues = ram_use - k_io_queues * k_opt_single_queue_size_bytes;
        std::uint64_t n_ram_queues = ram_for_nonio_ram_queues / k_opt_single_queue_size_bytes;
        std::uint64_t items_per_ram_queue = std::max(1UL, k_opt_single_queue_size_bytes / sizeof(pair_type));
        init(radix_logs, filename, n_ram_queues, items_per_ram_queue);
      } else {
        // Not enough RAM to use optimal queue size. We shrink
        // the queue size and allocate only the required amount.
        std::uint64_t single_queue_size_bytes = ram_use / (required_ram_queues_count + k_io_queues);
        std::uint64_t n_ram_queues = required_ram_queues_count;
        std::uint64_t items_per_ram_queue = std::max(1UL, single_queue_size_bytes / sizeof(pair_type));
        init(radix_logs, filename, n_ram_queues, items_per_ram_queue);
      }
    }

    em_radix_heap(std::vector<std::uint64_t> radix_logs, std::string filename,
        std::uint64_t n_ram_queues, std::uint64_t items_per_ram_queue) {
      init(radix_logs, filename, n_ram_queues, items_per_ram_queue);
    }

    void init(std::vector<std::uint64_t> radix_logs, std::string filename,
        std::uint64_t n_ram_queues, std::uint64_t items_per_ram_queue) {
      std::uint64_t radix_logs_sum = std::accumulate(radix_logs.begin(), radix_logs.end(), 0UL);
      // Compute m_level_mask lookup table.
      m_level_mask = std::vector<std::uint64_t>(radix_logs.size());
      for (std::uint64_t i = 0; i < radix_logs.size(); ++i)
        m_level_mask[i] = (1UL << radix_logs[radix_logs.size() - 1 - i]) - 1;

      // Compute m_bin_len_to_level_id lookup table.
      m_bin_len_to_level_id = std::vector<std::uint64_t>(radix_logs_sum + 1);
      std::uint64_t level_cnt = 0, ptr = 0;
      for (std::uint64_t i = radix_logs.size(); i > 0; --i) {
        for (std::uint64_t j = 0; j < radix_logs[i - 1]; ++j)
          m_bin_len_to_level_id[++ptr] = level_cnt;
        ++level_cnt;
      }

      // Compute m_sum_of_radix_logs lookup table.
      m_sum_of_radix_logs = std::vector<std::uint64_t>(radix_logs.size());
      for (std::uint64_t i = 0, s = 0; i < radix_logs.size(); ++i) {
        m_sum_of_radix_logs[i] = s;
        s += radix_logs[radix_logs.size() - 1 - i];
      }

      // Compute m_sum_of_radixes lookup table.
      m_sum_of_radixes = std::vector<std::uint64_t>(radix_logs.size());
      std::uint64_t sum_of_radixes = 0;
      for (std::uint64_t i = 0; i < radix_logs.size(); ++i) {
        m_sum_of_radixes[i] = sum_of_radixes - i;
        sum_of_radixes += (1UL << radix_logs[radix_logs.size() - 1 - i]);
      }

      m_size = 0;
      m_key_lower_bound = 0;
      m_cur_bottom_level_queue_ptr = 0;
      m_get_empty_ram_queue_ptr = 0;
      m_min_compare_ptr = 0;
      m_bottom_level_radix = (1UL << radix_logs.back());

      // Allocate EM queues.
      m_em_queue_count = sum_of_radixes - (radix_logs.size() - 1);
      m_queues = new em_queue_type*[m_em_queue_count];
      for (std::uint64_t i = 0; i < m_em_queue_count; ++i) {
        std::string queue_filename = filename + ".queue." +
          utils::intToStr(i) + "." + utils::random_string_hash();
        m_queues[i] = new em_queue_type(items_per_ram_queue, queue_filename, this);
      }
      m_queue_min = std::vector<std::uint64_t>(m_em_queue_count,
          std::numeric_limits<std::uint64_t>::max());

      // Allocate empty RAM queues.
      n_ram_queues = std::max(n_ram_queues, m_em_queue_count + 1);
      for (std::uint64_t j = 0; j < n_ram_queues; ++j)
        m_empty_ram_queues.push_back(new ram_queue_type(items_per_ram_queue));

      // Allocate collection of empty I/O queue.
      m_empty_io_queues = new ram_queue_collection_type(k_io_queues, items_per_ram_queue);

      // Start I/O thread.
      m_io_thread = new std::thread(async_io_thread_code<key_type, value_type>, this);
    }

    // Note: the read request is blocking!
    void issue_read_request(ram_queue_type *q, std::uint64_t pos, std::FILE *f) {
      // Creare read request.
      request_type req(q, f, read_request, pos);
      m_read_io_request_complete = false;

      // Add to requests queue.
      m_io_request_queue.add(req);
      m_io_request_queue.m_cv.notify_one();

      // Wait for completion.
      std::unique_lock<std::mutex> lk(m_read_request_mutex);
      while (!m_read_io_request_complete)
        m_read_request_cv.wait(lk);
      lk.unlock();
    }

    // Note: the write request is non-blocking!
    // Returns the new, empty ram queue.
    ram_queue_type* issue_write_request(ram_queue_type *q, std::FILE *f) {
      // Creat write request.
      request_type req(q, f, write_request);

      // Add to requests queue and exit
      // (unlike in the case of read req,
      // we don't wait for completion).
      m_io_request_queue.add(req);
      m_io_request_queue.m_cv.notify_one();

      // Extract and return an empty ram queue.
      std::unique_lock<std::mutex> lk(m_empty_io_queues->m_mutex);
      while (m_empty_io_queues->empty())
        m_empty_io_queues->m_cv.wait(lk);
      ram_queue_type *ret = m_empty_io_queues->get();
      lk.unlock();

      return ret;
    }

  private:
    inline std::uint64_t get_queue_id(key_type key) const {
      std::uint64_t x = (std::uint64_t)key;
      if (x == m_key_lower_bound)
        return (x & (m_bottom_level_radix - 1));

      std::uint64_t level_id = m_bin_len_to_level_id[64 - __builtin_clzll(x ^ m_key_lower_bound)];
      std::uint64_t bucket_id = (x >> m_sum_of_radix_logs[level_id]) & m_level_mask[level_id];
      std::uint64_t queue_id = m_sum_of_radixes[level_id] + bucket_id;

      return queue_id;
    }

  public:
    inline void push(key_type key, value_type value) {
      ++m_size;
      std::uint64_t id = get_queue_id(key);
      if (m_queues[id]->push(pair_type(key, value)))
        m_get_empty_ram_queue_ptr = std::max(m_get_empty_ram_queue_ptr, id);
      m_queue_min[id] = std::min(m_queue_min[id], (std::uint64_t)key);
      m_min_compare_ptr = std::min(m_min_compare_ptr, id);
    }

    // Return true iff x <= key, where x is the
    // smallest element currently stored in the heap.
    inline bool min_compare(key_type key) {
      if (empty()) return false;
      if (!m_queues[m_min_compare_ptr]->empty())
        return (m_queue_min[m_min_compare_ptr] <= (std::uint64_t)key);
      std::uint64_t id = get_queue_id(key);
      while (m_min_compare_ptr != id && m_queues[m_min_compare_ptr]->empty())
        ++m_min_compare_ptr;
      return (!m_queues[m_min_compare_ptr]->empty() &&
          m_queue_min[m_min_compare_ptr] <= (std::uint64_t)key);
    }

    // Remove and return the item with the smallest key.
    inline std::pair<key_type, value_type> extract_min() {
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        redistribute();
      key_type key = m_queues[m_cur_bottom_level_queue_ptr]->front().first;
      value_type value = m_queues[m_cur_bottom_level_queue_ptr]->front().second;
      m_queues[m_cur_bottom_level_queue_ptr]->pop();
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty()) {
        m_queues[m_cur_bottom_level_queue_ptr]->reset_buffers();
        m_queues[m_cur_bottom_level_queue_ptr]->reset_file();
      }
      --m_size;
      return std::make_pair(key, value);
    }

    inline std::uint64_t size() const {
      return m_size;
    }

    inline bool empty() const {
      return m_size == 0;
    }

    inline std::uint64_t io_volume() const {
      std::uint64_t result = 0;
      for (std::uint64_t i = 0; i < m_em_queue_count; ++i)
        result += m_queues[i]->io_volume();
      return result;
    }

    ram_queue_type* get_empty_ram_queue() {
      if (m_empty_ram_queues.empty()) {
        // The loop below is correct, because every time, during the push()
        // operation on one of the queues we check, whether m_full_ram_queues
        // just became non-empty, and if yes, the push returns true, which
        // causes the update of m_get_empty_ram_queue_ptr.

        for (std::uint64_t j = m_get_empty_ram_queue_ptr + 1; j < m_em_queue_count; ++j) {
          if (m_queues[j]->full_ram_queue_available()) {
            fprintf(stderr, "Error: m_get_empty_ram_queue_ptr incorrect!\n");
            std::exit(EXIT_FAILURE);
          }
        }

        while (!m_queues[m_get_empty_ram_queue_ptr]->full_ram_queue_available())
          --m_get_empty_ram_queue_ptr;
        m_empty_ram_queues.push_back(m_queues[m_get_empty_ram_queue_ptr]->flush_front_ram_queue());
      }

      ram_queue_type *q = m_empty_ram_queues.back();
      m_empty_ram_queues.pop_back();
      return q;
    }

    void add_empty_ram_queue(ram_queue_type *q) {
      m_empty_ram_queues.push_back(q);
    }

    ~em_radix_heap() {
      // Let the I/O thread know it should finish.
      std::unique_lock<std::mutex> lk(m_io_request_queue.m_mutex);
      m_io_request_queue.m_no_more_requests = true;
      lk.unlock();
      m_io_request_queue.m_cv.notify_one();

      // Wait for the I/O thread to finish.
      m_io_thread->join();
      delete m_io_thread;

      // Clean up.
      delete m_empty_io_queues;
      for (std::uint64_t i = 0; i < m_em_queue_count; ++i)
        delete m_queues[i];
      delete[] m_queues;
      for (std::uint64_t i = 0; i < m_empty_ram_queues.size(); ++i)
        delete m_empty_ram_queues[i];
    }

  private:
    void redistribute() {
      while (m_cur_bottom_level_queue_ptr < m_bottom_level_radix && m_queues[m_cur_bottom_level_queue_ptr]->empty())
        m_queue_min[m_cur_bottom_level_queue_ptr++] = std::numeric_limits<std::uint64_t>::max();

      if (m_cur_bottom_level_queue_ptr < m_bottom_level_radix) {
        m_key_lower_bound = m_queue_min[m_cur_bottom_level_queue_ptr];
      } else {
        std::uint64_t id = m_bottom_level_radix;
        while (m_queues[id]->empty()) ++id;
        m_key_lower_bound = m_queue_min[id];

        // Redistribute elements in m_queues[id].
        std::uint64_t queue_size = m_queues[id]->size();
        for (std::uint64_t i = 0; i < queue_size; ++i) {
          pair_type p = m_queues[id]->front(); m_queues[id]->pop();
          std::uint64_t newid = get_queue_id(p.first);
          if (m_queues[newid]->push(p))
            m_get_empty_ram_queue_ptr = std::max(m_get_empty_ram_queue_ptr, newid);
          m_queue_min[newid] = std::min(m_queue_min[newid], (std::uint64_t)p.first);
          if (newid < m_cur_bottom_level_queue_ptr)
            m_cur_bottom_level_queue_ptr = newid;
        }
        m_queues[id]->reset_file();
        m_queues[id]->reset_buffers();
        m_queue_min[id] = std::numeric_limits<std::uint64_t>::max();
      }
      m_min_compare_ptr = m_cur_bottom_level_queue_ptr;
    }
};

#endif  // __EM_RADIX_HEAP_HPP_INCLUDED
