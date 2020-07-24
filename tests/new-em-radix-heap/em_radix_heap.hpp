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
      q->write_to_file(m_file);
      m_io_volume += q->size_in_bytes();
      m_file_size += q->size();
      q->set_empty();
      return q;
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
          std::fseek(m_file, m_file_head * sizeof(value_type), SEEK_SET);
          m_head_ram_queue->read_from_file(m_file);
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
          std::fseek(m_file, m_file_head * sizeof(value_type), SEEK_SET);
          m_head_ram_queue->read_from_file(m_file);
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
    typedef std::pair<key_type, value_type> pair_type;
    typedef ram_queue<pair_type> ram_queue_type;
    typedef em_queue<pair_type, radix_heap_type> em_queue_type;

  private:
    std::uint64_t m_size;
    std::uint64_t m_key_lower_bound;
    std::uint64_t m_cur_bottom_level_queue_ptr;
    std::uint64_t m_min_compare_ptr;
    std::uint64_t m_get_empty_ram_queue_ptr;
    std::uint64_t m_radix_log;
    std::uint64_t m_radix;
    std::uint64_t m_radix_mask;
    std::uint64_t m_em_queue_count;
    std::uint64_t m_div_ceil_radix_log[64];

    em_queue_type **m_queues;
    std::vector<std::uint64_t> m_queue_min;
    std::vector<ram_queue_type*> m_empty_ram_queues;

  public:
    em_radix_heap(std::uint64_t radix_log, std::string filename,
        std::uint64_t n_ram_queues, std::uint64_t items_per_ram_queue) {
      for (std::uint64_t i = 0; i < 64; ++i)
        m_div_ceil_radix_log[i] = (i + radix_log - 1) / radix_log;

      m_size = 0;
      m_key_lower_bound = 0;
      m_cur_bottom_level_queue_ptr = 0;
      m_get_empty_ram_queue_ptr = 0;
      m_min_compare_ptr = 0;
      m_radix_log = radix_log;
      m_radix = (1UL << m_radix_log);
      m_radix_mask = m_radix - 1;

      // Allocate EM queues.
      std::uint64_t m_depth = ((8UL * sizeof(key_type)) + m_radix_log - 1) / m_radix_log;
      m_em_queue_count = m_depth * (m_radix - 1) + 1;
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
    }

  private:
    inline std::uint64_t get_queue_id(key_type key) const {
      std::uint64_t x = (std::uint64_t)key;
      if (x == m_key_lower_bound) return (x & m_radix_mask);

      std::uint64_t n_digs = m_div_ceil_radix_log[64 - __builtin_clzll(x ^ m_key_lower_bound)];
      std::uint64_t most_sig_digit = ((x >> ((n_digs - 1) * m_radix_log)) & m_radix_mask);
      std::uint64_t queue_id = (((n_digs - 1) << m_radix_log) - (n_digs - 1)) + most_sig_digit;
      return queue_id;
    }

  public:
    inline void push(key_type key, value_type value) {
      ++m_size;
      std::uint64_t id = get_queue_id(key);
      if (m_queues[id]->push(std::make_pair(key, value)))
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
      std::uint64_t m_depth = ((8UL * sizeof(key_type)) + m_radix_log - 1) / m_radix_log;
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
      // Clean up.
      for (std::uint64_t i = 0; i < m_em_queue_count; ++i)
        delete m_queues[i];
      delete[] m_queues;
      for (std::uint64_t i = 0; i < m_empty_ram_queues.size(); ++i)
        delete m_empty_ram_queues[i];
    }

  private:
    void redistribute() {
      while (m_cur_bottom_level_queue_ptr < m_radix && m_queues[m_cur_bottom_level_queue_ptr]->empty())
        m_queue_min[m_cur_bottom_level_queue_ptr++] = std::numeric_limits<std::uint64_t>::max();

      if (m_cur_bottom_level_queue_ptr < m_radix) {
        m_key_lower_bound = m_queue_min[m_cur_bottom_level_queue_ptr];
      } else {
        std::uint64_t id = m_radix;
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
