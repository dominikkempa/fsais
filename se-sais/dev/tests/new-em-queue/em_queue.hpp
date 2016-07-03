#ifndef __EM_QUEUE_HPP_INCLUDED
#define __EM_QUEUE_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <queue>
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
//      fprintf(stderr, "  ram_queue(%lu)\n", max_size);
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

template<typename T> class em_queue;
template<typename ValueType>
class em_manager {
  public:
    typedef ValueType value_type;
    typedef ram_queue<value_type> ram_queue_type;
    typedef em_queue<value_type> em_queue_type;

  private:
    std::vector<ram_queue_type*> m_queues;
    em_queue_type *m_em_queue;

  public:
    em_manager(em_queue_type *queue, std::uint64_t n_queues) {
      for (std::uint64_t i = 0; i < n_queues; ++i)
        m_queues.push_back(new ram_queue_type(queue->get_items_per_ram_queue()));
      m_em_queue = queue;
//      fprintf(stderr, "  m_queues.size() = %lu\n", m_queues.size());
    }

    void flush_latest_needed_queue() {
      // Find the queue with largest id such that
      // full_ram_queue_available() return true
      // and flush the front queue.
      m_queues.push_back(m_em_queue->flush_front_ram_queue());
    }

    ram_queue_type *get_free_ram_queue() {
//      fprintf(stderr, "  get_free_ram_queue():");
      if (m_queues.empty())
        flush_latest_needed_queue();
//      fprintf(stderr, "  obtaining the result from the back\n");
      ram_queue_type *result = m_queues.back();
      m_queues.pop_back();
//      fprintf(stderr, "  about to return result\n");
      return result;
    }

    void add_empty_ram_queue(ram_queue_type *q) {
      m_queues.push_back(q);
    }

    ~em_manager() {
      while (!m_queues.empty()) {
        ram_queue_type *ram_queue = m_queues.back();
        m_queues.pop_back();
        delete ram_queue;
      }
    }
};


template<typename ValueType>
class em_queue {
  public:
    typedef ValueType value_type;

  private:
    typedef ram_queue<value_type> ram_queue_type;
    typedef em_manager<value_type> manager_type;

    manager_type *m_manager;
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
      if (m_full_ram_queues.empty()) {
        fprintf(stderr, "Error: trying to flush empty full_ram_queues!\n");
        std::exit(EXIT_FAILURE);
      }
      ram_queue_type *q = m_full_ram_queues.front();
      m_full_ram_queues.pop();

      q->write_to_file(m_file);
      m_io_volume += q->size_in_bytes();
      m_file_size += q->size();
      q->set_empty();
      return q;
    }

    void set_manager(manager_type *manager) {
      m_manager = manager;
    }

  public:
    em_queue(std::uint64_t items_per_ram_queue, std::string filename) {
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
    // collection). m_head_ram_queue can be empty and can be full.
    inline void push(value_type value) {
      if (m_tail_ram_queue == NULL)
        m_tail_ram_queue = m_manager->get_free_ram_queue();

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
              m_manager->add_empty_ram_queue(m_tail_ram_queue);
              m_tail_ram_queue = NULL;
            }
          } else m_head_ram_queue->refill(m_tail_ram_queue);
        } else {
          m_full_ram_queues.push(m_tail_ram_queue);
          m_tail_ram_queue = NULL;
        }
      }

      ++m_size;
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
          m_head_ram_queue = m_manager->get_free_ram_queue();
          std::fseek(m_file, m_file_head * sizeof(value_type), SEEK_SET);
          m_head_ram_queue->read_from_file(m_file);
          m_io_volume += m_head_ram_queue->size_in_bytes();
          m_file_head += m_head_ram_queue->size();
        }
      } else if (m_head_ram_queue->empty()) {
        if (m_file_head == m_file_size) {
          // The next item is in RAM.
          m_manager->add_empty_ram_queue(m_head_ram_queue);
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

    ~em_queue() {
      // Close and delete the file.
      std::fclose(m_file);
      if (utils::file_exists(m_filename))
        utils::file_delete(m_filename);

      // Move all ram queues back to
      // the poll of empty queues.
      if (m_tail_ram_queue == m_head_ram_queue)
        m_tail_ram_queue = NULL;
      if (m_head_ram_queue != NULL) {
        m_head_ram_queue->set_empty();
        m_manager->add_empty_ram_queue(m_head_ram_queue);
      }
      if (m_tail_ram_queue != NULL) {
        m_tail_ram_queue->set_empty();
        m_manager->add_empty_ram_queue(m_tail_ram_queue);
      }
      while (!m_full_ram_queues.empty()) {
        ram_queue_type *q = m_full_ram_queues.front();
        m_full_ram_queues.pop();
        q->set_empty();
        m_manager->add_empty_ram_queue(q);
      }
    }
};

#endif  // __EM_QUEUE_HPP_INCLUDED
