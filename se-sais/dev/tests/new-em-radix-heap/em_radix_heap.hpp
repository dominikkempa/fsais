#ifndef __EM_RADIX_HEAP_HPP_INCLUDED
#define __EM_RADIX_HEAP_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <vector>
#include <limits>
#include <type_traits>
#include <algorithm>

#include "em_queue.hpp"


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
    em_radix_heap(std::uint64_t radix_log, std::string filename, std::uint64_t n_ram_queues, std::uint64_t items_per_ram_queue) {
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
    inline bool min_compare(value_type key) {
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
        #if 1  // debug check
        for (std::uint64_t j = m_get_empty_ram_queue_ptr + 1; j < m_em_queue_count; ++j) {
          if (m_queues[j]->full_ram_queue_available()) {
            fprintf(stderr, "Error: m_get_empty_ram_queue_ptr incorrect!\n");
            std::exit(EXIT_FAILURE);
          }
        }
        #endif

        // debug //
        /*if (m_queues[m_get_empty_ram_queue_ptr]->full_ram_queue_available())
          fprintf(stderr, "m_get_empty_ram_queue_ptr spot on!\n");
        else fprintf(stderr, "m_get_empty_ram_queue_ptr was not exact\n");*/
        ///////////

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
