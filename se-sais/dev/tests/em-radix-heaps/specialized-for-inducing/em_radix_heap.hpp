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

  private:
    typedef std::pair<key_type, value_type> pair_type;
    typedef em_queue<pair_type> queue_type;

  private:
    std::uint64_t m_size;
    std::uint64_t m_key_lower_bound;
    std::uint64_t m_cur_bottom_level_queue_ptr;
    std::uint64_t m_aux_bottom_level_queue_ptr;
    std::uint64_t m_radix_log;
    std::uint64_t m_radix;
    std::uint64_t m_radix_mask;
    std::uint64_t m_div_ceil_radix_log[64];

    queue_type **m_queues;
    std::vector<std::uint64_t> m_queue_min;

  public:
    em_radix_heap(std::uint64_t radix_log, std::uint64_t buffer_size, std::string filename) {
      for (std::uint64_t i = 0; i < 64; ++i)
        m_div_ceil_radix_log[i] = (i + radix_log - 1) / radix_log;

      m_size = 0;
      m_key_lower_bound = 0;
      m_cur_bottom_level_queue_ptr = 0;
      m_aux_bottom_level_queue_ptr = 0;
      m_radix_log = radix_log;
      m_radix = (1UL << m_radix_log);
      m_radix_mask = m_radix - 1;

      // Allocate queues.
      std::uint64_t m_depth = ((8UL * sizeof(key_type)) + m_radix_log - 1) / m_radix_log;
      std::uint64_t n_queues = m_depth * (m_radix - 1) + 1;
      m_queues = new queue_type*[n_queues];
      for (std::uint64_t i = 0; i < n_queues; ++i) {
        std::string queue_filename = filename + ".queue." +
          utils::intToStr(i) + "." + utils::random_string_hash();
        m_queues[i] = new queue_type(buffer_size, queue_filename);
      }
      m_queue_min = std::vector<std::uint64_t>(n_queues,
          std::numeric_limits<std::uint64_t>::max());
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

    inline key_type top_key_private() {
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        redistribute();
      return m_queues[m_cur_bottom_level_queue_ptr]->front().first;
    }

    inline value_type& top_value_private() {
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        redistribute();
      return m_queues[m_cur_bottom_level_queue_ptr]->front().second;
    }

    inline void pop() {
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        redistribute();
      --m_size;
      m_queues[m_cur_bottom_level_queue_ptr]->pop();
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        m_queues[m_cur_bottom_level_queue_ptr]->reset_file();
    }

  public:
    inline void push(key_type key, value_type value) {
      ++m_size;
      std::uint64_t id = get_queue_id(key);
      m_queues[id]->push(std::make_pair(key, value));
      m_queue_min[id] = std::min(m_queue_min[id], (std::uint64_t)key);
      m_aux_bottom_level_queue_ptr = std::min(m_aux_bottom_level_queue_ptr, id);
    }

    inline bool is_top_key_leq(value_type key) {
      if (empty()) return false;
      if (!m_queues[m_aux_bottom_level_queue_ptr]->empty())
        return (m_queue_min[m_aux_bottom_level_queue_ptr] <= (std::uint64_t)key);
      std::uint64_t id = get_queue_id(key);
      while (m_aux_bottom_level_queue_ptr != id && m_queues[m_aux_bottom_level_queue_ptr]->empty())
        ++m_aux_bottom_level_queue_ptr;
      return (!m_queues[m_aux_bottom_level_queue_ptr]->empty() &&
          m_queue_min[m_aux_bottom_level_queue_ptr] <= (std::uint64_t)key);
    }

    inline std::pair<key_type, value_type> extract_min() {
      key_type key = top_key_private();
      value_type value = top_value_private();
      pop();
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
      std::uint64_t n_queues = m_depth * (m_radix - 1) + 1;
      for (std::uint64_t i = 0; i < n_queues; ++i)
        result += m_queues[i]->io_volume();
      return result;
    }

    ~em_radix_heap() {
      // Clean up.
      std::uint64_t m_depth = ((8UL * sizeof(key_type)) + m_radix_log - 1) / m_radix_log;
      std::uint64_t n_queues = m_depth * (m_radix - 1) + 1;
      for (std::uint64_t i = 0; i < n_queues; ++i)
        delete m_queues[i];
      delete[] m_queues;
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
          m_queues[newid]->push(p);
          m_queue_min[newid] = std::min(m_queue_min[newid], (std::uint64_t)p.first);
          if (newid < m_cur_bottom_level_queue_ptr)
            m_cur_bottom_level_queue_ptr = newid;
        }
        m_queues[id]->reset_file();
        m_queue_min[id] = std::numeric_limits<std::uint64_t>::max();
      }
      m_aux_bottom_level_queue_ptr = m_cur_bottom_level_queue_ptr;
    }
};

#endif  // __EM_RADIX_HEAP_HPP_INCLUDED
