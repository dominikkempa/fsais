#ifndef __RADIX_HEAP_H_INCLUDED
#define __RADIX_HEAP_H_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <queue>
#include <vector>
#include <limits>
#include <type_traits>
#include <algorithm>


template<typename KeyType, typename ValueType>
class radix_heap {
  static_assert(sizeof(KeyType) <= 8,
      "radix_heap: sizeof(KeyType) > 8!");
  static_assert(std::is_unsigned<KeyType>::value,
      "radix_heap: KeyType not unsigned!");

  public:
    typedef KeyType key_type;
    typedef ValueType value_type;

  private:
    typedef std::pair<key_type, value_type> pair_type;
    typedef std::queue<pair_type> queue_type;

  private:
    std::uint64_t m_size;
    std::uint64_t m_min_key;
    std::uint64_t m_cur_bottom_level_queue_ptr;
    std::uint64_t m_radix_log_log;
    std::uint64_t m_radix_log;
    std::uint64_t m_radix;
    std::uint64_t m_radix_mask;

    queue_type **m_queues;
    std::vector<std::uint64_t> m_queue_min;

  public:
    radix_heap(std::uint64_t radix_log) {
      if (__builtin_popcountll(radix_log) != 1) {
        fprintf(stderr, "radix_heap: radix_log has to be a power of two!\n");
        std::exit(EXIT_FAILURE);
      }

      m_size = 0;
      m_min_key = 0;
      m_cur_bottom_level_queue_ptr = 0;
      m_radix_log_log = 63 - __builtin_clzll(radix_log);
      m_radix_log = radix_log;
      m_radix = (1UL << m_radix_log);
      m_radix_mask = m_radix - 1;

      // Allocate queues.
      std::uint64_t m_depth = ((8UL * sizeof(key_type)) + m_radix_log - 1) / m_radix_log;
      std::uint64_t n_queues = m_depth * (m_radix - 1) + 1;
      m_queues = new queue_type*[n_queues];
      for (std::uint64_t i = 0; i < n_queues; ++i)
        m_queues[i] = new queue_type();
      m_queue_min = std::vector<std::uint64_t>(n_queues,
          std::numeric_limits<std::uint64_t>::max());
    }

  private:
    inline std::uint64_t get_queue_id(key_type key) const {
      std::uint64_t x = (std::uint64_t)key;
      if (x == m_min_key) return (x & m_radix_mask);

      std::uint64_t n_digs = ((64 - __builtin_clzll(x ^ m_min_key)) + m_radix_log - 1) >> m_radix_log_log;
      std::uint64_t most_sig_digit = ((x >> ((n_digs - 1) << m_radix_log_log)) & m_radix_mask);
      std::uint64_t queue_id = (((n_digs - 1) << m_radix_log) - (n_digs - 1)) + most_sig_digit;
      return queue_id;
    }

  public:
    inline void push(key_type key, value_type value) {
      ++m_size;
      std::uint64_t id = get_queue_id(key);
      m_queues[id]->push(std::make_pair(key, value));
      m_queue_min[id] = std::min(m_queue_min[id], (std::uint64_t)key);
    }

    inline key_type top_key() {
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        redistribute();
      return m_queues[m_cur_bottom_level_queue_ptr]->front().first;
    }

    inline value_type& top_value() {
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        redistribute();
      return m_queues[m_cur_bottom_level_queue_ptr]->front().second;
    }

    inline void pop() {
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        redistribute();
      m_queues[m_cur_bottom_level_queue_ptr]->pop();
      --m_size;
    }

    inline std::uint64_t size() const {
      return m_size;
    }

    inline bool empty() const {
      return m_size == 0;
    }

    ~radix_heap() {
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
        m_min_key = m_queue_min[m_cur_bottom_level_queue_ptr];
      } else {
        std::uint64_t id = m_radix;
        while (m_queues[id]->empty()) ++id;
        m_min_key = m_queue_min[id];

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
        m_queue_min[id] = std::numeric_limits<std::uint64_t>::max();
      }
    }
};

#endif  // __RADIX_HEAP_H_INCLUDED
