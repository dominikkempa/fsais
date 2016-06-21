#ifndef __RADIX_HEAP_H_INCLUDED
#define __RADIX_HEAP_H_INCLUDED

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

    queue_type **m_queues;
    std::vector<std::uint64_t> m_queue_min;

  public:
    radix_heap() {
      m_size = 0;
      m_min_key = 0;
      m_cur_bottom_level_queue_ptr = 0;

      // Allocate queues.
      std::uint64_t n_queues = 8 * sizeof(key_type) + 1;
      m_queues = new queue_type*[n_queues];
      for (std::uint64_t i = 0; i < n_queues; ++i)
        m_queues[i] = new queue_type();
      m_queue_min = std::vector<std::uint64_t>(n_queues,
          std::numeric_limits<std::uint64_t>::max());
    }

  private:
    inline std::uint64_t get_queue_id(key_type key) const {
      std::uint64_t x = (std::uint64_t)key;
      if (x == m_min_key) return (x & 1);
      else return 64 - __builtin_clzll(x ^ m_min_key);
    }

  public:
    inline void push(key_type key, value_type value) {
//      fprintf(stderr, "radix_heap::push(%lu, %lu)\n", (std::uint64_t)key, (std::uint64_t)value);
      ++m_size;
      std::uint64_t id = get_queue_id(key);
//      fprintf(stderr, "  id = %lu\n", id);
//      fprintf(stderr, "  m_min_key = %lu\n", m_min_key);
      m_queues[id]->push(std::make_pair(key, value));
      m_queue_min[id] = std::min(m_queue_min[id], (std::uint64_t)key);
    }

    inline key_type top_key() {
//      fprintf(stderr, "radix_heap::top_key()\n");
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        redistribute();
//      fprintf(stderr, "  m_cur_bottom_level_queue_ptr = %lu\n", m_cur_bottom_level_queue_ptr);
//      fprintf(stderr, "  m_min_key = %lu\n", m_min_key);
      return m_queues[m_cur_bottom_level_queue_ptr]->front().first;
    }

    inline value_type& top_value() {
//      fprintf(stderr, "radix_heap::top_value()\n");
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        redistribute();
//      fprintf(stderr, "  m_cur_bottom_level_queue_ptr = %lu\n", m_cur_bottom_level_queue_ptr);
//      fprintf(stderr, "  m_min_key = %lu\n", m_min_key);
      return m_queues[m_cur_bottom_level_queue_ptr]->front().second;
    }

    inline void pop() {
//      fprintf(stderr, "radix_heap::pop()\n");
      if (m_queues[m_cur_bottom_level_queue_ptr]->empty())
        redistribute();
//      fprintf(stderr, "  m_cur_bottom_level_queue_ptr = %lu\n", m_cur_bottom_level_queue_ptr);
//      fprintf(stderr, "  m_min_key = %lu\n", m_min_key);
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
      std::uint64_t n_queues = 8 * sizeof(key_type) + 1;
      for (std::uint64_t i = 0; i < n_queues; ++i)
        delete m_queues[i];
      delete[] m_queues;
    }

  private:
    void redistribute() {
//      fprintf(stderr, "redistrbute()\n");
      while (m_cur_bottom_level_queue_ptr < 2 && m_queues[m_cur_bottom_level_queue_ptr]->empty())
        m_queue_min[m_cur_bottom_level_queue_ptr++] = std::numeric_limits<std::uint64_t>::max();

      if (m_cur_bottom_level_queue_ptr < 2) {
        m_min_key = m_queue_min[m_cur_bottom_level_queue_ptr];
//        fprintf(stderr, "  m_cur_bottom_level_queue_ptr = %lu, returning\n", m_cur_bottom_level_queue_ptr);
//        fprintf(stderr, "  m_min_key = %lu\n", m_min_key);
      } else {
        std::uint64_t id = 2;
        while (m_queues[id]->empty()) ++id;
        m_min_key = m_queue_min[id];
//        fprintf(stderr, "  found id = %lu, m_min_key = %lu\n", id, m_min_key);

        // Redistribute elements in m_queues[id].
        std::uint64_t queue_size = m_queues[id]->size();
        for (std::uint64_t i = 0; i < queue_size; ++i) {
          pair_type p = m_queues[id]->front(); m_queues[id]->pop();
          std::uint64_t newid = get_queue_id(p.first);
          m_queues[newid]->push(p);
          m_queue_min[newid] = std::min(m_queue_min[newid], (std::uint64_t)p.first);
//          fprintf(stderr, "  writing (%lu, %lu) to bucket %lu\n", (std::uint64_t)p.first, (std::uint64_t)p.second, newid);
          if (newid < m_cur_bottom_level_queue_ptr)
            m_cur_bottom_level_queue_ptr = newid;
        }
//        fprintf(stderr, "  m_cur_bottom_level_queue_ptr after update = %lu\n", m_cur_bottom_level_queue_ptr);
        m_queue_min[id] = std::numeric_limits<std::uint64_t>::max();
      }
    }
};

#endif  // __RADIX_HEAP_H_INCLUDED
