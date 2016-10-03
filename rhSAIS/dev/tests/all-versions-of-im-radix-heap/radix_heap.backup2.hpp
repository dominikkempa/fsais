#ifndef __RHSAIS_SRC_RADIX_HEAP_HPP_INCLUDED
#define __RHSAIS_SRC_RADIX_HEAP_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <limits>
#include <type_traits>
#include <algorithm>


namespace rhsais_private {

/*template<typename T>
class page {
  public:
    typedef T value_type;
    typedef page<T> page_type;

    page_type *m_next;
    value_type *m_data;

    page(std::uint64_t size) {
      m_data = new value_type[size];
      m_next = NULL;
    }

    ~page() {
      delete[] m_data;
    }
};*/

template<typename T, typename RadixHeapType>
class paged_queue {
  public:
    typedef T value_type;

  private:
    //typedef page<T> page_type;
    typedef RadixHeapType radix_heap_type;

    radix_heap_type *m_radix_heap;
    //page_type *m_head;
    //page_type *m_tail;
    std::uint64_t m_head_page_id;
    std::uint64_t m_tail_page_id;



    std::uint64_t m_head_ptr;
    std::uint64_t m_tail_ptr;

  public:
    paged_queue(radix_heap_type *radix_heap) {
      m_radix_heap = radix_heap;
      //m_head = NULL;
      //m_tail = NULL;
      m_head_page_id = std::numeric_limits<std::uint64_t>::max();
      m_tail_page_id = std::numeric_limits<std::uint64_t>::max();
      m_head_ptr = 0;
      m_tail_ptr = 0;
    }

    inline bool empty() const {
      //return (m_tail == NULL) || (m_tail == m_head && m_tail_ptr == m_head_ptr);
      return (m_tail_page_id == std::numeric_limits<std::uint64_t>::max()) ||
        (m_tail_page_id == m_head_page_id && m_tail_ptr == m_head_ptr);
    }

    inline value_type& front() const {
      //return m_tail->m_data[m_tail_ptr];
      return m_radix_heap->m_pages_mem[m_tail_page_id * m_radix_heap->m_pagesize + m_tail_ptr];
    }

    inline void pop() {
      ++m_tail_ptr;
      if (m_tail_ptr == m_radix_heap->m_pagesize) {
        //page_type *next_tail = m_tail->m_next;
        //m_radix_heap->m_empty_pages.push_back(m_tail);
        //m_tail = next_tail;
        //m_tail_ptr = 0;
        std::uint64_t next_tail_page_id = m_radix_heap->m_pages_next[m_tail_page_id];
        m_radix_heap->m_empty_pages.push_back(m_tail_page_id);
        m_tail_page_id = next_tail_page_id;
        m_tail_ptr = 0;
      } else if (m_tail_ptr == m_head_ptr && /*m_tail == m_head*/m_tail_page_id == m_head_page_id) {
        //m_radix_heap->m_empty_pages.push_back(m_tail);
        //m_tail = NULL;
        //m_head = NULL;
        m_radix_heap->m_empty_pages.push_back(m_tail_page_id);
        m_tail_page_id = std::numeric_limits<std::uint64_t>::max();
        m_head_page_id = std::numeric_limits<std::uint64_t>::max();
      }
    }

    inline void push(value_type x) {
      //if (m_head == NULL) {
      if (m_head_page_id == std::numeric_limits<std::uint64_t>::max()) {
        //m_head = m_radix_heap->m_empty_pages.back();
        //m_radix_heap->m_empty_pages.pop_back();
        //m_head->m_next = NULL;
        //m_tail = m_head;
        //m_head_ptr = 0;
        //m_tail_ptr = 0;
        m_head_page_id = m_radix_heap->m_empty_pages.back();
        m_radix_heap->m_empty_pages.pop_back();
        m_radix_heap->m_pages_next[m_head_page_id] = std::numeric_limits<std::uint64_t>::max();
        m_tail_page_id = m_head_page_id;
        m_head_ptr = 0;
        m_tail_ptr = 0;
      }

      //m_head->m_data[m_head_ptr++] = x;
      m_radix_heap->m_pages_mem[m_head_page_id * m_radix_heap->m_pagesize + m_head_ptr++] = x;
      if (m_head_ptr == m_radix_heap->m_pagesize) {
        //page_type *new_head = m_radix_heap->m_empty_pages.back();
        //m_radix_heap->m_empty_pages.pop_back();
        //new_head->m_next = NULL;
        //m_head->m_next = new_head;
        //m_head = new_head;
        //m_head_ptr = 0;
        std::uint64_t new_head_page_id = m_radix_heap->m_empty_pages.back();
        m_radix_heap->m_empty_pages.pop_back();
        m_radix_heap->m_pages_next[new_head_page_id] = std::numeric_limits<std::uint64_t>::max();
        m_radix_heap->m_pages_next[m_head_page_id] = new_head_page_id;
        m_head_page_id = new_head_page_id;
        m_head_ptr = 0;
      }
    }

    ~paged_queue() {
      //if (m_tail != NULL) {
      //  page_type *p = m_tail;
      //  while (p != NULL) {
      //    page_type *next = p->m_next;
      //    m_radix_heap->m_empty_pages.push_back(p);
      //    p = next;
      //  }
      //}
    }
};

template<typename KeyType, typename ValueType>
class radix_heap {
  static_assert(sizeof(KeyType) <= 8,
      "radix_heap: sizeof(KeyType) > 8!");
  static_assert(std::is_unsigned<KeyType>::value,
      "radix_heap: KeyType not unsigned!");

  public:
    typedef KeyType key_type;
    typedef ValueType value_type;
    typedef radix_heap<key_type, value_type> radix_heap_type;

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
    //typedef std::vector<pair_type> queue_type;
    //typedef page<pair_type> page_type;
    typedef paged_queue<pair_type, radix_heap_type> queue_type;

  private:
    std::uint64_t m_size;
    std::uint64_t m_key_lower_bound;
    std::uint64_t m_cur_bottom_level_queue_ptr;
    std::uint64_t m_min_compare_ptr;
    std::uint64_t m_em_queue_count;
    std::uint64_t m_bottom_level_radix;

    // Lookup tables used to compute bucket ID.
    std::vector<std::uint64_t> m_bin_len_to_level_id;
    std::vector<std::uint64_t> m_level_mask;
    std::vector<std::uint64_t> m_sum_of_radix_logs;
    std::vector<std::uint64_t> m_sum_of_radixes;

    queue_type **m_queues;
    //std::vector<std::uint64_t> m_queue_ptr;
    std::vector<std::uint64_t> m_queue_min;

  public:
    std::uint64_t m_pagesize;
    //std::vector<page_type*> m_empty_pages;
    std::vector<std::uint64_t> m_empty_pages;

    pair_type *m_pages_mem;
    std::uint64_t *m_pages_next;

  public:
    radix_heap(std::vector<std::uint64_t> radix_logs,
        std::uint64_t max_items,
        std::uint64_t pagesize = (std::uint64_t)1/*4096*/) {
      m_pagesize = pagesize;
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
      m_min_compare_ptr = 0;
      m_bottom_level_radix = (1UL << radix_logs.back());

      m_em_queue_count = sum_of_radixes - (radix_logs.size() - 1);
      m_queues = new queue_type*[m_em_queue_count];
      for (std::uint64_t i = 0; i < m_em_queue_count; ++i)
        m_queues[i] = new queue_type(this);
      m_queue_min = std::vector<std::uint64_t>(m_em_queue_count,
          std::numeric_limits<std::uint64_t>::max());
      std::uint64_t n_pages = max_items / m_pagesize + (std::uint64_t)2 * m_em_queue_count;

      //for (std::uint64_t i = 0; i < n_pages; ++i)
      //  m_empty_pages.push_back(new page_type(m_pagesize));
      m_pages_mem = new pair_type[n_pages * m_pagesize];
      m_pages_next = new std::uint64_t[n_pages];
      for (std::uint64_t i = 0; i < n_pages; ++i)
        m_empty_pages.push_back(i);
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
      //m_queues[id]->push_back(pair_type(key, value));
      m_queues[id]->push(pair_type(key, value));
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
      pair_type p = m_queues[m_cur_bottom_level_queue_ptr]->front();
      m_queues[m_cur_bottom_level_queue_ptr]->pop();
      key_type key = p.first;
      value_type value = p.second;
//      key_type key = (*m_queues[m_cur_bottom_level_queue_ptr])[m_queue_ptr[m_cur_bottom_level_queue_ptr]].first;
//      value_type value = (*m_queues[m_cur_bottom_level_queue_ptr])[m_queue_ptr[m_cur_bottom_level_queue_ptr]++].second;
//      if (m_queue_ptr[m_cur_bottom_level_queue_ptr] == m_queues[m_cur_bottom_level_queue_ptr]->size()) {
//        queue_type v;
//        m_queues[m_cur_bottom_level_queue_ptr]->clear();
//        m_queues[m_cur_bottom_level_queue_ptr]->swap(v);
//        m_queue_ptr[m_cur_bottom_level_queue_ptr] = 0;
//      }
      --m_size;
      return std::make_pair(key, value);
    }

    inline std::uint64_t size() const {
      return m_size;
    }

    inline bool empty() const {
      return m_size == 0;
    }

    ~radix_heap() {
      for (std::uint64_t i = 0; i < m_em_queue_count; ++i)
        delete m_queues[i];
      delete[] m_queues;
      //while (!m_empty_pages.empty()) {
      //  page_type *p = m_empty_pages.back();
      //  m_empty_pages.pop_back();
      //  delete p;
      //}
      delete[] m_pages_mem;
      delete[] m_pages_next;
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
        //for (std::uint64_t i = m_queue_ptr[id]; i < m_queues[id]->size(); ++i) {
        //  pair_type p = (*m_queues[id])[i];
        //  std::uint64_t newid = get_queue_id(p.first);
        //  m_queues[newid]->push_back(p);
        //  m_queue_min[newid] = std::min(m_queue_min[newid], (std::uint64_t)p.first);
        //  if (newid < m_cur_bottom_level_queue_ptr)
        //    m_cur_bottom_level_queue_ptr = newid;
        //}
        //queue_type v; m_queues[id]->clear(); m_queues[id]->swap(v); m_queue_ptr[id] = 0;
        while (!m_queues[id]->empty()) {
          pair_type p = m_queues[id]->front(); m_queues[id]->pop();
          std::uint64_t newid = get_queue_id(p.first);
          m_queues[newid]->push(p);
          m_queue_min[newid] = std::min(m_queue_min[newid], (std::uint64_t)p.first);
          if (newid < m_cur_bottom_level_queue_ptr)
            m_cur_bottom_level_queue_ptr = newid;
        }
        m_queue_min[id] = std::numeric_limits<std::uint64_t>::max();
      }
      m_min_compare_ptr = m_cur_bottom_level_queue_ptr;
    }
};

}  // namespace rhsais_private

#endif  // __RHSAIS_SRC_RADIX_HEAP_HPP_INCLUDED
