#ifndef __RHSAIS_SRC_RADIX_HEAP_HPP_INCLUDED
#define __RHSAIS_SRC_RADIX_HEAP_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <limits>
#include <type_traits>
#include <numeric>
#include <algorithm>

#include "utils.hpp"


namespace rhsais_private {

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
    struct queue_header {
      std::uint64_t m_head_page_id;
      std::uint64_t m_tail_page_id;
      std::uint64_t m_head_ptr;
      std::uint64_t m_tail_ptr;
    } __attribute__((packed));

  private:
    typedef packed_pair<key_type, value_type> pair_type;

  private:
    std::uint64_t m_size;
    std::uint64_t m_key_lower_bound;
    std::uint64_t m_bottom_level_queue_ptr;
    std::uint64_t m_min_compare_ptr;
    std::uint64_t m_queue_count;
    std::uint64_t m_bottom_level_radix;
    std::uint64_t m_pagesize;

    // Internal queue minimas.
    std::vector<std::uint64_t> m_queue_min;

    // Lookup tables used to compute bucket ID.
    std::vector<std::uint64_t> m_bin_len_to_level_id;
    std::vector<std::uint64_t> m_level_mask;
    std::vector<std::uint64_t> m_sum_of_radix_logs;
    std::vector<std::uint64_t> m_sum_of_radixes;

    // Pointers used to locate the smallest non-empty queue.
    std::vector<std::uint64_t> m_level_ptr;

    // Internal queues.
    std::uint64_t m_empty_pages_list_head;
    std::uint64_t *m_pages_next;
    pair_type *m_pages_mem;
    queue_header *m_queue_headers;

  private:
    inline bool is_internal_queue_empty(std::uint64_t queue_id) const {
      queue_header &h = m_queue_headers[queue_id];
      return (h.m_tail_page_id == std::numeric_limits<std::uint64_t>::max()) ||
        (h.m_tail_page_id == h.m_head_page_id && h.m_tail_ptr == h.m_head_ptr);
    }

    inline pair_type& internal_queue_front(std::uint64_t queue_id) const {
      queue_header &h = m_queue_headers[queue_id];
      return m_pages_mem[h.m_tail_page_id * m_pagesize + h.m_tail_ptr];
    }

    inline void internal_queue_pop(std::uint64_t queue_id) {
      queue_header &h = m_queue_headers[queue_id];
      ++h.m_tail_ptr;
      if (h.m_tail_ptr == m_pagesize) {
        std::uint64_t next_tail_page_id = m_pages_next[h.m_tail_page_id];
        m_pages_next[h.m_tail_page_id] = m_empty_pages_list_head;
        m_empty_pages_list_head = h.m_tail_page_id;
        h.m_tail_page_id = next_tail_page_id;
        h.m_tail_ptr = 0;
      } else if (h.m_tail_ptr == h.m_head_ptr && h.m_tail_page_id == h.m_head_page_id) {
        m_pages_next[h.m_tail_page_id] = m_empty_pages_list_head;
        m_empty_pages_list_head = h.m_tail_page_id;
        h.m_tail_page_id = std::numeric_limits<std::uint64_t>::max();
        h.m_head_page_id = std::numeric_limits<std::uint64_t>::max();
      }
    }

    inline void internal_queue_push(std::uint64_t queue_id, pair_type x) {
      queue_header &h = m_queue_headers[queue_id];
      if (h.m_head_page_id == std::numeric_limits<std::uint64_t>::max()) {
        h.m_head_page_id = m_empty_pages_list_head;
        m_empty_pages_list_head = m_pages_next[m_empty_pages_list_head];
        m_pages_next[h.m_head_page_id] = std::numeric_limits<std::uint64_t>::max();
        h.m_tail_page_id = h.m_head_page_id;
        h.m_head_ptr = 0;
        h.m_tail_ptr = 0;
      }

      m_pages_mem[h.m_head_page_id * m_pagesize + h.m_head_ptr++] = x;
      if (h.m_head_ptr == m_pagesize) {
        std::uint64_t new_head_page_id = m_empty_pages_list_head;
        m_empty_pages_list_head = m_pages_next[m_empty_pages_list_head];
        m_pages_next[new_head_page_id] = std::numeric_limits<std::uint64_t>::max();
        m_pages_next[h.m_head_page_id] = new_head_page_id;
        h.m_head_page_id = new_head_page_id;
        h.m_head_ptr = 0;
      }
    }

  public:
    radix_heap(std::vector<std::uint64_t> radix_logs,
        std::uint64_t max_items,
        std::uint64_t pagesize =
#ifdef SAIS_DEBUG
          (std::uint64_t)1
#else
          (std::uint64_t)4096
#endif
        ) {
      m_pagesize = pagesize;
      std::uint64_t radix_logs_sum = std::accumulate(radix_logs.begin(), radix_logs.end(), 0UL);
      if (radix_logs_sum == 0) {
        fprintf(stderr, "\nError: radix_logs_sum == 0 in radix_heap constructor!\n");
        std::exit(EXIT_FAILURE);
      }

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
      m_sum_of_radixes = std::vector<std::uint64_t>(radix_logs.size() + 1);
      m_level_ptr = std::vector<std::uint64_t>(radix_logs.size());
      std::uint64_t sum_of_radixes = 0;
      for (std::uint64_t i = 0; i < radix_logs.size(); ++i) {
        m_sum_of_radixes[i] = sum_of_radixes - i;
        m_level_ptr[i] = m_sum_of_radixes[i] + 1;
        sum_of_radixes += (1UL << radix_logs[radix_logs.size() - 1 - i]);
      }
      m_sum_of_radixes[radix_logs.size()] = sum_of_radixes - radix_logs.size();

      m_size = 0;
      m_key_lower_bound = 0;
      m_bottom_level_queue_ptr = 0;
      m_min_compare_ptr = 0;
      m_bottom_level_radix = (1UL << radix_logs.back());

      m_queue_count = sum_of_radixes - (radix_logs.size() - 1);
      m_queue_min = std::vector<std::uint64_t>(m_queue_count,
          std::numeric_limits<std::uint64_t>::max());
      std::uint64_t n_pages = max_items / m_pagesize +
        (std::uint64_t)2 * m_queue_count;

      m_pages_mem = utils::allocate_array<pair_type>(n_pages * m_pagesize);
      m_pages_next = utils::allocate_array<std::uint64_t>(n_pages);
      m_queue_headers = utils::allocate_array<queue_header>(m_queue_count);
      m_empty_pages_list_head = 0;

      for (std::uint64_t i = 0; i < m_queue_count; ++i) {
        queue_header &h = m_queue_headers[i];
        h.m_tail_page_id = std::numeric_limits<std::uint64_t>::max();
        h.m_head_page_id = std::numeric_limits<std::uint64_t>::max();
      }

      for (std::uint64_t i = 0; i < n_pages; ++i) {
        if (i + 1 != n_pages) m_pages_next[i] = i + 1;
        else m_pages_next[i] = std::numeric_limits<std::uint64_t>::max();
      }
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
      internal_queue_push(id, pair_type(key, value));
      m_queue_min[id] = std::min(m_queue_min[id], (std::uint64_t)key);
      m_min_compare_ptr = std::min(m_min_compare_ptr, id);
    }

    // Return true iff x <= key, where x is the
    // smallest element currently stored in the heap.
    inline bool min_compare(key_type key) {
      if (empty()) return false;
      if (!is_internal_queue_empty(m_min_compare_ptr))
        return (m_queue_min[m_min_compare_ptr] <= (std::uint64_t)key);
      std::uint64_t id = get_queue_id(key);
      while (m_min_compare_ptr != id && is_internal_queue_empty(m_min_compare_ptr))
        ++m_min_compare_ptr;
      return (!is_internal_queue_empty(m_min_compare_ptr) &&
          m_queue_min[m_min_compare_ptr] <= (std::uint64_t)key);
    }

    // Remove and return the item with the smallest key.
    inline std::pair<key_type, value_type> extract_min() {
      if (is_internal_queue_empty(m_bottom_level_queue_ptr))
        redistribute();
      pair_type p = internal_queue_front(m_bottom_level_queue_ptr);
      internal_queue_pop(m_bottom_level_queue_ptr);
      key_type key = p.first;
      value_type value = p.second;
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
      utils::deallocate(m_pages_mem);
      utils::deallocate(m_pages_next);
      utils::deallocate(m_queue_headers);
    }

  private:
    void redistribute() {
      while (m_bottom_level_queue_ptr < m_bottom_level_radix && is_internal_queue_empty(m_bottom_level_queue_ptr))
        m_queue_min[m_bottom_level_queue_ptr++] = std::numeric_limits<std::uint64_t>::max();

      if (m_bottom_level_queue_ptr < m_bottom_level_radix) {
        m_key_lower_bound = m_queue_min[m_bottom_level_queue_ptr];
      } else {
        // Find the non-empty queue with the smallest id.
        std::uint64_t level = 1;
        while (true) {
          // Scan current level.
          while (m_level_ptr[level] < m_sum_of_radixes[level + 1] + 1 &&
              is_internal_queue_empty(m_level_ptr[level]))
            ++m_level_ptr[level];

          // If not found, reset the level pointer
          // and move up. Otherwise break.
          if (m_level_ptr[level] == m_sum_of_radixes[level + 1] + 1) {
            m_level_ptr[level] = m_sum_of_radixes[level] + 1;
            ++level;
          } else break;
        }

        std::uint64_t id = m_level_ptr[level];
        m_key_lower_bound = m_queue_min[id];

        // Redistribute elements in internal queue.
        while (!is_internal_queue_empty(id)) {
          pair_type p = internal_queue_front(id);
          internal_queue_pop(id);
          std::uint64_t newid = get_queue_id(p.first);
          internal_queue_push(newid, p);
          m_queue_min[newid] = std::min(m_queue_min[newid], (std::uint64_t)p.first);
        }
        m_bottom_level_queue_ptr = get_queue_id(m_key_lower_bound);
        m_queue_min[id] = std::numeric_limits<std::uint64_t>::max();
      }
      m_min_compare_ptr = m_bottom_level_queue_ptr;
    }
};

}  // namespace rhsais_private

#endif  // __RHSAIS_SRC_RADIX_HEAP_HPP_INCLUDED
