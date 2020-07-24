#ifndef __CICRULAR_QUEUE_HPP_INCLUDED
#define __CICRULAR_QUEUE_HPP_INCLUDED

#include <cstdint>
#include <algorithm>


template<typename T>
struct circular_queue {
  private:
    std::uint64_t m_size;
    std::uint64_t m_filled;
    std::uint64_t m_head;
    std::uint64_t m_tail;
    T *m_data;

  public:
    circular_queue()
      : m_size(1),
        m_filled(0),
        m_head(0),
        m_tail(0),
        m_data(new T[m_size]) {}

    inline void push(T x) {
      m_data[m_head++] = x;
      if (m_head == m_size)
        m_head = 0;
      ++m_filled;
      if (m_filled == m_size)
        enlarge();
    }

    inline T &front() const {
      return m_data[m_tail];
    }

    inline void pop() {
      ++m_tail;
      if (m_tail == m_size)
        m_tail = 0;
      --m_filled;
    }

    inline bool empty() const { return (m_filled == 0); }
    inline std::uint64_t size() const { return m_filled; }

    ~circular_queue() {
      delete[] m_data;
    }

  private:
    void enlarge() {
      T *new_data = new T[2 * m_size];
      std::uint64_t left = m_filled;
      m_filled = 0;
      while (left > 0) {
        std::uint64_t tocopy = std::min(left, m_size - m_tail);
        std::copy(m_data + m_tail, m_data + m_tail + tocopy, new_data + m_filled);
        m_tail += tocopy;
        if (m_tail == m_size)
          m_tail = 0;
        left -= tocopy;
        m_filled += tocopy;
      }
      m_head = m_filled;
      m_tail = 0;
      m_size <<= 1;
      std::swap(m_data, new_data);
      delete[] new_data;
    }
};

#endif  // __CICRULAR_QUEUE_HPP_INCLUDED
