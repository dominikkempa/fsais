#ifndef __ASYNC_BACKWARD_BIT_STREAM_READER_HPP_INCLUDED
#define __ASYNC_BACKWARD_BIT_STREAM_READER_HPP_INCLUDED

#include <cstdio>
#include <cstdint>
#include <queue>
#include <string>
#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "async_backward_stream_reader.hpp"


class async_backward_bit_stream_reader {
  private:
    typedef async_backward_stream_reader<std::uint64_t> internal_reader_type;
    internal_reader_type *m_internal_reader;

    std::uint64_t m_data;
    std::uint64_t m_pos;
    bool m_is_filled;

  public:
    async_backward_bit_stream_reader(std::string filename,
        std::uint64_t total_buf_size_items = (8UL << 20),
        std::uint64_t n_buffers = 4) {
      m_internal_reader = new internal_reader_type(filename,
          total_buf_size_items, n_buffers);
      m_data = 0;
      m_pos = 0;
      m_is_filled = false;
    }

    inline std::uint8_t read() {
      if (m_is_filled == false) {
        std::uint64_t bit_cnt = m_internal_reader->read();
        m_pos = bit_cnt % 64;
        if (m_pos == 0)
          m_pos = 64;
        m_data = m_internal_reader->read();
        m_is_filled = true;
      } else if (m_pos == 0) {
        m_data = m_internal_reader->read();
        m_pos = 64;
      }

      return (m_data & (1UL << (--m_pos))) > 0;
    }

    inline std::uint64_t bytes_read() const {
      return m_internal_reader->bytes_read();
    }

    ~async_backward_bit_stream_reader() {
      delete m_internal_reader;
    }
};

#endif  // __ASYNC_BACKWARD_BIT_STREAM_READER_HPP_INCLUDED
