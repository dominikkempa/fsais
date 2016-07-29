#ifndef __ASYNC_BIT_STREAM_READER_HPP_INCLUDED
#define __ASYNC_BIT_STREAM_READER_HPP_INCLUDED

#include <cstdio>
#include <cstdint>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <string>
#include <algorithm>

#include "async_stream_reader.hpp"


class async_bit_stream_reader {
  private:
    typedef async_stream_reader<std::uint64_t> internal_reader_type;
    internal_reader_type *m_internal_reader;

    std::uint64_t m_buf_data;
    std::uint64_t m_buf_pos;
    bool m_buf_filled;

  public:
    async_bit_stream_reader(std::string filename) {
      m_internal_reader = new internal_reader_type(filename);
      m_buf_filled = false;
      m_buf_pos = 0;
      m_buf_data = 0;
    }

    ~async_bit_stream_reader() {
      delete m_internal_reader;
    }

    inline std::uint64_t bytes_read() {
      return m_internal_reader->bytes_read();
    }

    inline std::uint8_t read() {
      if (!m_buf_filled || m_buf_pos == 64) {
        m_buf_pos = 0;
        m_buf_data = m_internal_reader->read();
        m_buf_filled = true;
      }

      return (m_buf_data & (1UL << (m_buf_pos++))) > 0;
    }

    inline std::uint8_t peek() {
      if (!m_buf_filled || m_buf_pos == 64) {
        m_buf_pos = 0;
        m_buf_data = m_internal_reader->read();
        m_buf_filled = true;
      }

      return (m_buf_data & (1UL << m_buf_pos)) > 0;
    }
};

#endif  // __ASYNC_BIT_STREAM_READER_HPP_INCLUDED
