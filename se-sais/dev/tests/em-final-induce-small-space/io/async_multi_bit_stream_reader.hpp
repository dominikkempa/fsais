#ifndef __ASYNC_MULTI_BIT_STREAM_READER_HPP_INCLUDED
#define __ASYNC_MULTI_BIT_STREAM_READER_HPP_INCLUDED

#include <cstdio>
#include <cstdint>
#include <queue>
#include <string>
#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "async_multi_stream_reader.hpp"


class async_multi_bit_stream_reader {
  private:
    typedef async_multi_stream_reader<std::uint64_t> internal_reader_type;
    internal_reader_type *m_internal_reader;

    struct bit_buffer {
      std::uint64_t m_data;
      std::uint64_t m_pos;
      bool m_is_filled;

      bit_buffer() {
        m_data = 0;
        m_pos = 0;
        m_is_filled = false;
      }
    };

    std::vector<bit_buffer> m_buffers;

  public:
    async_multi_bit_stream_reader(std::uint64_t number_of_files,
        std::uint64_t bufsize_per_file_in_bytes = (1UL << 20)) {
      m_internal_reader = new internal_reader_type(number_of_files, bufsize_per_file_in_bytes);
      m_buffers = std::vector<bit_buffer>(number_of_files);
    }

    void add_file(std::string filename) {
      m_internal_reader->add_file(filename);
    }

    inline std::uint8_t read_from_ith_file(std::uint64_t i) {
      if (!m_buffers[i].m_is_filled || m_buffers[i].m_pos == 64) {
        m_buffers[i].m_data = m_internal_reader->read_from_ith_file(i);
        m_buffers[i].m_pos = 0;
        m_buffers[i].m_is_filled = true;
      }

      return (m_buffers[i].m_data & (1UL << (m_buffers[i].m_pos++))) > 0;
    }

    inline std::uint64_t bytes_read() const {
      return m_internal_reader->bytes_read();
    }

    ~async_multi_bit_stream_reader() {
      delete m_internal_reader;
    }
};

#endif  // __ASYNC_MULTI_BIT_STREAM_READER_HPP_INCLUDED
