/**
 * @file    rhsais_src/io/async_multi_bit_stream_reader.hpp
 * @section LICENCE
 *
 * This file is part of rhSAIS v0.1.0
 * See: http://www.cs.helsinki.fi/group/pads/
 *
 * Copyright (C) 2017
 *   Juha Karkkainen <juha.karkkainen (at) cs.helsinki.fi>
 *   Dominik Kempa <dominik.kempa (at) gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 **/

#ifndef __RHSAIS_SRC_IO_ASYNC_MULTI_BIT_STREAM_READER_HPP_INCLUDED
#define __RHSAIS_SRC_IO_ASYNC_MULTI_BIT_STREAM_READER_HPP_INCLUDED

#include <cstdint>
#include <string>
#include <vector>

#include "async_multi_stream_reader.hpp"


namespace rhsais_private {

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

}  // namespace rhsais_private

#endif  // __RHSAIS_SRC_IO_ASYNC_MULTI_BIT_STREAM_READER_HPP_INCLUDED
