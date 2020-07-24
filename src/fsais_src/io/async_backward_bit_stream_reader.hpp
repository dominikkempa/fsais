/**
 * @file    fsais_src/io/async_backward_bit_stream_reader.hpp
 * @section LICENCE
 *
 * This file is part of fSAIS v0.1.0
 * See: https://github.com/dkempa/fsais
 *
 * Copyright (C) 2016-2020
 *   Dominik Kempa <dominik.kempa (at) gmail.com>
 *   Juha Karkkainen <juha.karkkainen (at) cs.helsinki.fi>
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

#ifndef __FSAIS_SRC_IO_ASYNC_BACKWARD_BIT_STREAM_READER_HPP_INCLUDED
#define __FSAIS_SRC_IO_ASYNC_BACKWARD_BIT_STREAM_READER_HPP_INCLUDED

#include <cstdint>
#include <string>

#include "async_backward_stream_reader.hpp"


namespace fsais_private {

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

    void stop_reading() {
      m_internal_reader->stop_reading();
    }

    inline std::uint64_t bytes_read() const {
      return m_internal_reader->bytes_read();
    }

    ~async_backward_bit_stream_reader() {
      delete m_internal_reader;
    }
};

}  // namespace fsais_private

#endif  // __FSAIS_SRC_IO_ASYNC_BACKWARD_BIT_STREAM_READER_HPP_INCLUDED
