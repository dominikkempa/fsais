/**
 * @file    fsais_src/io/simple_accessor.hpp
 * @section LICENCE
 *
 * This file is part of fSAIS v0.1.0
 * See: https://github.com/dominikkempa/fsais
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

#ifndef __FSAIS_SRC_IO_SIMPLE_ACCESSOR_HPP_INCLUDED
#define __FSAIS_SRC_IO_SIMPLE_ACCESSOR_HPP_INCLUDED

#include <cstdio>
#include <algorithm>

#include "../utils.hpp"


namespace fsais_private {

template<typename ValueType>
class simple_accessor {
  public:
    typedef ValueType value_type;

  private:
    std::uint64_t m_bytes_read;
    std::uint64_t m_file_items;
    std::uint64_t m_items_per_buf;
    std::uint64_t m_buf_pos;
    std::uint64_t m_buf_filled;

    value_type *m_buf;
    std::FILE *m_file;

  public:
    simple_accessor(std::string filename, std::uint64_t bufsize = (2UL << 20)) {
      m_items_per_buf = utils::disk_block_size<value_type>(bufsize);
      m_file_items = utils::file_size(filename) / sizeof(value_type);
      m_file = utils::file_open_nobuf(filename, "r");

      m_buf = utils::allocate_array<value_type>(m_items_per_buf);
      m_buf_filled = 0;
      m_buf_pos = 0;
      m_bytes_read = 0;
    }

    inline value_type access(std::uint64_t i) {
      if (!(m_buf_pos <= i && i < m_buf_pos + m_buf_filled)) {
        if (i >= m_items_per_buf / 2) m_buf_pos = i - m_items_per_buf / 2;
        else m_buf_pos = 0;
        m_buf_filled = std::min(m_file_items - m_buf_pos, m_items_per_buf);
        std::fseek(m_file, m_buf_pos * sizeof(value_type), SEEK_SET);
        utils::read_from_file(m_buf, m_buf_filled, m_file);
        m_bytes_read += m_buf_filled * sizeof(value_type);
      }

      return m_buf[i - m_buf_pos];
    }

    inline std::uint64_t bytes_read() const {
      return m_bytes_read;
    }

    ~simple_accessor() {
      std::fclose(m_file);
      utils::deallocate(m_buf);
    }
};

}  // namespace fsais_private

#endif  // __FSAIS_SRC_IO_SIMPLE_ACCESSOR_HPP_INCLUDED
