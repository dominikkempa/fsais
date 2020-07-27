/**
 * @file    fsais_src/naive_compute_sa.hpp
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

#ifndef __FSAIS_SRC_NAIVE_COMPUTE_SA_HPP_INCLUDED
#define __FSAIS_SRC_NAIVE_COMPUTE_SA_HPP_INCLUDED

#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>


namespace fsais_private {
namespace naive_compute_sa {

template<typename char_type>
class substring {
  public:
    typedef substring<char_type> substring_type;

    substring() {}
    substring(
        const char_type * const text,
        const std::uint64_t beg,
        const std::uint64_t length,
        const std::uint64_t text_length) {

      m_beg = beg;
      m_text_length = text_length;
      for (std::uint64_t j = 0; j < length; ++j)
        m_data.push_back(text[beg + j]);
    }

    inline bool operator < (const substring_type &s) const {
      std::uint64_t lcp = 0;
      while (m_beg + lcp < m_text_length &&
          s.m_beg + lcp < m_text_length &&
          m_data[lcp] == s.m_data[lcp])
        ++lcp;

      return (m_beg + lcp == m_text_length ||
          (s.m_beg + lcp < m_text_length &&
           (std::uint64_t)m_data[lcp] < (std::uint64_t)s.m_data[lcp]));
    }

    std::uint64_t m_beg;
    std::uint64_t m_text_length;
    std::vector<char_type> m_data;
};

template<
  typename char_type,
  typename text_offset_type>
void naive_compute_sa(
    const char_type * const text,
    const std::uint64_t text_length,
    text_offset_type * const sa) {

  typedef substring<char_type> substring_type;
  std::vector<substring_type> substrings;

  for (std::uint64_t i = 0; i < text_length; ++i)
    substrings.push_back(
        substring_type(text, i, text_length - i, text_length));

  std::sort(substrings.begin(), substrings.end());
  for (std::uint64_t i = 0; i < text_length; ++i)
    sa[i] = substrings[i].m_beg;
}

}  // namespace naive_compute_sa
}  // namespace fsais_private

#endif  // __FSAIS_SRC_NAIVE_COMPUTE_SA_HPP_INCLUDED
