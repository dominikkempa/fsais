/**
 * @file    uint40.hpp
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

#ifndef __UINT40_HPP_INCLUDED
#define __UINT40_HPP_INCLUDED

#include <cstdint>
#include <limits>


class uint40 {
  private:
    std::uint32_t low;
    std::uint8_t high;

  public:
    uint40() {}
    uint40(std::uint32_t l, std::uint8_t h) : low(l), high(h) {}
    uint40(const uint40& a) : low(a.low), high(a.high) {}
    uint40(const std::int32_t& a) : low(a), high(0) {}
    uint40(const std::uint32_t& a) : low(a), high(0) {}
    uint40(const std::uint64_t& a) :
      low(a & 0xFFFFFFFF), high((a >> 32) & 0xFF) {}
    uint40(const std::int64_t& a) :
      low(a & 0xFFFFFFFFL), high((a >> 32) & 0xFF) {}

    inline operator uint64_t() const {
      return (((std::uint64_t)high) << 32) | (std::uint64_t)low;  }
    inline bool operator == (const uint40& b) const {
      return (low == b.low) && (high == b.high); }
    inline bool operator != (const uint40& b) const {
      return (low != b.low) || (high != b.high); }
} __attribute__((packed));

namespace std {

template<>
struct is_unsigned<uint40> {
  public:
    static const bool value = true;
};

template<>
class numeric_limits<uint40> {
  public:
    static uint40 min() {
      return uint40(std::numeric_limits<std::uint32_t>::min(),
          std::numeric_limits<std::uint8_t>::min());
    }

    static uint40 max() {
      return uint40(std::numeric_limits<std::uint32_t>::max(),
          std::numeric_limits<std::uint8_t>::max());
    }
};

}  // namespace std

#endif  // __UINT40_HPP_INCLUDED
