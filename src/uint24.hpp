/**
 * @file    uint24.hpp
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

#ifndef __UINT24_HPP_INCLUDED
#define __UINT24_HPP_INCLUDED

#include <cstdint>
#include <limits>


class uint24 {
  private:
    std::uint16_t low;
    std::uint8_t high;

  public:
    uint24() {}
    uint24(std::uint16_t l, std::uint8_t h) : low(l), high(h) {}
    uint24(const uint24& a) : low(a.low), high(a.high) {}
    uint24(const std::int32_t& a) : low(a & 0xFFFF), high((a >> 16) & 0xFF) {}
    uint24(const std::uint32_t& a) : low(a & 0xFFFF), high((a >> 16) & 0xFF) {}
    uint24(const std::uint64_t& a) : low(a & 0xFFFF), high((a >> 16) & 0xFF) {}
    uint24(const std::int64_t& a) : low(a & 0xFFFFL), high((a >> 16) & 0xFF) {}

    inline operator uint64_t() const { return (((uint64_t)high) << 16) | (uint64_t)low; }
    inline bool operator == (const uint24& b) const { return (low == b.low) && (high == b.high); }
    inline bool operator != (const uint24& b) const { return (low != b.low) || (high != b.high); }
} __attribute__((packed));

namespace std {

template<>
struct is_unsigned<uint24> {
  public:
    static const bool value = true;
};

template<>
class numeric_limits<uint24> {
  public:
    static uint24 min() {
      return uint24(std::numeric_limits<std::uint16_t>::min(),
          std::numeric_limits<std::uint8_t>::min());
    }

    static uint24 max() {
      return uint24(std::numeric_limits<std::uint16_t>::max(),
          std::numeric_limits<std::uint8_t>::max());
    }
};

}  // namespace std

#endif  // __UINT24_HPP_INCLUDED
