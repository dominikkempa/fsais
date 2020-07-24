#ifndef __UINT24_HPP_INCLUDED
#define __UINT24_HPP_INCLUDED

#include <cstdint>
#include <cassert>
#include <limits>


class uint24 {
  private:
    uint16_t low;
    uint8_t high;

  public:
    uint24() {}
    uint24(uint16_t l, uint8_t h) : low(l), high(h) {}
    uint24(const uint24& a) : low(a.low), high(a.high) {}

    uint24(const int& a)
      : low(a & 0xFFFF), high((a >> 16) & 0xFF) {
        assert(a <= 0xFFFFFF);
    }

    uint24(const unsigned int& a)
      : low(a & 0xFFFF), high((a >> 16) & 0xFF) {
        assert(a <= 0xFFFFFFUL);
    }

    uint24(const uint64_t& a)
      : low(a & 0xFFFF), high((a >> 16) & 0xFF) {
        assert(a <= 0xFFFFFFUL);
    }

    uint24(const int64_t& a)
      : low(a & 0xFFFFL), high((a >> 16) & 0xFF) {
        assert(a <= 0xFFFFFFL);
    }

    inline operator uint64_t() const {
      return (((uint64_t)high) << 16) | (uint64_t)low;
    }

    inline uint24& operator += (const uint24& b) {
      uint64_t add = (uint64_t)low + b.low;
      low = add & 0xFFFF;
      high += b.high + ((add >> 16) & 0xFFU);
      return *this;
    }

    inline bool operator == (const uint24& b) const {
      return (low == b.low) && (high == b.high);
    }

    inline bool operator != (const uint24& b) const {
      return (low != b.low) || (high != b.high);
    }
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
      return uint24(std::numeric_limits<uint16_t>::min(),
          std::numeric_limits<uint8_t>::min());
    }

    static uint24 max() {
      return uint24(std::numeric_limits<uint16_t>::max(),
          std::numeric_limits<uint8_t>::max());
    }
};

}  // namespace std

#endif  // __UINT24_HPP_INCLUDED
