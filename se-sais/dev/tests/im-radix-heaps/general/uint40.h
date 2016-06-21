#ifndef __UINT40_H_INCLUDED
#define __UINT40_H_INCLUDED

#include <cstdint>
#include <cassert>
#include <limits>


class uint40 {
  private:
    uint32_t low;
    uint8_t high;

  public:
    uint40() {}
    uint40(uint32_t l, uint8_t h) : low(l), high(h) {}
    uint40(const uint40& a) : low(a.low), high(a.high) {}
    uint40(const int& a) : low(a), high(0) {}
    uint40(const unsigned int& a) : low(a), high(0) {}

    uint40(const uint64_t& a)
      : low(a & 0xFFFFFFFF), high((a >> 32) & 0xFF) {
        assert(a <= 0xFFFFFFFFFFLU);
    }

    uint40(const int64_t& a)
      : low(a & 0xFFFFFFFFL), high((a >> 32) & 0xFF) {
        assert(a <= 0xFFFFFFFFFFL);
    }

    inline operator uint64_t() const {
      return (((uint64_t)high) << 32) | (uint64_t)low;
    }

    inline uint40& operator += (const uint40& b) {
      uint64_t add = (uint64_t)low + b.low;
      low = add & 0xFFFFFFFF;
      high += b.high + ((add >> 32) & 0xFFU);
      return *this;
    }

    inline bool operator == (const uint40& b) const {
      return (low == b.low) && (high == b.high);
    }

    inline bool operator != (const uint40& b) const {
      return (low != b.low) || (high != b.high);
    }
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
      return uint40(std::numeric_limits<uint32_t>::min(),
          std::numeric_limits<uint8_t>::min());
    }

    static uint40 max() {
      return uint40(std::numeric_limits<uint32_t>::max(),
          std::numeric_limits<uint8_t>::max());
    }
};

}  // namespace std

#endif  // __UINT40_H_INCLUDED
