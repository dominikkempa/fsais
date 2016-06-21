#ifndef __UINT48_H_INCLUDED
#define __UINT48_H_INCLUDED

#include <cstdint>
#include <cassert>
#include <limits>


class uint48 {
  private:
    uint32_t low;
    uint16_t high;

  public:
    uint48() {}
    uint48(uint32_t l, uint16_t h) : low(l), high(h) {}
    uint48(const uint48& a) : low(a.low), high(a.high) {}
    uint48(const int& a) : low(a), high(0) {}
    uint48(const unsigned int& a) : low(a), high(0) {}

    uint48(const uint64_t& a)
      : low(a & 0xFFFFFFFF), high((a >> 32) & 0xFFFF) {
        assert(a <= 0xFFFFFFFFFFFFLU);
    }

    uint48(const int64_t& a)
      : low(a & 0xFFFFFFFFL), high((a >> 32) & 0xFFFF) {
        assert(a <= 0xFFFFFFFFFFFFL);
    }

    inline operator uint64_t() const {
      return (((uint64_t)high) << 32) | (uint64_t)low;
    }

    inline uint48& operator += (const uint48& b) {
      uint64_t add = (uint64_t)low + b.low;
      low = add & 0xFFFFFFFF;
      high += b.high + ((add >> 32) & 0xFFFFU);
      return *this;
    }

    inline bool operator == (const uint48& b) const {
      return (low == b.low) && (high == b.high);
    }

    inline bool operator != (const uint48& b) const {
      return (low != b.low) || (high != b.high);
    }
} __attribute__((packed));

namespace std {

template<>
struct is_unsigned<uint48> {
  public:
    static const bool value = true;
};

template<>
class numeric_limits<uint48> {
  public:
    static uint48 min() {
      return uint48(std::numeric_limits<uint32_t>::min(),
          std::numeric_limits<uint16_t>::min());
    }

    static uint48 max() {
      return uint48(std::numeric_limits<uint32_t>::max(),
          std::numeric_limits<uint16_t>::max());
    }
};

}  // namespace std

#endif  // __UINT48_H_INCLUDED
