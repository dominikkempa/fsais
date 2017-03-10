#ifndef __FSAIS_SRC_PACKED_PAIR_HPP_INCLUDED
#define __FSAIS_SRC_PACKED_PAIR_HPP_INCLUDED


namespace fsais_private {

template<typename S, typename T>
struct packed_pair {
  packed_pair() {}
  packed_pair(S &f, T &s) {
    first = f;
    second = s;
  }

  packed_pair(S f, T s) {
    first = f;
    second = s;
  }

  S first;
  T second;
} __attribute__((packed));

}  // namespace fsais_private

#endif  // __FSAIS_SRC_PACKED_PAIR_HPP_INCLUDED
