#ifndef __PACKED_PAIR_HPP_INCLUDED
#define __PACKED_PAIR_HPP_INCLUDED


namespace rhsais_private {

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

}  // namespace rhsais_private

#endif  // __PACKED_PAIR_HPP_INCLUDED
