#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <queue>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "utils.hpp"
#include "circular_queue.hpp"


void test(std::uint64_t operations) {
  fprintf(stderr, "TEST, operations = %lu\n", operations);
  typedef std::int64_t value_type;

  circular_queue<value_type> q1;
  std::queue<value_type> q2;

  for (std::uint64_t i = 0; i < operations; ++i) {
    if (i % 1000 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * i) / operations);
    std::uint64_t op = utils::random_int64(1L, 5L);
    if (op == 1) {  // push
      std::int64_t value = utils::random_int64(1L, 1000000000000000000L);
      q1.push(value);
      q2.push(value);
    } else if (op == 2) { // pop
      if (q1.empty() != q2.empty()) {
        fprintf(stderr, "\nError: q1.empty() != q2.empty()!\n");
        std::exit(EXIT_FAILURE);
      }
      if (!q1.empty()) {
        q1.pop();
        q2.pop();
      }
    } else if (op == 3) { // front
      if (q1.empty() != q2.empty()) {
        fprintf(stderr, "\nError: q1.empty() != q2.empty()!\n");
        std::exit(EXIT_FAILURE);
      }
      if (!q1.empty()) {
        if (q1.front() != q2.front()) {
          fprintf(stderr, "\nError: q1.front() != q2.front()!\n");
          std::exit(EXIT_FAILURE);
        }
      }
    } else if (op == 4) { // empty
      if (q1.empty() != q2.empty()) {
        fprintf(stderr, "\nError: q1.empty() != q2.empty()!\n");
        std::exit(EXIT_FAILURE);
      }
    } else {  // size
      if (q1.size() != q2.size()) {
        fprintf(stderr, "\nError: q1.size() != q2.size()!\n");
        std::exit(EXIT_FAILURE);
      }
    }
  }
}

int main() {
  srand(time(0) + getpid());
  for (std::uint64_t i = 1; i <= 100000000; i *= 10)
    test(i);
  fprintf(stderr, "All tests passed.\n");
}

