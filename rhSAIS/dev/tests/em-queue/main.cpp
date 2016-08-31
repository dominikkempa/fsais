#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <queue>
#include <vector>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "uint40.h"
#include "utils.hpp"
#include "em_queue.hpp"


void test(std::uint64_t ram_use, std::uint64_t max_operations, std::uint64_t testcases) {
  fprintf(stderr, "TEST, ram_use=%lu, max_operations=%lu, testcases=%lu\n", ram_use, max_operations, testcases);
  for (std::uint64_t testid = 0; testid < testcases; ++testid) {
    if (testid % 10 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * testid) / testcases);
//    fprintf(stderr, "\n\nNEWTEST:\n");
    typedef uint40 value_type;
    typedef em_queue<value_type> queue_type;
    std::string filename = "./tmp";
    queue_type *queue = new queue_type(ram_use, filename);
    std::queue<value_type> *q = new std::queue<value_type>();

    std::uint64_t operations = utils::random_int64(1L, (std::int64_t)max_operations);
    for (std::uint64_t i = 0; i <operations; ++i) {
      std::uint64_t op = utils::random_int64(0L, 2L);
      if (op == 0) { // push
        value_type value = utils::random_int64(0L, 1000000000L);
//        fprintf(stderr, "PUSH(%lu)\n", (std::uint64_t)value);
        queue->push(value);
        q->push(value);
      } else if (op == 1) { // front
//        fprintf(stderr, "FRONT\n");
        if (!q->empty()) {
          if (queue->empty()) {
            fprintf(stderr, "Error: queue->empty() returned true, but the queue is not empty!\n");
            std::exit(EXIT_FAILURE);
          }
          value_type v1 = queue->front();
          value_type v2 = q->front();
          if (v1 != v2) {
            fprintf(stderr, "Error:\n");
            fprintf(stderr, "  my queue returned %lu\n", (std::uint64_t)v1);
            fprintf(stderr, "  correct queue returned %lu\n", (std::uint64_t)v2);
            std::exit(EXIT_FAILURE);
          }
        } else {
          if (!queue->empty()) {
            fprintf(stderr, "Error: queue->empty() returned false, but the queue is empty!\n");
            std::exit(EXIT_FAILURE);
          }
        }
      } else {  // pop
//        fprintf(stderr, "POP\n");
        if (!q->empty()) {
          if (queue->empty()) {
            fprintf(stderr, "Error: queue->empty() returned true, but the queue is not empty!\n");
            std::exit(EXIT_FAILURE);
          }
          queue->pop();
          q->pop();
        } else {
          if (!queue->empty()) {
            fprintf(stderr, "Error: queue->empty() returned false, but the queue is empty!\n");
            std::exit(EXIT_FAILURE);
          }
        }
      }
    }

    // Clean up.
    delete q;
    delete queue;
  }
}

int main() {
  srand(time(0) + getpid());

  for (std::uint64_t ram_use = 0; ram_use <= (1UL << 20); ram_use = std::max(ram_use + 1, (ram_use * 3 + 1) / 2)) {
    test(ram_use, 10,  100000);
    test(ram_use, 100, 100000);
    test(ram_use, 1000, 10000);
    test(ram_use, 10000, 1000);
  }

  fprintf(stderr, "All tests passed.\n");
}

