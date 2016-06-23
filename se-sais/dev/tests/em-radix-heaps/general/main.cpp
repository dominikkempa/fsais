#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "utils.hpp"
#include "em_radix_heap.hpp"
#include "uint40.hpp"
#include "uint48.hpp"


template<typename key_type>
void test(std::uint64_t, std::uint64_t, std::uint64_t, std::uint64_t, std::uint64_t) {}

struct comp8 {
  bool operator() (const std::pair<std::uint8_t, std::uint64_t> l, const std::pair<std::uint8_t, std::uint64_t> r) const {
    return l.first < r.first;
  }
};

struct comp16 {
  bool operator() (const std::pair<std::uint16_t, std::uint64_t> l, const std::pair<std::uint16_t, std::uint64_t> r) const {
    return l.first < r.first;
  }
};

struct comp32 {
  bool operator() (const std::pair<std::uint32_t, std::uint64_t> l, const std::pair<std::uint32_t, std::uint64_t> r) const {
    return l.first < r.first;
  }
};

struct comp40 {
  bool operator() (const std::pair<uint40, std::uint64_t> l, const std::pair<uint40, std::uint64_t> r) const {
    return (std::uint64_t)l.first < (std::uint64_t)r.first;
  }
};

struct comp48 {
  bool operator() (const std::pair<uint48, std::uint64_t> l, const std::pair<uint48, std::uint64_t> r) const {
    return (std::uint64_t)l.first < (std::uint64_t)r.first;
  }
};

struct comp64 {
  bool operator() (const std::pair<std::uint64_t, std::uint64_t> l, const std::pair<std::uint64_t, std::uint64_t> r) const {
    return l.first < r.first;
  }
};

template<>
void test<std::uint8_t>(std::uint64_t radix_log, std::uint64_t testcases, std::uint64_t max_operations, std::uint64_t max_key, std::uint64_t bufsize) {
  fprintf(stderr, "TEST, key_type=std::uint8_t, bufsize=%lu, radix_log=%lu, testcases=%lu, max_operations=%lu, max_key=%lu\n",
      bufsize, radix_log, testcases, max_operations, max_key);
  typedef em_radix_heap<std::uint8_t, std::uint64_t> heap_type;

  for (std::uint64_t test_id = 0; test_id < testcases; ++test_id) {
    if (test_id % 100 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * test_id) / testcases);

    heap_type *heap = new heap_type(radix_log, bufsize, "./tmp");
    std::vector<std::pair<std::uint8_t, std::uint64_t> > v;
    std::uint64_t min_heap_elem = 0;
    for (std::uint64_t j = 0; j < max_operations; ++j) {
      std::int64_t op = utils::random_int64(0, 2);
      if (op == 0) {  // push
        std::uint8_t key = 0;
        bool ok = false;
        for (std::uint64_t t = 0; t < 10; ++t) {
          key = (std::uint8_t)utils::random_int64(0L, (std::int64_t)max_key);
          if (key >= min_heap_elem) { ok = true; break; }
        }
        if (ok) {
          std::uint64_t value = (std::uint64_t)utils::random_int64(0L, 1000000000L);
          heap->push(key, value);
          v.push_back(std::make_pair(key, value));
          comp8 cmp;
          std::stable_sort(v.begin(), v.end(), cmp);
        }
      } else if (op == 1) { // print top
        if (v.empty() == false) {
          std::uint8_t key = heap->top_key();
          std::uint64_t value = heap->top_value();
          if (key != v[0].first || value != v[0].second) {
            fprintf(stderr, "Error:\n");
            fprintf(stderr, "  Correct min key = %lu\n", (std::uint64_t)v[0].first);
            fprintf(stderr, "  Correct min value = %lu\n", (std::uint64_t)v[0].second);
            fprintf(stderr, "  Returned values: key = %lu, value = %lu\n", (std::uint64_t)key, (std::uint64_t)value);
            std::exit(EXIT_FAILURE);
          }
          min_heap_elem = v[0].first;
        }
      } else {  // pop
        if (v.empty() == false) {
          min_heap_elem = v[0].first;
          v.erase(v.begin());
          heap->pop();
        }
      }
    }
    delete heap;
  }
}


template<>
void test<std::uint16_t>(std::uint64_t radix_log, std::uint64_t testcases, std::uint64_t max_operations, std::uint64_t max_key, std::uint64_t bufsize) {
  fprintf(stderr, "TEST, key_type=std::uint16_t, bufsize=%lu, radix_log=%lu, testcases=%lu, max_operations=%lu, max_key=%lu\n",
      bufsize, radix_log, testcases, max_operations, max_key);
  typedef em_radix_heap<std::uint16_t, std::uint64_t> heap_type;

  for (std::uint64_t test_id = 0; test_id < testcases; ++test_id) {
    if (test_id % 100 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * test_id) / testcases);

    heap_type *heap = new heap_type(radix_log, bufsize, "./tmp");
    std::vector<std::pair<std::uint16_t, std::uint64_t> > v;
    std::uint64_t min_heap_elem = 0;
    for (std::uint64_t j = 0; j < max_operations; ++j) {
      std::int64_t op = utils::random_int64(0, 2);
      if (op == 0) {  // push
        std::uint16_t key = 0;
        bool ok = false;
        for (std::uint64_t t = 0; t < 10; ++t) {
          key = (std::uint16_t)utils::random_int64(0L, (std::int64_t)max_key);
          if (key >= min_heap_elem) { ok = true; break; }
        }
        if (ok) {
          std::uint64_t value = (std::uint64_t)utils::random_int64(0L, 1000000000L);
          heap->push(key, value);
          v.push_back(std::make_pair(key, value));
          comp16 cmp;
          std::stable_sort(v.begin(), v.end(), cmp);
        }
      } else if (op == 1) { // print top
        if (v.empty() == false) {
          std::uint16_t key = heap->top_key();
          std::uint64_t value = heap->top_value();
          if (key != v[0].first || value != v[0].second) {
            fprintf(stderr, "Error:\n");
            fprintf(stderr, "  Correct min key = %lu\n", (std::uint64_t)v[0].first);
            fprintf(stderr, "  Correct min value = %lu\n", (std::uint64_t)v[0].second);
            fprintf(stderr, "  Returned values: key = %lu, value = %lu\n", (std::uint64_t)key, (std::uint64_t)value);
            std::exit(EXIT_FAILURE);
          }
          min_heap_elem = v[0].first;
        }
      } else {  // pop
        if (v.empty() == false) {
          min_heap_elem = v[0].first;
          v.erase(v.begin());
          heap->pop();
        }
      }
    }
    delete heap;
  }
}


template<>
void test<std::uint32_t>(std::uint64_t radix_log, std::uint64_t testcases, std::uint64_t max_operations, std::uint64_t max_key, std::uint64_t bufsize) {
  fprintf(stderr, "TEST, key_type=std::uint32_t, bufsize=%lu, radix_log=%lu, testcases=%lu, max_operations=%lu, max_key=%lu\n",
      bufsize, radix_log, testcases, max_operations, max_key);
  typedef em_radix_heap<std::uint32_t, std::uint64_t> heap_type;

  for (std::uint64_t test_id = 0; test_id < testcases; ++test_id) {
    if (test_id % 100 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * test_id) / testcases);

    heap_type *heap = new heap_type(radix_log, bufsize, "./tmp");
    std::vector<std::pair<std::uint32_t, std::uint64_t> > v;
    std::uint64_t min_heap_elem = 0;
    for (std::uint64_t j = 0; j < max_operations; ++j) {
      std::int64_t op = utils::random_int64(0, 2);
      if (op == 0) {  // push
        std::uint32_t key = 0;
        bool ok = false;
        for (std::uint64_t t = 0; t < 10; ++t) {
          key = (std::uint32_t)utils::random_int64(0L, (std::int64_t)max_key);
          if (key >= min_heap_elem) { ok = true; break; }
        }
        if (ok) {
          std::uint64_t value = (std::uint64_t)utils::random_int64(0L, 1000000000L);
          heap->push(key, value);
          v.push_back(std::make_pair(key, value));
          comp32 cmp;
          std::stable_sort(v.begin(), v.end(), cmp);
        }
      } else if (op == 1) { // print top
        if (v.empty() == false) {
          std::uint32_t key = heap->top_key();
          std::uint64_t value = heap->top_value();
          if (key != v[0].first || value != v[0].second) {
            fprintf(stderr, "Error:\n");
            fprintf(stderr, "  Correct min key = %lu\n", (std::uint64_t)v[0].first);
            fprintf(stderr, "  Correct min value = %lu\n", (std::uint64_t)v[0].second);
            fprintf(stderr, "  Returned values: key = %lu, value = %lu\n", (std::uint64_t)key, (std::uint64_t)value);
            std::exit(EXIT_FAILURE);
          }
          min_heap_elem = v[0].first;
        }
      } else {  // pop
        if (v.empty() == false) {
          min_heap_elem = v[0].first;
          v.erase(v.begin());
          heap->pop();
        }
      }
    }
    delete heap;
  }
}

template<>
void test<uint40>(std::uint64_t radix_log, std::uint64_t testcases, std::uint64_t max_operations, std::uint64_t max_key, std::uint64_t bufsize) {
  fprintf(stderr, "TEST, key_type=uint40, bufsize=%lu, radix_log=%lu, testcases=%lu, max_operations=%lu, max_key=%lu\n",
      bufsize, radix_log, testcases, max_operations, max_key);
  typedef em_radix_heap<uint40, std::uint64_t> heap_type;

  for (std::uint64_t test_id = 0; test_id < testcases; ++test_id) {
    if (test_id % 100 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * test_id) / testcases);

    heap_type *heap = new heap_type(radix_log, bufsize, "./tmp");
    std::vector<std::pair<uint40, std::uint64_t> > v;
    std::uint64_t min_heap_elem = 0;
    for (std::uint64_t j = 0; j < max_operations; ++j) {
      std::int64_t op = utils::random_int64(0, 2);
      if (op == 0) {  // push
        uint40 key = 0;
        bool ok = false;
        for (std::uint64_t t = 0; t < 10; ++t) {
          key = (uint40)((std::uint64_t)utils::random_int64(0L, (std::int64_t)max_key));
          if ((std::uint64_t)key >= min_heap_elem) { ok = true; break; }
        }
        if (ok) {
          std::uint64_t value = (std::uint64_t)utils::random_int64(0L, 1000000000L);
          heap->push(key, value);
          v.push_back(std::make_pair(key, value));
          comp40 cmp;
          std::stable_sort(v.begin(), v.end(), cmp);
        }
      } else if (op == 1) { // print top
        if (v.empty() == false) {
          uint40 key = heap->top_key();
          std::uint64_t value = heap->top_value();
          if (key != v[0].first || value != v[0].second) {
            fprintf(stderr, "Error:\n");
            fprintf(stderr, "  Correct min key = %lu\n", (std::uint64_t)v[0].first);
            fprintf(stderr, "  Correct min value = %lu\n", (std::uint64_t)v[0].second);
            fprintf(stderr, "  Returned values: key = %lu, value = %lu\n", (std::uint64_t)key, (std::uint64_t)value);
            fprintf(stderr, "  v: ");
            for (std::uint64_t tt = 0; tt < v.size(); ++tt)
              fprintf(stderr, "(%lu,%lu) ", (std::uint64_t)v[tt].first, (std::uint64_t)v[tt].second);
            fprintf(stderr, "\n");
            std::exit(EXIT_FAILURE);
          }
          min_heap_elem = (std::uint64_t)v[0].first;
        }
      } else {  // pop
        if (v.empty() == false) {
          min_heap_elem = (std::uint64_t)v[0].first;
          v.erase(v.begin());
          heap->pop();
        }
      }
    }
    delete heap;
  }
}

template<>
void test<uint48>(std::uint64_t radix_log, std::uint64_t testcases, std::uint64_t max_operations, std::uint64_t max_key, std::uint64_t bufsize) {
  fprintf(stderr, "TEST, key_type=uint48, bufsize=%lu, radix_log=%lu, testcases=%lu, max_operations=%lu, max_key=%lu\n",
      bufsize, radix_log, testcases, max_operations, max_key);
  typedef em_radix_heap<uint48, std::uint64_t> heap_type;

  for (std::uint64_t test_id = 0; test_id < testcases; ++test_id) {
    if (test_id % 100 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * test_id) / testcases);

    heap_type *heap = new heap_type(radix_log, bufsize, "./tmp");
    std::vector<std::pair<uint48, std::uint64_t> > v;
    std::uint64_t min_heap_elem = 0;
    for (std::uint64_t j = 0; j < max_operations; ++j) {
      std::int64_t op = utils::random_int64(0, 2);
      if (op == 0) {  // push
        uint48 key = 0;
        bool ok = false;
        for (std::uint64_t t = 0; t < 10; ++t) {
          key = (uint48)((std::uint64_t)utils::random_int64(0L, (std::int64_t)max_key));
          if ((std::uint64_t)key >= min_heap_elem) { ok = true; break; }
        }
        if (ok) {
          std::uint64_t value = (std::uint64_t)utils::random_int64(0L, 1000000000L);
          heap->push(key, value);
          v.push_back(std::make_pair(key, value));
          comp48 cmp;
          std::stable_sort(v.begin(), v.end(), cmp);
        }
      } else if (op == 1) { // print top
        if (v.empty() == false) {
          uint48 key = heap->top_key();
          std::uint64_t value = heap->top_value();
          if (key != v[0].first || value != v[0].second) {
            fprintf(stderr, "Error:\n");
            fprintf(stderr, "  Correct min key = %lu\n", (std::uint64_t)v[0].first);
            fprintf(stderr, "  Correct min value = %lu\n", (std::uint64_t)v[0].second);
            fprintf(stderr, "  Returned values: key = %lu, value = %lu\n", (std::uint64_t)key, (std::uint64_t)value);
            fprintf(stderr, "  v: ");
            for (std::uint64_t tt = 0; tt < v.size(); ++tt)
              fprintf(stderr, "(%lu,%lu) ", (std::uint64_t)v[tt].first, (std::uint64_t)v[tt].second);
            fprintf(stderr, "\n");
            std::exit(EXIT_FAILURE);
          }
          min_heap_elem = (std::uint64_t)v[0].first;
        }
      } else {  // pop
        if (v.empty() == false) {
          min_heap_elem = (std::uint64_t)v[0].first;
          v.erase(v.begin());
          heap->pop();
        }
      }
    }
    delete heap;
  }
}

template<>
void test<std::uint64_t>(std::uint64_t radix_log, std::uint64_t testcases, std::uint64_t max_operations, std::uint64_t max_key, std::uint64_t bufsize) {
  fprintf(stderr, "TEST, key_type=std::uint64_t, bufsize=%lu, radix_log=%lu, testcases=%lu, max_operations=%lu, max_key=%lu\n",
      bufsize, radix_log, testcases, max_operations, max_key);
  typedef em_radix_heap<std::uint64_t, std::uint64_t> heap_type;

  for (std::uint64_t test_id = 0; test_id < testcases; ++test_id) {
    if (test_id % 100 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * test_id) / testcases);

    heap_type *heap = new heap_type(radix_log, bufsize, "./tmp");
    std::vector<std::pair<std::uint64_t, std::uint64_t> > v;
    std::uint64_t min_heap_elem = 0;
    for (std::uint64_t j = 0; j < max_operations; ++j) {
      std::int64_t op = utils::random_int64(0, 2);
      if (op == 0) {  // push
        std::uint64_t key = 0;
        bool ok = false;
        for (std::uint64_t t = 0; t < 10; ++t) {
          key = (std::uint64_t)utils::random_int64(0L, (std::int64_t)max_key);
          if (key >= min_heap_elem) { ok = true; break; }
        }
        if (ok) {
          std::uint64_t value = (std::uint64_t)utils::random_int64(0L, 1000000000L);
          heap->push(key, value);
          v.push_back(std::make_pair(key, value));
          comp64 cmp;
          std::stable_sort(v.begin(), v.end(), cmp);
        }
      } else if (op == 1) { // print top
        if (v.empty() == false) {
          std::uint64_t key = heap->top_key();
          std::uint64_t value = heap->top_value();
          if (key != v[0].first || value != v[0].second) {
            fprintf(stderr, "Error:\n");
            fprintf(stderr, "  Correct min key = %lu\n", (std::uint64_t)v[0].first);
            fprintf(stderr, "  Correct min value = %lu\n", (std::uint64_t)v[0].second);
            fprintf(stderr, "  Returned values: key = %lu, value = %lu\n", (std::uint64_t)key, (std::uint64_t)value);
            std::exit(EXIT_FAILURE);
          }
          min_heap_elem = v[0].first;
        }
      } else {  // pop
        if (v.empty() == false) {
          min_heap_elem = v[0].first;
          v.erase(v.begin());
          heap->pop();
        }
      }
    }
    delete heap;
  }
}

void run_tests(std::uint64_t radix_log, std::uint64_t testcases, std::uint64_t max_operations, std::uint64_t bufsize) {
  for (std::uint64_t max_key = 1; max_key <= 250;                 max_key *= 2) test<std::uint8_t>(radix_log,  testcases, max_operations, max_key, bufsize);
  for (std::uint64_t max_key = 1; max_key <= 64000;               max_key *= 2) test<std::uint16_t>(radix_log, testcases, max_operations, max_key, bufsize);
  for (std::uint64_t max_key = 1; max_key <= 4000000000;          max_key *= 2) test<std::uint32_t>(radix_log, testcases, max_operations, max_key, bufsize);
  for (std::uint64_t max_key = 1; max_key <= 1000000000000;       max_key *= 2) test<uint40>(radix_log,        testcases, max_operations, max_key, bufsize);
  for (std::uint64_t max_key = 1; max_key <= 100000000000000;     max_key *= 2) test<uint48>(radix_log,        testcases, max_operations, max_key, bufsize);
  for (std::uint64_t max_key = 1; max_key <= 9000000000000000000; max_key *= 2) test<std::uint64_t>(radix_log, testcases, max_operations, max_key, bufsize);
}

void run_tests_with_given_radix_log(std::uint64_t radix_log, std::uint64_t bufsize) {
  run_tests(radix_log, 10,   10, bufsize);
  run_tests(radix_log, 10,  100, bufsize);
  run_tests(radix_log, 10, 1000, bufsize);
}

int main() {
  srand(time(0) + getpid());

  for (std::uint64_t bufsize = 0; bufsize <= (1UL << 20); bufsize = std::max(bufsize + 1, bufsize * 2))
    for (std::uint64_t radix_log = 1; radix_log < 9; ++radix_log)
      run_tests_with_given_radix_log(radix_log, bufsize);

  fprintf(stderr, "All tests passed.\n");
}

