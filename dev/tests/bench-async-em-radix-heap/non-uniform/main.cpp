#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "utils.hpp"
#include "em_radix_heap.hpp"


int main() {
  static const std::uint64_t n_items = (4UL << 30);
  static const std::uint64_t ram_use = (2UL << 30);
  typedef std::uint8_t key_type;
  typedef std::uint64_t value_type;

  typedef em_radix_heap<key_type, value_type> heap_type;
  std::vector<std::uint64_t> radix_logs;
  radix_logs.push_back(8UL);
  heap_type *heap = new heap_type(radix_logs, "/data/radix_heap_tmp", ram_use);

  static const std::uint64_t rand_buffer_size = (4UL << 20);
  key_type *rand_key_buffer = new key_type[rand_buffer_size];
  value_type *rand_value_buffer = new value_type[rand_buffer_size];
  for (std::uint64_t j = 0; j < rand_buffer_size; ++j) {
    rand_key_buffer[j] = (key_type)utils::random_int64(0L, 255L);
    rand_value_buffer[j] = (value_type)utils::random_int64(0L, 1000000000000000000L);
  }

  // insertions
  long double start = utils::wclock();
  fprintf(stderr, "Inserting:\n");
  std::uint64_t rand_buffer_ptr = 0;
  for (std::uint64_t j = 0; j < n_items; ++j) {
    if ((j % 1000) == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * j) / n_items);
    key_type key = rand_key_buffer[rand_buffer_ptr];
    value_type value = rand_value_buffer[rand_buffer_ptr];
    ++rand_buffer_ptr;
    if (rand_buffer_ptr == rand_buffer_size)
      rand_buffer_ptr = 0;
    heap->push(key, value);
  }
  fprintf(stderr, "Finished, I/O = %.2LfMiB/s\n", ((1.L * heap->io_volume()) / (1L << 20)) / (utils::wclock() - start));

  // extractions
  fprintf(stderr, "Extractions:\n");
  std::uint64_t key_checksum = 0;
  std::uint64_t value_checksum = 0;
  for (std::uint64_t j = 0; j < n_items; ++j) {
    if ((j % 1000) == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * j) / n_items);
    std::pair<key_type, value_type> p = heap->extract_min();
    key_checksum += p.first;
    value_checksum += p.second;
  }
  fprintf(stderr, "Finished. I/O = %.2LfMiB/s\n", ((1.L * heap->io_volume()) / (1L << 20)) / (utils::wclock() - start));
  fprintf(stderr, "key_checksum=%lu, value_checksum=%lu\n", key_checksum, value_checksum);

  if (!heap->empty())
    fprintf(stderr, "Error: heap still not empty!\n");
  else fprintf(stderr, "All is OK\n");
  fprintf(stderr, "Runtime: %.2Lfs\n", utils::wclock() - start);

  delete heap;
  delete[] rand_key_buffer;
  delete[] rand_value_buffer;
}

