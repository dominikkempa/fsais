#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "io/async_stream_writer.hpp"
#include "uint40.hpp"


int main(int argc, char **argv) {
  srand(time(0) + getpid());
  if (argc != 2) {
    fprintf(stderr, "Usage: %s SIZE\n"
        "Generate random text over integer alphabet (32-bit) of length SIZE MiB.\n"
        "The output is written on standard output.\n", argv[0]);
    std::exit(EXIT_FAILURE);
  }

  std::uint64_t text_length = (atol(argv[1]) << 20);

  typedef std::uint32_t char_type;
  typedef async_stream_writer<char_type> writer_type;

  fprintf(stderr, "Text length = %lu\n", text_length);
  fprintf(stderr, "sizeof(char_type) = %lu\n", sizeof(char_type));

  writer_type *writer = new writer_type();
  for (std::uint64_t j = 0; j < text_length; ++j) {
    if (j % 1000000 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * j) / text_length);
    writer->write(utils::random_int32(0L, (1L << 31) - 1));
  }
  delete writer;
}
