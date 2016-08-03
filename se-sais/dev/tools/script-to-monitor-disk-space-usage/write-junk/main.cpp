#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "async_stream_writer.hpp"
#include "utils.hpp"


int main(int argc, char **argv) {
  if (argc != 2) std::exit(EXIT_FAILURE);

  std::uint64_t gib_of_data = std::atol(argv[1]);
  std::uint64_t bytes_of_data = (gib_of_data << 30);

  typedef std::uint8_t value_type;
  typedef async_stream_writer<value_type> writer_type;
  writer_type *writer = new writer_type();

  for (std::uint64_t i = 0; i < bytes_of_data; ++i)
    writer->write(utils::random_int64(0L, 255L));

  delete writer;
}

