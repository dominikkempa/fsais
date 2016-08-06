#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <algorithm>

#include "utils.hpp"
#include "async_stream_reader_multipart.hpp"
#include "async_stream_writer_multipart.hpp"


int main() {
  static const std::uint64_t items = (1UL << 30);

  // Write data to disk.
  typedef std::uint8_t value_type;
  std::string filename = "tmp." + utils::random_string_hash();
  std::uint64_t checksum = 0;
  {
    std::uint64_t part_size = (1UL << 20);
    typedef async_stream_writer_multipart<value_type> writer_type;
    writer_type *writer = new writer_type(filename, part_size);
    for (std::uint64_t i = 0; i < items; ++i) {
      std::uint64_t value = utils::random_int64(0L, 255L);
      checksum += value;
      writer->write(value);
    }
    delete writer;
  }

  // Read data from disk.
  std::uint64_t checksum2 = 0;
  {
    typedef async_stream_reader_multipart<value_type> reader_type;
    reader_type *reader = new reader_type(filename);
    for (std::uint64_t i = 0; i < items; ++i) {
      value_type value = reader->read();
      checksum2 += value;
    }
    delete reader;
  }

  fprintf(stderr, "checksum = %lu\n", checksum);
  fprintf(stderr, "checksum2 = %lu\n", checksum2);
}

