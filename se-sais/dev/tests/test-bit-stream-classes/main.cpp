#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "io/async_bit_stream_reader.hpp"
#include "io/async_bit_stream_writer.hpp"
#include "utils.hpp"


int main() {
  srand(time(0) + getpid());
  static const std::uint64_t n_items = (1L << 7);
  
  // write
  typedef async_bit_stream_writer writer_type;
  writer_type *writer = new writer_type("./tmp");
  fprintf(stderr, "write: ");
  for (std::uint64_t j = 0; j < n_items; ++j) {
    std::uint8_t bit = utils::random_int64(0L, 1L);
    writer->write(bit);
    fprintf(stderr, "%d", (std::int32_t)bit);
  }
  fprintf(stderr, "\n");
  delete writer;

  // read
  typedef async_bit_stream_reader reader_type;
  reader_type *reader = new reader_type("./tmp");
  fprintf(stderr, "read:  ");
  for (std::uint64_t j = 0; j < n_items; ++j) {
#if 0
    std::uint8_t bit = reader->read();
#else
    std::uint8_t bit = reader->peek();
    (void) reader->read();
#endif
    fprintf(stderr, "%d", (std::int32_t)bit);
  }
  fprintf(stderr, "\n");
  delete reader;
  utils::file_delete("./tmp");
}
