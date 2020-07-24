#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "io/async_multi_bit_stream_reader.hpp"
#include "io/async_bit_stream_writer.hpp"
#include "utils.hpp"


int main() {
  srand(time(0) + getpid());
  static const std::uint64_t n_items = (1L << 7);
  static const std::uint64_t n_files = 8;

  // write
  std::vector<std::string> filenames;
  for (std::uint64_t i = 0; i < n_files; ++i) {
    typedef async_bit_stream_writer writer_type;
    std::string filename = "./tmp." + utils::random_string_hash();
    filenames.push_back(filename);
    writer_type *writer = new writer_type(filename);
    for (std::uint64_t j = 0; j < n_items; ++j) {
      std::uint8_t bit = utils::random_int64(0L, 1L);
      writer->write(bit);
      fprintf(stderr, "%d", (std::int32_t)bit);
    }
    fprintf(stderr, "\n");
    delete writer;
  }
  fprintf(stderr, "\n");

  // read
  typedef async_multi_bit_stream_reader reader_type;
  reader_type *reader = new reader_type(n_files);
  for (std::uint64_t j = 0; j < n_files; ++j)
    reader->add_file(filenames[j]);
  std::vector<std::uint64_t> items_read_count(n_files, 0UL);
  std::vector<std::vector<std::uint8_t> > ret(n_files);
  std::uint64_t total_items_read_count = 0;
  while (total_items_read_count < n_files * n_items) {
    std::uint64_t file_id = utils::random_int64(0L, (std::int64_t)n_files - 1);
    if (items_read_count[file_id] < n_items) {
      ret[file_id].push_back(reader->read_from_ith_file(file_id));
      items_read_count[file_id]++;
      ++total_items_read_count;
    }
  }
  delete reader;
  for (std::uint64_t i = 0; i < n_files; ++i) {
    for (std::uint64_t j = 0; j < ret[i].size(); ++j)
      fprintf(stderr, "%d", (std::int32_t)ret[i][j]);
    fprintf(stderr, "\n");
  }

  // clean up
  for (std::uint64_t i = 0; i < n_files; ++i)
    utils::file_delete(filenames[i]);
}
