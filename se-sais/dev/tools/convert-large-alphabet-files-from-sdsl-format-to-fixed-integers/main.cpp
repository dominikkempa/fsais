#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <algorithm>

#include "sdsl/int_vector_buffer.hpp"
#include "io/async_stream_writer.hpp"
#include "uint40.hpp"


int main(int argc, char **argv) {
  if (argc != 3)
    std::exit(EXIT_FAILURE);

  typedef async_stream_writer<std::uint32_t> writer_type;
  writer_type *writer = new writer_type(argv[2]);

  const char* file_name = argv[1];
  sdsl::int_vector_buffer<> buf(file_name,std::ios::in);

  std::uint64_t maxsym = 0;
  std::uint64_t length = 0;

  std::vector<std::uint64_t> freq(200000000, 0UL);

  for(size_t i=0;i<buf.size();i++) {
    if (i % 1000000 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * i) / buf.size());
    uint64_t sym = buf[i];
    writer->write(sym);
    
    maxsym = std::max(maxsym, sym);
    ++freq[sym];
    ++length;
  }

  delete writer;

  fprintf(stderr, "\nn = %lu\n", length);
  fprintf(stderr, "maxsym = %lu\n", maxsym);

  std::uint64_t zero_freq = 0;
  for (std::uint64_t i = 0; i < freq.size(); ++i)
    if (freq[i] == 0) ++zero_freq;
  fprintf(stderr, "zero_freq = %lu\n", zero_freq);
}
