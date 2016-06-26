#ifndef __INDUCE_PLUS_SUFFIXES_HPP_INCLUDED
#define __INDUCE_PLUS_SUFFIXES_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "utils.hpp"
#include "em_radix_heap.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"


template<typename chr_t, typename saidx_t>
void induce_plus_suffixes(const chr_t *text, std::uint64_t text_length,
    //std::uint64_t ram_use,
    std::string minus_sufs_filename,
    std::string plus_sufs_filename, std::uint64_t &total_io_volume,
    // temporary:
    std::uint64_t radix_heap_bufsize, std::uint64_t radix_log) {
//  fprintf(stderr, "Induce plus-suffixes:\n");
//  long double start = utils::wclock();

#if 0
  // Decide on the radix and buffer size for the radix heap.
  std::uint64_t radix_heap_bufsize = (1UL << 20);
  std::uint64_t radix_log = 1;
  {
    std::uint64_t ram_left = ram_use - text_length * sizeof(chr_t);
    while (radix_log <= 16) {
      std::uint64_t depth = (8UL * sizeof(chr_t) + radix_log - 1) / radix_log;
      std::uint64_t radix = (1UL << radix_log);
      std::uint64_t n_queues = depth * (radix - 1) + 1;
      std::uint64_t ram_needed = n_queues * radix_heap_bufsize;
      if (ram_needed > ram_left) break;
      else ++radix_log;
    }

    if (radix_log > 1) --radix_log;
    else {
      // ram_use was very small. Use the smallest
      // possible radix and shrink the buffer size.
      std::uint64_t depth = (8UL * sizeof(chr_t) + radix_log - 1) / radix_log;
      std::uint64_t radix = (1UL << radix_log);
      std::uint64_t n_queues = depth * (radix - 1) + 1;
      radix_heap_bufsize = ram_left / n_queues;
    }
  }
#endif

  // Print decided values.
//  fprintf(stderr, "  Radix log = %lu\n", radix_log);
//  fprintf(stderr, "  Radix buffer size = %lu\n", radix_heap_bufsize);

  // Initialize radix heap.
  typedef em_radix_heap<chr_t, saidx_t> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, minus_sufs_filename);

  // Initialize reading of sorted minus-suffixes (in reverse order).
  typedef async_backward_stream_reader<saidx_t> minus_reader_type;
  minus_reader_type *minus_reader = new minus_reader_type(minus_sufs_filename);

  // Initialize writer of sorted plus-suffixes.
  typedef async_stream_writer<saidx_t> plus_writer_type;
  plus_writer_type *plus_writer = new plus_writer_type(plus_sufs_filename);

  // Inducing of plus-suffixes follows.
//  fprintf(stderr, "  Induce: ");
  bool is_next_minus_suffix = false;
  chr_t next_minus_suffix_bucket = 0;
  if (!minus_reader->empty()) {
    is_next_minus_suffix = true;
    next_minus_suffix_bucket = text[(std::uint64_t)minus_reader->peek()];
  }
  while (!radix_heap->empty() || is_next_minus_suffix) {
    // Process plus-suffixes.
    while (!radix_heap->empty() && (!is_next_minus_suffix ||
          (std::numeric_limits<chr_t>::max() - radix_heap->top_key()) >= next_minus_suffix_bucket)) {
      std::pair<chr_t, saidx_t> p = radix_heap->extract_min();
      chr_t ch = std::numeric_limits<chr_t>::max() - p.first;
      saidx_t pos = p.second;
      plus_writer->write(pos);
      std::uint64_t pos_uint64 = pos;
      if (pos_uint64 > 0 && text[pos_uint64 - 1] <= ch)
        radix_heap->push(std::numeric_limits<chr_t>::max() - text[pos_uint64 - 1], (saidx_t)(pos_uint64 - 1));
    }

    // Process minus-suffixes.
    if (is_next_minus_suffix) {
      is_next_minus_suffix = false;
      while (!minus_reader->empty()) {
        std::uint64_t pos = minus_reader->peek();
        chr_t ch = text[pos];
        if (ch == next_minus_suffix_bucket) {
          minus_reader->read();
          if (pos > 0 && text[pos - 1] < ch)
            radix_heap->push(std::numeric_limits<chr_t>::max() - text[pos - 1], (saidx_t)(pos - 1));
        } else {
          is_next_minus_suffix = true;
          next_minus_suffix_bucket = ch;
          break;
        }
      }
    }
  }

  // Update I/O volume.
  std::uint64_t io_volume = minus_reader->bytes_read() +
    plus_writer->bytes_written() + radix_heap->io_volume();
  total_io_volume += io_volume;

  // Clean up.
  delete minus_reader;
  delete plus_writer;
  delete radix_heap;

  // Print summary.
//  long double total_time = utils::wclock() - start;
//  fprintf(stderr, "  time = %.2Lfs, I/O = %.2LfMiB/s, "
//      "total I/O vol = %.2Lfn\n", total_time,
//      (1.L * io_volume / (1L << 20)) / total_time,
//      (1.L * total_io_volume) / text_length);
}

#endif  // __INDUCE_PLUS_SUFFIXES_HPP_INCLUDED
