#ifndef __INDUCE_PLUS_SUBSTRINGS_HPP_INCLUDED
#define __INDUCE_PLUS_SUBSTRINGS_HPP_INCLUDED

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


template<typename T>
struct radix_heap_pair {
  T m_pos;
  bool m_is_plus_suffix;

  radix_heap_pair() {}
  radix_heap_pair(T pos, bool is_plus_suffix) {
    m_pos = pos;
    m_is_plus_suffix = is_plus_suffix;
  }
} __attribute__ ((packed));

template<typename chr_t, typename saidx_t>
void induce_plus_substrings(const chr_t *text, std::uint64_t text_length,
    // std::uint64_t ram_use,
    std::string minus_star_positions_filename,
    std::string plus_substrings_filename, std::uint64_t &total_io_volume,
    // temporary:
    std::uint64_t radix_heap_bufsize, std::uint64_t radix_log) {

//  fprintf(stderr, "Induce plus-substrings:\n");
//  long double start = utils::wclock();

#if 0
  // Decide on the radix and buffer size for the radix heap.
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
  typedef radix_heap_pair<saidx_t> heap_pair_type;
  typedef em_radix_heap<chr_t, heap_pair_type> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, plus_substrings_filename);

  // Initialize reading of lexicographically unsorted (but
  // sorted by position) minus-substrings (in reverse order).
  typedef async_backward_stream_reader<saidx_t> minus_reader_type;
  minus_reader_type *minus_reader = new minus_reader_type(minus_star_positions_filename);

  // Initialize writer of sorted plus-substrings.
  typedef async_stream_writer<saidx_t> plus_writer_type;
  plus_writer_type *plus_writer = new plus_writer_type(plus_substrings_filename);

  // Inducing of plus-substrings follows.
//  fprintf(stderr, "  Induce: ");
  // First, sort start positions of all
  // minus-star-substrings by the first
  // symbol by adding them to the heap.
  while (!minus_reader->empty()) {
    saidx_t pos = minus_reader->read();
    std::uint64_t pos_uint64 = pos;
    // We invert the rank of a symbol, since
    // radix_heap implements only extract_min().
    radix_heap->push(std::numeric_limits<chr_t>::max() - text[pos_uint64], heap_pair_type(pos, false));
  }
  // Process plus-substrings.
  while (!radix_heap->empty()) {
    std::pair<chr_t, heap_pair_type> p = radix_heap->extract_min();
    chr_t ch = std::numeric_limits<chr_t>::max() - p.first;
    saidx_t pos = p.second.m_pos;
    std::uint64_t pos_uint64 = pos;
    if (p.second.m_is_plus_suffix) {
      plus_writer->write(pos);
      --ch;
    }
    // Note the +1 below. This is because we want the item in bucket c from the input to be processed.
    // after the items that were inserted in the line below. One way to do this is to insert items from
    // the input with a key decreased by one. Since we might not be able to always decrease, instead
    // we increase the key of the item inserted below. We can always do that, because there is no
    // plus-substring that begins with the maximal symbol in the alphabet.
    if (pos_uint64 > 0 && text[pos_uint64 - 1] <= ch)
      radix_heap->push(std::numeric_limits<chr_t>::max() - (text[pos_uint64 - 1] + 1), heap_pair_type((saidx_t)(pos_uint64 - 1), true));
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

#endif  // __INDUCE_PLUS_SUBSTRINGS_HPP_INCLUDED
