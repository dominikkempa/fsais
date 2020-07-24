#ifndef __INDUCE_MINUS_SUBSTRINGS_HPP_INCLUDED
#define __INDUCE_MINUS_SUBSTRINGS_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "utils.hpp"
#include "em_radix_heap.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"


template<typename T>
struct radix_heap_item {
  T m_pos;
  T m_name;
  std::uint8_t m_type;

  radix_heap_item() {}
  radix_heap_item(T pos, std::uint8_t type) {
    m_pos = pos;
    m_type = type;
  }
  radix_heap_item(T pos, T name, std::uint8_t type) {
    m_pos = pos;
    m_name = name;
    m_type = type;
  }
} __attribute__ ((packed));

template<typename chr_t, typename saidx_t>
void induce_minus_substrings(const chr_t *text, std::uint64_t text_length,
    std::uint64_t ram_use, std::string plus_substrings_filename,
    std::string minus_substrings_filename, std::uint64_t &total_io_volume,
    std::uint64_t radix_heap_bufsize = (1UL << 20)) {
  if (ram_use <= text_length * sizeof(chr_t)) {
    fprintf(stderr, "induce_plus_substrings: insufficient ram_use\n");
    std::exit(EXIT_FAILURE);
  }

//  fprintf(stderr, "Induce minus-substrings:\n");
//  long double start = utils::wclock();

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

  // Print decided values.
//  fprintf(stderr, "  Radix log = %lu\n", radix_log);
//  fprintf(stderr, "  Radix buffer size = %lu\n", radix_heap_bufsize);

  // Initialize radix heap.
  typedef radix_heap_item<saidx_t> heap_item_type;
  typedef em_radix_heap<chr_t, heap_item_type> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, minus_substrings_filename);

  // Initialize reading of sorted plus-substrings.
  typedef async_stream_reader<saidx_t> plus_reader_type;
  plus_reader_type *plus_reader = new plus_reader_type(plus_substrings_filename);

  // Initialize writer of sorted minus-substrings.
  typedef async_stream_writer<saidx_t> minus_writer_type;
  minus_writer_type *minus_writer = new minus_writer_type(minus_substrings_filename);

  // Inducing of minus-substrings follows.
//  fprintf(stderr, "  Induce: ");
  bool is_next_plus_substring = false;
  chr_t next_plus_substring_bucket = 0;
  if (!plus_reader->empty()) {
    is_next_plus_substring = true;
    next_plus_substring_bucket = text[(std::uint64_t)plus_reader->peek()];
  }

  radix_heap->push(text[text_length - 1],
      heap_item_type((saidx_t)(text_length - 1), (saidx_t)0, 1));

  bool empty_output = true;
  std::uint64_t diff_items_written = 0;

  bool is_prev_tail_minus = 0;
  bool is_prev_tail_name_defined = 0;
  chr_t prev_head_char = 0;
  saidx_t prev_tail_name = 0;

  while (!radix_heap->empty() || is_next_plus_substring) {
    // Process minus-substrings.
    while (!radix_heap->empty() && (!is_next_plus_substring ||
          radix_heap->top_key() <= next_plus_substring_bucket)) {
      std::pair<chr_t, heap_item_type> p = radix_heap->extract_min();
      chr_t head_char = p.first;
      saidx_t head_pos = p.second.m_pos;
      saidx_t tail_name = p.second.m_name;
      bool is_tail_minus = (p.second.m_type & 0x01);
      bool is_tail_name_defined = (p.second.m_type & 0x02);
      std::uint64_t head_pos_uint64 = head_pos;

      // Update diff_items_written.
      if (!empty_output) {
        if (!is_prev_tail_name_defined || is_prev_tail_minus != is_tail_minus ||
            prev_head_char != head_char ||  prev_tail_name != tail_name)
          ++diff_items_written;
      } else ++diff_items_written;
      empty_output = false;

      minus_writer->write(head_pos);
      minus_writer->write(diff_items_written - 1);

      if (head_pos_uint64 > 0 && text[head_pos_uint64 - 1] >= head_char)
        radix_heap->push(text[head_pos_uint64 - 1],
            heap_item_type((saidx_t)(head_pos_uint64 - 1), (saidx_t)(diff_items_written - 1), 3));

      prev_head_char = head_char;
      prev_tail_name = tail_name;
      is_prev_tail_minus = is_tail_minus;
      is_prev_tail_name_defined = is_tail_name_defined;
    }

    // Process plus-substrings.
    if (is_next_plus_substring) {
      is_next_plus_substring = false;
      while (!plus_reader->empty()) {
        std::uint64_t head_pos = plus_reader->peek();
        chr_t head_char = text[head_pos];
        if (head_char == next_plus_substring_bucket) {
          plus_reader->read();
          saidx_t name = plus_reader->read();
          if (head_pos > 0 && text[head_pos - 1] > head_char)
            radix_heap->push(text[head_pos - 1], heap_item_type(
                  (saidx_t)(head_pos - 1), name, 2));
        } else {
          is_next_plus_substring = true;
          next_plus_substring_bucket = head_char;
          break;
        }
      }
    }
  }

  // Update I/O volume.
  std::uint64_t io_volume = plus_reader->bytes_read() +
    minus_writer->bytes_written() + radix_heap->io_volume();
  total_io_volume += io_volume;

  // Clean up.
  delete plus_reader;
  delete minus_writer;
  delete radix_heap;

  // Print summary.
//  long double total_time = utils::wclock() - start;
//  fprintf(stderr, "  time = %.2Lfs, I/O = %.2LfMiB/s, "
//      "total I/O vol = %.2Lfn\n", total_time,
//      (1.L * io_volume / (1L << 20)) / total_time,
//      (1.L * total_io_volume) / text_length);
}

#endif  // __INDUCE_MINUS_SUBSTRINGS_HPP_INCLUDED
