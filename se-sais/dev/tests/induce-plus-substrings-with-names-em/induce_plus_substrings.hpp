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
#include "io/async_multi_stream_reader.hpp"
#include "io/async_multi_bit_stream_reader.hpp"
#include "io/async_backward_multi_bit_stream_reader.hpp"


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
void induce_plus_substrings(const chr_t *text, std::uint64_t text_length,
    std::string minus_star_positions_filename,
    std::string minus_symbols_filename,
    std::string plus_substrings_filename,
    std::vector<std::string> &plus_type_filenames,
    std::uint64_t &total_io_volume,
    std::uint64_t radix_heap_bufsize, std::uint64_t radix_log,
    std::uint64_t max_block_size) {
  std::uint64_t io_volume = 0;

  // Initialize radix heap.
  typedef radix_heap_item<saidx_t> heap_item_type;
  typedef em_radix_heap<chr_t, heap_item_type> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, plus_substrings_filename);

  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  typedef async_backward_multi_bit_stream_reader plus_type_reader_type;
  plus_type_reader_type *plus_type_reader = new plus_type_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    plus_type_reader->add_file(plus_type_filenames[block_id]);
  }

#if 0
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  typedef async_multi_stream_reader<chr_t> plus_symbols_reader_type;
  plus_symbols_reader_type *plus_symbols_reader = new plus_symbols_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    plus_symbols_reader->add_file(plus_symbols_filenames[block_id]);
  }
#endif

  // Initialize writer of sorted plus-substrings.
  typedef async_stream_writer<saidx_t> plus_writer_type;
  plus_writer_type *plus_writer = new plus_writer_type(plus_substrings_filename);

  // Inducing of plus-substrings follows.
  // First, sort start positions of all
  // minus-star-substrings by the first
  // symbol by adding them to the heap.
  {
    // Initialize reading of lexicographically unsorted (but
    // sorted by position) minus-substrings (in reverse order).
    typedef async_backward_stream_reader<saidx_t> minus_pos_reader_type;
    typedef async_backward_stream_reader<chr_t> minus_symbols_reader_type;
    minus_pos_reader_type *minus_pos_reader = new minus_pos_reader_type(minus_star_positions_filename);
    minus_symbols_reader_type *minus_symbols_reader = new minus_symbols_reader_type(minus_symbols_filename);

    while (!minus_pos_reader->empty()) {
      saidx_t pos = minus_pos_reader->read();
      chr_t ch = minus_symbols_reader->read();

      // We invert the rank of a symbol, since
      // radix_heap implements only extract_min().
      radix_heap->push(std::numeric_limits<chr_t>::max() - ch, heap_item_type(pos, 0));
    }

    // Update I/O volume.
    io_volume += minus_pos_reader->bytes_read() + minus_symbols_reader->bytes_read();

    // Clean up.
    delete minus_pos_reader;
    delete minus_symbols_reader;
  }

  bool empty_output = true;
  std::uint64_t diff_written_items = 0;
  bool is_prev_head_plus = false;
  bool is_prev_tail_plus = false;
  chr_t prev_head_char = 0;
  saidx_t prev_tail_name = 0;

  while (!radix_heap->empty()) {
    std::pair<chr_t, heap_item_type> p = radix_heap->extract_min();
    chr_t head_char = std::numeric_limits<chr_t>::max() - p.first;
    saidx_t head_pos = p.second.m_pos;
    saidx_t tail_name = p.second.m_name;
    std::uint64_t head_pos_uint64 = head_pos;
    std::uint64_t block_id = head_pos_uint64 / max_block_size;
    bool is_head_plus = (p.second.m_type & 0x01);
    bool is_tail_plus = (p.second.m_type & 0x02);
    chr_t prev_char = 0;
    if (head_pos_uint64 > 0)
//      prev_char = plus_symbols_reader->read_from_ith_file(block_id);
      prev_char = text[head_pos_uint64 - 1];

    if (is_head_plus) {
      --head_char;
      if (!empty_output) {
        if (!is_prev_head_plus || is_tail_plus != is_prev_tail_plus ||
            head_char != prev_head_char || tail_name != prev_tail_name)
          ++diff_written_items;
      } else ++diff_written_items;
      empty_output = false;

      plus_writer->write(head_pos);
      plus_writer->write(diff_written_items - 1);
      bool is_star = plus_type_reader->read_from_ith_file(block_id);

      // Note the +1 below. This is because we want the item in bucket c from the input to be processed.
      // after the items that were inserted in the line below. One way to do this is to insert items from
      // the input with a key decreased by one. Since we might not be able to always decrease, instead
      // we increase the key of the item inserted below. We can always do that, because there is no
      // plus-substring that begins with the maximal symbol in the alphabet.
      if (head_pos_uint64 > 0 && !is_star)
        radix_heap->push(std::numeric_limits<chr_t>::max() - (prev_char + 1),
            heap_item_type((saidx_t)(head_pos_uint64 - 1), (saidx_t)(diff_written_items - 1), 3));
  } else radix_heap->push(std::numeric_limits<chr_t>::max() - (prev_char + 1),
      heap_item_type((saidx_t)(head_pos_uint64 - 1), head_char, 1));

    is_prev_head_plus = is_head_plus;
    is_prev_tail_plus = is_tail_plus;
    prev_head_char = head_char;
    prev_tail_name = tail_name;
  }

  // Update I/O volume.
  io_volume += plus_writer->bytes_written() + radix_heap->io_volume() +
    plus_type_reader->bytes_read();
#if 0
    plus_symbols_reader->bytes_read();
#endif
  total_io_volume += io_volume;

  // Clean up.
  delete plus_writer;
  delete plus_type_reader;
#if 0
  delete plus_symbols_reader;
#endif
  delete radix_heap;
}

#endif  // __INDUCE_PLUS_SUBSTRINGS_HPP_INCLUDED
