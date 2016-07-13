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


template<typename S, typename T>
struct radix_heap_item {
  S m_block_idx;
  T m_name;
  std::uint8_t m_type;

  radix_heap_item() {}
  radix_heap_item(S block_idx, std::uint8_t type) {
    m_block_idx = block_idx;
    m_type = type;
  }
  radix_heap_item(S block_idx, T name, std::uint8_t type) {
    m_block_idx = block_idx;
    m_name = name;
    m_type = type;
  }
} __attribute__ ((packed));

template<typename chr_t, typename saidx_t, typename blockidx_t>
void induce_plus_substrings(std::uint64_t text_length,
    std::string minus_star_positions_filename,
    std::string minus_symbols_filename,
    std::string output_filename,
    std::vector<std::string> &plus_type_filenames,
    std::vector<std::string> &symbols_filenames,
    std::vector<std::string> &pos_filenames,
    std::vector<std::uint64_t> &block_count_target,
    std::uint64_t &total_io_volume,
    std::uint64_t radix_heap_bufsize, std::uint64_t radix_log,
    std::uint64_t max_block_size) {
  std::uint64_t io_volume = 0;

  // Initialize radix heap.
  typedef radix_heap_item<blockidx_t, saidx_t> heap_item_type;
  typedef em_radix_heap<chr_t, heap_item_type> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, output_filename);

  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  typedef async_backward_multi_bit_stream_reader plus_type_reader_type;
  typedef async_backward_multi_stream_reader<chr_t> symbols_reader_type;
  typedef async_backward_multi_stream_reader<saidx_t> pos_reader_type;
  plus_type_reader_type *plus_type_reader = new plus_type_reader_type(n_blocks);
  symbols_reader_type *symbols_reader = new symbols_reader_type(n_blocks);
  pos_reader_type *pos_reader = new pos_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    plus_type_reader->add_file(plus_type_filenames[block_id]);
    symbols_reader->add_file(symbols_filenames[block_id]);
    pos_reader->add_file(pos_filenames[block_id]);
  }

  // Initialize the output writer.
  typedef async_stream_writer<saidx_t> output_writer_type;
  output_writer_type *output_writer = new output_writer_type(output_filename);

  // Inducing of plus-substrings follows. First, sort start positions of all
  // minus-star-substrings by the first symbol by adding them to the heap.
  {
    // Initialize reading of lexicographically unsorted (but
    // sorted by position) minus-substrings (in reverse order).
    typedef async_backward_stream_reader<saidx_t> minus_pos_reader_type;
    typedef async_backward_stream_reader<chr_t> minus_symbols_reader_type;
    minus_pos_reader_type *minus_pos_reader = new minus_pos_reader_type(minus_star_positions_filename);
    minus_symbols_reader_type *minus_symbols_reader = new minus_symbols_reader_type(minus_symbols_filename);

    while (!minus_pos_reader->empty()) {
      saidx_t pos = minus_pos_reader->read();
      std::uint64_t pos_uint64 = pos;
      std::uint64_t block_id = pos_uint64 / max_block_size;
      chr_t ch = minus_symbols_reader->read();

      // We invert the rank of a symbol, since
      // radix_heap implements only extract_min().
      radix_heap->push(std::numeric_limits<chr_t>::max() - ch, heap_item_type(block_id, 0));
    }

    // Update I/O volume.
    io_volume += minus_pos_reader->bytes_read() + minus_symbols_reader->bytes_read();

    // Clean up.
    delete minus_pos_reader;
    delete minus_symbols_reader;
  }

  bool empty_output = true;
  bool was_extract_min = false;
  std::uint64_t diff_items = 0;
  std::uint64_t diff_items_snapshot = 0;
  std::uint64_t diff_written_items = 0;
  bool is_prev_head_plus = false;
  bool is_prev_tail_plus = false;
  chr_t prev_head_char = 0;
  saidx_t prev_tail_name = 0;

  std::vector<std::uint64_t> block_count(n_blocks, 0UL);
  while (!radix_heap->empty()) {
    std::pair<chr_t, heap_item_type> p = radix_heap->extract_min();
    chr_t head_char = std::numeric_limits<chr_t>::max() - p.first;
    std::uint64_t block_id = p.second.m_block_idx;
    saidx_t tail_name = p.second.m_name;
    bool is_head_plus = (p.second.m_type & 0x01);
    bool is_tail_plus = (p.second.m_type & 0x02);

    saidx_t head_pos = pos_reader->read_from_ith_file(block_id);
    std::uint64_t head_pos_uint64 = head_pos;

    ++block_count[block_id];
    std::uint8_t head_pos_at_block_beg = (block_count[block_id] ==
        block_count_target[block_id]);

    if (is_head_plus) {
      --head_char;
      bool is_diff = false;
      if (was_extract_min) {
        if (!is_prev_head_plus || is_tail_plus != is_prev_tail_plus ||
            head_char != prev_head_char || tail_name != prev_tail_name)
          is_diff = true;
      } else is_diff = true;
      was_extract_min = true;
      diff_items += is_diff;

      // Note the +1 below. This is because we want the item in bucket c from the input to be processed.
      // after the items that were inserted in the line below. One way to do this is to insert items from
      // the input with a key decreased by one. Since we might not be able to always decrease, instead
      // we increase the key of the item inserted below. We can always do that, because there is no
      // plus-substring that begins with the maximal symbol in the alphabet.
      bool is_star = plus_type_reader->read_from_ith_file(block_id);
      if (is_star == true) {
        if (empty_output == false) {
          if (diff_items != diff_items_snapshot)
            ++diff_written_items;
        } else ++diff_written_items;
        output_writer->write(head_pos);
        output_writer->write(diff_written_items - 1);
        empty_output = false;
        diff_items_snapshot = diff_items;
      } else if (head_pos_uint64 > 0) {
        chr_t prev_char = symbols_reader->read_from_ith_file(block_id);
        std::uint64_t prev_pos_block_idx = block_id - head_pos_at_block_beg;
        radix_heap->push(std::numeric_limits<chr_t>::max() - (prev_char + 1),
            heap_item_type(prev_pos_block_idx, (saidx_t)(diff_items - 1), 3));
      }
    } else {
      chr_t prev_char = symbols_reader->read_from_ith_file(block_id);
      std::uint64_t prev_pos_block_idx = (head_pos_uint64 - 1) / max_block_size;
      radix_heap->push(std::numeric_limits<chr_t>::max() - (prev_char + 1),
          heap_item_type(prev_pos_block_idx, head_char, 1));
    }

    is_prev_head_plus = is_head_plus;
    is_prev_tail_plus = is_tail_plus;
    prev_head_char = head_char;
    prev_tail_name = tail_name;
  }

  // Update I/O volume.
  io_volume += output_writer->bytes_written() + radix_heap->io_volume() +
    plus_type_reader->bytes_read() + symbols_reader->bytes_read() +
    pos_reader->bytes_read();
  total_io_volume += io_volume;

  // Clean up.
  delete output_writer;
  delete plus_type_reader;
  delete symbols_reader;
  delete pos_reader;
  delete radix_heap;
}

#endif  // __INDUCE_PLUS_SUBSTRINGS_HPP_INCLUDED
