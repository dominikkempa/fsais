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
#include "io/async_backward_stream_reader.hpp"
#include "io/async_multi_bit_stream_reader.hpp"


template<typename S, typename T>
struct radix_heap_item {
  S m_pos;
  T m_name;
  std::uint8_t m_type;

  radix_heap_item() {}
  radix_heap_item(S pos, std::uint8_t type) {
    m_pos = pos;
    m_type = type;
  }
  radix_heap_item(S pos, T name, std::uint8_t type) {
    m_pos = pos;
    m_name = name;
    m_type = type;
  }
} __attribute__ ((packed));

template<typename chr_t, typename saidx_t, typename blockidx_t = std::uint16_t>
void induce_minus_substrings(std::uint64_t text_length,
    std::string plus_substrings_filename,
    std::string output_filename,
    std::string plus_count_filename,
    std::string plus_symbols_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_symbols_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::uint64_t &total_io_volume,
    std::uint64_t radix_heap_bufsize,
    std::uint64_t radix_log,
    std::uint64_t max_block_size,
    chr_t last_text_symbol) {

  // Initialize radix heap.
  typedef radix_heap_item<blockidx_t, saidx_t> heap_item_type;
  typedef em_radix_heap<chr_t, heap_item_type> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, output_filename);

  typedef async_backward_stream_reader<saidx_t> plus_count_reader_type;
  typedef async_backward_stream_reader<chr_t> plus_symbols_reader_type;
  plus_count_reader_type *plus_count_reader = new plus_count_reader_type(plus_count_filename);
  plus_symbols_reader_type *plus_symbols_reader = new plus_symbols_reader_type(plus_symbols_filename);

  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  typedef async_multi_bit_stream_reader minus_type_reader_type;
  typedef async_multi_stream_reader<chr_t> minus_symbols_reader_type;
  typedef async_multi_stream_reader<saidx_t> minus_pos_reader_type;
  minus_type_reader_type *minus_type_reader = new minus_type_reader_type(n_blocks);
  minus_symbols_reader_type *minus_symbols_reader = new minus_symbols_reader_type(n_blocks);
  minus_pos_reader_type *minus_pos_reader = new minus_pos_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    minus_type_reader->add_file(minus_type_filenames[block_id]);
    minus_symbols_reader->add_file(minus_symbols_filenames[block_id]);
    minus_pos_reader->add_file(minus_pos_filenames[block_id]);
  }

  // Initialize reading of sorted plus-substrings.
  typedef async_stream_reader<saidx_t> plus_reader_type;
  plus_reader_type *plus_reader = new plus_reader_type(plus_substrings_filename);

  // Initialize the output writer.
  typedef async_stream_writer<saidx_t> output_writer_type;
  output_writer_type *output_writer = new output_writer_type(output_filename);

  // Induce minus substrings.
  bool empty_output = true;
  std::uint64_t diff_items_written = 0;
  bool is_prev_tail_minus = 0;
  bool is_prev_tail_name_defined = 0;
  chr_t prev_head_char = 0;
  saidx_t prev_tail_name = 0;
  std::uint64_t cur_symbol = 0;
  while (cur_symbol <= (std::uint64_t)last_text_symbol || !plus_count_reader->empty() || !radix_heap->empty()) {
    // Process minus substrings.
    if (cur_symbol == (std::uint64_t)last_text_symbol) {
      chr_t head_char = cur_symbol;
      std::uint64_t block_id = (text_length - 1) / max_block_size;
      saidx_t head_pos = minus_pos_reader->read_from_ith_file(block_id);
      std::uint64_t head_pos_uint64 = head_pos;
      saidx_t tail_name = 0;
      bool is_tail_minus = true;

      // Update diff_items_written.
      if (!empty_output) {
        if (!is_prev_tail_name_defined || is_prev_tail_minus != is_tail_minus ||
            prev_head_char != head_char ||  prev_tail_name != tail_name)
          ++diff_items_written;
      } else ++diff_items_written;
      empty_output = false;

      output_writer->write(head_pos);
      output_writer->write(diff_items_written - 1);

      // Watch for the order of minus-substrings with the same name!!!
      std::uint8_t is_star = minus_type_reader->read_from_ith_file(block_id);
      if (head_pos_uint64 > 0 && !is_star) {
        chr_t prev_char = minus_symbols_reader->read_from_ith_file(block_id);
        std::uint64_t prev_pos_block_id = (block_id * max_block_size == head_pos_uint64) ? block_id - 1 : block_id;
        radix_heap->push(prev_char, heap_item_type(prev_pos_block_id, (saidx_t)(diff_items_written - 1), 3));
      }

      prev_head_char = head_char;
      prev_tail_name = tail_name;
      is_prev_tail_minus = is_tail_minus;
      is_prev_tail_name_defined = false;
    }
    while (!radix_heap->empty() && radix_heap->min_compare(cur_symbol)) {
      std::pair<chr_t, heap_item_type> p = radix_heap->extract_min();
      chr_t head_char = cur_symbol;
      std::uint64_t block_id = p.second.m_pos;
      saidx_t tail_name = p.second.m_name;
      bool is_tail_minus = (p.second.m_type & 0x01);

      // Update diff_items_written.
      if (!empty_output) {
        if (!is_prev_tail_name_defined || is_prev_tail_minus != is_tail_minus ||
            prev_head_char != head_char ||  prev_tail_name != tail_name)
          ++diff_items_written;
      } else ++diff_items_written;
      empty_output = false;

      saidx_t head_pos = minus_pos_reader->read_from_ith_file(block_id);
      std::uint64_t head_pos_uint64 = head_pos;
      output_writer->write(head_pos);
      output_writer->write(diff_items_written - 1);

      // Watch for the order of minus-substrings with the same name!!!
      std::uint8_t is_star = minus_type_reader->read_from_ith_file(block_id);
      if (head_pos_uint64 > 0 && !is_star) {
        chr_t prev_char = minus_symbols_reader->read_from_ith_file(block_id);
        std::uint64_t prev_pos_block_id = (block_id * max_block_size == head_pos_uint64) ? block_id - 1 : block_id;
        radix_heap->push(prev_char, heap_item_type(prev_pos_block_id, (saidx_t)(diff_items_written - 1), 1));
      }

      prev_head_char = head_char;
      prev_tail_name = tail_name;
      is_prev_tail_minus = is_tail_minus;
      is_prev_tail_name_defined = true;
    }

    // Process plus substrings.
    std::uint64_t plus_substr_count = plus_count_reader->read();
    for (std::uint64_t i = 0; i < plus_substr_count; ++i) {
      std::uint64_t head_pos = plus_reader->read();
      chr_t prev_char = plus_symbols_reader->read();
      std::uint64_t prev_pos_block_id = (head_pos - 1) / max_block_size;
      saidx_t name = plus_reader->read();
      radix_heap->push(prev_char, heap_item_type(prev_pos_block_id, name, 0));
    }

    // Update current char.
    ++cur_symbol;
  }

  // Update I/O volume.
  std::uint64_t io_volume = plus_reader->bytes_read() +
    output_writer->bytes_written() + radix_heap->io_volume() +
    plus_count_reader->bytes_read() + minus_type_reader->bytes_read() +
    minus_symbols_reader->bytes_read() + plus_symbols_reader->bytes_read() +
    minus_pos_reader->bytes_read();
  total_io_volume += io_volume;

  // Clean up.
  delete plus_reader;
  delete radix_heap;
  delete plus_count_reader;
  delete minus_type_reader;
  delete minus_symbols_reader;
  delete plus_symbols_reader;
  delete minus_pos_reader;
  delete output_writer;
}

#endif  // __INDUCE_MINUS_SUBSTRINGS_HPP_INCLUDED
