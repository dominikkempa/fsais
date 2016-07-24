#ifndef __EM_INDUCE_MINUS_STAR_SUFFIXES_HPP_INCLUDED
#define __EM_INDUCE_MINUS_STAR_SUFFIXES_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "utils.hpp"
#include "em_radix_heap.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_reader.hpp"
#include "io/async_vbyte_stream_reader.hpp"
#include "io/async_multi_bit_stream_reader.hpp"
#include "io/async_multi_stream_reader.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_backward_bit_stream_reader.hpp"


template<typename char_type,
  typename text_offset_type,
  typename block_offset_type,
  typename block_id_type>
void em_induce_minus_star_suffixes(
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::vector<std::uint64_t> &target_block_count,
    char_type last_text_symbol,
    std::string output_filename,
    std::string plus_pos_filename,
    std::string plus_count_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;

  // Initialize radix heap.
  std::vector<std::uint64_t> radix_logs;
  {
    std::uint64_t target_sum = 8UL * sizeof(char_type);
    std::uint64_t cur_sum = 0;
    while (cur_sum < target_sum) {
      std::uint64_t radix_log = std::min(10UL, target_sum - cur_sum);
      radix_logs.push_back(radix_log);
      cur_sum += radix_log;
    }
  }
  typedef em_radix_heap<char_type, block_id_type> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_logs, output_filename, ram_use);

  // Initialize the readers for plus suffixes.
  typedef async_backward_stream_reader<block_id_type> plus_pos_reader_type;
  typedef async_backward_stream_reader<text_offset_type> plus_count_reader_type;
  plus_pos_reader_type *plus_pos_reader = new plus_pos_reader_type(plus_pos_filename);
  plus_count_reader_type *plus_count_reader = new plus_count_reader_type(plus_count_filename);

  // Initialize readers and writers for minus suffixes.
  typedef async_multi_stream_reader<block_offset_type> minus_pos_reader_type;
  typedef async_multi_bit_stream_reader minus_type_reader_type;
  minus_pos_reader_type *minus_pos_reader = new minus_pos_reader_type(n_blocks);
  minus_type_reader_type *minus_type_reader = new minus_type_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    minus_pos_reader->add_file(minus_pos_filenames[block_id]);
    minus_type_reader->add_file(minus_type_filenames[block_id]);
  }

  // Initialize the readers of data associated with both types of suffixes.
  typedef async_multi_stream_reader<char_type> symbols_reader_type;
  symbols_reader_type *symbols_reader = new symbols_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
    symbols_reader->add_file(symbols_filenames[block_id]);

  // Initialize output writer.
  typedef async_stream_writer<text_offset_type> output_writer_type;
  output_writer_type *output_writer = new output_writer_type(output_filename);

  // Induce minus suffixes.
  radix_heap->push(last_text_symbol, (text_length - 1) / max_block_size);
  char_type cur_symbol = 0;
  std::vector<std::uint64_t> block_count(n_blocks, 0UL);

  while (!plus_count_reader->empty() || !radix_heap->empty()) {
    // Process minus suffixes.
    while (!radix_heap->empty() && radix_heap->min_compare(cur_symbol)) {
      std::pair<char_type, block_id_type> p = radix_heap->extract_min();
      std::uint64_t head_pos_block_id = p.second;
      std::uint8_t is_head_pos_star = minus_type_reader->read_from_ith_file(head_pos_block_id);

      ++block_count[head_pos_block_id];
      bool is_head_pos_at_block_beg = (block_count[head_pos_block_id] == target_block_count[head_pos_block_id]);

      if (is_head_pos_star) {
        std::uint64_t head_pos_block_beg = head_pos_block_id * max_block_size;
        std::uint64_t head_pos = head_pos_block_beg + minus_pos_reader->read_from_ith_file(head_pos_block_id);
        output_writer->write(head_pos);
      } else if (head_pos_block_id > 0 || !is_head_pos_at_block_beg) {
        std::uint64_t prev_pos_char = symbols_reader->read_from_ith_file(head_pos_block_id);
        std::uint64_t prev_pos_block_id = head_pos_block_id - is_head_pos_at_block_beg;
        radix_heap->push(prev_pos_char, prev_pos_block_id);
      }
    }

    // Process plus suffixes.
    if (!plus_count_reader->empty()) {
      std::uint64_t plus_suf_count = plus_count_reader->read();
      for (std::uint64_t i = 0; i < plus_suf_count; ++i) {
        std::uint64_t head_pos_block_id = plus_pos_reader->read();
        ++block_count[head_pos_block_id];
        bool head_pos_at_block_boundary = (block_count[head_pos_block_id] == target_block_count[head_pos_block_id]);
        std::uint64_t prev_pos_block_id = head_pos_block_id - head_pos_at_block_boundary;
        std::uint64_t prev_pos_char = symbols_reader->read_from_ith_file(head_pos_block_id);
        radix_heap->push(prev_pos_char, prev_pos_block_id);
      }
    }

    // Update current symbol.
    ++cur_symbol;
  }

  // Update I/O volume.
  std::uint64_t io_volume = radix_heap->io_volume() +
    plus_pos_reader->bytes_read() + plus_count_reader->bytes_read() +
    minus_pos_reader->bytes_read() + minus_type_reader->bytes_read() +
    symbols_reader->bytes_read() + output_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete radix_heap;
  delete plus_pos_reader;
  delete plus_count_reader;
  delete minus_pos_reader;
  delete minus_type_reader;
  delete symbols_reader;
  delete output_writer;
}

#endif  // __EM_INDUCE_MINUS_STAR_SUFFIXES_HPP_INCLUDED
