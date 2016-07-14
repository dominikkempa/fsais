#ifndef __EM_INDUCE_PLUS_SUFFIXES_HPP_INCLUDED
#define __EM_INDUCE_PLUS_SUFFIXES_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "utils.hpp"
#include "em_radix_heap.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_backward_multi_stream_reader.hpp"
#include "io/async_backward_multi_bit_stream_reader.hpp"


template<typename chr_t, typename saidx_t, typename blockidx_t>
void em_induce_plus_suffixes(
    std::uint64_t text_length,
    std::uint64_t radix_heap_bufsize,
    std::uint64_t radix_log,
    std::uint64_t max_block_size,
    chr_t max_char,
    std::string output_pos_filename,
    std::string minus_pos_filename,
    std::string minus_count_filename,
    std::vector<std::string> &plus_type_filenames,
    std::vector<std::string> &plus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::uint64_t &total_io_volume) {

  // Initialize radix heap.
  typedef em_radix_heap<chr_t, blockidx_t> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, output_pos_filename);

  // Initialize readers of data associated with minus suffixes.
  typedef async_backward_stream_reader<saidx_t> minus_count_reader_type;
  typedef async_backward_stream_reader<saidx_t> minus_pos_reader_type;
  minus_count_reader_type *minus_count_reader = new minus_count_reader_type(minus_count_filename);
  minus_pos_reader_type *minus_pos_reader = new minus_pos_reader_type(minus_pos_filename);

  // Initialize readers of data associated with plus suffixes.
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  typedef async_backward_multi_bit_stream_reader plus_type_reader_type;
  typedef async_backward_multi_stream_reader<saidx_t> plus_pos_reader_type;
  plus_type_reader_type *plus_type_reader = new plus_type_reader_type(n_blocks);
  plus_pos_reader_type *plus_pos_reader = new plus_pos_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    plus_type_reader->add_file(plus_type_filenames[block_id]);
    plus_pos_reader->add_file(plus_pos_filenames[block_id]);
  }

  // Initialize the readers of data associated with both types of suffixes.
  typedef async_backward_multi_stream_reader<chr_t> symbols_reader_type;
  symbols_reader_type *symbols_reader = new symbols_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
    symbols_reader->add_file(symbols_filenames[block_id]);

  // Initialize output writer.
  typedef async_stream_writer<saidx_t> output_pos_writer_type;
  output_pos_writer_type *output_pos_writer = new output_pos_writer_type(output_pos_filename);

  // Induce plus suffixes.
  chr_t cur_char = max_char;
  std::vector<std::uint64_t> block_count(n_blocks, 0UL);
  while (!radix_heap->empty() || !minus_count_reader->empty()) {
    // Process plus suffixes.
    while (!radix_heap->empty() && radix_heap->min_compare(std::numeric_limits<chr_t>::max() - cur_char)) {
      std::pair<chr_t, blockidx_t> p = radix_heap->extract_min();
      std::uint64_t block_id = p.second;
      saidx_t pos = plus_pos_reader->read_from_ith_file(block_id);
      output_pos_writer->write(pos);
      std::uint64_t pos_uint64 = pos;
      std::uint8_t is_star = plus_type_reader->read_from_ith_file(block_id);

      if (pos_uint64 > 0 && !is_star) {
        chr_t prev_ch = symbols_reader->read_from_ith_file(block_id);
        std::uint64_t prev_pos_block_id = (block_id * max_block_size == pos_uint64) ?
          block_id - 1 : block_id;
        radix_heap->push(std::numeric_limits<chr_t>::max() - prev_ch, prev_pos_block_id);
      }
    }

    // Process minus suffixes.
    std::uint64_t minus_sufs_count = minus_count_reader->read();
    for (std::uint64_t i = 0; i < minus_sufs_count; ++i) {
      /*
      std::uint64_t head_pos_block_id = minus_pos_reader->read();
      ++block_count[block_id];
      bool pos_starts_at_block_beg = (block_count[block_id] == block_count_target[block_id]);
      std::uint64_t prev_pos_block_id = head_pos_block_id - pos_starts_at_block_beg;
      */
      std::uint64_t pos_uint64 = minus_pos_reader->read();
      std::uint64_t head_pos_block_id = pos_uint64 / max_block_size;
      std::uint64_t prev_pos_block_id = (pos_uint64 - 1) / max_block_size;
      chr_t prev_char = symbols_reader->read_from_ith_file(head_pos_block_id);
      radix_heap->push(std::numeric_limits<chr_t>::max() - prev_char, prev_pos_block_id);
    }

    // Update current symbol.
    --cur_char;
  }

  // Update I/O volume.
  std::uint64_t io_volume = radix_heap->io_volume() +
    minus_pos_reader->bytes_read() + minus_count_reader->bytes_read() +
    plus_type_reader->bytes_read() + plus_pos_reader->bytes_read() +
    symbols_reader->bytes_read() + output_pos_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete radix_heap;
  delete minus_pos_reader;
  delete minus_count_reader;
  delete plus_type_reader;
  delete plus_pos_reader;
  delete symbols_reader;
  delete output_pos_writer;
}

#endif  // __EM_INDUCE_PLUS_SUFFIXES_HPP_INCLUDED
