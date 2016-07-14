#ifndef __INDUCE_PLUS_SUFFIXES_HPP_INCLUDED
#define __INDUCE_PLUS_SUFFIXES_HPP_INCLUDED

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


template<typename chr_t, typename saidx_t, typename blockidx_t = std::uint16_t>
void induce_plus_suffixes(std::uint64_t text_length,
    std::string minus_sufs_filename,
    std::string output_pos_filename,
    std::string output_symbols_filename,
    std::uint64_t &total_io_volume,
    std::uint64_t radix_heap_bufsize, std::uint64_t radix_log,
    std::uint64_t max_block_size,
    chr_t max_char, std::string minus_count_filename,
    std::string minus_symbols_filename,
    std::vector<std::string> &plus_symbols_filenames,
    std::vector<std::string> &plus_type_filenames,
    std::vector<std::string> &plus_pos_filenames) {

  // Initialize radix heap.
  typedef em_radix_heap<chr_t, blockidx_t> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, minus_sufs_filename);

  typedef async_backward_stream_reader<saidx_t> minus_count_reader_type;
  typedef async_backward_stream_reader<chr_t> minus_symbols_reader_type;
  minus_count_reader_type *minus_count_reader = new minus_count_reader_type(minus_count_filename);
  minus_symbols_reader_type *minus_symbols_reader = new minus_symbols_reader_type(minus_symbols_filename);

  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  typedef async_backward_multi_stream_reader<chr_t> plus_symbols_reader_type;
  typedef async_backward_multi_bit_stream_reader plus_type_reader_type;
  typedef async_backward_multi_stream_reader<saidx_t> plus_pos_reader_type;
  plus_symbols_reader_type *plus_symbols_reader = new plus_symbols_reader_type(n_blocks);
  plus_type_reader_type *plus_type_reader = new plus_type_reader_type(n_blocks);
  plus_pos_reader_type *plus_pos_reader = new plus_pos_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    plus_symbols_reader->add_file(plus_symbols_filenames[block_id]);
    plus_type_reader->add_file(plus_type_filenames[block_id]);
    plus_pos_reader->add_file(plus_pos_filenames[block_id]);
  }

  // Initialize reading of sorted minus-suffixes.
  typedef async_backward_stream_reader<saidx_t> minus_reader_type;
  minus_reader_type *minus_reader = new minus_reader_type(minus_sufs_filename);

  // Initialize writer of sorted plus-suffixes.
  typedef async_stream_writer<saidx_t> output_pos_writer_type;
  typedef async_stream_writer<chr_t> output_symbols_writer_type;
  output_pos_writer_type *output_pos_writer = new output_pos_writer_type(output_pos_filename);
  output_symbols_writer_type *output_symbols_writer = new output_symbols_writer_type(output_symbols_filename);

  // Induce plus suffixes.
  chr_t cur_char = max_char;
  while (!radix_heap->empty() || !minus_count_reader->empty()) {
    // Process plus suffixes.
    while (!radix_heap->empty() && radix_heap->min_compare(std::numeric_limits<chr_t>::max() - cur_char)) {
      std::pair<chr_t, blockidx_t> p = radix_heap->extract_min();
      std::uint64_t block_id = p.second;
      saidx_t pos = plus_pos_reader->read_from_ith_file(block_id);
      output_pos_writer->write(pos);
      std::uint64_t pos_uint64 = pos;
      std::uint8_t is_star = plus_type_reader->read_from_ith_file(block_id);
#if 0
      if (pos_uint64 > 0 && !is_star) {
        chr_t prev_ch = plus_symbols_reader->read_from_ith_file(block_id);
        std::uint64_t prev_pos_block_id = (block_id * max_block_size == pos_uint64) ?
          block_id - 1 : block_id;
        radix_heap->push(std::numeric_limits<chr_t>::max() - prev_ch, prev_pos_block_id);
      }
#else
      if (pos_uint64 > 0) {
        chr_t prev_ch = plus_symbols_reader->read_from_ith_file(block_id);
        if (!is_star) {
          std::uint64_t prev_pos_block_id = (block_id * max_block_size == pos_uint64) ? block_id - 1 : block_id;
          radix_heap->push(std::numeric_limits<chr_t>::max() - prev_ch, prev_pos_block_id);
        } else  output_symbols_writer->write(prev_ch);
      }
#endif
    }

    // Process minus suffixes.
    std::uint64_t minus_sufs_count = minus_count_reader->read();
    for (std::uint64_t i = 0; i < minus_sufs_count; ++i) {
      std::uint64_t pos_uint64 = minus_reader->read();
      chr_t prev_ch = minus_symbols_reader->read();
      std::uint64_t prev_pos_block_id = (pos_uint64 - 1) / max_block_size;
      radix_heap->push(std::numeric_limits<chr_t>::max() - prev_ch, prev_pos_block_id);
    }

    // Update current symbol.
    --cur_char;
  }

  // Update I/O volume.
  std::uint64_t io_volume = minus_reader->bytes_read() +
    output_pos_writer->bytes_written() + output_symbols_writer->bytes_written() +
    radix_heap->io_volume() + minus_count_reader->bytes_read() +
    minus_symbols_reader->bytes_read() + plus_symbols_reader->bytes_read() +
    plus_type_reader->bytes_read() + plus_pos_reader->bytes_read();
  total_io_volume += io_volume;

  // Clean up.
  delete radix_heap;
  delete minus_reader;
  delete output_pos_writer;
  delete output_symbols_writer;
  delete minus_count_reader;
  delete minus_symbols_reader;
  delete plus_symbols_reader;
  delete plus_type_reader;
  delete plus_pos_reader;
}

#endif  // __INDUCE_PLUS_SUFFIXES_HPP_INCLUDED
