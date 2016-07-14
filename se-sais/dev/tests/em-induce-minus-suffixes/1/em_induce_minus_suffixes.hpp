#ifndef __EM_INDUCE_MINUS_SUFFIXES_HPP_INCLUDED
#define __EM_INDUCE_MINUS_SUFFIXES_HPP_INCLUDED

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


template<typename chr_t, typename saidx_t, typename blockidx_t>
void em_induce_minus_suffixes(
    std::uint64_t text_length,
    std::uint64_t radix_heap_bufsize,
    std::uint64_t radix_log,
    std::uint64_t max_block_size,
    chr_t last_text_symbol,
    std::string output_filename,
    std::string plus_pos_filename,
    std::string plus_type_filename,
    std::string plus_count_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::uint64_t &total_io_volume) {

  // Initialize radix heap.
  typedef em_radix_heap<chr_t, blockidx_t> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, output_filename);

  // Initialize the readers for plus suffixes.
  typedef async_backward_stream_reader<saidx_t> plus_pos_reader_type;
  typedef async_backward_bit_stream_reader plus_type_reader_type;
  typedef async_backward_stream_reader<saidx_t> plus_count_reader_type;
  plus_pos_reader_type *plus_pos_reader = new plus_pos_reader_type(plus_pos_filename);
  plus_type_reader_type *plus_type_reader = new plus_type_reader_type(plus_type_filename);
  plus_count_reader_type *plus_count_reader = new plus_count_reader_type(plus_count_filename);

  // Initialize readers and writers for minus suffixes.
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  typedef async_multi_stream_reader<saidx_t> minus_pos_reader_type;
  typedef async_multi_bit_stream_reader minus_type_reader_type;
  minus_pos_reader_type *minus_pos_reader = new minus_pos_reader_type(n_blocks);
  minus_type_reader_type *minus_type_reader = new minus_type_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    minus_pos_reader->add_file(minus_pos_filenames[block_id]);
    minus_type_reader->add_file(minus_type_filenames[block_id]);
  }

  // Initialize the readers of data associated with both types of suffixes.
  typedef async_multi_stream_reader<chr_t> symbols_reader_type;
  symbols_reader_type *symbols_reader = new symbols_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
    symbols_reader->add_file(symbols_filenames[block_id]);

  // Initialize output writer.
  typedef async_stream_writer<saidx_t> output_writer_type;
  output_writer_type *output_writer = new output_writer_type(output_filename);

  // Induce minus suffixes.
  radix_heap->push(last_text_symbol, (blockidx_t)((text_length - 1) / max_block_size));
  chr_t cur_symbol = 0;
  while (!plus_count_reader->empty() || !radix_heap->empty()) {
    // Process minus suffixes.
    while (!radix_heap->empty() && radix_heap->min_compare(cur_symbol)) {
      std::pair<chr_t, blockidx_t> p = radix_heap->extract_min();
      std::uint64_t block_id = p.second;
      saidx_t pos = minus_pos_reader->read_from_ith_file(block_id);
      std::uint8_t is_star = minus_type_reader->read_from_ith_file(block_id);
      std::uint64_t pos_uint64 = pos;
      output_writer->write(pos);

      if (pos_uint64 > 0 && !is_star) {
        chr_t prev_ch = symbols_reader->read_from_ith_file(block_id);
        std::uint64_t prev_pos_block_id = (block_id * max_block_size == pos_uint64) ?
          block_id - 1 : block_id;
        radix_heap->push(prev_ch, (blockidx_t)prev_pos_block_id);
      }
    }

    // Process plus suffixes.
    std::uint64_t plus_suf_count = plus_count_reader->read();
    for (std::uint64_t i = 0; i < plus_suf_count; ++i) {
      std::uint64_t head_pos = plus_pos_reader->read();
      if (plus_type_reader->read()) {
        std::uint64_t head_pos_block_id = head_pos / max_block_size;
        bool head_pos_at_block_boundary = (head_pos_block_id * max_block_size == head_pos);
        std::uint64_t prev_pos_block_id = head_pos_block_id - head_pos_at_block_boundary;
        chr_t prev_ch = symbols_reader->read_from_ith_file(head_pos_block_id);
        radix_heap->push(prev_ch, prev_pos_block_id);
      }
    }

    // Update current symbol.
    ++cur_symbol;
  }

  // Update I/O volume.
  std::uint64_t io_volume = radix_heap->io_volume() +
    plus_pos_reader->bytes_read() + plus_type_reader->bytes_read() +
    plus_count_reader->bytes_read() + minus_pos_reader->bytes_read() +
    minus_type_reader->bytes_read() + symbols_reader->bytes_read() +
    output_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete radix_heap;
  delete plus_pos_reader;
  delete plus_type_reader;
  delete plus_count_reader;
  delete minus_pos_reader;
  delete minus_type_reader;
  delete symbols_reader;
  delete output_writer;
}

#endif  // __EM_INDUCE_MINUS_SUFFIXES_HPP_INCLUDED
