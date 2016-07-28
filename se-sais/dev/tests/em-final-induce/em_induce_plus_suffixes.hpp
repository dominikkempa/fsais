#ifndef __EM_INDUCE_PLUS_SUFFIXES_HPP_INCLUDED
#define __EM_INDUCE_PLUS_SUFFIXES_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>

#include "utils.hpp"
#include "em_radix_heap.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_multi_stream_reader.hpp"
#include "io/async_multi_bit_stream_reader.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_bit_stream_writer.hpp"


template<typename char_type,
  typename text_offset_type,
  typename block_id_type>
void em_induce_plus_suffixes(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::vector<std::uint64_t> &block_count_target,
    std::string output_pos_filename,
    std::string output_type_filename,
    std::string output_count_filename,
    std::string minus_pos_filename,
    std::string minus_count_filename,
    std::vector<std::string> &plus_type_filenames,
    std::vector<std::string> &plus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
//  fprintf(stderr, "em_induce_plus_suffixes:\n");

  if (text_length == 0) {
    fprintf(stderr, "\nError: text_length = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (max_block_size == 0) {
    fprintf(stderr, "\nError: max_block_size = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (text_alphabet_size == 0) {
    fprintf(stderr, "Error: text_alphabet_size = 0\n");
    std::exit(EXIT_FAILURE);
  }
  
  if (n_blocks == 0) {
    fprintf(stderr, "\nError: n_blocks = 0\n");
    std::exit(EXIT_FAILURE);
  }

  // Check that all types are sufficiently large.
  if ((std::uint64_t)std::numeric_limits<char_type>::max() < text_alphabet_size - 1) {
    fprintf(stderr, "\nError: char_type in im_induce_minus_and_plus_suffixes too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<block_id_type>::max() < n_blocks - 1) {
    fprintf(stderr, "\nError: block_id_type in im_induce_minus_and_plus_suffixes_small too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<text_offset_type>::max() < text_length * 2UL) {
    fprintf(stderr, "\nError: text_offset_type in im_induce_minus_and_plus_suffixes too small!\n");
    std::exit(EXIT_FAILURE);
  }

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
  radix_heap_type *radix_heap = new radix_heap_type(radix_logs, output_pos_filename, ram_use);

//  fprintf(stderr, "  initialize readers 1\n");
  // Initialize readers of data associated with minus suffixes.
  typedef async_backward_stream_reader<text_offset_type> minus_count_reader_type;
  typedef async_backward_stream_reader<std::uint16_t> minus_pos_reader_type;
  minus_count_reader_type *minus_count_reader = new minus_count_reader_type(minus_count_filename);
  minus_pos_reader_type *minus_pos_reader = new minus_pos_reader_type(minus_pos_filename);
//  fprintf(stderr, "  size(minus_pos_filename) = %lu\n", utils::file_size(minus_pos_filename));

  // Initialize readers of data associated with plus suffixes.
//  fprintf(stderr, "  initialize readers 2\n");
  typedef async_multi_bit_stream_reader plus_type_reader_type;
  typedef async_multi_stream_reader<text_offset_type> plus_pos_reader_type;
  plus_type_reader_type *plus_type_reader = new plus_type_reader_type(n_blocks);
  plus_pos_reader_type *plus_pos_reader = new plus_pos_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    plus_type_reader->add_file(plus_type_filenames[block_id]);
    plus_pos_reader->add_file(plus_pos_filenames[block_id]);
  }

  // Initialize the readers of data associated with both types of suffixes.
//  fprintf(stderr, "  initialize readers 3\n");
  typedef async_multi_stream_reader<char_type> symbols_reader_type;
  symbols_reader_type *symbols_reader = new symbols_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
    symbols_reader->add_file(symbols_filenames[block_id]);

  // Initialize output writers.
//  fprintf(stderr, "  initialize writers\n");
  typedef async_stream_writer<text_offset_type> output_pos_writer_type;
  typedef async_bit_stream_writer output_type_writer_type;
  typedef async_stream_writer<text_offset_type> output_count_writer_type;
  output_pos_writer_type *output_pos_writer = new output_pos_writer_type(output_pos_filename);
  output_type_writer_type *output_type_writer = new output_type_writer_type(output_type_filename);
  output_count_writer_type *output_count_writer = new output_count_writer_type(output_count_filename);

  bool empty_output = true;
  std::uint64_t max_char = std::numeric_limits<char_type>::max();
  std::uint64_t head_char = 0;
  {
    std::uint64_t size = utils::file_size(minus_count_filename);
    if (size > 0)
      head_char = size / sizeof(text_offset_type) - 1;
  }
  std::uint64_t prev_written_head_char = 0;
  std::uint64_t cur_bucket_size = 0;
  std::vector<std::uint64_t> block_count(n_blocks, 0UL);
//  fprintf(stderr, "  about to start, head_char = %lu\n", head_char);

  // Induce plus suffixes.
  while (!radix_heap->empty() || !minus_count_reader->empty()) {
    // Process plus suffixes.
    while (!radix_heap->empty() && radix_heap->min_compare(max_char - head_char)) {
      std::pair<char_type, block_id_type> p = radix_heap->extract_min();
      std::uint64_t head_pos_block_id = p.second;
      std::uint64_t head_pos_block_beg = head_pos_block_id * max_block_size;
      std::uint64_t head_pos = head_pos_block_beg + plus_pos_reader->read_from_ith_file(head_pos_block_id);
      output_pos_writer->write(head_pos);

      bool is_head_pos_star = plus_type_reader->read_from_ith_file(head_pos_block_id);
      output_type_writer->write(is_head_pos_star);

      if (!empty_output) {
        if (head_char == prev_written_head_char) ++cur_bucket_size;
        else {
          output_count_writer->write(cur_bucket_size);
          for (std::uint64_t ch = prev_written_head_char; ch > head_char + 1; --ch)
            output_count_writer->write(0);
          cur_bucket_size = 1;
          prev_written_head_char = head_char;
        }
      } else {
        cur_bucket_size = 1;
        prev_written_head_char = head_char;
      }

      empty_output = false;
      if (head_pos > 0 && !is_head_pos_star) {
        std::uint64_t prev_pos_char = symbols_reader->read_from_ith_file(head_pos_block_id);
        std::uint64_t prev_pos_block_id = (head_pos_block_id * max_block_size == head_pos) ? head_pos_block_id - 1 : head_pos_block_id;
        radix_heap->push(max_char - prev_pos_char, prev_pos_block_id);
      }
    }

    // Process minus suffixes.
    std::uint64_t minus_sufs_count = minus_count_reader->read();
//    fprintf(stderr, "  minus_sufs_count = %lu\n", minus_sufs_count);
    for (std::uint64_t i = 0; i < minus_sufs_count; ++i) {
      std::uint64_t head_pos_block_id = minus_pos_reader->read();
      ++block_count[head_pos_block_id];
      bool pos_starts_at_block_beg = (block_count[head_pos_block_id] == block_count_target[head_pos_block_id]);
      std::uint64_t prev_pos_block_id = head_pos_block_id - pos_starts_at_block_beg;
      std::uint64_t prev_pos_char = symbols_reader->read_from_ith_file(head_pos_block_id);
      radix_heap->push(max_char - prev_pos_char, prev_pos_block_id);
    }

    // Update current symbol.
    --head_char;
  }

  if (empty_output == false) {
    output_count_writer->write(cur_bucket_size);
    for (std::uint64_t ch = prev_written_head_char; ch > 0; --ch)
      output_count_writer->write(0);
  }

  // Update I/O volume.
  std::uint64_t io_volume = radix_heap->io_volume() +
    minus_pos_reader->bytes_read() + minus_count_reader->bytes_read() +
    plus_type_reader->bytes_read() + plus_pos_reader->bytes_read() +
    symbols_reader->bytes_read() + output_pos_writer->bytes_written() +
    output_type_writer->bytes_written() + output_count_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete radix_heap;
  delete minus_pos_reader;
  delete minus_count_reader;
  delete plus_type_reader;
  delete plus_pos_reader;
  delete symbols_reader;
  delete output_pos_writer;
  delete output_type_writer;
  delete output_count_writer;
}

#endif  // __EM_INDUCE_PLUS_SUFFIXES_HPP_INCLUDED
