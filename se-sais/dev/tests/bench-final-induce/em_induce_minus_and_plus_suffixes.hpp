#ifndef __EM_INDUCE_MINUS_AND_PLUS_SUFFIXES_HPP_INCLUDED
#define __EM_INDUCE_MINUS_AND_PLUS_SUFFIXES_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "im_induce_suffixes.hpp"
#include "em_induce_plus_suffixes.hpp"

#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_reader.hpp"
#include "io/async_vbyte_stream_reader.hpp"
#include "io/async_multi_bit_stream_reader.hpp"
#include "io/async_multi_stream_reader.hpp"
#include "io/async_multi_stream_writer.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_backward_bit_stream_reader.hpp"

#include "em_radix_heap.hpp"
#include "utils.hpp"
#include "uint24.hpp"
#include "uint40.hpp"
#include "uint48.hpp"


template<typename char_type,
  typename text_offset_type,
  typename block_id_type>
void em_induce_minus_and_plus_suffixes(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    char_type last_text_symbol,
    std::string output_filename,
    std::string plus_pos_filename,
    std::string plus_type_filename,
    std::string plus_count_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;

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

  // Start the timer.
  long double start = utils::wclock();
  fprintf(stderr, "  EM induce minus and plus suffixes: ");

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
  typedef async_backward_stream_reader<text_offset_type> plus_pos_reader_type;
  typedef async_backward_bit_stream_reader plus_type_reader_type;
  typedef async_backward_stream_reader<text_offset_type> plus_count_reader_type;
  plus_pos_reader_type *plus_pos_reader = new plus_pos_reader_type(plus_pos_filename);
  plus_type_reader_type *plus_type_reader = new plus_type_reader_type(plus_type_filename);
  plus_count_reader_type *plus_count_reader = new plus_count_reader_type(plus_count_filename);

  // Initialize readers and writers for minus suffixes.
  typedef async_multi_stream_reader<text_offset_type> minus_pos_reader_type;
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
  std::uint64_t cur_symbol = 0;
  while (!plus_count_reader->empty() || !radix_heap->empty()) {
    // Process minus suffixes.
    while (!radix_heap->empty() && radix_heap->min_compare(cur_symbol)) {
      std::pair<char_type, block_id_type> p = radix_heap->extract_min();
      std::uint64_t head_pos_block_id = p.second;
      std::uint64_t head_pos_block_beg = head_pos_block_id * max_block_size;
      std::uint64_t head_pos = head_pos_block_beg + minus_pos_reader->read_from_ith_file(head_pos_block_id);
      bool is_head_pos_star = minus_type_reader->read_from_ith_file(head_pos_block_id);
      output_writer->write(head_pos);

      if (head_pos > 0 && !is_head_pos_star) {
        std::uint64_t prev_pos_char = symbols_reader->read_from_ith_file(head_pos_block_id);
        std::uint64_t prev_pos_block_id = (head_pos_block_id * max_block_size == head_pos) ? head_pos_block_id - 1 : head_pos_block_id;
        radix_heap->push(prev_pos_char, prev_pos_block_id);
      }
    }

    // Process plus suffixes.
    if (!plus_count_reader->empty()) {
      std::uint64_t plus_suf_count = plus_count_reader->read();
      for (std::uint64_t i = 0; i < plus_suf_count; ++i) {
        std::uint64_t head_pos = plus_pos_reader->read();
        std::uint64_t head_pos_uint64 = head_pos;
        output_writer->write(head_pos);
        if (plus_type_reader->read()) {
          std::uint64_t head_pos_block_id = head_pos_uint64 / max_block_size;
          bool is_head_pos_at_block_boundary = (head_pos_block_id * max_block_size == head_pos);
          std::uint64_t prev_pos_block_id = head_pos_block_id - is_head_pos_at_block_boundary;
          std::uint64_t prev_pos_char = symbols_reader->read_from_ith_file(head_pos_block_id);
          radix_heap->push(prev_pos_char, prev_pos_block_id);
        }
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

  // Print summary.
  long double total_time = utils::wclock() - start;
  fprintf(stderr, "time = %.2Lfs, I/O = %.2LfMiB/s, total I/O vol = %.1Lfn bytes\n", total_time,
      (1.L * io_volume / (1L << 20)) / total_time, (1.L * total_io_volume) / text_length);
}

template<typename char_type,
  typename text_offset_type,
  typename block_id_type>
void induce_minus_and_plus_suffixes(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::vector<std::uint64_t> &next_block_leftmost_minus_star_plus_rank,
    std::string text_filename,
    std::string minus_pos_filename,
    std::string minus_count_filename,
    std::string output_filename,
    std::vector<std::string> &init_minus_pos_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;

  fprintf(stderr, "EM induce minus and plus suffixes:\n");
  fprintf(stderr, "  sizeof(char_type) = %lu\n", sizeof(char_type));
  fprintf(stderr, "  sizeof(text_offset_type) = %lu\n", sizeof(text_offset_type));
  fprintf(stderr, "  sizeof(block_id_type) = %lu\n", sizeof(block_id_type));

  char_type last_text_symbol;
  utils::read_at_offset(&last_text_symbol, text_length - 1, 1, text_filename);

  std::vector<std::string> plus_type_filenames(n_blocks);
  std::vector<std::string> minus_type_filenames(n_blocks);
  std::vector<std::string> plus_symbols_filenames(n_blocks);
  std::vector<std::string> minus_symbols_filenames(n_blocks);
  std::vector<std::string> plus_pos_filenames(n_blocks);
  std::vector<std::string> minus_pos_filenames(n_blocks);
  std::vector<std::uint64_t> block_count_target(n_blocks, std::numeric_limits<std::uint64_t>::max());
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    plus_pos_filenames[block_id] = output_filename + "tmp." + utils::random_string_hash();
    plus_symbols_filenames[block_id] = output_filename + "tmp." + utils::random_string_hash();
    plus_type_filenames[block_id] = output_filename + "tmp." + utils::random_string_hash();
    minus_pos_filenames[block_id] = output_filename + "tmp." + utils::random_string_hash();
    minus_type_filenames[block_id] = output_filename + "tmp." + utils::random_string_hash();
    minus_symbols_filenames[block_id] = output_filename + "tmp." + utils::random_string_hash();
  }

  im_induce_suffixes_small_alphabet<
    char_type,
    text_offset_type>(
        text_alphabet_size,
        text_length,
        max_block_size,
        next_block_leftmost_minus_star_plus_rank,
        text_filename,
        init_minus_pos_filenames,
        plus_pos_filenames,
        plus_symbols_filenames,
        plus_type_filenames,
        minus_pos_filenames,
        minus_type_filenames,
        minus_symbols_filenames,
        block_count_target,
        total_io_volume);

  std::string plus_type_filename = output_filename + "tmp." + utils::random_string_hash();
  std::string plus_count_filename = output_filename + "tmp." + utils::random_string_hash();
  std::string plus_pos_filename = output_filename + "tmp." + utils::random_string_hash();

  em_induce_plus_suffixes<
    char_type,
    text_offset_type,
    block_id_type>(
        text_alphabet_size,
        text_length,
        max_block_size,
        ram_use,
        block_count_target,
        plus_pos_filename,
        plus_type_filename,
        plus_count_filename,
        minus_pos_filename,
        minus_count_filename,
        plus_type_filenames,
        plus_pos_filenames,
        plus_symbols_filenames,
        total_io_volume);

  utils::file_delete(minus_pos_filename);
  utils::file_delete(minus_count_filename);
  for (std::uint64_t i = 0; i < n_blocks; ++i) {
    if (utils::file_exists(plus_symbols_filenames[i])) utils::file_delete(plus_symbols_filenames[i]);
    if (utils::file_exists(plus_type_filenames[i])) utils::file_delete(plus_type_filenames[i]);
    if (utils::file_exists(plus_pos_filenames[i])) utils::file_delete(plus_pos_filenames[i]);
  }

  em_induce_minus_and_plus_suffixes<
    char_type,
    text_offset_type,
    block_id_type>(
        text_alphabet_size,
        text_length,
        max_block_size,
        ram_use,
        last_text_symbol,
        output_filename,
        plus_pos_filename,
        plus_type_filename,
        plus_count_filename,
        minus_type_filenames,
        minus_pos_filenames,
        minus_symbols_filenames,
        total_io_volume);

  utils::file_delete(plus_pos_filename);
  utils::file_delete(plus_type_filename);
  utils::file_delete(plus_count_filename);
  for (std::uint64_t j = 0; j < n_blocks; ++j) {
    if (utils::file_exists(minus_symbols_filenames[j])) utils::file_delete(minus_symbols_filenames[j]);
    if (utils::file_exists(minus_type_filenames[j])) utils::file_delete(minus_type_filenames[j]);
    if (utils::file_exists(minus_pos_filenames[j])) utils::file_delete(minus_pos_filenames[j]);
  }
}

template<typename char_type,
  typename text_offset_type>
void induce_minus_and_plus_suffixes(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::vector<std::uint64_t> &next_block_leftmost_minus_star_plus_rank,
    std::string text_filename,
    std::string minus_pos_filename,
    std::string minus_count_filename,
    std::string output_filename,
    std::vector<std::string> &init_minus_pos_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  if (n_blocks < (1UL << 8)) {
    induce_minus_and_plus_suffixes<char_type, text_offset_type, std::uint8_t>(text_alphabet_size, text_length,
        max_block_size, ram_use, next_block_leftmost_minus_star_plus_rank, text_filename, minus_pos_filename,
        minus_count_filename, output_filename, init_minus_pos_filenames, total_io_volume);
  } else if (n_blocks < (1UL << 16)) {
    induce_minus_and_plus_suffixes<char_type, text_offset_type, std::uint16_t>(text_alphabet_size, text_length,
        max_block_size, ram_use, next_block_leftmost_minus_star_plus_rank, text_filename, minus_pos_filename,
        minus_count_filename, output_filename, init_minus_pos_filenames, total_io_volume);
  } else if (n_blocks < (1UL << 24)) {
    induce_minus_and_plus_suffixes<char_type, text_offset_type, uint24>(text_alphabet_size, text_length,
        max_block_size, ram_use, next_block_leftmost_minus_star_plus_rank, text_filename, minus_pos_filename,
        minus_count_filename, output_filename, init_minus_pos_filenames, total_io_volume);
  } else {
    induce_minus_and_plus_suffixes<char_type, text_offset_type, std::uint64_t>(text_alphabet_size, text_length,
        max_block_size, ram_use, next_block_leftmost_minus_star_plus_rank, text_filename, minus_pos_filename,
        minus_count_filename, output_filename, init_minus_pos_filenames, total_io_volume);
  }
}

template<typename char_type,
  typename text_offset_type,
  typename block_id_type>
void em_induce_minus_and_plus_suffixes(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    char_type last_text_symbol,
    std::string tempfile_basename,
    std::string plus_pos_filename,
    std::string plus_type_filename,
    std::string plus_count_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::vector<std::uint64_t> &block_count,
    std::string input_lex_sorted_suffixes_block_ids_filename,
    std::vector<std::string> &input_lex_sorted_suffixes_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;

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

  // Start the timer.
  long double start = utils::wclock();
  fprintf(stderr, "  EM induce minus and plus suffixes: ");

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
  radix_heap_type *radix_heap = new radix_heap_type(radix_logs, tempfile_basename, ram_use);

  // Initialize the readers for plus suffixes.
  typedef async_backward_stream_reader<text_offset_type> plus_pos_reader_type;
  typedef async_backward_bit_stream_reader plus_type_reader_type;
  typedef async_backward_stream_reader<text_offset_type> plus_count_reader_type;
  plus_pos_reader_type *plus_pos_reader = new plus_pos_reader_type(plus_pos_filename);
  plus_type_reader_type *plus_type_reader = new plus_type_reader_type(plus_type_filename);
  plus_count_reader_type *plus_count_reader = new plus_count_reader_type(plus_count_filename);

  // Initialize readers and writers for minus suffixes.
  typedef async_multi_stream_reader<text_offset_type> minus_pos_reader_type;
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

  // Initialize output writers.
  typedef async_multi_stream_writer<text_offset_type> pos_writer_type;
  pos_writer_type *pos_writer = new pos_writer_type();
  for (std::uint64_t i = 0; i < block_count.size(); ++i)
    pos_writer->add_file(input_lex_sorted_suffixes_filenames[i]);
  typedef async_stream_writer<std::uint16_t> block_id_writer_type;
  block_id_writer_type *block_id_writer = new block_id_writer_type(input_lex_sorted_suffixes_block_ids_filename);

  // Induce minus suffixes.
  radix_heap->push(last_text_symbol, (text_length - 1) / max_block_size);
  std::uint64_t cur_symbol = 0;
  while (!plus_count_reader->empty() || !radix_heap->empty()) {
    // Process minus suffixes.
    while (!radix_heap->empty() && radix_heap->min_compare(cur_symbol)) {
      std::pair<char_type, block_id_type> p = radix_heap->extract_min();
      std::uint64_t head_pos_block_id = p.second;
      std::uint64_t head_pos_block_beg = head_pos_block_id * max_block_size;
      std::uint64_t head_pos = head_pos_block_beg + minus_pos_reader->read_from_ith_file(head_pos_block_id);
      bool is_head_pos_star = minus_type_reader->read_from_ith_file(head_pos_block_id);

      // XXX add lookup table.
      {
        std::uint64_t output_block_id = 0;
        std::uint64_t prev_blocks_sum = 0;
        while (output_block_id < block_count.size() && prev_blocks_sum + block_count[output_block_id] <= head_pos)
          prev_blocks_sum += block_count[output_block_id++];
        std::uint64_t block_offset = head_pos - prev_blocks_sum;
        block_id_writer->write(output_block_id);
        pos_writer->write_to_ith_file(output_block_id, block_offset);
      }

      if (head_pos > 0 && !is_head_pos_star) {
        std::uint64_t prev_pos_char = symbols_reader->read_from_ith_file(head_pos_block_id);
        std::uint64_t prev_pos_block_id = (head_pos_block_id * max_block_size == head_pos) ? head_pos_block_id - 1 : head_pos_block_id;
        radix_heap->push(prev_pos_char, prev_pos_block_id);
      }
    }

    // Process plus suffixes.
    if (!plus_count_reader->empty()) {
      std::uint64_t plus_suf_count = plus_count_reader->read();
      for (std::uint64_t i = 0; i < plus_suf_count; ++i) {
        std::uint64_t head_pos = plus_pos_reader->read();
        std::uint64_t head_pos_uint64 = head_pos;

        // XXX add lookup table.
        {
          std::uint64_t output_block_id = 0;
          std::uint64_t prev_blocks_sum = 0;
          while (output_block_id < block_count.size() && prev_blocks_sum + block_count[output_block_id] <= head_pos)
            prev_blocks_sum += block_count[output_block_id++];
          std::uint64_t block_offset = head_pos - prev_blocks_sum;
          block_id_writer->write(output_block_id);
          pos_writer->write_to_ith_file(output_block_id, block_offset);
        }

        if (plus_type_reader->read()) {
          std::uint64_t head_pos_block_id = head_pos_uint64 / max_block_size;
          bool is_head_pos_at_block_boundary = (head_pos_block_id * max_block_size == head_pos);
          std::uint64_t prev_pos_block_id = head_pos_block_id - is_head_pos_at_block_boundary;
          std::uint64_t prev_pos_char = symbols_reader->read_from_ith_file(head_pos_block_id);
          radix_heap->push(prev_pos_char, prev_pos_block_id);
        }
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
    block_id_writer->bytes_written() + pos_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete radix_heap;
  delete plus_pos_reader;
  delete plus_type_reader;
  delete plus_count_reader;
  delete minus_pos_reader;
  delete minus_type_reader;
  delete symbols_reader;
  delete block_id_writer;
  delete pos_writer;

  // Print summary.
  long double total_time = utils::wclock() - start;
  fprintf(stderr, "time = %.2Lfs, I/O = %.2LfMiB/s, total I/O vol = %.1Lfn bytes\n", total_time,
      (1.L * io_volume / (1L << 20)) / total_time, (1.L * total_io_volume) / text_length);
}


template<typename char_type,
  typename text_offset_type,
  typename block_id_type>
void induce_minus_and_plus_suffixes(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::vector<std::uint64_t> &next_block_leftmost_minus_star_plus_rank,
    std::string tempfile_basename,
    std::string text_filename,
    std::string minus_pos_filename,
    std::string minus_count_filename,
    std::vector<std::string> &init_minus_pos_filenames,
    std::vector<std::uint64_t> &block_count,
    std::string input_lex_sorted_suffixes_block_ids_filename,
    std::vector<std::string> &input_lex_sorted_suffixes_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;

  fprintf(stderr, "EM induce minus and plus suffixes:\n");
  fprintf(stderr, "  sizeof(char_type) = %lu\n", sizeof(char_type));
  fprintf(stderr, "  sizeof(text_offset_type) = %lu\n", sizeof(text_offset_type));
  fprintf(stderr, "  sizeof(block_id_type) = %lu\n", sizeof(block_id_type));

  char_type last_text_symbol;
  utils::read_at_offset(&last_text_symbol, text_length - 1, 1, text_filename);

  std::vector<std::string> plus_type_filenames(n_blocks);
  std::vector<std::string> minus_type_filenames(n_blocks);
  std::vector<std::string> plus_symbols_filenames(n_blocks);
  std::vector<std::string> minus_symbols_filenames(n_blocks);
  std::vector<std::string> plus_pos_filenames(n_blocks);
  std::vector<std::string> minus_pos_filenames(n_blocks);
  std::vector<std::uint64_t> block_count_target(n_blocks, std::numeric_limits<std::uint64_t>::max());
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    plus_pos_filenames[block_id] = tempfile_basename + "tmp." + utils::random_string_hash();
    plus_symbols_filenames[block_id] = tempfile_basename + "tmp." + utils::random_string_hash();
    plus_type_filenames[block_id] = tempfile_basename + "tmp." + utils::random_string_hash();
    minus_pos_filenames[block_id] = tempfile_basename + "tmp." + utils::random_string_hash();
    minus_type_filenames[block_id] = tempfile_basename + "tmp." + utils::random_string_hash();
    minus_symbols_filenames[block_id] = tempfile_basename + "tmp." + utils::random_string_hash();
  }

  im_induce_suffixes_small_alphabet<
    char_type,
    text_offset_type>(
        text_alphabet_size,
        text_length,
        max_block_size,
        next_block_leftmost_minus_star_plus_rank,
        text_filename,
        init_minus_pos_filenames,
        plus_pos_filenames,
        plus_symbols_filenames,
        plus_type_filenames,
        minus_pos_filenames,
        minus_type_filenames,
        minus_symbols_filenames,
        block_count_target,
        total_io_volume);

  utils::file_delete(text_filename);

  std::string plus_type_filename = tempfile_basename + "tmp." + utils::random_string_hash();
  std::string plus_count_filename = tempfile_basename + "tmp." + utils::random_string_hash();
  std::string plus_pos_filename = tempfile_basename + "tmp." + utils::random_string_hash();

  em_induce_plus_suffixes<
    char_type,
    text_offset_type,
    block_id_type>(
        text_alphabet_size,
        text_length,
        max_block_size,
        ram_use,
        block_count_target,
        plus_pos_filename,
        plus_type_filename,
        plus_count_filename,
        minus_pos_filename,
        minus_count_filename,
        plus_type_filenames,
        plus_pos_filenames,
        plus_symbols_filenames,
        total_io_volume);

  utils::file_delete(minus_pos_filename);
  utils::file_delete(minus_count_filename);
  for (std::uint64_t i = 0; i < n_blocks; ++i) {
    if (utils::file_exists(plus_symbols_filenames[i])) utils::file_delete(plus_symbols_filenames[i]);
    if (utils::file_exists(plus_type_filenames[i])) utils::file_delete(plus_type_filenames[i]);
    if (utils::file_exists(plus_pos_filenames[i])) utils::file_delete(plus_pos_filenames[i]);
  }

  em_induce_minus_and_plus_suffixes<
    char_type,
    text_offset_type,
    block_id_type>(
        text_alphabet_size,
        text_length,
        max_block_size,
        ram_use,
        last_text_symbol,
        tempfile_basename,
        plus_pos_filename,
        plus_type_filename,
        plus_count_filename,
        minus_type_filenames,
        minus_pos_filenames,
        minus_symbols_filenames,
        block_count,
        input_lex_sorted_suffixes_block_ids_filename,
        input_lex_sorted_suffixes_filenames,
        total_io_volume);

  utils::file_delete(plus_pos_filename);
  utils::file_delete(plus_type_filename);
  utils::file_delete(plus_count_filename);
  for (std::uint64_t j = 0; j < n_blocks; ++j) {
    if (utils::file_exists(minus_symbols_filenames[j])) utils::file_delete(minus_symbols_filenames[j]);
    if (utils::file_exists(minus_type_filenames[j])) utils::file_delete(minus_type_filenames[j]);
    if (utils::file_exists(minus_pos_filenames[j])) utils::file_delete(minus_pos_filenames[j]);
  }
}

template<typename char_type,
  typename text_offset_type>
void induce_minus_and_plus_suffixes(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::vector<std::uint64_t> &next_block_leftmost_minus_star_plus_rank,
    std::string tempfile_basename,
    std::string text_filename,
    std::string minus_pos_filename,
    std::string minus_count_filename,
    std::vector<std::string> &init_minus_pos_filenames,
    std::vector<std::uint64_t> &block_count,
    std::string input_lex_sorted_suffixes_block_ids_filename,
    std::vector<std::string> &input_lex_sorted_suffixes_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  if (n_blocks < (1UL << 8)) {
    induce_minus_and_plus_suffixes<char_type, text_offset_type, std::uint8_t>(text_alphabet_size, text_length,
        max_block_size, ram_use, next_block_leftmost_minus_star_plus_rank, tempfile_basename, text_filename, minus_pos_filename,
        minus_count_filename, init_minus_pos_filenames, block_count, input_lex_sorted_suffixes_block_ids_filename,
        input_lex_sorted_suffixes_filenames, total_io_volume);
  } else if (n_blocks < (1UL << 16)) {
    induce_minus_and_plus_suffixes<char_type, text_offset_type, std::uint16_t>(text_alphabet_size, text_length,
        max_block_size, ram_use, next_block_leftmost_minus_star_plus_rank, tempfile_basename, text_filename, minus_pos_filename,
        minus_count_filename, init_minus_pos_filenames, block_count, input_lex_sorted_suffixes_block_ids_filename,
        input_lex_sorted_suffixes_filenames, total_io_volume);
  } else if (n_blocks < (1UL << 24)) {
    induce_minus_and_plus_suffixes<char_type, text_offset_type, uint24>(text_alphabet_size, text_length,
        max_block_size, ram_use, next_block_leftmost_minus_star_plus_rank, tempfile_basename, text_filename, minus_pos_filename,
        minus_count_filename, init_minus_pos_filenames, block_count, input_lex_sorted_suffixes_block_ids_filename,
        input_lex_sorted_suffixes_filenames, total_io_volume);
  } else {
    induce_minus_and_plus_suffixes<char_type, text_offset_type, std::uint64_t>(text_alphabet_size, text_length,
        max_block_size, ram_use, next_block_leftmost_minus_star_plus_rank, tempfile_basename, text_filename, minus_pos_filename,
        minus_count_filename, init_minus_pos_filenames, block_count, input_lex_sorted_suffixes_block_ids_filename,
        input_lex_sorted_suffixes_filenames, total_io_volume);
  }
}


#endif  // __EM_INDUCE_MINUS_AND_PLUS_SUFFIXES_HPP_INCLUDED
