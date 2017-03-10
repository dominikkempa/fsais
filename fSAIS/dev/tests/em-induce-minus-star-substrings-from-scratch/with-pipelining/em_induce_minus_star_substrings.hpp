#ifndef __EM_INDUCE_MINUS_STAR_SUBSTRINGS_HPP_INCLUDED
#define __EM_INDUCE_MINUS_STAR_SUBSTRINGS_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "packed_pair.hpp"
#include "em_radix_heap.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_reader.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_backward_bit_stream_reader.hpp"
#include "io/async_multi_stream_writer.hpp"

#include "im_induce_substrings.hpp"
#include "em_induce_plus_star_substrings.hpp"

#include "uint24.hpp"
#include "uint40.hpp"
#include "uint48.hpp"
#include "utils.hpp"


// TODO: split the ram_use into radix heap and readers
// Right now radix_heap uses all the ram, and I/O readers
// (non-contant number of them!) use the space beyond that.
template<typename char_type,
  typename text_offset_type,
  typename block_offset_type,
  typename block_id_type,
  typename ext_block_id_type>
std::uint64_t
em_induce_minus_star_substrings_large_alphabet(
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::uint64_t max_permute_block_size,
    char_type last_text_symbol,
    std::vector<std::uint64_t> &block_count_target,
    std::string plus_pos_filename,
    std::string plus_count_filename,
    std::string plus_diff_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::string tempfile_basename,
    std::string output_count_filename,
    std::vector<std::string> &output_pos_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  std::uint64_t is_tail_minus_bit = ((std::uint64_t)std::numeric_limits<ext_block_id_type>::max() + 1) / 2;
  std::uint64_t io_volume = 0;

  if (max_block_size == 0) {
    fprintf(stderr, "\nError: max_block_size = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (text_length == 0) {
    fprintf(stderr, "\nError: text_length = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (n_blocks == 0) {
    fprintf(stderr, "\nError: n_blocks = 0\n");
    std::exit(EXIT_FAILURE);
  }

  // Check that all types are sufficiently large.
  if ((std::uint64_t)std::numeric_limits<block_offset_type>::max() < max_block_size - 1) {
    fprintf(stderr, "\nError: block_offset_type in em_induce_minus_star_substrings_large_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<text_offset_type>::max() < text_length - 1) {
    fprintf(stderr, "\nError: text_offset_type in em_induce_minus_star_substrings_large_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<block_id_type>::max() < n_blocks - 1) {
    fprintf(stderr, "\nError: block_id_type in em_induce_minus_star_substrings_large_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<ext_block_id_type>::max() < n_blocks / 2UL) {
    fprintf(stderr, "\nError: ext_block_id_type in em_induce_minus_star_substrings_large_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }

  // Start the timer.
  long double start = utils::wclock();
  fprintf(stderr, "  EM induce minus substrings (large alphabet):\n");
  fprintf(stderr, "    sizeof(ext_block_id_type) = %lu\n", sizeof(ext_block_id_type));

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
  typedef packed_pair<ext_block_id_type, text_offset_type> ext_pair_type;
  typedef em_radix_heap<char_type, ext_pair_type> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_logs, tempfile_basename, ram_use);

  // Initialize the readers of data associated with plus suffixes.
  typedef async_backward_stream_reader<block_id_type> plus_pos_reader_type;
  typedef async_backward_stream_reader<text_offset_type> plus_count_reader_type;
  typedef async_backward_bit_stream_reader plus_diff_reader_type;
  plus_pos_reader_type *plus_pos_reader = new plus_pos_reader_type(plus_pos_filename);
  plus_count_reader_type *plus_count_reader = new plus_count_reader_type(plus_count_filename);
  plus_diff_reader_type *plus_diff_reader = new plus_diff_reader_type(plus_diff_filename);

  // Initialize readers of data associated with minus suffixes.
  typedef async_multi_bit_stream_reader minus_type_reader_type;
  typedef async_multi_stream_reader<block_offset_type> minus_pos_reader_type;
  minus_type_reader_type *minus_type_reader = new minus_type_reader_type(n_blocks);
  minus_pos_reader_type *minus_pos_reader = new minus_pos_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    minus_type_reader->add_file(minus_type_filenames[block_id]);
    minus_pos_reader->add_file(minus_pos_filenames[block_id]);
  }

  // Initialize the reading of data associated with both types of suffixes.
  typedef async_multi_stream_reader<char_type> symbols_reader_type;
  symbols_reader_type *symbols_reader = new symbols_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
    symbols_reader->add_file(symbols_filenames[block_id]);

  // Initialize the output writers.
  std::uint64_t n_permute_blocks = (text_length + max_permute_block_size - 1) / max_permute_block_size;
  typedef async_multi_stream_writer<text_offset_type> output_pos_writer_type;
  output_pos_writer_type *output_pos_writer = new output_pos_writer_type(n_permute_blocks);
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
    output_pos_writer->add_file(output_pos_filenames[permute_block_id]);
  typedef async_stream_writer<text_offset_type> output_count_writer_type;
  output_count_writer_type *output_count_writer = new output_count_writer_type(output_count_filename);

  // Induce minus substrings.
  bool empty_output = true;
  bool was_extract_min = false;
  bool is_prev_tail_minus = 0;
  bool is_prev_tail_name_defined = 0;
  bool was_prev_plus_name = false;
  std::uint64_t prev_head_char = 0;
  std::uint64_t prev_written_head_char = 0;
  std::uint64_t prev_tail_name = 0;
  std::uint64_t diff_str = 0;
  std::uint64_t cur_symbol = 0;
  std::uint64_t diff_str_snapshot = 0;
  std::uint64_t diff_items_written = 0;
  std::uint64_t cur_plus_name = 0;
  std::uint64_t cur_bucket_size = 0;
  std::vector<std::uint64_t> block_count(n_blocks, 0UL);

  while (cur_symbol <= (std::uint64_t)last_text_symbol || !plus_count_reader->empty() || !radix_heap->empty()) {
    // Process minus substrings.
    if (cur_symbol == (std::uint64_t)last_text_symbol) {
      std::uint64_t head_char = cur_symbol;
      std::uint64_t block_id = (text_length - 1) / max_block_size;
      std::uint64_t tail_name = 0;
      bool is_tail_minus = true;

      // Update diff_str.
      if (was_extract_min == true) {
        if (!is_prev_tail_name_defined || is_prev_tail_minus != is_tail_minus ||
            prev_head_char != head_char ||  prev_tail_name != tail_name)
          ++diff_str;
      } else ++diff_str;
      was_extract_min = true;

      ++block_count[block_id];
      bool head_pos_at_block_beg = (block_count[block_id] == block_count_target[block_id]);

      // Watch for the order of minus-substrings with the same name!!!
      bool is_star = minus_type_reader->read_from_ith_file(block_id);
      if (block_id > 0 || head_pos_at_block_beg == false) {
        if (!is_star) {
          char_type prev_char = symbols_reader->read_from_ith_file(block_id);
          std::uint64_t prev_pos_block_id = block_id - head_pos_at_block_beg;
          radix_heap->push(prev_char, ext_pair_type(prev_pos_block_id | is_tail_minus_bit, diff_str - 1));
        } else {
          if (!empty_output) {
            if (diff_str_snapshot != diff_str)
              ++diff_items_written;
          } else ++diff_items_written;

          if (empty_output == false) {
            if (head_char == prev_written_head_char) ++cur_bucket_size;
            else {
              output_count_writer->write(cur_bucket_size);
              for (std::uint64_t ch = prev_written_head_char + 1; ch < head_char; ++ch)
                output_count_writer->write(0);
              cur_bucket_size = 1;
              prev_written_head_char = head_char;
            }
          } else {
            for (std::uint64_t ch = 0; ch < head_char; ++ch)
              output_count_writer->write(0);
            cur_bucket_size = 1;
            prev_written_head_char = head_char;
          }

          empty_output = false;
          std::uint64_t head_pos = block_id * max_block_size + minus_pos_reader->read_from_ith_file(block_id);
          std::uint64_t permute_block_id = head_pos / max_permute_block_size;
          output_pos_writer->write_to_ith_file(permute_block_id, head_pos);
          output_pos_writer->write_to_ith_file(permute_block_id, diff_items_written - 1);
          diff_str_snapshot = diff_str;
        }
      }

      prev_head_char = head_char;
      prev_tail_name = tail_name;
      is_prev_tail_minus = is_tail_minus;
      is_prev_tail_name_defined = false;
    }
    while (!radix_heap->empty() && radix_heap->min_compare(cur_symbol)) {
      std::pair<char_type, ext_pair_type> p = radix_heap->extract_min();
      std::uint64_t head_char = cur_symbol;
      std::uint64_t block_id = p.second.first;
      std::uint64_t tail_name = p.second.second;

      // Unpack the flag from block_id.
      bool is_tail_minus = false;
      if (block_id & is_tail_minus_bit) {
        block_id -= is_tail_minus_bit;
        is_tail_minus = true;
      }

      // Update diff_str.
      if (was_extract_min == true) {
        if (!is_prev_tail_name_defined || is_prev_tail_minus != is_tail_minus ||
            prev_head_char != head_char ||  prev_tail_name != tail_name)
          ++diff_str;
      } else ++diff_str;
      was_extract_min = true;

      ++block_count[block_id];
      bool head_pos_at_block_beg = (block_count[block_id] == block_count_target[block_id]);

      // Watch for the order of minus-substrings with the same name!!!
      bool is_star = minus_type_reader->read_from_ith_file(block_id);
      if (block_id > 0 || head_pos_at_block_beg == false) {
        if (!is_star) {
          char_type prev_char = symbols_reader->read_from_ith_file(block_id);
          std::uint64_t prev_pos_block_id = block_id - head_pos_at_block_beg;
          radix_heap->push(prev_char, ext_pair_type(prev_pos_block_id | is_tail_minus_bit, diff_str - 1));
        } else {
          bool is_next_diff = false;
          if (!empty_output) {
            if (diff_str_snapshot != diff_str)
              is_next_diff = true;
          } else is_next_diff = true;
          diff_items_written += is_next_diff;

          if (empty_output == false) {
            if (head_char == prev_written_head_char) ++cur_bucket_size;
            else {
              output_count_writer->write(cur_bucket_size);
              for (std::uint64_t ch = prev_written_head_char + 1; ch < head_char; ++ch)
                output_count_writer->write(0);
              cur_bucket_size = 1;
              prev_written_head_char = head_char;
            }
          } else {
            for (std::uint64_t ch = 0; ch < head_char; ++ch)
              output_count_writer->write(0);
            cur_bucket_size = 1;
            prev_written_head_char = head_char;
          }

          empty_output = false;
          std::uint64_t head_pos = block_id * max_block_size + minus_pos_reader->read_from_ith_file(block_id);
          std::uint64_t permute_block_id = head_pos / max_permute_block_size;
          output_pos_writer->write_to_ith_file(permute_block_id, head_pos);
          output_pos_writer->write_to_ith_file(permute_block_id, diff_items_written - 1);
          diff_str_snapshot = diff_str;
        }
      }

      prev_head_char = head_char;
      prev_tail_name = tail_name;
      is_prev_tail_minus = is_tail_minus;
      is_prev_tail_name_defined = true;
    }

    // Process plus substrings.
    std::uint64_t plus_substr_count = 0;
    if (!plus_count_reader->empty())
      plus_substr_count = plus_count_reader->read();
    for (std::uint64_t i = 0; i < plus_substr_count; ++i) {
      // Compute pos_block_id and prev_pos_block_id.
      std::uint64_t pos_block_id = plus_pos_reader->read();
      ++block_count[pos_block_id];
      bool head_pos_at_block_beg = (block_count[pos_block_id] == block_count_target[pos_block_id]);
      std::uint64_t prev_pos_block_id = pos_block_id - head_pos_at_block_beg;

      // Update current name, compute prev_char, and add item to the heap.
      if (was_prev_plus_name)
        cur_plus_name += plus_diff_reader->read();
      char_type prev_char = symbols_reader->read_from_ith_file(pos_block_id);
      was_prev_plus_name = true;
      radix_heap->push(prev_char, ext_pair_type(prev_pos_block_id, cur_plus_name));
    }

    // Update current char.
    ++cur_symbol;
  }

  if (cur_bucket_size > 0)
    output_count_writer->write(cur_bucket_size);

  // Update I/O volume.
  io_volume += radix_heap->io_volume() +
    plus_pos_reader->bytes_read() +  plus_count_reader->bytes_read() +
    plus_diff_reader->bytes_read() + minus_type_reader->bytes_read() +
    minus_pos_reader->bytes_read() + symbols_reader->bytes_read() +
    output_pos_writer->bytes_written() + output_count_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete radix_heap;
  delete plus_pos_reader;
  delete plus_count_reader;
  delete plus_diff_reader;
  delete minus_type_reader;
  delete minus_pos_reader;
  delete symbols_reader;
  delete output_pos_writer;
  delete output_count_writer;

  long double total_time = utils::wclock() - start;
  fprintf(stderr, "    time = %.2Lfs, I/O = %.2LfMiB/s, total I/O vol = %.1Lfn bytes\n", total_time,
      (1.L * io_volume / (1L << 20)) / total_time, (1.L * total_io_volume) / text_length);

  return diff_items_written;
}

template<typename char_type,
  typename text_offset_type,
  typename block_offset_type,
  typename block_id_type>
std::uint64_t
em_induce_minus_star_substrings_large_alphabet(
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::uint64_t max_permute_block_size,
    char_type last_text_symbol,
    std::vector<std::uint64_t> &block_count_target,
    std::string plus_pos_filename,
    std::string plus_count_filename,
    std::string plus_diff_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::string tempfile_basename,
    std::string output_count_filename,
    std::vector<std::string> &output_pos_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  if (n_blocks < (1UL << 7))
    return em_induce_minus_star_substrings_large_alphabet<char_type, text_offset_type, block_offset_type, block_id_type, std::uint8_t>
      (text_length, max_block_size, ram_use, max_permute_block_size, last_text_symbol, block_count_target, plus_pos_filename,
       plus_count_filename, plus_diff_filename, minus_type_filenames, minus_pos_filenames, symbols_filenames, tempfile_basename,
       output_count_filename, output_pos_filenames, total_io_volume);
  else if (n_blocks < (1UL << 15))
    return em_induce_minus_star_substrings_large_alphabet<char_type, text_offset_type, block_offset_type, block_id_type, std::uint16_t>
      (text_length, max_block_size, ram_use, max_permute_block_size, last_text_symbol, block_count_target, plus_pos_filename,
       plus_count_filename, plus_diff_filename, minus_type_filenames, minus_pos_filenames, symbols_filenames, tempfile_basename,
       output_count_filename, output_pos_filenames, total_io_volume);
  else
    return em_induce_minus_star_substrings_large_alphabet<char_type, text_offset_type, block_offset_type, block_id_type, std::uint64_t>
      (text_length, max_block_size, ram_use, max_permute_block_size, last_text_symbol, block_count_target, plus_pos_filename,
       plus_count_filename, plus_diff_filename, minus_type_filenames, minus_pos_filenames, symbols_filenames, tempfile_basename,
       output_count_filename, output_pos_filenames, total_io_volume);
}

// TODO: split the ram_use into radix heap and readers
// Right now radix_heap uses all the ram, and I/O readers
// (non-contant number of them!) use the space beyond that.
template<typename char_type,
  typename text_offset_type,
  typename block_offset_type,
  typename block_id_type,
  typename ext_block_id_type>
std::uint64_t
em_induce_minus_star_substrings_small_alphabet(
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t text_alphabet_size,
    std::uint64_t ram_use,
    std::uint64_t max_permute_block_size,
    char_type last_text_symbol,
    std::vector<std::uint64_t> &block_count_target,
    std::string plus_pos_filename,
    std::string plus_count_filename,
    std::string plus_diff_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::string tempfile_basename,
    std::string output_count_filename,
    std::vector<std::string> &output_pos_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  std::uint64_t msb_bit = ((std::uint64_t)std::numeric_limits<ext_block_id_type>::max() + 1) / 2;
  std::uint64_t io_volume = 0;

  if (max_block_size == 0) {
    fprintf(stderr, "\nError: max_block_size = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (text_length == 0) {
    fprintf(stderr, "\nError: text_length = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (n_blocks == 0) {
    fprintf(stderr, "\nError: n_blocks = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (text_alphabet_size == 0) {
    fprintf(stderr, "\nError: text_alphabet_size = 0\n");
    std::exit(EXIT_FAILURE);
  }


  // Check that all types are sufficiently large.
  if ((std::uint64_t)std::numeric_limits<char_type>::max() < text_alphabet_size - 1) {
    fprintf(stderr, "\nError: char_type in em_induce_minus_star_substrings_small_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<block_offset_type>::max() < max_block_size - 1) {
    fprintf(stderr, "\nError: block_offset_type in em_induce_minus_star_substrings_small_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<text_offset_type>::max() < text_length - 1) {
    fprintf(stderr, "\nError: text_offset_type in em_induce_minus_star_substrings_small_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<block_id_type>::max() < n_blocks - 1) {
    fprintf(stderr, "\nError: block_id_type in em_induce_minus_star_substrings_small_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<ext_block_id_type>::max() < n_blocks / 2UL) {
    fprintf(stderr, "\nError: ext_block_id_type in em_induce_minus_star_substrings_small_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }

  // Start the timer.
  long double start = utils::wclock();
  fprintf(stderr, "  EM induce minus substrings (small alphabet):\n");
  fprintf(stderr, "    sizeof(ext_block_id_type) = %lu\n", sizeof(ext_block_id_type));

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
  typedef em_radix_heap<char_type, ext_block_id_type> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_logs, tempfile_basename, ram_use);

  // Initialize the readers of data associated with plus suffixes.
  typedef async_backward_stream_reader<block_id_type> plus_pos_reader_type;
  typedef async_backward_stream_reader<text_offset_type> plus_count_reader_type;
  typedef async_backward_bit_stream_reader plus_diff_reader_type;
  plus_pos_reader_type *plus_pos_reader = new plus_pos_reader_type(plus_pos_filename);
  plus_count_reader_type *plus_count_reader = new plus_count_reader_type(plus_count_filename);
  plus_diff_reader_type *plus_diff_reader = new plus_diff_reader_type(plus_diff_filename);

  // Initialize readers of data associated with minus suffixes.
  typedef async_multi_bit_stream_reader minus_type_reader_type;
  typedef async_multi_stream_reader<block_offset_type> minus_pos_reader_type;
  minus_type_reader_type *minus_type_reader = new minus_type_reader_type(n_blocks);
  minus_pos_reader_type *minus_pos_reader = new minus_pos_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    minus_type_reader->add_file(minus_type_filenames[block_id]);
    minus_pos_reader->add_file(minus_pos_filenames[block_id]);
  }

  // Initialize readers of data associated with both types of suffixes.
  typedef async_multi_stream_reader<char_type> symbols_reader_type;
  symbols_reader_type *symbols_reader = new symbols_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
    symbols_reader->add_file(symbols_filenames[block_id]);

  // Initialize the output writers.
  std::uint64_t n_permute_blocks = (text_length + max_permute_block_size - 1) / max_permute_block_size;
  typedef async_multi_stream_writer<text_offset_type> output_pos_writer_type;
  output_pos_writer_type *output_pos_writer = new output_pos_writer_type(n_permute_blocks);
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
    output_pos_writer->add_file(output_pos_filenames[permute_block_id]);
  typedef async_stream_writer<text_offset_type> output_count_writer_type;
  output_count_writer_type *output_count_writer = new output_count_writer_type(output_count_filename);

  // Induce minus substrings.
  bool empty_output = true;
  bool was_extract_min = false;
  bool was_plus_subtr = false;
  std::uint64_t prev_written_head_char = 0;
  std::uint64_t cur_symbol = 0;
  std::uint64_t cur_substring_name_snapshot = 0;
  std::uint64_t diff_items_written = 0;
  std::uint64_t current_timestamp = 0;
  std::uint64_t cur_substring_name = 0;
  std::uint64_t cur_bucket_size = 0;
  std::vector<std::uint64_t> block_count(n_blocks, 0UL);
  std::vector<text_offset_type> symbol_timestamps(text_alphabet_size, (text_offset_type)0);
  while (cur_symbol <= (std::uint64_t)last_text_symbol || !plus_count_reader->empty() || !radix_heap->empty()) {
    // Extract all minus substrings starting
    // with cur_symbol from the heap.
    {
      // Simulate extracting last suffix from the heap.
      if (cur_symbol == (std::uint64_t)last_text_symbol) {
        std::uint64_t head_char = cur_symbol;
        std::uint64_t block_id = (text_length - 1) / max_block_size;
        cur_substring_name += was_extract_min;
        ++current_timestamp;
        was_extract_min = true;
        ++block_count[block_id];
        bool head_pos_at_block_beg = (block_count[block_id] == block_count_target[block_id]);

        // Watch for the order of minus-substrings with the same name!!!
        bool is_star = minus_type_reader->read_from_ith_file(block_id);
        if (block_id > 0 || head_pos_at_block_beg == false) {
          if (!is_star) {
            char_type prev_char = symbols_reader->read_from_ith_file(block_id);
            std::uint64_t prev_pos_block_id = block_id - head_pos_at_block_beg;
            std::uint64_t heap_value = prev_pos_block_id;

            // Set the most significant of heap_value if necessary.
            if ((std::uint64_t)symbol_timestamps[prev_char] != current_timestamp)
              heap_value |= msb_bit;

            // Add the item to the heap and update timestamp of prev_char.
            radix_heap->push(prev_char, heap_value);
            symbol_timestamps[prev_char] = current_timestamp;
          } else {
            if (!empty_output) {
              if (cur_substring_name_snapshot != cur_substring_name)
                ++diff_items_written;
            } else ++diff_items_written;

            if (empty_output == false) {
              if (head_char == prev_written_head_char) ++cur_bucket_size;
              else {
                output_count_writer->write(cur_bucket_size);
                for (std::uint64_t ch = prev_written_head_char + 1; ch < head_char; ++ch)
                  output_count_writer->write(0);
                cur_bucket_size = 1;
                prev_written_head_char = head_char;
              }
            } else {
              for (std::uint64_t ch = 0; ch < head_char; ++ch)
                output_count_writer->write(0);
              cur_bucket_size = 1;
              prev_written_head_char = head_char;
            }

            empty_output = false;
            text_offset_type head_pos = block_id * max_block_size + minus_pos_reader->read_from_ith_file(block_id);
            std::uint64_t permute_block_id = head_pos / max_permute_block_size;
            output_pos_writer->write_to_ith_file(permute_block_id, head_pos);
            output_pos_writer->write_to_ith_file(permute_block_id, diff_items_written - 1);
            cur_substring_name_snapshot = cur_substring_name;
          }
        }
      }

      // Process minus substrings.
      while (!radix_heap->empty() && radix_heap->min_compare(cur_symbol)) {
        std::pair<char_type, ext_block_id_type> p = radix_heap->extract_min();
        std::uint64_t head_char = p.first;
        std::uint64_t block_id = p.second;
        bool is_substr_different_from_prev_extracted_from_heap = false;
        if (block_id & msb_bit) {
          block_id -= msb_bit;
          is_substr_different_from_prev_extracted_from_heap = true;
        }

        cur_substring_name += (was_extract_min && is_substr_different_from_prev_extracted_from_heap);
        if (is_substr_different_from_prev_extracted_from_heap == true)
          ++current_timestamp;
        was_extract_min = true;
        ++block_count[block_id];
        bool head_pos_at_block_beg = (block_count[block_id] == block_count_target[block_id]);

        // Watch for the order of minus-substrings with the same name!!!
        bool is_star = minus_type_reader->read_from_ith_file(block_id);
        if (block_id > 0 || head_pos_at_block_beg == false) {
          if (!is_star) {
            char_type prev_char = symbols_reader->read_from_ith_file(block_id);
            std::uint64_t prev_pos_block_id = block_id - head_pos_at_block_beg;
            std::uint64_t heap_value = prev_pos_block_id;

            // Set the most significant of heap_value if necessary.
            if ((std::uint64_t)symbol_timestamps[prev_char] != current_timestamp)
              heap_value |= msb_bit;

            // Add the item to the heap and update timestamp of prev_char.
            radix_heap->push(prev_char, heap_value);
            symbol_timestamps[prev_char] = current_timestamp;
          } else {
            if (!empty_output) {
              if (cur_substring_name_snapshot != cur_substring_name)
                ++diff_items_written;
            } else ++diff_items_written;

            if (empty_output == false) {
              if (head_char == prev_written_head_char) ++cur_bucket_size;
              else {
                output_count_writer->write(cur_bucket_size);
                for (std::uint64_t ch = prev_written_head_char + 1; ch < head_char; ++ch)
                  output_count_writer->write(0);
                cur_bucket_size = 1;
                prev_written_head_char = head_char;
              }
            } else {
              for (std::uint64_t ch = 0; ch < head_char; ++ch)
                output_count_writer->write(0);
              cur_bucket_size = 1;
              prev_written_head_char = head_char;
            }

            empty_output = false;
            std::uint64_t head_pos = block_id * max_block_size + minus_pos_reader->read_from_ith_file(block_id);
            std::uint64_t permute_block_id = head_pos / max_permute_block_size;
            output_pos_writer->write_to_ith_file(permute_block_id, head_pos);
            output_pos_writer->write_to_ith_file(permute_block_id, diff_items_written - 1);
            cur_substring_name_snapshot = cur_substring_name;
          }
        }
      }
    }

    // Induce minus substrings from plus star substrings.
    std::uint64_t plus_substr_count = 0;
    if (!plus_count_reader->empty())
      plus_substr_count = plus_count_reader->read();
    for (std::uint64_t i = 0; i < plus_substr_count; ++i) {
      // Compute pos_block_id and prev_pos_block_id.
      std::uint64_t pos_block_id = plus_pos_reader->read();
      ++block_count[pos_block_id];
      bool head_pos_at_block_beg = (block_count[pos_block_id] == block_count_target[pos_block_id]);
      std::uint64_t prev_pos_block_id = pos_block_id - head_pos_at_block_beg;
      std::uint64_t heap_value = prev_pos_block_id;

      // Update timestamp and compute prev_char.
      if (was_plus_subtr == false || plus_diff_reader->read())
        ++current_timestamp;
      was_plus_subtr = true;
      char_type prev_char = symbols_reader->read_from_ith_file(pos_block_id);

      // Set the most significant of heap_value if necessary.
      if ((std::uint64_t)symbol_timestamps[prev_char] != current_timestamp)
        heap_value |= msb_bit;

      // Add the item to the heap and update timestamp of prev_char.
      radix_heap->push(prev_char, heap_value);
      symbol_timestamps[prev_char] = current_timestamp;
    }

    // Update current char.
    ++cur_symbol;
  }

  if (cur_bucket_size > 0)
    output_count_writer->write(cur_bucket_size);

  // Update I/O volume.
  io_volume += radix_heap->io_volume() +
    plus_pos_reader->bytes_read() + plus_count_reader->bytes_read() +
    plus_diff_reader->bytes_read() + minus_type_reader->bytes_read() +
    minus_pos_reader->bytes_read() + symbols_reader->bytes_read() +
    output_pos_writer->bytes_written() + output_count_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete radix_heap;
  delete plus_pos_reader;
  delete plus_count_reader;
  delete plus_diff_reader;
  delete minus_type_reader;
  delete minus_pos_reader;
  delete symbols_reader;
  delete output_pos_writer;
  delete output_count_writer;

  long double total_time = utils::wclock() - start;
  fprintf(stderr, "    time = %.2Lfs, I/O = %.2LfMiB/s, total I/O vol = %.1Lfn bytes\n", total_time,
      (1.L * io_volume / (1L << 20)) / total_time, (1.L * total_io_volume) / text_length);

  return diff_items_written;
}

template<typename char_type,
  typename text_offset_type,
  typename block_offset_type,
  typename block_id_type>
std::uint64_t
em_induce_minus_star_substrings_small_alphabet(
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t text_alphabet_size,
    std::uint64_t ram_use,
    std::uint64_t max_permute_block_size,
    char_type last_text_symbol,
    std::vector<std::uint64_t> &block_count_target,
    std::string plus_pos_filename,
    std::string plus_count_filename,
    std::string plus_diff_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::string tempfile_basename,
    std::string output_count_filename,
    std::vector<std::string> &output_pos_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  if (n_blocks < (1UL << 7))
    return em_induce_minus_star_substrings_small_alphabet<char_type, text_offset_type, block_offset_type, block_id_type, std::uint8_t>(
        text_length, max_block_size, text_alphabet_size, ram_use, max_permute_block_size, last_text_symbol, block_count_target,
        plus_pos_filename, plus_count_filename, plus_diff_filename, minus_type_filenames, minus_pos_filenames, symbols_filenames,
        tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
  else if (n_blocks < (1UL << 15))
    return em_induce_minus_star_substrings_small_alphabet<char_type, text_offset_type, block_offset_type, block_id_type, std::uint16_t>(
        text_length, max_block_size, text_alphabet_size, ram_use, max_permute_block_size, last_text_symbol, block_count_target,
        plus_pos_filename, plus_count_filename, plus_diff_filename, minus_type_filenames, minus_pos_filenames, symbols_filenames,
        tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
  else
    return em_induce_minus_star_substrings_small_alphabet<char_type, text_offset_type, block_offset_type, block_id_type, std::uint64_t>(
        text_length, max_block_size, text_alphabet_size, ram_use, max_permute_block_size, last_text_symbol, block_count_target,
        plus_pos_filename, plus_count_filename, plus_diff_filename, minus_type_filenames, minus_pos_filenames, symbols_filenames,
        tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
}

template<typename char_type,
  typename text_offset_type,
  typename block_offset_type,
  typename block_id_type>
std::uint64_t
em_induce_minus_star_substrings(
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t text_alphabet_size,
    std::uint64_t ram_use,
    std::uint64_t max_permute_block_size,
    char_type last_text_symbol,
    std::vector<std::uint64_t> &block_count_target,
    std::string plus_pos_filename,
    std::string plus_count_filename,
    std::string plus_diff_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &symbols_filenames,
    std::string tempfile_basename,
    std::string output_count_filename,
    std::vector<std::string> &output_pos_filenames,
    std::uint64_t &total_io_volume) {
  // TODO more sophisticated criterion
  if (text_alphabet_size <= 2000000)
    return em_induce_minus_star_substrings_small_alphabet<char_type, text_offset_type, block_offset_type, block_id_type>(
        text_length, max_block_size, text_alphabet_size, ram_use, max_permute_block_size, last_text_symbol, block_count_target,
        plus_pos_filename, plus_count_filename, plus_diff_filename, minus_type_filenames, minus_pos_filenames, symbols_filenames,
        tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
  else
    return em_induce_minus_star_substrings_large_alphabet<char_type, text_offset_type, block_offset_type, block_id_type>(
        text_length, max_block_size, ram_use, max_permute_block_size, last_text_symbol, block_count_target, plus_pos_filename,
        plus_count_filename, plus_diff_filename, minus_type_filenames, minus_pos_filenames, symbols_filenames, tempfile_basename,
        output_count_filename, output_pos_filenames, total_io_volume);
}

template<typename char_type,
  typename text_offset_type,
  typename block_offset_type,
  typename block_id_type>
std::uint64_t
em_induce_minus_star_substrings(
    std::uint64_t text_length,
    std::uint64_t text_alphabet_size,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::uint64_t max_permute_block_size,
    std::string text_filename,
    std::string tempfile_basename,
    std::string output_count_filename,
    std::vector<std::string> &output_pos_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;

  fprintf(stderr, "EM induce substrings:\n");
  fprintf(stderr, "  sizeof(block_offset_type) = %lu\n", sizeof(block_offset_type));
  fprintf(stderr, "  sizeof(block_id_type) = %lu\n", sizeof(block_id_type));
  fprintf(stderr, "  block size = %lu (%.2LfMiB)\n", max_block_size, (1.L * max_block_size) / (1L << 20));

  std::vector<std::uint64_t> plus_block_count_targets(n_blocks, 0UL);
  std::vector<std::uint64_t> minus_block_count_targets(n_blocks, 0UL);
  std::vector<std::string> plus_symbols_filenames(n_blocks);
  std::vector<std::string> plus_type_filenames(n_blocks);
  std::vector<std::string> minus_pos_filenames(n_blocks);
  std::vector<std::string> minus_symbols_filenames(n_blocks);
  std::vector<std::string> minus_type_filenames(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    plus_symbols_filenames[block_id] = tempfile_basename + ".tmp" + utils::random_string_hash();
    plus_type_filenames[block_id] = tempfile_basename + ".tmp" + utils::random_string_hash();
    minus_pos_filenames[block_id] = tempfile_basename + ".tmp" + utils::random_string_hash();
    minus_symbols_filenames[block_id] = tempfile_basename + ".tmp" + utils::random_string_hash();
    minus_type_filenames[block_id] = tempfile_basename + ".tmp" + utils::random_string_hash();
  }

  // Internal memory preprocessing of blocks.
  im_induce_substrings<
    char_type,
    block_offset_type>(
        text_alphabet_size,
        text_length,
        max_block_size,
        text_filename,
        plus_symbols_filenames,
        plus_type_filenames,
        minus_pos_filenames,
        minus_type_filenames,
        minus_symbols_filenames,
        plus_block_count_targets,
        minus_block_count_targets,
        total_io_volume);

  // Induce plus star substrings.
  std::string plus_count_filename = tempfile_basename + ".tmp" + utils::random_string_hash();
  std::string plus_pos_filename = tempfile_basename + ".tmp" + utils::random_string_hash();
  std::string plus_diff_filename = tempfile_basename + ".tmp" + utils::random_string_hash();
  em_induce_plus_star_substrings<
    char_type,
    text_offset_type,
    block_id_type>(
        text_length,
        max_block_size,
        text_alphabet_size,
        ram_use,
        plus_block_count_targets,
        text_filename,
        plus_pos_filename,
        plus_diff_filename,
        plus_count_filename,
        plus_type_filenames,
        plus_symbols_filenames,
        total_io_volume);

  // Delete input files.
  for (std::uint64_t j = 0; j < n_blocks; ++j) {
    if (utils::file_exists(plus_type_filenames[j])) utils::file_delete(plus_type_filenames[j]);
    if (utils::file_exists(plus_symbols_filenames[j])) utils::file_delete(plus_symbols_filenames[j]);
  }

  // Read last symbol of text.
  char_type last_text_symbol;
  utils::read_at_offset(&last_text_symbol, text_length - 1, 1, text_filename);

  // Induce minus star substrings.
  std::uint64_t n_names = em_induce_minus_star_substrings<
    char_type,
    text_offset_type,
    block_offset_type,
    block_id_type>(
      text_length,
      max_block_size,
      text_alphabet_size,
      ram_use,
      max_permute_block_size,
      last_text_symbol,
      minus_block_count_targets,
      plus_pos_filename,
      plus_count_filename,
      plus_diff_filename,
      minus_type_filenames,
      minus_pos_filenames,
      minus_symbols_filenames,
      tempfile_basename,
      output_count_filename,
      output_pos_filenames,
      total_io_volume);

  // Delete input files.
  utils::file_delete(plus_pos_filename);
  utils::file_delete(plus_count_filename);
  utils::file_delete(plus_diff_filename);
  for (std::uint64_t j = 0; j < n_blocks; ++j) {
    if (utils::file_exists(minus_type_filenames[j])) utils::file_delete(minus_type_filenames[j]);
    if (utils::file_exists(minus_symbols_filenames[j])) utils::file_delete(minus_symbols_filenames[j]);
    if (utils::file_exists(minus_pos_filenames[j])) utils::file_delete(minus_pos_filenames[j]);
  }

  return n_names;
}

template<typename char_type,
  typename text_offset_type,
  typename block_offset_type>
std::uint64_t
em_induce_minus_star_substrings(
    std::uint64_t text_length,
    std::uint64_t text_alphabet_size,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::uint64_t max_permute_block_size,
    std::string text_filename,
    std::string tempfile_basename,
    std::string output_count_filename,
    std::vector<std::string> &output_pos_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  if (n_blocks < (1UL << 8))
    return em_induce_minus_star_substrings<char_type, text_offset_type, block_offset_type, std::uint8_t>
      (text_length, text_alphabet_size, max_block_size, ram_use, max_permute_block_size, text_filename,
       tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
  else if (n_blocks < (1UL << 16))
    return em_induce_minus_star_substrings<char_type, text_offset_type, block_offset_type, std::uint16_t>
      (text_length, text_alphabet_size, max_block_size, ram_use, max_permute_block_size, text_filename,
       tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
  else
    return em_induce_minus_star_substrings<char_type, text_offset_type, block_offset_type, std::uint64_t>
      (text_length, text_alphabet_size, max_block_size, ram_use, max_permute_block_size, text_filename,
       tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
}

template<typename char_type,
  typename text_offset_type>
std::uint64_t
em_induce_minus_star_substrings(
    std::uint64_t text_length,
    std::uint64_t text_alphabet_size,
    std::uint64_t max_block_size,
    std::uint64_t ram_use,
    std::uint64_t max_permute_block_size,
    std::string text_filename,
    std::string tempfile_basename,
    std::string output_count_filename,
    std::vector<std::string> &output_pos_filenames,
    std::uint64_t &total_io_volume) {
  if (max_block_size < (1UL << 32))
    return em_induce_minus_star_substrings<char_type, text_offset_type, std::uint32_t>(text_length,
        text_alphabet_size, max_block_size, ram_use, max_permute_block_size, text_filename,
        tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
  else if (max_block_size < (1UL << 40))
    return em_induce_minus_star_substrings<char_type, text_offset_type, uint40>(text_length,
        text_alphabet_size, max_block_size, ram_use, max_permute_block_size, text_filename,
        tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
  else
    return em_induce_minus_star_substrings<char_type, text_offset_type, std::uint64_t>(text_length,
        text_alphabet_size, max_block_size, ram_use, max_permute_block_size, text_filename,
        tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
}

template<typename char_type,
  typename text_offset_type>
std::uint64_t
em_induce_minus_star_substrings(
    std::uint64_t text_length,
    std::uint64_t text_alphabet_size,
    std::uint64_t ram_use,
    std::uint64_t max_permute_block_size,
    std::string text_filename,
    std::string tempfile_basename,
    std::string output_count_filename,
    std::vector<std::uint64_t> &output_pos_filenames,
    std::uint64_t &total_io_volume) {
  // Binary search for the largest block size that
  // can be processed using the given ram_budget.
  std::uint64_t low = 1, high = text_length + 1;
  while (high - low > 1) {
    std::uint64_t mid = (low + high) / 2;

    // Compute RAM usage assuming max block size is equal to mid.
    // TODO take buffers for I/O streamers into account.
    std::uint64_t ext_max_block_size_sizeof = 0;
    if (2 * mid < (1UL << 32)) ext_max_block_size_sizeof = 4;
    else if (2 * mid < (1UL << 40)) ext_max_block_size_sizeof = 5;
    else if (2 * mid < (1UL << 48)) ext_max_block_size_sizeof = 6;
    else ext_max_block_size_sizeof = 8;
#if 0
    std::uint64_t required_ram = mid / 4UL + mid * sizeof(char_type) +
      (text_alphabet_size + 2UL * mid) * ext_max_block_size_sizeof;
#else
    // XXX this is not entirely correct, but true in practice.
    std::uint64_t required_ram = mid / 4UL + mid * sizeof(char_type) +
      (text_alphabet_size + mid) * ext_max_block_size_sizeof;
#endif

    // Update bounds.
    if (required_ram <= ram_use) low = mid;
    else high = mid;
  }

  std::uint64_t max_block_size = low;
  return em_induce_minus_star_substrings<char_type, text_offset_type>(text_length,
      text_alphabet_size, max_block_size, ram_use, max_permute_block_size, text_filename,
      tempfile_basename, output_count_filename, output_pos_filenames, total_io_volume);
}

template<typename char_type,
  typename text_offset_type>
std::uint64_t  // return the number of different names
em_induce_minus_star_substrings(
    std::uint64_t text_length,
    std::uint64_t ram_use,
    std::uint64_t max_permute_block_size,
    std::string text_filename,
    std::string tempfile_basename,
    std::string output_count_filename,
    std::vector<std::string> &output_pos_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t text_alphabet_size = (std::uint64_t)std::numeric_limits<char_type>::max() + 1;
  return em_induce_minus_star_substrings<char_type, text_offset_type>(text_length,
      text_alphabet_size, ram_use, max_permute_block_size, text_filename, tempfile_basename,
      output_count_filename, output_pos_filenames, total_io_volume);
}


#endif  // __EM_INDUCE_MINUS_STAR_SUBSTRINGS_HPP_INCLUDED
