#ifndef __EM_INDUCE_MINUS_STAR_SUBSTRINGS_HPP_INCLUDED
#define __EM_INDUCE_MINUS_STAR_SUBSTRINGS_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "utils.hpp"
#include "packed_pair.hpp"
#include "em_radix_heap.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_multi_bit_stream_reader.hpp"
#include "io/async_bit_stream_reader.hpp"


// Note: ext_blockidx_t needs to hold block is and one extra bit.
template<typename chr_t, typename saidx_t, typename blockidx_t, typename ext_blockidx_t>
void em_induce_minus_star_substrings(
    std::uint64_t text_length,
    std::string plus_substrings_filename,
    std::string output_filename,
    std::string plus_count_filename,
    std::string plus_symbols_filename,
    std::string plus_diff_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_symbols_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::uint64_t> &block_count_target,
    std::uint64_t &total_io_volume,
    std::uint64_t radix_heap_bufsize,
    std::uint64_t radix_log,
    std::uint64_t max_block_size,
    chr_t last_text_symbol) {
  std::uint64_t is_tail_minus_bit = ((std::uint64_t)std::numeric_limits<ext_blockidx_t>::max() + 1) / 2;
  std::uint64_t io_volume = 0;

  // Initialize radix heap.
  typedef packed_pair<ext_blockidx_t, saidx_t> ext_pair_type;
  typedef em_radix_heap<chr_t, ext_pair_type> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, output_filename);

  // Initialize the readers of data associated with plus suffixes.
  typedef async_stream_reader<blockidx_t> plus_reader_type;
  typedef async_backward_stream_reader<saidx_t> plus_count_reader_type;
  typedef async_backward_stream_reader<chr_t> plus_symbols_reader_type;
  typedef async_bit_stream_reader plus_diff_reader_type;
  plus_reader_type *plus_reader = new plus_reader_type(plus_substrings_filename);
  plus_count_reader_type *plus_count_reader = new plus_count_reader_type(plus_count_filename);
  plus_symbols_reader_type *plus_symbols_reader = new plus_symbols_reader_type(plus_symbols_filename);
  plus_diff_reader_type *plus_diff_reader = new plus_diff_reader_type(plus_diff_filename);

  // Initialize readers of data associated with minus suffixes.
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

  // Initialize the output writer.
  typedef async_stream_writer<saidx_t> output_writer_type;
  output_writer_type *output_writer = new output_writer_type(output_filename);

  // Induce minus substrings.
  bool empty_output = true;
  bool was_extract_min = false;
  bool is_prev_tail_minus = 0;
  bool is_prev_tail_name_defined = 0;
  bool was_prev_plus_name = false;
  chr_t prev_head_char = 0;
  saidx_t prev_tail_name = 0;
  std::uint64_t diff_str = 0;
  std::uint64_t cur_symbol = 0;
  std::uint64_t diff_str_snapshot = 0;
  std::uint64_t diff_items_written = 0;
  std::uint64_t cur_plus_name = 0;
  std::vector<std::uint64_t> block_count(n_blocks, 0UL);

  while (cur_symbol <= (std::uint64_t)last_text_symbol || !plus_count_reader->empty() || !radix_heap->empty()) {
    // Process minus substrings.
    if (cur_symbol == (std::uint64_t)last_text_symbol) {
      chr_t head_char = cur_symbol;
      std::uint64_t block_id = (text_length - 1) / max_block_size;
      saidx_t tail_name = 0;
      bool is_tail_minus = true;

      // Update diff_str.
      if (was_extract_min == true) {
        if (!is_prev_tail_name_defined || is_prev_tail_minus != is_tail_minus ||
            prev_head_char != head_char ||  prev_tail_name != tail_name)
          ++diff_str;
      } else ++diff_str;
      was_extract_min = true;

      ++block_count[block_id];
      std::uint8_t head_pos_at_block_beg = (block_count[block_id] == block_count_target[block_id]);

      // Watch for the order of minus-substrings with the same name!!!
      std::uint8_t is_star = minus_type_reader->read_from_ith_file(block_id);
      if (block_id > 0 || head_pos_at_block_beg == false) {
        if (!is_star) {
          chr_t prev_char = minus_symbols_reader->read_from_ith_file(block_id);
          std::uint64_t prev_pos_block_id = block_id - head_pos_at_block_beg;
          radix_heap->push(prev_char, ext_pair_type(prev_pos_block_id | is_tail_minus_bit, diff_str - 1));
        } else {
          if (!empty_output) {
            if (diff_str_snapshot != diff_str)
              ++diff_items_written;
          } else ++diff_items_written;
          empty_output = false;
          saidx_t head_pos = minus_pos_reader->read_from_ith_file(block_id);
          output_writer->write(head_pos);
          output_writer->write(diff_items_written - 1);
          diff_str_snapshot = diff_str;
        }
      }

      prev_head_char = head_char;
      prev_tail_name = tail_name;
      is_prev_tail_minus = is_tail_minus;
      is_prev_tail_name_defined = false;
    }
    while (!radix_heap->empty() && radix_heap->min_compare(cur_symbol)) {
      std::pair<chr_t, ext_pair_type> p = radix_heap->extract_min();
      chr_t head_char = cur_symbol;
      std::uint64_t block_id = p.second.first;
      saidx_t tail_name = p.second.second;

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
      std::uint8_t head_pos_at_block_beg = (block_count[block_id] == block_count_target[block_id]);

      // Watch for the order of minus-substrings with the same name!!!
      std::uint8_t is_star = minus_type_reader->read_from_ith_file(block_id);
      if (block_id > 0 || head_pos_at_block_beg == false) {
        if (!is_star) {
          chr_t prev_char = minus_symbols_reader->read_from_ith_file(block_id);
          std::uint64_t prev_pos_block_id = block_id - head_pos_at_block_beg;
          radix_heap->push(prev_char, ext_pair_type(prev_pos_block_id | is_tail_minus_bit, diff_str - 1));
        } else {
          bool is_next_diff = false;
          if (!empty_output) {
            if (diff_str_snapshot != diff_str)
              is_next_diff = true;
          } else is_next_diff = true;
          diff_items_written += is_next_diff;
          empty_output = false;
          saidx_t head_pos = minus_pos_reader->read_from_ith_file(block_id);
          output_writer->write(head_pos);
          output_writer->write(diff_items_written - 1);
          diff_str_snapshot = diff_str;
        }
      }

      prev_head_char = head_char;
      prev_tail_name = tail_name;
      is_prev_tail_minus = is_tail_minus;
      is_prev_tail_name_defined = true;
    }

    // Process plus substrings.
    std::uint64_t plus_substr_count = plus_count_reader->read();
    for (std::uint64_t i = 0; i < plus_substr_count; ++i) {
      blockidx_t prev_pos_block_id = plus_reader->read();
      std::uint8_t is_different = plus_diff_reader->read();
      if (was_prev_plus_name && is_different)
        ++cur_plus_name;
      was_prev_plus_name = true;
      chr_t prev_char = plus_symbols_reader->read();
      radix_heap->push(prev_char, ext_pair_type(prev_pos_block_id, cur_plus_name));
    }

    // Update current char.
    ++cur_symbol;
  }

  // Update I/O volume.
  io_volume += radix_heap->io_volume() +
    plus_reader->bytes_read() +  plus_count_reader->bytes_read() +
    plus_symbols_reader->bytes_read() + plus_diff_reader->bytes_read() +
    minus_type_reader->bytes_read() + minus_symbols_reader->bytes_read() +
    minus_pos_reader->bytes_read() + output_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete radix_heap;
  delete plus_reader;
  delete plus_count_reader;
  delete plus_symbols_reader;
  delete plus_diff_reader;
  delete minus_type_reader;
  delete minus_symbols_reader;
  delete minus_pos_reader;
  delete output_writer;
}

// Note: ext_blockidx_t has to be able to hold block id and have one extra bit.
template<typename chr_t, typename saidx_t, typename blockidx_t, typename ext_blockidx_t>
void em_induce_minus_star_substrings(
    std::uint64_t text_length,
    std::string plus_substrings_filename,
    std::string output_filename,
    std::string plus_count_filename,
    std::string plus_symbols_filename,
    std::string plus_diff_filename,
    std::vector<std::string> &minus_type_filenames,
    std::vector<std::string> &minus_symbols_filenames,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::uint64_t> &block_count_target,
    std::uint64_t &total_io_volume,
    std::uint64_t radix_heap_bufsize,
    std::uint64_t radix_log,
    std::uint64_t max_block_size,
    std::uint64_t max_text_symbol,
    chr_t last_text_symbol) {
  std::uint64_t msb_bit = ((std::uint64_t)std::numeric_limits<ext_blockidx_t>::max() + 1) / 2;
  std::uint64_t io_volume = 0;

  // Initialize radix heap.
  typedef em_radix_heap<chr_t, ext_blockidx_t> radix_heap_type;
  radix_heap_type *radix_heap = new radix_heap_type(radix_log,
      radix_heap_bufsize, output_filename);

  // Initialize the readers of data associated with plus suffixes.
  typedef async_stream_reader<blockidx_t> plus_reader_type;
  typedef async_backward_stream_reader<saidx_t> plus_count_reader_type;
  typedef async_backward_stream_reader<chr_t> plus_symbols_reader_type;
  typedef async_bit_stream_reader plus_diff_reader_type;
  plus_reader_type *plus_reader = new plus_reader_type(plus_substrings_filename);
  plus_count_reader_type *plus_count_reader = new plus_count_reader_type(plus_count_filename);
  plus_symbols_reader_type *plus_symbols_reader = new plus_symbols_reader_type(plus_symbols_filename);
  plus_diff_reader_type *plus_diff_reader = new plus_diff_reader_type(plus_diff_filename);

  // Initialize readers of data associated with minus suffixes.
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

  // Initialize the output writer.
  typedef async_stream_writer<saidx_t> output_writer_type;
  output_writer_type *output_writer = new output_writer_type(output_filename);

  // Induce minus substrings.
  bool empty_output = true;
  bool was_extract_min = false;
  std::uint64_t cur_symbol = 0;
  std::uint64_t cur_substring_name_snapshot = 0;
  std::uint64_t diff_items_written = 0;
  std::uint64_t current_timestamp = 0;
  std::uint64_t cur_substring_name = 0;
  std::vector<std::uint64_t> block_count(n_blocks, 0UL);
  std::vector<saidx_t> symbol_timestamps(max_text_symbol + 1, (saidx_t)0);
  while (cur_symbol <= (std::uint64_t)last_text_symbol || !plus_count_reader->empty() || !radix_heap->empty()) {
    // Extract all minus substrings starting
    // with cur_symbol from the heap.
    {
      // Simulate extracting last suffix from the heap.
      if (cur_symbol == (std::uint64_t)last_text_symbol) {
        std::uint64_t block_id = (text_length - 1) / max_block_size;
        cur_substring_name += was_extract_min;
        ++current_timestamp;
        was_extract_min = true;
        ++block_count[block_id];
        std::uint8_t head_pos_at_block_beg = (block_count[block_id] == block_count_target[block_id]);

        // Watch for the order of minus-substrings with the same name!!!
        std::uint8_t is_star = minus_type_reader->read_from_ith_file(block_id);
        if (block_id > 0 || head_pos_at_block_beg == false) {
          if (!is_star) {
            chr_t prev_char = minus_symbols_reader->read_from_ith_file(block_id);
            std::uint64_t prev_pos_block_id = block_id - head_pos_at_block_beg;
            std::uint64_t heap_value = prev_pos_block_id;

            // Set the most significant of heap_value if necessary.
            if (symbol_timestamps[prev_char] != current_timestamp)
              heap_value |= msb_bit;

            // Add the item to the heap and update timestamp of prev_char.
            radix_heap->push(prev_char, heap_value);
            symbol_timestamps[prev_char] = current_timestamp;
          } else {
            if (!empty_output) {
              if (cur_substring_name_snapshot != cur_substring_name)
                ++diff_items_written;
            } else ++diff_items_written;
            empty_output = false;
            saidx_t head_pos = minus_pos_reader->read_from_ith_file(block_id);
            output_writer->write(head_pos);
            output_writer->write(diff_items_written - 1);
            cur_substring_name_snapshot = cur_substring_name;
          }
        }
      }

      // Process minus substrings.
      while (!radix_heap->empty() && radix_heap->min_compare(cur_symbol)) {
        std::pair<chr_t, ext_blockidx_t> p = radix_heap->extract_min();
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
        std::uint8_t head_pos_at_block_beg = (block_count[block_id] == block_count_target[block_id]);

        // Watch for the order of minus-substrings with the same name!!!
        std::uint8_t is_star = minus_type_reader->read_from_ith_file(block_id);
        if (block_id > 0 || head_pos_at_block_beg == false) {
          if (!is_star) {
            chr_t prev_char = minus_symbols_reader->read_from_ith_file(block_id);
            std::uint64_t prev_pos_block_id = block_id - head_pos_at_block_beg;
            std::uint64_t heap_value = prev_pos_block_id;

            // Set the most significant of heap_value if necessary.
            if (symbol_timestamps[prev_char] != current_timestamp)
              heap_value |= msb_bit;

            // Add the item to the heap and update timestamp of prev_char.
            radix_heap->push(prev_char, heap_value);
            symbol_timestamps[prev_char] = current_timestamp;
          } else {
            if (!empty_output) {
              if (cur_substring_name_snapshot != cur_substring_name)
                ++diff_items_written;
            } else ++diff_items_written;
            empty_output = false;
            saidx_t head_pos = minus_pos_reader->read_from_ith_file(block_id);
            output_writer->write(head_pos);
            output_writer->write(diff_items_written - 1);
            cur_substring_name_snapshot = cur_substring_name;
          }
        }
      }
    }

    // Induce minus substrings from plus star substrings.
    std::uint64_t plus_substr_count = plus_count_reader->read();
    for (std::uint64_t i = 0; i < plus_substr_count; ++i) {
      std::uint64_t prev_pos_block_id = plus_reader->read();
      std::uint8_t is_different = plus_diff_reader->read();
      if (i == 0 || is_different)
        ++current_timestamp;
      chr_t prev_char = plus_symbols_reader->read();
      std::uint64_t heap_value = prev_pos_block_id;

      // Set the most significant of heap_value if necessary.
      if (symbol_timestamps[prev_char] != current_timestamp)
        heap_value |= msb_bit;

      // Add the item to the heap and update timestamp of prev_char.
      radix_heap->push(prev_char, heap_value);
      symbol_timestamps[prev_char] = current_timestamp;
    }

    // Update current char.
    ++cur_symbol;
  }

  // Update I/O volume.
  io_volume += radix_heap->io_volume() +
    plus_reader->bytes_read() + plus_count_reader->bytes_read() +
    plus_symbols_reader->bytes_read() + plus_diff_reader->bytes_read() +
    minus_type_reader->bytes_read() + minus_symbols_reader->bytes_read() +
    minus_pos_reader->bytes_read() + output_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete radix_heap;
  delete plus_reader;
  delete plus_count_reader;
  delete plus_symbols_reader;
  delete plus_diff_reader;
  delete minus_type_reader;
  delete minus_symbols_reader;
  delete minus_pos_reader;
  delete output_writer;
}

#endif  // __EM_INDUCE_MINUS_STAR_SUBSTRINGS_HPP_INCLUDED
