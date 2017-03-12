/**
 * @file    fsais_src/em_induce_suffixes.hpp
 * @section LICENCE
 *
 * This file is part of fSAIS v0.1.0
 * See: http://www.cs.helsinki.fi/group/pads/
 *
 * Copyright (C) 2017
 *   Juha Karkkainen <juha.karkkainen (at) cs.helsinki.fi>
 *   Dominik Kempa <dominik.kempa (at) gmail.com>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 **/

#ifndef __FSAIS_SRC_IM_INDUCE_SUFFIXES_HPP_INCLUDED
#define __FSAIS_SRC_IM_INDUCE_SUFFIXES_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <limits>
#include <queue>
#include <algorithm>

#include "io/async_backward_stream_reader.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_stream_writer_multipart.hpp"
#include "io/async_bit_stream_writer.hpp"
#include "io/simple_accessor.hpp"

#include "packed_pair.hpp"
#include "radix_heap.hpp"
#include "utils.hpp"
#include "../uint40.hpp"
#include "../uint48.hpp"

namespace fsais_private {

//=============================================================================
// Assumptions:
// - char_type has to be able to hold any symbol from the text.
// - text_offset_type can encode integer in range [0..2 * text_length)
//
// All types are assumed to be unsigned.
//
// This version of the function uses
//   block_size / 4                                 // type bitvector
// + block_size * sizeof(char_type)                 // text block
// + 2 * block_size * sizeof(text_offset_type)      // buckets
// + text_alphabet_size * sizeof(text_offset_type)  // bucket pointers
// bytes of RAM.
//=============================================================================
// TODO:
// - optimize random accessed using buffer
// - reduce the RAM usage for integer array.
//=============================================================================

struct local_buf_item_2 {
  local_buf_item_2() {}
  std::uint64_t m_head_pos;
  std::uint64_t m_prev_pos_head_char;
  std::uint64_t m_idx_1;
  std::uint64_t m_idx_2;
  bool m_is_head_minus;
  bool m_is_prev_pos_minus;
};

struct local_buf_item_3 {
  std::uint64_t m_pos;
  std::uint64_t m_char;
};

template<typename char_type,
  typename text_offset_type>
std::pair<std::uint64_t, bool>
im_induce_suffixes_large_alphabet(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t block_beg,
    std::uint64_t next_block_leftmost_minus_star_plus,
    std::uint64_t next_block_leftmost_minus_star_plus_rank,
    std::uint64_t max_part_size,
    bool is_last_minus,
    std::string text_filename,
    std::string minus_pos_filename,
    std::string output_plus_pos_filename,
    std::string output_plus_symbols_filename,
    std::string output_plus_type_filename,
    std::string output_minus_pos_filename,
    std::string output_minus_type_filename,
    std::string output_minus_symbols_filename,
    std::uint64_t &minus_block_count_target,
    std::uint64_t &total_io_volume) {
  std::uint64_t block_end = std::min(text_length, block_beg + max_block_size);
  std::uint64_t block_size = block_end - block_beg;
  std::uint64_t next_block_size = std::min(max_block_size, text_length - block_end);
  std::uint64_t total_block_size = block_size + next_block_size;
  std::uint64_t io_volume = 0;

  if (text_alphabet_size == 0) {
    fprintf(stderr, "\nError: text_alphabet_size = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (max_block_size == 0) {
    fprintf(stderr, "\nError: max_block_size = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (text_length == 0) {
    fprintf(stderr, "\nError: text_length = 0\n");
    std::exit(EXIT_FAILURE);
  }

  // Check that all types are sufficiently large.
  if ((std::uint64_t)std::numeric_limits<char_type>::max() < text_alphabet_size - 1) {
    fprintf(stderr, "\nError: char_type in im_induce_suffixes_large_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<text_offset_type>::max() < text_length * 2UL) {
    fprintf(stderr, "\nError: text_offset_type in im_induce_suffixes_large_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }





  // Start the timer.
  long double start = utils::wclock();
  fprintf(stderr, "      Process block [%lu..%lu): ", block_beg, block_end);




  // Read block into RAM.
  char_type *block = utils::allocate_array<char_type>(block_size);
  std::fill(block, block + block_size, (char_type)0);
  utils::read_at_offset(block, block_beg, block_size, text_filename);
  io_volume += block_size * sizeof(char_type);






  // Initialize text accessor.
  typedef simple_accessor<char_type> accessor_type;
  accessor_type *text_accessor = new accessor_type(text_filename, (2UL << 20));







  // Read the symbol preceding block.
  char_type block_prec_symbol = 0;
  if (block_beg > 0)
    block_prec_symbol = text_accessor->access(block_beg - 1);








  // Initialize output writers.
  typedef async_stream_writer_multipart<text_offset_type> output_plus_pos_writer_type;
  typedef async_stream_writer_multipart<char_type> output_plus_symbols_writer_type;
  typedef async_bit_stream_writer output_plus_type_writer_type;
  output_plus_pos_writer_type *output_plus_pos_writer = new output_plus_pos_writer_type(output_plus_pos_filename, max_part_size, (2UL << 20), 4UL);
  output_plus_type_writer_type *output_plus_type_writer = new output_plus_type_writer_type(output_plus_type_filename, (2UL << 20), 4UL);
  output_plus_symbols_writer_type *output_plus_symbols_writer = new output_plus_symbols_writer_type(output_plus_symbols_filename, max_part_size, (2UL << 20), 4UL);







  // Compute type_bv that stores whether each of the
  // position is a minus position (true) or not (false).
  std::uint64_t bv_size = (total_block_size + 63) / 64;
  std::uint64_t *type_bv = utils::allocate_array<std::uint64_t>(bv_size);
  std::fill(type_bv, type_bv + bv_size, 0UL);
  {
    if (is_last_minus) {
      std::uint64_t i = total_block_size - 1;
      type_bv[i >> 6] |= (1UL << (i & 63));
    }
    bool is_next_minus = is_last_minus;
    std::uint64_t next_char = (next_block_size == 0) ? block[block_size - 1] : text_accessor->access(block_end + next_block_size - 1);
    for (std::uint64_t iplus = total_block_size - 1; iplus > 0; --iplus) {
      std::uint64_t i = iplus - 1;
      std::uint64_t head_char = (i < block_size) ? block[i] : text_accessor->access(block_beg + i);
      bool is_minus = (head_char == next_char) ? is_next_minus : (head_char > next_char);
      if (is_minus)
        type_bv[i >> 6] |= (1UL << (i & 63));
      is_next_minus = is_minus;
      next_char = head_char;
    }
  }



  // Determine whether the first position in the block is of minus star type.
  bool is_first_minus_star = ((block_beg > 0 && (type_bv[0] & 1UL) && (std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]));




  // Find the leftmost minus-star position in the current block.
  std::uint64_t this_block_leftmost_minus_star_plus = 1;  // plus because it's one index past actual position
  if (!is_first_minus_star) {
    while (this_block_leftmost_minus_star_plus < block_size && (type_bv[(this_block_leftmost_minus_star_plus - 1) >> 6] &
          (1UL << ((this_block_leftmost_minus_star_plus - 1) & 63))) > 0) ++this_block_leftmost_minus_star_plus;
    while (this_block_leftmost_minus_star_plus < block_size && (type_bv[(this_block_leftmost_minus_star_plus - 1) >> 6] &
          (1UL << ((this_block_leftmost_minus_star_plus - 1) & 63))) == 0) ++this_block_leftmost_minus_star_plus;
  }




  std::uint64_t max_char = (std::uint64_t)std::numeric_limits<char_type>::max();
  std::vector<std::uint64_t> radix_logs;
  {
    std::uint64_t target_sum = 8UL * sizeof(char_type);
    std::uint64_t cur_sum = 0;
    while (cur_sum < target_sum) {
      std::uint64_t radix_log = std::min(8UL, target_sum - cur_sum);
      radix_logs.push_back(radix_log);
      cur_sum += radix_log;
    }
  }






  typedef packed_pair<char_type, text_offset_type> pair_type;
  typedef std::vector<pair_type> vector_type;
  vector_type *temp_storage = new vector_type();






  std::uint64_t lastpos = block_size + next_block_leftmost_minus_star_plus;
  bool is_lastpos_minus = (type_bv[(lastpos - 1) >> 6] & (1UL << ((lastpos - 1) & 63)));





  typedef radix_heap<char_type, text_offset_type> heap_type;
  heap_type *heap = new heap_type(radix_logs, lastpos/*XXX ???*/);








  {
    typedef async_backward_stream_reader<text_offset_type> reader_type;
    reader_type *reader = new reader_type(minus_pos_filename, (2UL << 20), 4UL);

    std::uint64_t items_count = utils::file_size(minus_pos_filename) / sizeof(text_offset_type);
    if (next_block_leftmost_minus_star_plus_rank == items_count) {
      // Separatelly handle position lastpos - 1 if it
      // was in next block and it was minus star.
      std::uint64_t ii = lastpos - 1;
      std::uint64_t head_char = text_accessor->access(block_beg + ii);
      heap->push(max_char - head_char, ii);
    }

    std::uint64_t rank = 0;
#if 0
    // Unbuffered version left for readability.
    while (!reader->empty()) {
      {
        std::uint64_t i = reader->read();
        std::uint64_t head_char = (i < block_size) ? block[i] : text_accessor->access(block_beg + i);
        heap->push(max_char - head_char, i);
      }

      ++rank;
      if (items_count - next_block_leftmost_minus_star_plus_rank == rank) {
        // Separatelly handle position lastpos - 1 if it
        // was in next block and it was minus star.
        std::uint64_t ii = lastpos - 1;
        std::uint64_t head_char = text_accessor->access(block_beg + ii);
        heap->push(max_char - head_char, ii);
      }
    }
#else
#ifdef SAIS_DEBUG
    std::uint64_t local_bufsize = utils::random_int64(1L, 10L);
#else
    static const std::uint64_t local_bufsize = (1L << 15);
#endif
    local_buf_item_3 *local_buf = new local_buf_item_3[local_bufsize];
    text_offset_type *local_buf_pos = new text_offset_type[local_bufsize];
    std::uint64_t items_left = items_count;
    while (items_left > 0) {
      // Compute buffer.
      std::uint64_t filled = std::min(local_bufsize, items_left);
      reader->read(local_buf_pos, filled);
      for (std::uint64_t t = 0; t < filled; ++t) {
        std::uint64_t pos = (std::uint64_t)local_buf_pos[t];
        if (pos >= block_size) pos = 0;
        local_buf[t].m_pos = pos;
      }
      for (std::uint64_t t = 0; t < filled; ++t) {
        std::uint64_t pos = local_buf[t].m_pos;
        local_buf[t].m_char = block[pos];
      }

      // Process buffer.
      for (std::uint64_t t = 0; t < filled; ++t) {
        {
          std::uint64_t i = local_buf_pos[t];
          std::uint64_t head_char = local_buf[t].m_char;
          if (i >= block_size) head_char = text_accessor->access(block_beg + i);
          heap->push(max_char - head_char, i);
        }

        ++rank;
        if (items_count - next_block_leftmost_minus_star_plus_rank == rank) {
          // Separatelly handle position lastpos - 1 if it
          // was in next block and it was minus star.
          std::uint64_t ii = lastpos - 1;
          std::uint64_t head_char = text_accessor->access(block_beg + ii);
          heap->push(max_char - head_char, ii);
        }
      }

      // Update items_left.
      items_left -= filled;
    }
    delete[] local_buf;
    delete[] local_buf_pos;
#endif

    // Update I/O volume.
    io_volume += reader->bytes_read();

    // Clean up.
    delete reader;
    utils::file_delete(minus_pos_filename);
  }














  // Induce plus suffixes.
  std::uint64_t local_minus_block_count_target = 0;
  bool seen_block_beg = false;
  if (!is_lastpos_minus) {
    // Add the last suffix if it was a plus type.
    std::uint64_t i = lastpos - 1;
    std::uint64_t head_char = (i < block_size) ? block[i] : text_accessor->access(block_beg + i);
    heap->push(max_char - (head_char + 1), i);
  }
  while (!heap->empty()) {
    std::pair<char_type, text_offset_type> pp = heap->extract_min();
    std::uint64_t head_pos = pp.second;
    std::uint64_t prev_pos = head_pos - 1;
    std::uint64_t head_char = max_char - (std::uint64_t)pp.first;

    bool is_head_minus = (type_bv[head_pos >> 6] & (1UL << (head_pos & 63)));
    bool is_prev_pos_minus = (!head_pos) ? false : (type_bv[prev_pos >> 6] & (1UL << (prev_pos & 63)));
    std::uint64_t idx = (0 < head_pos && prev_pos < block_size) ? prev_pos : 0;
    std::uint64_t prev_pos_head_char = block[idx];

    if (!is_head_minus) head_char -= 1;
    if (is_head_minus) {
      if (head_pos < block_size) {
        bool is_head_star = ((head_pos > 0 && !is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]));
        if (is_head_star) {
          if (!seen_block_beg) ++local_minus_block_count_target;
          if (head_pos == 0) seen_block_beg = true;
        }
      }
    } else {
      bool is_head_star = ((head_pos > 0 && is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]));
      if (head_pos < block_size) {
        output_plus_pos_writer->write(head_pos);
        output_plus_type_writer->write(is_head_star);
      }
      if (is_head_star) temp_storage->push_back(pair_type((char_type)(head_char + 1), (text_offset_type)head_pos));
    }
    if (head_pos > 0) {
      if (!is_prev_pos_minus) {
        if (prev_pos >= block_size) prev_pos_head_char = text_accessor->access(block_beg + prev_pos);
        heap->push(max_char - (prev_pos_head_char + 1), prev_pos);
        if (head_pos < block_size) output_plus_symbols_writer->write(prev_pos_head_char);
      }
    } else if (block_beg > 0) {
      bool is_head_star = is_head_minus ? ((std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]) : ((std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]);
      if (is_head_minus == is_head_star) output_plus_symbols_writer->write(block_prec_symbol);
    }
  }
  delete heap;
  if (!seen_block_beg)
    local_minus_block_count_target = std::numeric_limits<std::uint64_t>::max();





  // Update I/O volume.
  io_volume += output_plus_pos_writer->bytes_written() +
    output_plus_symbols_writer->bytes_written() +
    output_plus_type_writer->bytes_written();



  // Clean up.
  delete output_plus_pos_writer;
  delete output_plus_symbols_writer;
  delete output_plus_type_writer;






  // Initialize output writers.
  typedef async_stream_writer_multipart<text_offset_type> output_minus_pos_writer_type;
  typedef async_bit_stream_writer output_minus_type_writer_type;
  typedef async_stream_writer_multipart<char_type> output_minus_symbols_writer_type;
  output_minus_pos_writer_type *output_minus_pos_writer = new output_minus_pos_writer_type(output_minus_pos_filename, max_part_size, (2UL << 20), 4UL);
  output_minus_type_writer_type *output_minus_type_writer = new output_minus_type_writer_type(output_minus_type_filename, (2UL << 20), 4UL);
  output_minus_symbols_writer_type *output_minus_symbols_writer = new output_minus_symbols_writer_type(output_minus_symbols_filename, max_part_size, (2UL << 20), 4UL);







  heap_type *heap2 = new heap_type(radix_logs, lastpos/*XXX ???*/);
  std::reverse(temp_storage->begin(), temp_storage->end());
  for (std::uint64_t t = 0; t < temp_storage->size(); ++t)
    heap2->push((*temp_storage)[t].first, (*temp_storage)[t].second);
  delete temp_storage;







  // Induce minus suffixes.
  if (is_lastpos_minus) {
    // Add the last suffix if it was a minus type.
    std::uint64_t i = lastpos - 1;
    std::uint64_t head_char = (i < block_size) ? block[i] : text_accessor->access(block_beg + i);
    heap2->push(head_char, i);
  }
  while (!heap2->empty()) {
    std::pair<char_type, text_offset_type> pp = heap2->extract_min();
    std::uint64_t head_pos = pp.second;
    std::uint64_t prev_pos = head_pos - 1;

    bool is_head_minus = (type_bv[head_pos >> 6] & (1UL << (head_pos & 63)));
    bool is_prev_pos_minus = (!head_pos) ? false : (type_bv[prev_pos >> 6] & (1UL << (prev_pos & 63)));
    std::uint64_t idx = (prev_pos < block_size) ? prev_pos : 0;
    std::uint64_t prev_pos_head_char = block[idx];

    if (is_head_minus && head_pos < block_size) {
      bool is_star = ((head_pos > 0 && !is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]));
      output_minus_type_writer->write(is_star);
      output_minus_pos_writer->write(head_pos);
    }
    if (head_pos > 0) {
      if (is_prev_pos_minus) {
        if (prev_pos >= block_size) prev_pos_head_char = text_accessor->access(block_beg + prev_pos);
        heap2->push(prev_pos_head_char, prev_pos);
        if (head_pos < block_size) output_minus_symbols_writer->write(prev_pos_head_char);
      }
    } else if (block_beg > 0) {
      bool is_head_star = is_head_minus ? ((std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]) : ((std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]);
      if (is_head_minus ^ is_head_star) output_minus_symbols_writer->write(block_prec_symbol);
    }
  }
  delete heap2;



  // Update reference variables.
  minus_block_count_target = local_minus_block_count_target;





  // Update I/O volume.
  io_volume += output_minus_pos_writer->bytes_written() +
    output_minus_type_writer->bytes_written() +
    output_minus_symbols_writer->bytes_written() +
    text_accessor->bytes_read();





  // Update total I/O volume.
  total_io_volume += io_volume;




  // Store result.
  bool result = (type_bv[(block_size - 1) >> 6] & (1UL << ((block_size - 1) & 63)));





  // Clean up.
  delete output_minus_pos_writer;
  delete output_minus_type_writer;
  delete output_minus_symbols_writer;
  delete text_accessor;
  utils::deallocate(type_bv);
  utils::deallocate(block);





  // Print summary.
  long double total_time = utils::wclock() - start;
  fprintf(stderr, "time = %.2Lfs, I/O = %.2LfMiB/s\n", total_time,
      (1.L * io_volume / (1L << 20)) / total_time);




  // Return result.
  return std::make_pair(this_block_leftmost_minus_star_plus, result);
}

template<typename char_type,
  typename text_offset_type>
void im_induce_suffixes_large_alphabet(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t initial_text_length,
    std::uint64_t max_block_size,
    std::vector<std::uint64_t> &next_block_leftmost_minus_star_plus_rank,
    std::string text_filename,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &output_plus_pos_filenames,
    std::vector<std::string> &output_plus_symbols_filenames,
    std::vector<std::string> &output_plus_type_filenames,
    std::vector<std::string> &output_minus_pos_filenames,
    std::vector<std::string> &output_minus_type_filenames,
    std::vector<std::string> &output_minus_symbols_filenames,
    std::vector<std::uint64_t> &minus_block_count_targets,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  std::uint64_t io_volume = 0;

  fprintf(stderr, "    IM induce suffixes (large alphabet):\n");
  long double start = utils::wclock();

#ifdef SAIS_DEBUG
  std::uint64_t max_part_size = utils::random_int64(1L, 50L);
#else
  std::uint64_t max_part_size = std::max((1UL << 20), max_block_size / 10UL);
  fprintf(stderr, "      Max part size = %lu (%.1LfMiB)\n", max_part_size, (1.L * max_part_size) / (1UL << 20));
#endif

  bool is_last_minus = true;
  std::uint64_t next_block_leftmost_minus_star = 0;
  for (std::uint64_t block_id_plus = n_blocks; block_id_plus > 0; --block_id_plus) {
    std::uint64_t block_id = block_id_plus - 1;
    std::uint64_t block_beg = block_id * max_block_size;

    std::pair<std::uint64_t, bool > ret;
    ret = im_induce_suffixes_large_alphabet<
      char_type,
      text_offset_type>(
          text_alphabet_size,
          text_length,
          max_block_size,
          block_beg,
          next_block_leftmost_minus_star,
          next_block_leftmost_minus_star_plus_rank[block_id],
          max_part_size,
          is_last_minus,
          text_filename,
          minus_pos_filenames[block_id],
          output_plus_pos_filenames[block_id],
          output_plus_symbols_filenames[block_id],
          output_plus_type_filenames[block_id],
          output_minus_pos_filenames[block_id],
          output_minus_type_filenames[block_id],
          output_minus_symbols_filenames[block_id],
          minus_block_count_targets[block_id],
          io_volume);

    next_block_leftmost_minus_star = ret.first;
    is_last_minus = ret.second;
  }

  // Update I/O volume.
  total_io_volume += io_volume;

  // Print summary.
  long double total_time = utils::wclock() - start;
  fprintf(stderr, "      Total time = %.2Lfs, I/O = %.2LfMiB/s, total I/O vol = %.1Lf bytes/symbol (of initial text)\n", total_time,
      (1.L * io_volume / (1L << 20)) / total_time, (1.L * total_io_volume) / initial_text_length);
}

template<typename char_type,
  typename text_offset_type>
std::pair<std::uint64_t, bool>
im_induce_suffixes_small_alphabet(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t block_beg,
    std::uint64_t next_block_leftmost_minus_star_plus,
    std::uint64_t next_block_leftmost_minus_star_plus_rank,
    std::uint64_t max_part_size,
    bool is_last_minus,
    std::string text_filename,
    std::string minus_pos_filename,
    std::string output_plus_pos_filename,
    std::string output_plus_symbols_filename,
    std::string output_plus_type_filename,
    std::string output_minus_pos_filename,
    std::string output_minus_type_filename,
    std::string output_minus_symbols_filename,
    std::uint64_t &minus_block_count_target,
    std::uint64_t &total_io_volume) {
  std::uint64_t block_end = std::min(text_length, block_beg + max_block_size);
  std::uint64_t block_size = block_end - block_beg;
  std::uint64_t next_block_size = std::min(max_block_size, text_length - block_end);
  std::uint64_t total_block_size = block_size + next_block_size;
  std::uint64_t io_volume = 0;

  if (text_alphabet_size == 0) {
    fprintf(stderr, "\nError: text_alphabet_size = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (max_block_size == 0) {
    fprintf(stderr, "\nError: max_block_size = 0\n");
    std::exit(EXIT_FAILURE);
  }

  if (text_length == 0) {
    fprintf(stderr, "\nError: text_length = 0\n");
    std::exit(EXIT_FAILURE);
  }

  // Check that all types are sufficiently large.
  if ((std::uint64_t)std::numeric_limits<char_type>::max() < text_alphabet_size - 1) {
    fprintf(stderr, "\nError: char_type in im_induce_suffixes_small_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<text_offset_type>::max() < text_length * 2UL) {
    fprintf(stderr, "\nError: text_offset_type in im_induce_suffixes_small_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }





  // Start the timer.
  long double start = utils::wclock();
  fprintf(stderr, "      Process block [%lu..%lu): ", block_beg, block_end);




  // Read block into RAM.
  char_type *block = utils::allocate_array<char_type>(block_size);
  std::fill(block, block + block_size, (char_type)0);
  utils::read_at_offset(block, block_beg, block_size, text_filename);
  io_volume += block_size * sizeof(char_type);






  // Initialize text accessor.
  typedef simple_accessor<char_type> accessor_type;
  accessor_type *text_accessor = new accessor_type(text_filename, (2UL << 20));







  // Read the symbol preceding block.
  char_type block_prec_symbol = 0;
  if (block_beg > 0)
    block_prec_symbol = text_accessor->access(block_beg - 1);








  // Initialize output writers.
  typedef async_stream_writer_multipart<text_offset_type> output_plus_pos_writer_type;
  typedef async_stream_writer_multipart<char_type> output_plus_symbols_writer_type;
  typedef async_bit_stream_writer output_plus_type_writer_type;
  output_plus_pos_writer_type *output_plus_pos_writer = new output_plus_pos_writer_type(output_plus_pos_filename, max_part_size, (2UL << 20), 4UL);
  output_plus_type_writer_type *output_plus_type_writer = new output_plus_type_writer_type(output_plus_type_filename, (2UL << 20), 4UL);
  output_plus_symbols_writer_type *output_plus_symbols_writer = new output_plus_symbols_writer_type(output_plus_symbols_filename, max_part_size, (2UL << 20), 4UL);







  // Compute type_bv that stores whether each of the
  // position is a minus position (true) or not (false).
  std::uint64_t bv_size = (total_block_size + 63) / 64;
  std::uint64_t *type_bv = utils::allocate_array<std::uint64_t>(bv_size);
  std::fill(type_bv, type_bv + bv_size, 0UL);
  {
    if (is_last_minus) {
      std::uint64_t i = total_block_size - 1;
      type_bv[i >> 6] |= (1UL << (i & 63));
    }
    bool is_next_minus = is_last_minus;
    std::uint64_t next_char = (next_block_size == 0) ? block[block_size - 1] : text_accessor->access(block_end + next_block_size - 1);
    for (std::uint64_t iplus = total_block_size - 1; iplus > 0; --iplus) {
      std::uint64_t i = iplus - 1;
      std::uint64_t head_char = (i < block_size) ? block[i] : text_accessor->access(block_beg + i);
      bool is_minus = (head_char == next_char) ? is_next_minus : (head_char > next_char);
      if (is_minus)
        type_bv[i >> 6] |= (1UL << (i & 63));
      is_next_minus = is_minus;
      next_char = head_char;
    }
  }



  // Determine whether the first position in the block is of minus star type.
  bool is_first_minus_star = ((block_beg > 0 && (type_bv[0] & 1UL) && (std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]));




  // Find the leftmost minus-star position in the current block.
  std::uint64_t this_block_leftmost_minus_star_plus = 1;  // plus because it's one index past actual position
  if (!is_first_minus_star) {
    while (this_block_leftmost_minus_star_plus < block_size && (type_bv[(this_block_leftmost_minus_star_plus - 1) >> 6] &
          (1UL << ((this_block_leftmost_minus_star_plus - 1) & 63))) > 0) ++this_block_leftmost_minus_star_plus;
    while (this_block_leftmost_minus_star_plus < block_size && (type_bv[(this_block_leftmost_minus_star_plus - 1) >> 6] &
          (1UL << ((this_block_leftmost_minus_star_plus - 1) & 63))) == 0) ++this_block_leftmost_minus_star_plus;
  }







  // Compute bucket sizes.
  text_offset_type *bucket_ptr = utils::allocate_array<text_offset_type>(text_alphabet_size);
  std::fill(bucket_ptr, bucket_ptr + text_alphabet_size, (text_offset_type)0);
  std::uint64_t lastpos = block_size + next_block_leftmost_minus_star_plus;
  bool is_lastpos_minus = (type_bv[(lastpos - 1) >> 6] & (1UL << ((lastpos - 1) & 63)));
  for (std::uint64_t i = 0; i < lastpos; ++i) {
    std::uint64_t head_char = (i < block_size) ? block[i] : text_accessor->access(block_beg + i);
    std::uint64_t ptr = bucket_ptr[head_char];
    bucket_ptr[head_char] = ++ptr;
  }









  // Compute pointers to bucket beginnings.
  std::uint64_t total_bucket_size = 0;
  for (std::uint64_t j = 0; j < text_alphabet_size; ++j) {
    std::uint64_t temp = bucket_ptr[j];
    bucket_ptr[j] = total_bucket_size;
    total_bucket_size += temp;
  }









  // Allocate buckets.
  text_offset_type *buckets = utils::allocate_array<text_offset_type>(total_bucket_size);
  std::fill(buckets, buckets + total_bucket_size, (text_offset_type)0);









  // Add minus positions at the beginning of buckets.
  std::uint64_t zero_item_pos = total_bucket_size;
  {
    typedef async_stream_reader<text_offset_type> reader_type;
    reader_type *reader = new reader_type(minus_pos_filename, (2UL << 20), 4UL);
    std::uint64_t rank = 0;
#if 0
    // Unubuffered version left for readability.
    while (!reader->empty()) {
      if (next_block_leftmost_minus_star_plus_rank == rank) {
        // Separatelly handle position lastpos - 1 if it
        // was in next block and it was minus star.
        std::uint64_t ii = lastpos - 1;
        std::uint64_t head_char = text_accessor->access(block_beg + ii);
        std::uint64_t ptr = bucket_ptr[head_char];
        buckets[ptr++] = ii;
        bucket_ptr[head_char] = ptr;
      }
      ++rank;

      {
        std::uint64_t i = reader->read();
        std::uint64_t head_char = (i < block_size) ? block[i] : text_accessor->access(block_beg + i);
        std::uint64_t ptr = bucket_ptr[head_char];
        if (i == 0) {
          zero_item_pos = ptr++;
          buckets[zero_item_pos] = 1;
        } else buckets[ptr++] = i;
        bucket_ptr[head_char] = ptr;
      }
    }
#else
#ifdef SAIS_DEBUG
    std::uint64_t local_bufsize = utils::random_int64(1L, 10L);
#else
    static const std::uint64_t local_bufsize = (1L << 15);
#endif
    local_buf_item_3 *local_buf = new local_buf_item_3[local_bufsize];
    text_offset_type *local_buf_pos = new text_offset_type[local_bufsize];
    std::uint64_t items_left = utils::file_size(minus_pos_filename) / sizeof(text_offset_type);
    while (items_left > 0) {
      // Compute buffer.
      std::uint64_t filled = std::min(local_bufsize, items_left);
      reader->read(local_buf_pos, filled);
      for (std::uint64_t t = 0; t < filled; ++t) {
        std::uint64_t pos = (std::uint64_t)local_buf_pos[t];
        if (pos >= block_size) pos = 0;
        local_buf[t].m_pos = pos;
      }
      for (std::uint64_t t = 0; t < filled; ++t) {
        std::uint64_t pos = local_buf[t].m_pos;
        local_buf[t].m_char = block[pos];
      }

      // Process buffer.
      for (std::uint64_t t = 0; t < filled; ++t) {
        if (next_block_leftmost_minus_star_plus_rank == rank) {
          // Separatelly handle position lastpos - 1 if it
          // was in next block and it was minus star.
          std::uint64_t ii = lastpos - 1;
          std::uint64_t head_char = text_accessor->access(block_beg + ii);
          std::uint64_t ptr = bucket_ptr[head_char];
          buckets[ptr++] = ii;
          bucket_ptr[head_char] = ptr;
        }
        ++rank;
        std::uint64_t i = local_buf_pos[t];
        std::uint64_t head_char = local_buf[t].m_char;
        if (i >= block_size) head_char = text_accessor->access(block_beg + i);
        std::uint64_t ptr = bucket_ptr[head_char];
        if (i == 0) {
          zero_item_pos = ptr++;
          buckets[zero_item_pos] = 1;
        } else buckets[ptr++] = i;
        bucket_ptr[head_char] = ptr;
      }

      // Update items_left.
      items_left -= filled;
    }
    delete[] local_buf;
    delete[] local_buf_pos;
#endif

    if (next_block_leftmost_minus_star_plus_rank == rank) {
      // Separatelly handle position lastpos - 1 if it
      // was in next block and it was minus star.
      std::uint64_t ii = lastpos - 1;
      std::uint64_t head_char = text_accessor->access(block_beg + ii);
      std::uint64_t ptr = bucket_ptr[head_char];
      buckets[ptr++] = ii;
      bucket_ptr[head_char] = ptr;
    }

    // Update I/O volume.
    io_volume += reader->bytes_read();

    // Clean up.
    delete reader;
    utils::file_delete(minus_pos_filename);
  }








  // Move pointers at the end of each bucket.
  for (std::uint64_t ch = 0; ch < text_alphabet_size; ++ch) {
    std::uint64_t next_bucket_beg = (ch + 1 == text_alphabet_size) ? total_bucket_size : (std::uint64_t)bucket_ptr[ch + 1];
    std::uint64_t this_bucket_end = bucket_ptr[ch];
    while (this_bucket_end < next_bucket_beg && (std::uint64_t)buckets[this_bucket_end] == 0)
      ++this_bucket_end;
    bucket_ptr[ch] = this_bucket_end;
  }











  // Induce plus suffixes.
  std::uint64_t local_minus_block_count_target = 0;
  bool seen_block_beg = false;
  if (!is_lastpos_minus) {
    // Add the last suffix if it was a plus type.
    std::uint64_t i = lastpos - 1;
    std::uint64_t head_char = (i < block_size) ? block[i] : text_accessor->access(block_beg + i);
    std::uint64_t ptr = bucket_ptr[head_char];
    if (i == 0) {
      zero_item_pos = --ptr;
      buckets[zero_item_pos] = 1;
    } else buckets[--ptr] = i;
    bucket_ptr[head_char] = ptr;
  }
#if 0
  for (std::uint64_t iplus = total_bucket_size; iplus > 0; --iplus) {
    std::uint64_t i = iplus - 1;
    if ((std::uint64_t)buckets[i] == 0) continue;

    std::uint64_t head_pos = buckets[i];
    if (i == zero_item_pos) head_pos = 0;
    std::uint64_t prev_pos = head_pos - 1;
    bool is_head_minus = (type_bv[head_pos >> 6] & (1UL << (head_pos & 63)));
    bool is_prev_pos_minus = (!head_pos) ? false : (type_bv[prev_pos >> 6] & (1UL << (prev_pos & 63)));
    std::uint64_t idx = (0 < head_pos && prev_pos < block_size) ? prev_pos : 0;
    std::uint64_t prev_pos_head_char = block[idx];

    if (is_head_minus) {
      // Erase the item (minus substring) from bucket.
      buckets[i] = 0;
      if (i == zero_item_pos)
        zero_item_pos = total_bucket_size;

      if (head_pos < block_size) {
        bool is_head_star = ((head_pos > 0 && !is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]));
        if (is_head_star) {
          // Here we visit all minus star suffixes staring in a block in min-rev order.
          // A good opportunity to update block_count_target.
          if (!seen_block_beg)
            ++local_minus_block_count_target;
          if (head_pos == 0) seen_block_beg = true;
        }
      }
    } else if (head_pos < block_size) {
      output_plus_pos_writer->write(head_pos);
      bool is_head_star = ((head_pos > 0 && is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]));
      output_plus_type_writer->write(is_head_star);
      if (!is_head_star) {
        // Erase the item (non-star plus substring) from the bucket.
        buckets[i] = 0;
        if (i == zero_item_pos)
          zero_item_pos = total_bucket_size;
      }
    }

    if (head_pos > 0) {
      if (!is_prev_pos_minus) {
        // Correct the value of prev_pos_char.
        if (prev_pos >= block_size)
          prev_pos_head_char = text_accessor->access(block_beg + prev_pos);
        std::uint64_t ptr = bucket_ptr[prev_pos_head_char];
        if (prev_pos == 0) {
          zero_item_pos = --ptr;
          buckets[zero_item_pos] = 1;
        } else buckets[--ptr] = prev_pos;
        bucket_ptr[prev_pos_head_char] = ptr;

        if (head_pos < block_size)
          output_plus_symbols_writer->write(prev_pos_head_char);
      }
    } else if (block_beg > 0) {
      bool is_head_star = is_head_minus ? ((std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]) : ((std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]);
      if (is_head_minus == is_head_star)
        output_plus_symbols_writer->write(block_prec_symbol);
    }
  }
#else
  {
#ifdef SAIS_DEBUG
    std::uint64_t local_bufsize = utils::random_int64(1L, 10L);
#else
    static const std::uint64_t local_bufsize = (1UL << 15);
#endif
    local_buf_item_2 *local_buf = new local_buf_item_2[local_bufsize];
    std::uint64_t iplus = total_bucket_size;
    while (iplus > 0) {
      // Skip empty positions.
      while (iplus > 0 && (std::uint64_t)buckets[iplus - 1] == 0) --iplus;

      // Compute buffer.
      std::uint64_t local_buf_filled = 0;
      while (local_buf_filled < local_bufsize && iplus > 0 && (std::uint64_t)buckets[iplus - 1] != 0) {
        --iplus;
        std::uint64_t head_pos = buckets[iplus];
        if (iplus == zero_item_pos) head_pos = 0;
        std::uint64_t prev_pos = head_pos - 1;
        local_buf[local_buf_filled].m_head_pos = head_pos;
        local_buf[local_buf_filled].m_idx_1 = (0 < head_pos && prev_pos < block_size) ? prev_pos : 0;
        local_buf[local_buf_filled].m_idx_2 = (0 < head_pos) ? prev_pos : 0;
        ++local_buf_filled;
      }
      for (std::uint64_t j = 0; j < local_buf_filled; ++j) {
        std::uint64_t idx_1 = local_buf[j].m_idx_1;
        std::uint64_t idx_2 = local_buf[j].m_idx_2;
        std::uint64_t head_pos = local_buf[j].m_head_pos;
        local_buf[j].m_prev_pos_head_char = block[idx_1];
        local_buf[j].m_is_head_minus = (type_bv[head_pos >> 6] & (1UL << (head_pos & 63)));
        local_buf[j].m_is_prev_pos_minus = (type_bv[idx_2 >> 6] & (1UL << (idx_2 & 63)));
      }

      // Process buffer.
      for (std::uint64_t j = 0; j < local_buf_filled; ++j) {
        std::uint64_t i = iplus + (local_buf_filled - j - 1);
        std::uint64_t head_pos = local_buf[j].m_head_pos;
        std::uint64_t prev_pos = head_pos - 1;
        std::uint64_t prev_pos_head_char = local_buf[j].m_prev_pos_head_char;
        bool is_head_minus = local_buf[j].m_is_head_minus;
        bool is_prev_pos_minus = local_buf[j].m_is_prev_pos_minus;

        if (is_head_minus) {
          buckets[i] = 0;  // erase the item
          if (i == zero_item_pos)
            zero_item_pos = total_bucket_size;
          if (head_pos < block_size) {
            bool is_head_star = ((head_pos > 0 && !is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]));
            if (is_head_star) {  // here we visi all minus star substrings so we can update block_count_target
              if (!seen_block_beg)
                ++local_minus_block_count_target;
              if (head_pos == 0) seen_block_beg = true;
            }
          }
        } else if (head_pos < block_size) {
          output_plus_pos_writer->write(head_pos);
          bool is_head_star = ((head_pos > 0 && is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]));
          output_plus_type_writer->write(is_head_star);
          if (!is_head_star) {
            buckets[i] = 0;  // erase the non-star plus substring
            if (i == zero_item_pos)
              zero_item_pos = total_bucket_size;
          }
        }
        if (head_pos > 0) {
          if (!is_prev_pos_minus) {
            if (prev_pos >= block_size)
              prev_pos_head_char = text_accessor->access(block_beg + prev_pos);
            std::uint64_t ptr = bucket_ptr[prev_pos_head_char];
            if (prev_pos == 0) {
              zero_item_pos = --ptr;
              buckets[zero_item_pos] = 1;
            } else buckets[--ptr] = prev_pos;
            bucket_ptr[prev_pos_head_char] = ptr;
            if (head_pos < block_size)
              output_plus_symbols_writer->write(prev_pos_head_char);
          }
        } else if (block_beg > 0) {
          bool is_head_star = is_head_minus ? ((std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]) : ((std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]);
          if (is_head_minus == is_head_star)
            output_plus_symbols_writer->write(block_prec_symbol);
        }
      }
    }
    delete[] local_buf;
  }
#endif
  if (!seen_block_beg)
    local_minus_block_count_target = std::numeric_limits<std::uint64_t>::max();





  // Update I/O volume.
  io_volume += output_plus_pos_writer->bytes_written() +
    output_plus_symbols_writer->bytes_written() + 
    output_plus_type_writer->bytes_written();



  // Clean up.
  delete output_plus_pos_writer;
  delete output_plus_symbols_writer;
  delete output_plus_type_writer;





  // Initialize output writers.
  typedef async_stream_writer_multipart<text_offset_type> output_minus_pos_writer_type;
  typedef async_bit_stream_writer output_minus_type_writer_type;
  typedef async_stream_writer_multipart<char_type> output_minus_symbols_writer_type;
  output_minus_pos_writer_type *output_minus_pos_writer = new output_minus_pos_writer_type(output_minus_pos_filename, max_part_size, (2UL << 20), 4UL);
  output_minus_type_writer_type *output_minus_type_writer = new output_minus_type_writer_type(output_minus_type_filename, (2UL << 20), 4UL);
  output_minus_symbols_writer_type *output_minus_symbols_writer = new output_minus_symbols_writer_type(output_minus_symbols_filename, max_part_size, (2UL << 20), 4UL);









  // Move pointers at the beginning of each buckets.
  for (std::uint64_t ch_plus = text_alphabet_size; ch_plus > 0; --ch_plus) {
    std::uint64_t ch = ch_plus - 1;
    std::uint64_t prev_bucket_end = (ch == 0) ? 0 : (std::uint64_t)bucket_ptr[ch - 1];
    std::uint64_t this_bucket_beg = bucket_ptr[ch];
    while (this_bucket_beg > prev_bucket_end && (std::uint64_t)buckets[this_bucket_beg - 1] == 0)
      --this_bucket_beg;
    bucket_ptr[ch] = this_bucket_beg;
  }









  // Induce minus suffixes.
  if (is_lastpos_minus) {
    // Add the last suffix if it was a minus type.
    std::uint64_t i = lastpos - 1;
    std::uint64_t head_char = (i < block_size) ? block[i] : text_accessor->access(block_beg + i);
    std::uint64_t ptr = bucket_ptr[head_char];
    if (i == 0) {
      zero_item_pos = ptr++;
      buckets[zero_item_pos] = 1;
    } else buckets[ptr++] = i;
    bucket_ptr[head_char] = ptr;
  }
#if 0
  for (std::uint64_t i = 0; i < total_bucket_size; ++i) {
    if ((std::uint64_t)buckets[i] == 0) continue;
    std::uint64_t head_pos = buckets[i];
    if (i == zero_item_pos) head_pos = 0;
    std::uint64_t prev_pos = head_pos - 1;

    bool is_head_minus = (type_bv[head_pos >> 6] & (1UL << (head_pos & 63)));
    bool is_prev_pos_minus = (!head_pos) ? false : (type_bv[prev_pos >> 6] & (1UL << (prev_pos & 63)));
    std::uint64_t idx = (prev_pos < block_size) ? prev_pos : 0;
    std::uint64_t prev_pos_head_char = block[idx];

    if (is_head_minus && head_pos < block_size) {
      bool is_star = ((head_pos > 0 && !is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]));
      output_minus_type_writer->write(is_star);
      output_minus_pos_writer->write(head_pos);
    }
    if (head_pos > 0) {
      if (is_prev_pos_minus) {
        if (prev_pos >= block_size)
          prev_pos_head_char = text_accessor->access(block_beg + prev_pos);
        std::uint64_t ptr = bucket_ptr[prev_pos_head_char];
        if (prev_pos == 0) {
          zero_item_pos = ptr++;
          buckets[zero_item_pos] = 1;
        } else buckets[ptr++] = prev_pos;
        bucket_ptr[prev_pos_head_char] = ptr;
        if (head_pos < block_size)
          output_minus_symbols_writer->write(prev_pos_head_char);
      }
    } else if (block_beg > 0) {
      bool is_head_star = is_head_minus ? ((std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]) : ((std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]);
      if (is_head_minus ^ is_head_star)
        output_minus_symbols_writer->write(block_prec_symbol);
    }
  }
#else
  {
#ifdef SAIS_DEBUG
    std::uint64_t local_bufsize = utils::random_int64(1L, 10L);
#else
    static const std::uint64_t local_bufsize = (1UL << 15);
#endif
    local_buf_item_2 *local_buf = new local_buf_item_2[local_bufsize];
    std::uint64_t i = 0;
    while (i < total_bucket_size) {
      // Skip empty positions.
      while (i < total_bucket_size && (std::uint64_t)buckets[i] == 0) ++i;

      // Compute buffer.
      std::uint64_t local_buf_filled = 0;
      while (local_buf_filled < local_bufsize && i < total_bucket_size && (std::uint64_t)buckets[i] != 0) {
        std::uint64_t head_pos = buckets[i];
        if (i == zero_item_pos) head_pos = 0;
        ++i;
        std::uint64_t prev_pos = head_pos - 1;
        local_buf[local_buf_filled].m_head_pos = head_pos;
        local_buf[local_buf_filled].m_idx_1 = (0 < head_pos && prev_pos < block_size) ? prev_pos : 0;
        local_buf[local_buf_filled].m_idx_2 = (0 < head_pos) ? prev_pos : 0;
        ++local_buf_filled;
      }
      for (std::uint64_t j = 0; j < local_buf_filled; ++j) {
        std::uint64_t idx_1 = local_buf[j].m_idx_1;
        std::uint64_t idx_2 = local_buf[j].m_idx_2;
        std::uint64_t head_pos = local_buf[j].m_head_pos;
        local_buf[j].m_prev_pos_head_char = block[idx_1];
        local_buf[j].m_is_head_minus = (type_bv[head_pos >> 6] & (1UL << (head_pos & 63)));
        local_buf[j].m_is_prev_pos_minus = (type_bv[idx_2 >> 6] & (1UL << (idx_2 & 63)));
      }

      // Process buffer.
      for (std::uint64_t j = 0; j < local_buf_filled; ++j) {
        std::uint64_t head_pos = local_buf[j].m_head_pos;
        std::uint64_t prev_pos = head_pos - 1;
        std::uint64_t prev_pos_head_char = local_buf[j].m_prev_pos_head_char;
        bool is_head_minus = local_buf[j].m_is_head_minus;
        bool is_prev_pos_minus = local_buf[j].m_is_prev_pos_minus;

        if (is_head_minus && head_pos < block_size) {
          bool is_star = ((head_pos > 0 && !is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]));
          output_minus_type_writer->write(is_star);
          output_minus_pos_writer->write(head_pos);
        }
        if (head_pos > 0) {
          if (is_prev_pos_minus) {
            if (prev_pos >= block_size)
              prev_pos_head_char = text_accessor->access(block_beg + prev_pos);
            std::uint64_t ptr = bucket_ptr[prev_pos_head_char];
            if (prev_pos == 0) {
              zero_item_pos = ptr++;
              buckets[zero_item_pos] = 1;
            } else buckets[ptr++] = prev_pos;
            bucket_ptr[prev_pos_head_char] = ptr;
            if (head_pos < block_size)
              output_minus_symbols_writer->write(prev_pos_head_char);
          }
        } else if (block_beg > 0) {
          bool is_head_star = is_head_minus ? ((std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]) : ((std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]);
          if (is_head_minus ^ is_head_star)
            output_minus_symbols_writer->write(block_prec_symbol);
        }
      }
    }
    delete[] local_buf;
  }
#endif



  // Update reference variables.
  minus_block_count_target = local_minus_block_count_target;





  // Update I/O volume.
  io_volume += output_minus_pos_writer->bytes_written() +
    output_minus_type_writer->bytes_written() +
    output_minus_symbols_writer->bytes_written() +
    text_accessor->bytes_read();





  // Update total I/O volume.
  total_io_volume += io_volume;




  // Store result.
  bool result = (type_bv[(block_size - 1) >> 6] & (1UL << ((block_size - 1) & 63)));





  // Clean up.
  delete output_minus_pos_writer;
  delete output_minus_type_writer;
  delete output_minus_symbols_writer;
  delete text_accessor;
  utils::deallocate(type_bv);
  utils::deallocate(buckets);
  utils::deallocate(bucket_ptr);
  utils::deallocate(block);





  // Print summary.
  long double total_time = utils::wclock() - start;
  fprintf(stderr, "time = %.2Lfs, I/O = %.2LfMiB/s\n", total_time,
      (1.L * io_volume / (1L << 20)) / total_time);




  // Return result.
  return std::make_pair(this_block_leftmost_minus_star_plus, result);
}

template<typename char_type,
  typename text_offset_type>
void im_induce_suffixes_small_alphabet(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t initial_text_length,
    std::uint64_t max_block_size,
    std::vector<std::uint64_t> &next_block_leftmost_minus_star_plus_rank,
    std::string text_filename,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &output_plus_pos_filenames,
    std::vector<std::string> &output_plus_symbols_filenames,
    std::vector<std::string> &output_plus_type_filenames,
    std::vector<std::string> &output_minus_pos_filenames,
    std::vector<std::string> &output_minus_type_filenames,
    std::vector<std::string> &output_minus_symbols_filenames,
    std::vector<std::uint64_t> &minus_block_count_targets,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  std::uint64_t io_volume = 0;

  fprintf(stderr, "    IM induce suffixes (small alphabet):\n");
  long double start = utils::wclock();

#ifdef SAIS_DEBUG
  std::uint64_t max_part_size = utils::random_int64(1L, 50L);
#else
  std::uint64_t max_part_size = std::max((1UL << 20), max_block_size / 10UL);
  fprintf(stderr, "      Max part size = %lu (%.1LfMiB)\n", max_part_size, (1.L * max_part_size) / (1UL << 20));
#endif

  bool is_last_minus = true;
  std::uint64_t next_block_leftmost_minus_star = 0;
  for (std::uint64_t block_id_plus = n_blocks; block_id_plus > 0; --block_id_plus) {
    std::uint64_t block_id = block_id_plus - 1;
    std::uint64_t block_beg = block_id * max_block_size;

    std::pair<std::uint64_t, bool > ret;
    ret = im_induce_suffixes_small_alphabet<
      char_type,
      text_offset_type>(
          text_alphabet_size,
          text_length,
          max_block_size,
          block_beg,
          next_block_leftmost_minus_star,
          next_block_leftmost_minus_star_plus_rank[block_id],
          max_part_size,
          is_last_minus,
          text_filename,
          minus_pos_filenames[block_id],
          output_plus_pos_filenames[block_id],
          output_plus_symbols_filenames[block_id],
          output_plus_type_filenames[block_id],
          output_minus_pos_filenames[block_id],
          output_minus_type_filenames[block_id],
          output_minus_symbols_filenames[block_id],
          minus_block_count_targets[block_id],
          io_volume);

    next_block_leftmost_minus_star = ret.first;
    is_last_minus = ret.second;
  }

  // Update I/O volume.
  total_io_volume += io_volume;

  // Print summary.
  long double total_time = utils::wclock() - start;
  fprintf(stderr, "      Total time = %.2Lfs, I/O = %.2LfMiB/s, total I/O vol = %.1Lf bytes/symbol (of initial text)\n", total_time,
      (1.L * io_volume / (1L << 20)) / total_time, (1.L * total_io_volume) / initial_text_length);
}

template<typename char_type,
  typename text_offset_type>
void im_induce_suffixes(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t initial_text_length,
    std::uint64_t max_block_size,
    std::vector<std::uint64_t> &next_block_leftmost_minus_star_plus_rank,
    std::string text_filename,
    std::vector<std::string> &minus_pos_filenames,
    std::vector<std::string> &output_plus_pos_filenames,
    std::vector<std::string> &output_plus_symbols_filenames,
    std::vector<std::string> &output_plus_type_filenames,
    std::vector<std::string> &output_minus_pos_filenames,
    std::vector<std::string> &output_minus_type_filenames,
    std::vector<std::string> &output_minus_symbols_filenames,
    std::vector<std::uint64_t> &minus_block_count_targets,
    std::uint64_t &total_io_volume,
    bool is_small_alphabet) {
  if (is_small_alphabet) {
    im_induce_suffixes_small_alphabet<char_type, text_offset_type>(text_alphabet_size, text_length, initial_text_length,
        max_block_size, next_block_leftmost_minus_star_plus_rank, text_filename, minus_pos_filenames, output_plus_pos_filenames,
        output_plus_symbols_filenames, output_plus_type_filenames, output_minus_pos_filenames, output_minus_type_filenames,
        output_minus_symbols_filenames, minus_block_count_targets, total_io_volume);
  } else {
    im_induce_suffixes_large_alphabet<char_type, text_offset_type>(text_alphabet_size, text_length, initial_text_length,
        max_block_size, next_block_leftmost_minus_star_plus_rank, text_filename, minus_pos_filenames, output_plus_pos_filenames,
        output_plus_symbols_filenames, output_plus_type_filenames, output_minus_pos_filenames, output_minus_type_filenames,
        output_minus_symbols_filenames, minus_block_count_targets, total_io_volume);
  }
}

}  // namespace fsais_private

#endif  // __FSAIS_SRC_IM_INDUCE_SUFFIXES_HPP_INCLUDED
