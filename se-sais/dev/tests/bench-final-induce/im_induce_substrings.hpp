#ifndef __IM_INDUCE_SUBSTRINGS_HPP_INCLUDED
#define __IM_INDUCE_SUBSTRINGS_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <limits>
#include <queue>
#include <algorithm>

#include "packed_pair.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_writer.hpp"
#include "io/simple_accessor.hpp"

#include "uint40.hpp"
#include "uint48.hpp"
#include "utils.hpp"


//=============================================================================
// Assumptions:
// - char_type has to be able to hold any symbol from the text.
// - text_offset_type can encode integer in range [0..text_length)
//   not necessarily integer text_length and larger.
// - block_offset_type can encode integers in range [0..max_block_size).
// - ext_block_offset_type can encode integer in range [0..2 * max_block_size],
//   not necessarily largers integers. *** Note the bound is inclusive ***
//
// All types are assumed to be unsigned.
//
// This version of the function uses
//   block_size / 4                                      // type bitvector
// + block_size * sizeof(char_type)                      // text block
// + 2 * block_size * sizeof(ext_block_offset_type)      // buckets
// + text_alphabet_size * sizeof(ext_block_offset_type)  // bucket pointers
// bytes of RAM.
//=============================================================================
// TODO:
// - optimize random accessed using buffer
// - reduce the RAM usage for integer array.
//=============================================================================

struct local_buf_item {
  local_buf_item() {}
  std::uint64_t m_head_pos;
  std::uint64_t m_prev_pos_head_char;
  std::uint64_t m_idx_1;
  std::uint64_t m_idx_2;
  bool m_is_head_minus;
  bool m_is_prev_pos_minus;
};

template<typename char_type,
  typename block_offset_type,
  typename ext_block_offset_type>
std::pair<std::uint64_t, bool>
im_induce_substrings_small_alphabet(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t block_beg,
    std::uint64_t next_block_leftmost_minus_star_plus,
    bool is_last_minus,
    std::string text_filename,
    std::string output_plus_symbols_filename,
    std::string output_plus_type_filename,
    std::string output_minus_pos_filename,
    std::string output_minus_type_filename,
    std::string output_minus_symbols_filename,
    std::uint64_t &plus_block_count_target,
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
    fprintf(stderr, "\nError: char_type in im_induce_substrings_small_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<block_offset_type>::max() < max_block_size - 1) {
    fprintf(stderr, "\nError: block_offset_type in im_induce_substrings_small_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }
  if ((std::uint64_t)std::numeric_limits<ext_block_offset_type>::max() < max_block_size / 2UL) {
    fprintf(stderr, "\nError: ext_block_offset_type in im_induce_substrings_small_alphabet too small!\n");
    std::exit(EXIT_FAILURE);
  }





  // Start the timer.
  long double start = utils::wclock();
  fprintf(stderr, "    Process block [%lu..%lu): ", block_beg, block_end);




  // Read block into RAM.
  char_type *block = new char_type[block_size];
  utils::read_at_offset(block, block_beg, block_size, text_filename);
  io_volume += block_size * sizeof(char_type);






  // Initialize text accessor.
  typedef simple_accessor<char_type> accessor_type;
  accessor_type *text_accessor = new accessor_type(text_filename);







  // Read the symbol preceding block.
  char_type block_prec_symbol = 0;
  if (block_beg > 0)
    block_prec_symbol = text_accessor->access(block_beg - 1);








  // Initialize output writers.
  typedef async_stream_writer<char_type> output_plus_symbols_writer_type;
  typedef async_bit_stream_writer output_plus_type_writer_type;
  typedef async_stream_writer<block_offset_type> output_minus_pos_writer_type;
  typedef async_bit_stream_writer output_minus_type_writer_type;
  typedef async_stream_writer<char_type> output_minus_symbols_writer_type;
  output_plus_symbols_writer_type *output_plus_symbols_writer = new output_plus_symbols_writer_type(output_plus_symbols_filename);
  output_plus_type_writer_type *output_plus_type_writer = new output_plus_type_writer_type(output_plus_type_filename);
  output_minus_pos_writer_type *output_minus_pos_writer = new output_minus_pos_writer_type(output_minus_pos_filename);
  output_minus_type_writer_type *output_minus_type_writer = new output_minus_type_writer_type(output_minus_type_filename);
  output_minus_symbols_writer_type *output_minus_symbols_writer = new output_minus_symbols_writer_type(output_minus_symbols_filename);







  // Compute type_bv that stores whether each of the
  // position is a minus position (true) or not (false).
  std::uint64_t bv_size = (total_block_size + 63) / 64;
  std::uint64_t *type_bv = new std::uint64_t[bv_size];
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
  ext_block_offset_type *bucket_ptr = new ext_block_offset_type[text_alphabet_size];
  std::fill(bucket_ptr, bucket_ptr + text_alphabet_size, (ext_block_offset_type)0);
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
  ext_block_offset_type *buckets = new ext_block_offset_type[total_bucket_size];
  std::fill(buckets, buckets + total_bucket_size, (ext_block_offset_type)0);










  // Add minus positions at the beginning of buckets.
  std::uint64_t zero_item_pos = total_bucket_size;
  for (std::uint64_t i = 0; i < block_size; ++i) {
    bool is_minus_star = false;
    if (i == 0) is_minus_star = is_first_minus_star;
    else is_minus_star = ((type_bv[i >> 6] & (1UL << (i & 63))) > 0 &&
        (type_bv[(i - 1) >> 6] & (1UL << ((i - 1) & 63))) == 0);

    if (is_minus_star) {
      std::uint64_t head_char = (i < block_size) ? block[i] : text_accessor->access(block_beg + i);
      std::uint64_t ptr = bucket_ptr[head_char];
      if (i == 0) {
        zero_item_pos = ptr++;
        buckets[zero_item_pos] = 1;
      } else buckets[ptr++] = i;
      bucket_ptr[head_char] = ptr;
    }
  }

  // Separatelly handle position lastpos - 1 if it
  // was in next block and it was minus star.
  if (lastpos > block_size && is_lastpos_minus) {
    std::uint64_t i = lastpos - 1;
    std::uint64_t head_char = text_accessor->access(block_beg + i);
    std::uint64_t ptr = bucket_ptr[head_char];
    buckets[ptr++] = i;
    bucket_ptr[head_char] = ptr;
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
  std::uint64_t local_plus_block_count_target = 0;
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
  // The non-buffered version is kept for readability.
  for (std::uint64_t iplus = total_bucket_size; iplus > 0; --iplus) {
    std::uint64_t i = iplus - 1;
    if ((std::uint64_t)buckets[i] == 0) continue;

    // Process buckets[i] ========================================================================================================================
    {
      std::uint64_t head_pos = buckets[i];
      if (i == zero_item_pos) head_pos = 0;
      std::uint64_t prev_pos = head_pos - 1;

      bool is_head_minus = (type_bv[head_pos >> 6] & (1UL << (head_pos & 63)));
      bool is_prev_pos_minus = (!head_pos) ? false : (type_bv[prev_pos >> 6] & (1UL << (prev_pos & 63)));
      std::uint64_t temp_idx = (0 < head_pos && prev_pos < block_size && !is_prev_pos_minus) ? prev_pos : 0;
      std::uint64_t prev_pos_head_char = block[temp_idx];

      if (!seen_block_beg && head_pos < block_size)
        ++local_plus_block_count_target;
      if (head_pos == 0) seen_block_beg = true;


      if (is_head_minus) {
        // Erase the item (minus substring) from bucket.
        buckets[i] = 0;
        if (i == zero_item_pos)
          zero_item_pos = total_bucket_size;
      } else if (head_pos < block_size) {
        bool is_head_star = ((head_pos > 0 && is_prev_pos_minus) || (!head_pos && block_beg && (std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]));
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
    //==========================================================================================================================================
  }
#else
  {
    static const std::uint64_t local_bufsize = (1UL << 15);
    local_buf_item *local_buf = new local_buf_item[local_bufsize];
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

        // Process next item in the bucket.
        if (!seen_block_beg && head_pos < block_size)
          ++local_plus_block_count_target;
        if (head_pos == 0) seen_block_beg = true;
        if (is_head_minus) {
          // Erase the item (minus substring) from bucket.
          buckets[i] = 0;
          if (i == zero_item_pos)
            zero_item_pos = total_bucket_size;
        } else if (head_pos < block_size) {
          bool is_head_star = ((head_pos > 0 && is_prev_pos_minus) || (!head_pos && block_beg && (std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]));
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
    local_plus_block_count_target = std::numeric_limits<std::uint64_t>::max();





  // Update I/O volume.
  io_volume += output_plus_symbols_writer->bytes_written() + 
    output_plus_type_writer->bytes_written();



  // Clean up.
  delete output_plus_symbols_writer;
  delete output_plus_type_writer;





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
  std::uint64_t local_minus_block_count_target = 0;
  seen_block_beg = false;
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

    // Random accesses.
    std::uint64_t head_pos = buckets[i];
    if (i == zero_item_pos) head_pos = 0;
    std::uint64_t prev_pos = head_pos - 1;
    bool is_head_minus = (type_bv[head_pos >> 6] & (1UL << (head_pos & 63)));
    bool is_prev_pos_minus = (!head_pos) ? false : (type_bv[prev_pos >> 6] & (1UL << (prev_pos & 63)));
    std::uint64_t temp_idx = (0 < head_pos && prev_pos < block_size && is_prev_pos_minus) ? prev_pos : 0;
    std::uint64_t prev_pos_head_char = block[temp_idx];

    if (!seen_block_beg && head_pos < block_size)
      ++local_minus_block_count_target;
    if (head_pos == 0) seen_block_beg = true;

    if (is_head_minus && head_pos < block_size) {
      bool is_star = ((head_pos > 0 && !is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]));
      output_minus_type_writer->write(is_star);
      if (is_star) output_minus_pos_writer->write(head_pos);
    }

    if (head_pos > 0) {
      if (is_prev_pos_minus) {
        if (prev_pos >= block_size) prev_pos_head_char = text_accessor->access(block_beg + prev_pos);
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
      bool is_head_star = is_head_minus ? ((std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]) :
        ((std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]);
      if (is_head_minus ^ is_head_star)
        output_minus_symbols_writer->write(block_prec_symbol);
    }
  }
#else
  {
    static const std::uint64_t local_bufsize = (1UL << 15);
    local_buf_item *local_buf = new local_buf_item[local_bufsize];
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

        if (!seen_block_beg && head_pos < block_size)
          ++local_minus_block_count_target;
        if (head_pos == 0) seen_block_beg = true;

        if (is_head_minus && head_pos < block_size) {
          bool is_star = ((head_pos > 0 && !is_prev_pos_minus) || (!head_pos && block_beg > 0 && (std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]));
          output_minus_type_writer->write(is_star);
          if (is_star) output_minus_pos_writer->write(head_pos);
        }

        if (head_pos > 0) {
          if (is_prev_pos_minus) {
            if (prev_pos >= block_size) prev_pos_head_char = text_accessor->access(block_beg + prev_pos);
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
          bool is_head_star = is_head_minus ? ((std::uint64_t)block_prec_symbol < (std::uint64_t)block[0]) :
            ((std::uint64_t)block_prec_symbol > (std::uint64_t)block[0]);
          if (is_head_minus ^ is_head_star)
            output_minus_symbols_writer->write(block_prec_symbol);
        }
      }
    }
    delete[] local_buf;
  }
#endif
  if (!seen_block_beg)
    local_minus_block_count_target = std::numeric_limits<std::uint64_t>::max();




  // Update reference variables.
  plus_block_count_target = local_plus_block_count_target;
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
  delete[] type_bv;
  delete[] buckets;
  delete[] bucket_ptr;
  delete[] block;





  // Print summary.
  long double total_time = utils::wclock() - start;
  fprintf(stderr, "time = %.2Lfs, I/O = %.2LfMiB/s\n", total_time,
      (1.L * io_volume / (1L << 20)) / total_time);




  // Return result.
  return std::make_pair(this_block_leftmost_minus_star_plus, result);
}

template<typename char_type,
  typename block_offset_type,
  typename ext_block_offset_type>
void im_induce_substrings_small_alphabet(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::string text_filename,
    std::vector<std::string> &output_plus_symbols_filenames,
    std::vector<std::string> &output_plus_type_filenames,
    std::vector<std::string> &output_minus_pos_filenames,
    std::vector<std::string> &output_minus_type_filenames,
    std::vector<std::string> &output_minus_symbols_filenames,
    std::vector<std::uint64_t> &plus_block_count_targets,
    std::vector<std::uint64_t> &minus_block_count_targets,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  std::uint64_t io_volume = 0;

  fprintf(stderr, "  IM induce substrings (small alphabet):\n");
  fprintf(stderr, "    sizeof(ext_block_offset_type) = %lu\n", sizeof(ext_block_offset_type));
  long double start = utils::wclock();

  bool is_last_minus = true;
  std::uint64_t next_block_leftmost_minus_star = 0;
  for (std::uint64_t block_id_plus = n_blocks; block_id_plus > 0; --block_id_plus) {
    std::uint64_t block_id = block_id_plus - 1;
    std::uint64_t block_beg = block_id * max_block_size;

    std::pair<std::uint64_t, bool > ret;
    ret = im_induce_substrings_small_alphabet<
      char_type,
      block_offset_type,
      ext_block_offset_type>(
          text_alphabet_size,
          text_length,
          max_block_size,
          block_beg,
          next_block_leftmost_minus_star,
          is_last_minus,
          text_filename,
          output_plus_symbols_filenames[block_id],
          output_plus_type_filenames[block_id],
          output_minus_pos_filenames[block_id],
          output_minus_type_filenames[block_id],
          output_minus_symbols_filenames[block_id],
          plus_block_count_targets[block_id],
          minus_block_count_targets[block_id],
          io_volume);

    next_block_leftmost_minus_star = ret.first;
    is_last_minus = ret.second;
  }

  // Update I/O volume.
  total_io_volume += io_volume;

  // Print summary.
  long double total_time = utils::wclock() - start;
  fprintf(stderr, "    Total time = %.2Lfs, I/O = %.2LfMiB/s, total I/O vol = %.1Lfn bytes\n", total_time,
      (1.L * io_volume / (1L << 20)) / total_time, (1.L * total_io_volume) / text_length);
}

template<typename char_type,
  typename block_offset_type>
void im_induce_substrings_small_alphabet(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::string text_filename,
    std::vector<std::string> &output_plus_symbols_filenames,
    std::vector<std::string> &output_plus_type_filenames,
    std::vector<std::string> &output_minus_pos_filenames,
    std::vector<std::string> &output_minus_type_filenames,
    std::vector<std::string> &output_minus_symbols_filenames,
    std::vector<std::uint64_t> &plus_block_count_targets,
    std::vector<std::uint64_t> &minus_block_count_targets,
    std::uint64_t &total_io_volume) {
  if (max_block_size < (1UL << 31))
    im_induce_substrings_small_alphabet<char_type, block_offset_type, std::uint32_t>(text_alphabet_size, text_length,
        max_block_size, text_filename, output_plus_symbols_filenames, output_plus_type_filenames, output_minus_pos_filenames,
        output_minus_type_filenames, output_minus_symbols_filenames, plus_block_count_targets, minus_block_count_targets, total_io_volume);
  else if (max_block_size < (1UL < 39))
    im_induce_substrings_small_alphabet<char_type, block_offset_type, uint40>(text_alphabet_size, text_length,
        max_block_size, text_filename, output_plus_symbols_filenames, output_plus_type_filenames, output_minus_pos_filenames,
        output_minus_type_filenames, output_minus_symbols_filenames, plus_block_count_targets, minus_block_count_targets, total_io_volume);
  else
    im_induce_substrings_small_alphabet<char_type, block_offset_type, std::uint64_t>(text_alphabet_size, text_length,
        max_block_size, text_filename, output_plus_symbols_filenames, output_plus_type_filenames, output_minus_pos_filenames,
        output_minus_type_filenames, output_minus_symbols_filenames, plus_block_count_targets, minus_block_count_targets, total_io_volume);
}

template<typename char_type,
  typename block_offset_type>
void im_induce_substrings(
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::string text_filename,
    std::vector<std::string> &output_plus_symbols_filenames,
    std::vector<std::string> &output_plus_type_filenames,
    std::vector<std::string> &output_minus_pos_filenames,
    std::vector<std::string> &output_minus_type_filenames,
    std::vector<std::string> &output_minus_symbols_filenames,
    std::vector<std::uint64_t> &plus_block_count_targets,
    std::vector<std::uint64_t> &minus_block_count_targets,
    std::uint64_t &total_io_volume) {
  if (text_alphabet_size <= 100000000) {  // XXX
    im_induce_substrings_small_alphabet<char_type, block_offset_type>(text_alphabet_size, text_length,
        max_block_size, text_filename, output_plus_symbols_filenames, output_plus_type_filenames, output_minus_pos_filenames,
        output_minus_type_filenames, output_minus_symbols_filenames, plus_block_count_targets, minus_block_count_targets, total_io_volume);
  } else {
    fprintf(stderr, "\nError: im_induce_substrings_large_alphabet not implemented yet (text_alphabet_size = %lu)!\n", text_alphabet_size);
    fprintf(stderr, "Try increasing the threshold in im_induce_substrings if text_alphabet_size does significantly exceed 2000000.\n");
    std::exit(EXIT_FAILURE);
  }
}

#endif  // __IM_INDUCE_SUBSTRINGS_HPP_INCLUDED
