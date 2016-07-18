#ifndef __IM_INDUCE_PLUS_STAR_SUBSTRINGS_HPP_INCLUDED
#define __IM_INDUCE_PLUS_STAR_SUBSTRINGS_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <queue>
#include <algorithm>

#include "utils.hpp"
#include "packed_pair.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"


//=============================================================================
// Assumptions:
// - char_type has to be able to hold any symbol from the text.
// - text_offset_type can encode integer in range [0..text_length),
//   not necessarily integer text_length and larger.
// - ext_block_offset_type can encode integer in range [0..2 * max_block_size),
//   not necessarily integer 2 * max_block_size and larger.
// All types are assumed to be unsigned.
//
// This version of the function uses
//   block_size / 4                                      // type bitvector
// + 2 * block_size * sizeof(char_type)                  // text blocks
// + 2 * block_size * sizeof(ext_block_offset_type)      // items in queues
// + text_alphabet_size * sizeof(ext_block_offset_type)  // bucket pointers
// bytes of RAM.
//=============================================================================

template<typename char_type, typename text_offset_type, typename ext_block_offset_type>
void im_induce_plus_star_substrings(
    const char_type *block,
    const char_type *nextblock,
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t block_beg,
    bool is_next_block_last_pos_minus,
    std::string plus_substrings_filename) {
  std::uint64_t block_end = std::min(text_length, block_beg + max_block_size);
  std::uint64_t block_size = block_end - block_beg;
  std::uint64_t next_block_size = std::min(max_block_size, text_length - block_end);
  std::uint64_t total_block_size = block_size + next_block_size;








  typedef async_stream_writer<text_offset_type> plus_writer_type;
  plus_writer_type *plus_writer = new plus_writer_type(plus_substrings_filename);







  // Compute type_bv that stores whether each of the
  // position is a minus position (true) or not (false).
  std::uint64_t bv_size = (total_block_size + 63) / 64;
  std::uint64_t *type_bv = new std::uint64_t[bv_size];
  std::fill(type_bv, type_bv + bv_size, 0UL);
  {
    bool is_next_minus = false;
    if (next_block_size > 0) {
      if (is_next_block_last_pos_minus) {
        std::uint64_t idx = total_block_size - 1;
        type_bv[idx >> 6] |= (1UL << (idx & 63));
      }
      is_next_minus = is_next_block_last_pos_minus;
      for (std::uint64_t iplus = next_block_size - 1; iplus > 0; --iplus) {
        std::uint64_t i = iplus - 1;
        bool is_minus = (nextblock[i] == nextblock[iplus]) ? is_next_minus : (nextblock[i] > nextblock[iplus]);
        if (is_minus) type_bv[(i + block_size) >> 6] |= (1UL << ((i + block_size) & 63));
        is_next_minus = is_minus;
      }
      std::uint64_t iplus = 0;
      std::uint64_t i = block_size - 1;
      bool is_minus = (block[i] == nextblock[iplus]) ? is_next_minus : (block[i] > nextblock[iplus]);
      if (is_minus) type_bv[i >> 6] |= (1UL << (i & 63));
      is_next_minus = is_minus;
    } else {
      std::uint64_t i = block_size - 1;
      type_bv[i >> 6] |= (1UL << (i & 63));
      is_next_minus = true;
    }
    for (std::uint64_t iplus = block_size - 1; iplus > 0; --iplus) {
      std::uint64_t i = iplus - 1;
      bool is_minus = (block[i] == block[iplus]) ? is_next_minus : (block[i] > block[iplus]);
      if (is_minus) type_bv[i >> 6] |= (1UL << (i & 63));
      is_next_minus = is_minus;
    }
  }






  // Compute queue pointers.
  ext_block_offset_type *queue_pointers = new ext_block_offset_type[text_alphabet_size];
  std::fill(queue_pointers, queue_pointers + text_alphabet_size, (ext_block_offset_type)0);
  {
    if ((type_bv[(block_size - 1) >> 6] & (1UL << ((block_size - 1) & 63))) == 0) {
      std::uint64_t pos = block_size;
      while (pos + 1 < total_block_size && (type_bv[pos >> 6] & (1UL << (pos & 63))) == 0)
        ++pos;

      {
        std::uint64_t head_char = nextblock[pos - block_size];
        if ((type_bv[pos >> 6] & (1UL << (pos & 63))) > 0)
          queue_pointers[head_char] = (std::uint64_t)queue_pointers[head_char] + 1;
        else queue_pointers[head_char + 1] = (std::uint64_t)queue_pointers[head_char + 1] + 1;
      }

      for (std::uint64_t i = block_size; i < pos; ++i) {
        std::uint64_t head_char = nextblock[i - block_size];
        queue_pointers[head_char + 1] = (std::uint64_t)queue_pointers[head_char + 1] + 1;
      }
    }
    for (std::uint64_t iplus = block_size; iplus > 0; --iplus) {
      std::uint64_t i = iplus - 1;
      if (i > 0 && (type_bv[i >> 6] & (1UL << (i & 63))) > 0 &&
          (type_bv[(i - 1) >> 6] & (1UL << ((i - 1) & 63))) == 0) {
        std::uint64_t head_char = block[i];
        queue_pointers[head_char] = (std::uint64_t)queue_pointers[head_char] + 1;
      } else if ((type_bv[i >> 6] & (1UL << (i & 63))) == 0) {
        std::uint64_t head_char = block[i];
        queue_pointers[head_char + 1] = (std::uint64_t)queue_pointers[head_char + 1] + 1;
      }
    }
  }
  for (std::uint64_t j = 1; j < text_alphabet_size; ++j)
    queue_pointers[j] = (std::uint64_t)queue_pointers[j] + (std::uint64_t)queue_pointers[j - 1];










  // Allocate queues.
  std::uint64_t total_queue_size = queue_pointers[text_alphabet_size - 1];
  ext_block_offset_type *combined_queues = new ext_block_offset_type[total_queue_size];









  // Add initial items to queues.
  {
    if ((type_bv[(block_size - 1) >> 6] & (1UL << ((block_size - 1) & 63))) == 0) {
      std::uint64_t pos = block_size;
      while (pos + 1 < total_block_size && (type_bv[pos >> 6] & (1UL << (pos & 63))) == 0)
        ++pos;
      std::uint64_t head_char = nextblock[pos - block_size];
      if ((type_bv[pos >> 6] & (1UL << (pos & 63))) > 0) {
        queue_pointers[head_char] = (std::uint64_t)queue_pointers[head_char] - 1;
        combined_queues[(std::uint64_t)queue_pointers[head_char]] = pos;
      } else {
        queue_pointers[head_char + 1] = (std::uint64_t)queue_pointers[head_char + 1] - 1;
        combined_queues[(std::uint64_t)queue_pointers[head_char + 1]] = pos;
      }
    }

    for (std::uint64_t iplus = block_size; iplus > 0; --iplus) {
      std::uint64_t i = iplus - 1;
      if (i > 0 && (type_bv[i >> 6] & (1UL << (i & 63))) > 0 &&
          (type_bv[(i - 1) >> 6] & (1UL << ((i - 1) & 63))) == 0) {
        std::uint64_t head_char = block[i];
        queue_pointers[head_char] = (std::uint64_t)queue_pointers[head_char] - 1;
        combined_queues[(std::uint64_t)queue_pointers[head_char]] = i;
      }
    }
  }







  // Inducing.
  for (std::uint64_t suf_ptr_plus = total_queue_size; suf_ptr_plus > 0; --suf_ptr_plus) {
    std::uint64_t head_pos = combined_queues[suf_ptr_plus - 1];
    if ((type_bv[head_pos >> 6] & (1UL << (head_pos & 63))) == 0 && head_pos < block_size)
      plus_writer->write(block_beg + head_pos);
    if (head_pos > 0) {
      std::uint64_t prev_pos = head_pos - 1;
      std::uint64_t prev_pos_head_char = (prev_pos < block_size) ? block[prev_pos] : nextblock[prev_pos - block_size];
      if ((type_bv[prev_pos >> 6] & (1UL << (prev_pos & 63))) == 0) {
        queue_pointers[prev_pos_head_char + 1] = (std::uint64_t)queue_pointers[prev_pos_head_char + 1] - 1;
        combined_queues[(std::uint64_t)queue_pointers[prev_pos_head_char + 1]] = head_pos - 1;
      }
    }
  }







  delete plus_writer;
  delete[] type_bv;
  delete[] combined_queues;
  delete[] queue_pointers;
}

#endif  // __IM_INDUCE_PLUS_STAR_SUBSTRINGS_HPP_INCLUDED
