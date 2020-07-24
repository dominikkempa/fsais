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
// - block_offset_type can encode integer in range [0..max_block_size),
//   not necessarily integer max_block_size and larger.
// All types are assumed to be unsigned.
//
// This version of the function uses
//   block_size / 8                           // for type bitvector
// + block_size * sizeof(char_type)           // for the text
// + block_size * sizeof(blockoff_t)          // for the items in queues
// + text_alphabet_size * sizeof(blockoff_t)  // for the bucket pointers
// bytes of RAM.
//
// NOTE: rather than using a sepaarate bitvector to store positions types,
// we could use the MSB of position integers, but that could be problematic.
// In some cases (such as inserting elements on the radix_heap) it is
// impossible to use the bitvector and only then we use the MSB trick.
//=============================================================================

template<typename char_type, typename text_offset_type, typename block_offset_type>
void im_induce_plus_star_substrings(
    const char_type *block,
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t block_beg,
    std::uint64_t extract_count_target,
    bool is_last_suf_minus,
    std::string plus_substrings_filename) {
  std::uint64_t block_end = std::min(text_length, block_beg + max_block_size);
  std::uint64_t block_size = block_end - block_beg;




  typedef std::queue<block_offset_type> queue_type;
  queue_type **queues = new queue_type*[text_alphabet_size];
  for (std::uint64_t queue_id = 0; queue_id < text_alphabet_size; ++queue_id)
    queues[queue_id] = new queue_type();




  typedef async_stream_writer<text_offset_type> plus_writer_type;
  plus_writer_type *plus_writer = new plus_writer_type(plus_substrings_filename);




  // Allocate type_bitvector that stores whether each of the
  // position in the block is a minus position (1) or not (0).
  std::uint64_t *type_bv;
  {
    std::uint64_t bv_size = (block_size + 63) / 64;
    type_bv = new std::uint64_t[bv_size];
    std::fill(type_bv, type_bv + bv_size, 0UL);
  }



  // Compute type_bv.
  {
    // Store the type of position block_size - 1.
    if (is_last_suf_minus) {
      std::uint64_t idx = block_size - 1;
      type_bv[idx >> 6] |= (1UL << (idx & 63));
    }

    // Compute the types for remaining positions.
    bool is_next_minus = is_last_suf_minus;
    for (std::uint64_t iplus = block_size - 1; iplus > 0; --iplus) {
      std::uint64_t i = iplus - 1;

      // Determine whether block[i] is a minus position.
      bool is_minus = false;
      if (block[i] == block[iplus]) is_minus = is_next_minus;
      else is_minus = (block[i] > block[iplus]);

      // Store the type of position i.
      if (is_minus)
        type_bv[i >> 6] |= (1UL << (i & 63));

      // Check if text[iplus] is minus star position
      // and is so, add the suffix to the queue.
      if (!is_minus && is_next_minus) {
        std::uint64_t head_char = block[iplus];
        queues[head_char]->push(iplus);
      }

      // Update is_next_minus.
      is_next_minus = is_minus;
    }
  }






  std::uint64_t extract_count = 0;
  for (std::uint64_t queue_id = text_alphabet_size; queue_id > 0; --queue_id) {
    queue_type &cur_queue = *queues[queue_id - 1];

    while (!cur_queue.empty() || (!is_last_suf_minus && extract_count == extract_count_target)) {
      std::uint64_t head_char = 0;
      std::uint64_t head_pos = 0;
      bool is_head_plus = false;

      if (!is_last_suf_minus && extract_count == extract_count_target) {
        head_char = block[block_size - 1] + 1;
        head_pos = block_size - 1;
        is_head_plus = true;
      } else {
        head_char = queue_id - 1;
        head_pos = (std::uint64_t)cur_queue.front();
        cur_queue.pop();
        is_head_plus = ((type_bv[head_pos >> 6] & (1UL << (head_pos & 63))) == 0);
      }
      ++extract_count;

      if (is_head_plus) {
        plus_writer->write(block_beg + head_pos);
        --head_char;
      }

      if (head_pos > 0 && (std::uint64_t)block[head_pos - 1] <= head_char) {
        std::uint64_t prev_pos_char = (std::uint64_t)block[head_pos - 1] + 1;
        queues[prev_pos_char]->push(head_pos - 1);
      }
    }
  }






  delete[] type_bv;
  delete plus_writer;
  for (std::uint64_t queue_id = 0; queue_id < text_alphabet_size; ++queue_id)
    delete queues[queue_id];
  delete[] queues;
}

#endif  // __IM_INDUCE_PLUS_STAR_SUBSTRINGS_HPP_INCLUDED
