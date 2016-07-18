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
//   block_size / 4                                  // for type bitvector
// + 2 * block_size * sizeof(char_type)              // for the text
// + 2 * block_size * sizeof(block_offset_type)      // for the items in queues
// + text_alphabet_size * sizeof(block_offset_type)  // for the bucket pointers
// bytes of RAM.
//=============================================================================

template<typename char_type, typename text_offset_type, typename block_offset_type>
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









  typedef std::queue<block_offset_type> queue_type;
  queue_type **queues = new queue_type*[text_alphabet_size];
  for (std::uint64_t queue_id = 0; queue_id < text_alphabet_size; ++queue_id)
    queues[queue_id] = new queue_type();









  typedef async_stream_writer<text_offset_type> plus_writer_type;
  plus_writer_type *plus_writer = new plus_writer_type(plus_substrings_filename);








  // Compute type_bv that stores whether each of the
  // position in the block is a minus position (1) or not (0).
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






  // Add initial items to queues.
  {
    if ((type_bv[(block_size - 1) >> 6] & (1UL << ((block_size - 1) & 63))) == 0) {
      std::uint64_t pos = block_size;
      while (pos + 1 < total_block_size && (type_bv[pos >> 6] & (1UL << (pos & 63))) == 0)
        ++pos;
      std::uint64_t head_char = nextblock[pos - block_size];
      if ((type_bv[pos >> 6] & (1UL << (pos & 63))) > 0)
        queues[head_char]->push(pos);
      else queues[head_char + 1]->push(pos);
    }

    for (std::uint64_t iplus = block_size; iplus > 0; --iplus) {
      std::uint64_t i = iplus - 1;
      if (i > 0 && (type_bv[i >> 6] & (1UL << (i & 63))) > 0 &&
          (type_bv[(i - 1) >> 6] & (1UL << ((i - 1) & 63))) == 0) {
        std::uint64_t head_char = block[i];
        queues[head_char]->push(i);
      }
    }
  }










  // Inducing.
  for (std::uint64_t queue_id_plus = text_alphabet_size; queue_id_plus > 0; --queue_id_plus) {
    std::uint64_t queue_id = queue_id_plus - 1;
    queue_type &cur_queue = *queues[queue_id];
    while (!cur_queue.empty()) {
      std::uint64_t head_char = queue_id;
      std::uint64_t head_pos = cur_queue.front();
      cur_queue.pop();
      if ((type_bv[head_pos >> 6] & (1UL << (head_pos & 63))) == 0) {
        if (head_pos < block_size)
          plus_writer->write(block_beg + head_pos);
        --head_char;
      }
      if (head_pos > 0) {
        std::uint64_t prev_pos = head_pos - 1;
        std::uint64_t prev_pos_head_char = (prev_pos < block_size) ? block[prev_pos] : nextblock[prev_pos - block_size];
        if (prev_pos_head_char <= head_char)
          queues[prev_pos_head_char + 1]->push(head_pos - 1);
      }
    }
  }







  delete plus_writer;
  delete[] type_bv;
  for (std::uint64_t queue_id = 0; queue_id < text_alphabet_size; ++queue_id)
    delete queues[queue_id];
  delete[] queues;
}

#endif  // __IM_INDUCE_PLUS_STAR_SUBSTRINGS_HPP_INCLUDED
