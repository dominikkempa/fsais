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


// Assumptions:
// - chr_t has to be able to hold any symbol from the text.
// - saidx_t can encode integer in range [0..text_length),
//   not necessarily integer text_length and larger.
// - blockidx_t can encode integer in range [0..max_block_size),
//   not necessarily integer max_block_size and larger.
// All types are assumed to be unsigned.
// This version of the function uses
//   block_size / 8                           // for type bitvector
// + block_size * sizeof(chr_t)               // for the text
// + block_size * sizeof(blockidx_t)          // for the items in queues
// + text_alphabet_size * sizeof(blockidx_t)  // for the bucket pointers
// bytes of RAM.
// NOTE: blockidx_t is now implemented right now, though we already
// assign values in the ranfe [0..block_size) to values of type saidx_t.
// NOTE2: rather than using a sepaarate bitvector, we could use the MSB
// of integers, but that could be more problematic. In some cases (such
// as inserting elements on the radix_heap) it is impossibly to use the
// bitvector and in thsoe cases the MSB trick should be used.
template<typename chr_t, typename saidx_t>
void im_induce_plus_star_substrings(
    const chr_t *block,
    std::uint64_t text_alphabet_size,
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t block_beg,
    std::uint64_t extract_count_target,
    bool is_last_suf_minus,
    std::string plus_substrings_filename) {
  std::uint64_t block_end = std::min(text_length, block_beg + max_block_size);
  std::uint64_t block_size = block_end - block_beg;




  typedef packed_pair<saidx_t, bool> pair_type;
  typedef std::queue<pair_type> queue_type;
  queue_type **queues = new queue_type*[text_alphabet_size];
  for (std::uint64_t queue_id = 0; queue_id < text_alphabet_size; ++queue_id)
    queues[queue_id] = new queue_type();



  typedef async_stream_writer<saidx_t> plus_writer_type;
  plus_writer_type *plus_writer = new plus_writer_type(plus_substrings_filename);



  // XXX allocate type_bitvector here



  bool is_next_minus = is_last_suf_minus;
  for (std::uint64_t i = block_end - 1; i > block_beg; --i) {
    bool is_minus = false;
    if (block[i - 1 - block_beg] == block[i - block_beg]) is_minus = is_next_minus;
    else is_minus = (block[i - 1 - block_beg] > block[i - block_beg]);
    if (!is_minus && is_next_minus)
      queues[block[i - block_beg]]->push(pair_type(i - block_beg, false));
    is_next_minus = is_minus;
  }



  std::uint64_t extract_count = 0;
  for (std::uint64_t queue_id = text_alphabet_size; queue_id > 0; --queue_id) {
    queue_type &cur_queue = *queues[queue_id - 1];

    while (!cur_queue.empty() || (!is_last_suf_minus && extract_count == extract_count_target)) {
      chr_t head_char = 0;
      std::uint64_t head_pos = 0;
      bool is_head_plus = false;

      if (!is_last_suf_minus && extract_count == extract_count_target) {
        head_char = block[block_size - 1] + 1;
        head_pos = block_end - 1;
        is_head_plus = true;
      } else {
        pair_type p = cur_queue.front();
        cur_queue.pop();
        head_char = queue_id - 1;
        head_pos = (std::uint64_t)p.first + block_beg;
        is_head_plus = p.second;
      }
      ++extract_count;

      if (is_head_plus) {
        plus_writer->write(head_pos);
        --head_char;
      }

      if (block_beg < head_pos && block[head_pos - 1 - block_beg] <= head_char)
        queues[block[head_pos - 1 - block_beg] + 1]->push(pair_type(head_pos - 1 - block_beg, true));
    }
  }




  delete plus_writer;
  for (std::uint64_t queue_id = 0; queue_id < text_alphabet_size; ++queue_id)
    delete queues[queue_id];
  delete[] queues;
}

#endif  // __IM_INDUCE_PLUS_STAR_SUBSTRINGS_HPP_INCLUDED
