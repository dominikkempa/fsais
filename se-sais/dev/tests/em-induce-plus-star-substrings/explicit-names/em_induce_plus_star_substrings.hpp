#ifndef __EM_INDUCE_PLUS_STAR_SUBSTRINGS_HPP_INCLUDED
#define __EM_INDUCE_PLUS_STAR_SUBSTRINGS_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "utils.hpp"
#include "packed_pair.hpp"
#include "em_radix_heap.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_multi_stream_reader.hpp"
#include "io/async_multi_bit_stream_reader.hpp"
#include "io/async_backward_multi_bit_stream_reader.hpp"


// Note: extext_blockidx_t type needs to store block id and two extra bits.
template<typename chr_t, typename saidx_t, typename blockidx_t, typename extext_blockidx_t>
void em_induce_plus_star_substrings(
    std::uint64_t text_length,
    std::string minus_data_filename,
    std::string output_filename,
    std::vector<std::string> &plus_type_filenames,
    std::vector<std::string> &symbols_filenames,
    std::vector<std::string> &pos_filenames,
    std::vector<std::uint64_t> &block_count_target,
    std::uint64_t &total_io_volume,
    std::uint64_t radix_heap_bufsize,
    std::uint64_t radix_log,
    std::uint64_t max_block_size) {
  std::uint64_t is_head_plus_bit = ((std::uint64_t)std::numeric_limits<extext_blockidx_t>::max() + 1) / 2;
  std::uint64_t is_tail_plus_bit = is_head_plus_bit / 2;
  std::uint64_t io_volume = 0;

  // Initialize radix heap.
  typedef packed_pair<extext_blockidx_t, saidx_t> ext_pair_type;
  typedef em_radix_heap<chr_t, ext_pair_type> heap_type;
  heap_type *radix_heap = new heap_type(radix_log, radix_heap_bufsize, output_filename);

  // Initialize the readers.
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  typedef async_backward_multi_bit_stream_reader plus_type_reader_type;
  typedef async_backward_multi_stream_reader<chr_t> symbols_reader_type;
  typedef async_backward_multi_stream_reader<saidx_t> pos_reader_type;
  plus_type_reader_type *plus_type_reader = new plus_type_reader_type(n_blocks);
  symbols_reader_type *symbols_reader = new symbols_reader_type(n_blocks);
  pos_reader_type *pos_reader = new pos_reader_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    plus_type_reader->add_file(plus_type_filenames[block_id]);
    symbols_reader->add_file(symbols_filenames[block_id]);
    pos_reader->add_file(pos_filenames[block_id]);
  }

  // Initialize the output writer.
  typedef async_stream_writer<saidx_t> output_writer_type;
  output_writer_type *output_writer = new output_writer_type(output_filename);

  // Sort start positions of all minus star substrings by
  // the first symbol by adding them to the heap.
  {
    typedef packed_pair<blockidx_t, chr_t> pair_type;
    typedef async_stream_reader<pair_type> reader_type;
    reader_type *reader = new reader_type(minus_data_filename);
    while (!reader->empty()) {
      pair_type p = reader->read();
      std::uint64_t block_id = p.first;
      chr_t ch = p.second;

      // We invert the rank of a symbol, since
      // radix_heap implements only extract_min().
      radix_heap->push(std::numeric_limits<chr_t>::max() - ch, ext_pair_type(block_id, 0));
    }

    // Update I/O volume.
    io_volume += reader->bytes_read();

    // Clean up.
    delete reader;
  }

  bool empty_output = true;
  bool was_extract_min = false;
  bool is_prev_head_plus = false;
  bool is_prev_tail_plus = false;
  std::uint64_t diff_items = 0;
  std::uint64_t diff_items_snapshot = 0;
  std::uint64_t diff_written_items = 0;
  std::uint64_t prev_tail_name = 0;
  chr_t prev_head_char = 0;

  std::vector<std::uint64_t> block_count(n_blocks, 0UL);
  while (!radix_heap->empty()) {
    std::pair<chr_t, ext_pair_type> p = radix_heap->extract_min();
    chr_t head_char = std::numeric_limits<chr_t>::max() - p.first;
    std::uint64_t block_id = p.second.first;
    std::uint64_t tail_name = p.second.second;

    // Unpack flags from block id.
    bool is_head_plus = false;
    bool is_tail_plus = false;
    if (block_id & is_head_plus_bit) {
      block_id -= is_head_plus_bit;
      is_head_plus = true;
    }
    if (block_id & is_tail_plus_bit) {
      block_id -= is_tail_plus_bit;
      is_tail_plus = true;
    }

    // Update block count.
    ++block_count[block_id];
    std::uint8_t head_pos_at_block_beg = (block_count[block_id] ==
        block_count_target[block_id]);

    if (is_head_plus) {
      --head_char;
      bool is_diff = false;
      if (was_extract_min) {
        if (!is_prev_head_plus || is_tail_plus != is_prev_tail_plus ||
            head_char != prev_head_char || tail_name != prev_tail_name)
          is_diff = true;
      } else is_diff = true;
      was_extract_min = true;
      diff_items += is_diff;

      // Note the +1 below. This is because we want the item in bucket c from the input to be processed.
      // after the items that were inserted in the line below. One way to do this is to insert items from
      // the input with a key decreased by one. Since we might not be able to always decrease, instead
      // we increase the key of the item inserted below. We can always do that, because there is no
      // plus-substring that begins with the maximal symbol in the alphabet.
      bool is_star = plus_type_reader->read_from_ith_file(block_id);
      if (is_star == true) {
        bool next_output_bit = false;
        if (empty_output == false) {
          if (diff_items != diff_items_snapshot)
            next_output_bit = true;
        } else next_output_bit = true;
        diff_written_items += next_output_bit;

        saidx_t head_pos = pos_reader->read_from_ith_file(block_id);
        output_writer->write(head_pos);
        output_writer->write(diff_written_items - 1);
        empty_output = false;
        diff_items_snapshot = diff_items;
      } else if (block_id > 0 || head_pos_at_block_beg == false) {
        chr_t prev_char = symbols_reader->read_from_ith_file(block_id);
        std::uint64_t prev_pos_block_idx = block_id - head_pos_at_block_beg;
        std::uint64_t new_block_id = (prev_pos_block_idx | is_head_plus_bit | is_tail_plus_bit);
        radix_heap->push(std::numeric_limits<chr_t>::max() - (prev_char + 1), ext_pair_type(new_block_id, diff_items - 1));
      }
    } else {
      chr_t prev_char = symbols_reader->read_from_ith_file(block_id);
      std::uint64_t prev_pos_block_idx = block_id - head_pos_at_block_beg;
      std::uint64_t new_block_id = (prev_pos_block_idx | is_head_plus_bit);
      radix_heap->push(std::numeric_limits<chr_t>::max() - (prev_char + 1), ext_pair_type(new_block_id, head_char));
    }

    is_prev_head_plus = is_head_plus;
    is_prev_tail_plus = is_tail_plus;
    prev_head_char = head_char;
    prev_tail_name = tail_name;
  }

  // Update I/O volume.
  io_volume += output_writer->bytes_written() + radix_heap->io_volume() +
    plus_type_reader->bytes_read() + symbols_reader->bytes_read() +
    pos_reader->bytes_read();
  total_io_volume += io_volume;

  // Clean up.
  delete output_writer;
  delete plus_type_reader;
  delete symbols_reader;
  delete pos_reader;
  delete radix_heap;
}

#endif  // __EM_INDUCE_PLUS_STAR_SUBSTRINGS_HPP_INCLUDED
