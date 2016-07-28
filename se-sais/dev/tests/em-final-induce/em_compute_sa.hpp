#ifndef __EM_COMPUTE_SA_HPP_INCULUDED
#define __EM_COMPUTE_SA_HPP_INCULUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <limits>
#include <algorithm>

#include "naive_compute_sa.hpp"
#include "em_induce_minus_star_substrings.hpp"
#include "em_induce_minus_and_plus_suffixes.hpp"

#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_multi_stream_reader.hpp"
#include "io/async_multi_stream_writer.hpp"
#include "utils.hpp"
#include "uint24.hpp"
#include "uint40.hpp"
#include "uint48.hpp"


template<typename name_type,
  typename text_offset_type>
std::uint64_t create_recursive_text(
    std::uint64_t text_length,
    std::uint64_t max_permute_block_size,
    std::vector<std::string> &lex_sorted_minus_star_substrings_for_normal_string_input_filenames,
    std::vector<std::string> &text_sorted_minus_star_substrings_for_normal_string_output_filenames,
    std::string recursive_text_output_filename,
    std::uint64_t &total_io_volume) {
  std::uint64_t io_volume = 0;
  std::uint64_t n_permute_blocks = (text_length + max_permute_block_size - 1) / max_permute_block_size; 

  // Allocate array with names and `used' bitvector.
  std::uint64_t used_bv_size = (max_permute_block_size + 63) / 64;
  std::uint64_t *used_bv = new std::uint64_t[used_bv_size];
  text_offset_type *names = new text_offset_type[max_permute_block_size];

  // Initialize the writer of text.
  typedef async_stream_writer<name_type> text_writer_type;
  text_writer_type *text_writer = new text_writer_type(recursive_text_output_filename);

  // Process permute blocks left to right.
  std::uint64_t new_text_length = 0;
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; permute_block_id++) {
    std::uint64_t permute_block_beg = permute_block_id * max_permute_block_size;
    std::uint64_t permute_block_end = std::min(text_length, permute_block_beg + max_permute_block_size);
    std::uint64_t permute_block_size = permute_block_end - permute_block_beg;
    std::fill(used_bv, used_bv + used_bv_size, 0UL);

    // Read names.
    {
      typedef async_stream_reader<text_offset_type> reader_type;
      reader_type *reader = new reader_type(lex_sorted_minus_star_substrings_for_normal_string_input_filenames[permute_block_id]);
      while (!reader->empty()) {
        std::uint64_t pos = (std::uint64_t)reader->read() - permute_block_beg;
        text_offset_type name = reader->read();
        names[pos] = name;
        used_bv[pos >> 6] |= (1UL << (pos & 63));
      }

      // Update I/O volume.
      io_volume += reader->bytes_read();

      // Clean up.
      delete reader;
    }

    // Write positions and append name.
    {
      typedef async_stream_writer<text_offset_type> pos_writer_type;
      pos_writer_type *pos_writer = new pos_writer_type(text_sorted_minus_star_substrings_for_normal_string_output_filenames[permute_block_id]);
      for (std::uint64_t i = 0; i < permute_block_size; ++i) {
        if (used_bv[i >> 6] & (1UL << (i & 63))) {
          pos_writer->write(i);
          text_writer->write(names[i]);
          ++new_text_length;
        }
      }

      // Update I/O volume.
      io_volume += pos_writer->bytes_written();

      // Clean up.
      delete pos_writer;
      utils::file_delete(lex_sorted_minus_star_substrings_for_normal_string_input_filenames[permute_block_id]);
    }
  }

  // Update I/O volume.
  io_volume += text_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete text_writer;
  delete[] used_bv;
  delete[] names;

  // Return result.
  return new_text_length;
}

// Permute sorted minus star suffixes of the
// original string from text to lex order.
template<typename text_offset_type>
void permute_minus_star_suffixes_for_normal_string_from_text_to_lex_order(
    std::uint64_t text_length,
    std::uint64_t max_block_size,
    std::uint64_t max_permute_block_size,
    std::vector<std::uint64_t> &next_block_leftmost_minus_star_plus_rank,
    std::string tempfile_basename,
    std::vector<std::string> &lex_sorted_suffixes_for_recursive_string_filenames,
    std::string lex_sorted_suffixes_for_recursive_string_block_ids_filename,
    std::vector<std::string> &text_sorted_minus_star_suffixes_for_normal_string_filenames,
    std::vector<std::string> &lex_sorted_minus_star_suffixes_for_normal_string_filenames,
    std::string lex_sorted_minus_star_suffixes_for_normal_string_block_ids_filename,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_permute_blocks = (text_length + max_permute_block_size - 1) / max_permute_block_size;
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
  std::uint64_t io_volume = 0;

  std::vector<std::string> temp_filenames(n_permute_blocks);
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
    temp_filenames[permute_block_id] = tempfile_basename + "tmp." + utils::random_string_hash();

  // Allocate array with positions of minus star suffixes for normal string.
  text_offset_type *text_sorted_suffixes_for_normal_string = new text_offset_type[max_permute_block_size];

  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id) {
    // Read text sorted minus star suffixes for normal string from file.
    std::uint64_t n_suffixes = utils::file_size(text_sorted_minus_star_suffixes_for_normal_string_filenames[permute_block_id]) / sizeof(text_offset_type);
    utils::read_from_file(text_sorted_suffixes_for_normal_string, n_suffixes,
      text_sorted_minus_star_suffixes_for_normal_string_filenames[permute_block_id]);
    io_volume += n_suffixes * sizeof(text_offset_type);

    // Initialize the reader of lex sorted minus star suffixes for recursive string.
    typedef async_stream_reader<text_offset_type> lex_sorted_suffixes_for_recursive_string_reader_type;
    lex_sorted_suffixes_for_recursive_string_reader_type *lex_sorted_suffixes_for_recursive_string_reader
      = new lex_sorted_suffixes_for_recursive_string_reader_type(
        lex_sorted_suffixes_for_recursive_string_filenames[permute_block_id]);

    // Initialize the writer of permuted positions.
    typedef async_stream_writer<text_offset_type> lex_sorted_minus_star_suffixes_for_normal_string_writer_type;
    lex_sorted_minus_star_suffixes_for_normal_string_writer_type *lex_sorted_minus_star_suffixes_for_normal_string_writer
      = new lex_sorted_minus_star_suffixes_for_normal_string_writer_type(temp_filenames[permute_block_id]);

    // XXX implement buffered, out-of-order reading here.
    while (!lex_sorted_suffixes_for_recursive_string_reader->empty()) {
      std::uint64_t pos = lex_sorted_suffixes_for_recursive_string_reader->read();
      lex_sorted_minus_star_suffixes_for_normal_string_writer->write(text_sorted_suffixes_for_normal_string[pos]);
    }

    // Update I/O volume.
    io_volume += lex_sorted_suffixes_for_recursive_string_reader->bytes_read()
      + lex_sorted_minus_star_suffixes_for_normal_string_writer->bytes_written();

    // Clean up.
    delete lex_sorted_suffixes_for_recursive_string_reader;
    delete lex_sorted_minus_star_suffixes_for_normal_string_writer;
    utils::file_delete(text_sorted_minus_star_suffixes_for_normal_string_filenames[permute_block_id]);
    utils::file_delete(lex_sorted_suffixes_for_recursive_string_filenames[permute_block_id]);
  }

  // Clean up.
  delete[] text_sorted_suffixes_for_normal_string;

  // Initialize the reader of block IDs.
  typedef async_stream_reader<std::uint16_t> lex_sorted_suffixes_for_recursive_string_block_ids_reader_type;
  lex_sorted_suffixes_for_recursive_string_block_ids_reader_type *lex_sorted_suffixes_for_recursive_string_block_ids_reader
    = new lex_sorted_suffixes_for_recursive_string_block_ids_reader_type(lex_sorted_suffixes_for_recursive_string_block_ids_filename);

  // Initialize the reader of lex sorted minus star suffixes of normal string (distributed into blocks of size max_permute_block_size).
  typedef async_multi_stream_reader<text_offset_type> lex_sorted_minus_star_suffixes_for_normal_string_reader_type;
  lex_sorted_minus_star_suffixes_for_normal_string_reader_type *lex_sorted_minus_star_suffixes_for_normal_string_reader
    = new lex_sorted_minus_star_suffixes_for_normal_string_reader_type(n_permute_blocks);
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
    lex_sorted_minus_star_suffixes_for_normal_string_reader->add_file(temp_filenames[permute_block_id]);

  // Initialize the writer of lex sorted minus star suffixes of normal string (distributed into blocks of size max_block_size).
  typedef async_multi_stream_writer<text_offset_type> lex_sorted_minus_star_suffixes_for_normal_string_writer_type;
  lex_sorted_minus_star_suffixes_for_normal_string_writer_type *lex_sorted_minus_star_suffixes_for_normal_string_writer
    = new lex_sorted_minus_star_suffixes_for_normal_string_writer_type(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
    std::string filename = lex_sorted_minus_star_suffixes_for_normal_string_filenames[block_id];
    lex_sorted_minus_star_suffixes_for_normal_string_writer->add_file(filename);
  }

  // Initialize the writer of the block IDs (of size max_block_size) for lex sorted minus star suffixes of the normal string.
  typedef async_stream_writer<std::uint16_t> lex_sorted_minus_star_suffixes_for_normal_string_block_ids_writer_type;
  lex_sorted_minus_star_suffixes_for_normal_string_block_ids_writer_type *lex_sorted_minus_star_suffixes_for_normal_string_block_ids_writer
    = new lex_sorted_minus_star_suffixes_for_normal_string_block_ids_writer_type(lex_sorted_minus_star_suffixes_for_normal_string_block_ids_filename);

  std::vector<std::uint64_t> leftmost_item_in_block(n_blocks, std::numeric_limits<std::uint64_t>::max());
  std::vector<std::uint64_t> items_written_to_block(n_blocks, 0UL);
  while (!lex_sorted_suffixes_for_recursive_string_block_ids_reader->empty()) {
    std::uint64_t permute_block_id = lex_sorted_suffixes_for_recursive_string_block_ids_reader->read();
    std::uint64_t permute_block_beg = permute_block_id * max_permute_block_size;
    std::uint64_t permute_block_offset = lex_sorted_minus_star_suffixes_for_normal_string_reader->read_from_ith_file(permute_block_id);
    std::uint64_t next_minus_star_suf = permute_block_beg + permute_block_offset;
    std::uint64_t block_id = next_minus_star_suf / max_block_size;
    std::uint64_t block_beg = block_id * max_block_size;
    std::uint64_t block_offset = next_minus_star_suf - block_beg;
    lex_sorted_minus_star_suffixes_for_normal_string_writer->write_to_ith_file(block_id, block_offset);
    lex_sorted_minus_star_suffixes_for_normal_string_block_ids_writer->write(block_id);
    ++items_written_to_block[block_id];
    if (block_id > 0 && block_offset < leftmost_item_in_block[block_id]) {
      leftmost_item_in_block[block_id] = block_offset;
      next_block_leftmost_minus_star_plus_rank[block_id - 1] = items_written_to_block[block_id - 1];
    }
  }

  // Update I/O volume.
  io_volume += lex_sorted_suffixes_for_recursive_string_block_ids_reader->bytes_read() +
    lex_sorted_minus_star_suffixes_for_normal_string_reader->bytes_read() +
    lex_sorted_minus_star_suffixes_for_normal_string_writer->bytes_written() +
    lex_sorted_minus_star_suffixes_for_normal_string_block_ids_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete lex_sorted_suffixes_for_recursive_string_block_ids_reader;
  delete lex_sorted_minus_star_suffixes_for_normal_string_reader;
  delete lex_sorted_minus_star_suffixes_for_normal_string_writer;
  delete lex_sorted_minus_star_suffixes_for_normal_string_block_ids_writer;
  utils::file_delete(lex_sorted_suffixes_for_recursive_string_block_ids_filename);
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
    utils::file_delete(temp_filenames[permute_block_id]);
}

template<typename char_type,
  typename text_offset_type>
void temp_compute_sa(
    std::uint64_t text_length,
    std::uint64_t,
    std::uint64_t,
    std::vector<std::uint64_t> &block_count,
    std::string,
    std::string text_filename,
    std::string lex_sorted_suffixes_block_ids_filename,
    std::vector<std::string> &lex_sorted_suffixes_filenames,
    std::uint64_t &total_io_volume) {
  std::uint64_t n_permute_blocks = block_count.size();
  std::uint64_t io_volume = 0;

  // Naive implementation.
  char_type *text = new char_type[text_length];
  utils::read_from_file(text, text_length, text_filename);
  text_offset_type *sa = new text_offset_type[text_length];
  naive_compute_sa::naive_compute_sa<char_type, text_offset_type>(text, text_length, sa);

  // Initialize the output writers.
  typedef async_multi_stream_writer<text_offset_type> pos_writer_type;
  pos_writer_type *pos_writer = new pos_writer_type(n_permute_blocks);
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
    pos_writer->add_file(lex_sorted_suffixes_filenames[permute_block_id]);
  typedef async_stream_writer<std::uint16_t> block_id_writer_type;
  block_id_writer_type *block_id_writer = new block_id_writer_type(lex_sorted_suffixes_block_ids_filename);

  for (std::uint64_t i = 0; i < text_length; ++i) {
    std::uint64_t pos = sa[i];
    std::uint64_t block_id = 0;
    std::uint64_t prev_blocks_sum = 0;
    while (block_id < n_permute_blocks && prev_blocks_sum + block_count[block_id] <= pos)
      prev_blocks_sum += block_count[block_id++];
    std::uint64_t block_offset = pos - prev_blocks_sum;
    block_id_writer->write(block_id);
    pos_writer->write_to_ith_file(block_id, block_offset);
  }

  // Update I/O volume.
  io_volume += pos_writer->bytes_written() +
    block_id_writer->bytes_written();
  total_io_volume += io_volume;

  // Clean up.
  delete[] text;
  delete[] sa;
  delete block_id_writer;
  delete pos_writer;
  utils::file_delete(text_filename);
}

template<typename char_type,
  typename text_offset_type>
void compute_sa(
    std::uint64_t text_length,
    std::uint64_t ram_use,
    std::uint64_t text_alphabet_size,
    std::vector<std::uint64_t> &input_block_count,
    std::string tempfile_basename,
    std::string text_filename,
    std::string input_lex_sorted_suffixes_block_ids_filename,
    std::vector<std::string> &input_lex_sorted_suffixes_filenames,
    std::uint64_t &total_io_volume) {
#if 0
  std::uint64_t max_permute_block_size = std::max(1UL, (std::uint64_t)(ram_use / (sizeof(text_offset_type) + 0.125L)));
  std::uint64_t n_permute_blocks = (text_length + max_permute_block_size - 1) / max_permute_block_size;
  std::uint64_t mbs_temp = ram_use - text_alphabet_size * sizeof(text_offset_type);
  std::uint64_t max_block_size = std::max(1UL, (std::uint64_t)(mbs_temp / (sizeof(text_offset_type) + sizeof(char_type) + 0.25L)));
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
#else
  // Testing:
  std::uint64_t max_permute_block_size = 0;
  std::uint64_t n_permute_blocks = 0;
  std::uint64_t max_block_size = 0;
  std::uint64_t n_blocks = 0;
  do {
    max_permute_block_size = utils::random_int64(1L, text_length);
    n_permute_blocks = (text_length + max_permute_block_size - 1) / max_permute_block_size;
  } while (n_permute_blocks > (1UL << 8));
  do {
    max_block_size = utils::random_int64(1L, text_length);
    n_blocks = (text_length + max_block_size - 1) / max_block_size;
  } while (n_blocks > (1UL << 8));
#endif

  // Induce minus star substrings of the normal text.
  std::vector<std::string> lex_sorted_minus_star_substrings_for_normal_string_filenames(n_permute_blocks);
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
    lex_sorted_minus_star_substrings_for_normal_string_filenames[permute_block_id] = tempfile_basename + "tmp." + utils::random_string_hash();
  std::string minus_star_suffixes_count_filename = tempfile_basename + "tmp." + utils::random_string_hash();
  std::uint64_t n_names = em_induce_minus_star_substrings<char_type, text_offset_type>(text_length, text_alphabet_size,
      ram_use, max_permute_block_size, text_filename, tempfile_basename, minus_star_suffixes_count_filename,
      lex_sorted_minus_star_substrings_for_normal_string_filenames, total_io_volume);

  std::vector<std::uint64_t> block_count(n_permute_blocks);
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id) {
    std::string filename = lex_sorted_minus_star_substrings_for_normal_string_filenames[permute_block_id];
    block_count[permute_block_id] = utils::file_size(filename) / (2UL * sizeof(text_offset_type));
  }

  std::vector<std::string> text_sorted_minus_star_substrings_for_normal_string_filenames(n_permute_blocks);
  std::vector<std::string> lex_sorted_suffixes_for_recursive_string_filenames(n_permute_blocks);
  std::string lex_sorted_suffixes_for_recursive_string_block_ids_filename = tempfile_basename + "tmp." + utils::random_string_hash();
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id) {
    text_sorted_minus_star_substrings_for_normal_string_filenames[permute_block_id] = tempfile_basename + "tmp." + utils::random_string_hash();
    lex_sorted_suffixes_for_recursive_string_filenames[permute_block_id] = tempfile_basename + "tmp." + utils::random_string_hash();
  }

  //if (n_names < (1UL << 16))
  {
    //typedef std::uint64_t recursive_char_type;
    typedef std::uint32_t recursive_char_type;

    // Permute substrings of the normal string from lex to text order
    // and at the same time create the recursive string
    std::string recursive_text_filename = tempfile_basename + "tmp." + utils::random_string_hash();
    std::uint64_t new_text_length = create_recursive_text<recursive_char_type, text_offset_type>(
        text_length, max_permute_block_size, lex_sorted_minus_star_substrings_for_normal_string_filenames,
        text_sorted_minus_star_substrings_for_normal_string_filenames, recursive_text_filename, total_io_volume);

    // Sort suffixes of the recursive string. The output is distributed
    // into permute blocks and also a sequence of permute block IDs
    // This function should delete the recursive_text upon exit.
    if (new_text_length <= 1)
      temp_compute_sa<recursive_char_type, text_offset_type>(new_text_length,
        ram_use, n_names, block_count, tempfile_basename, recursive_text_filename,
        lex_sorted_suffixes_for_recursive_string_block_ids_filename,
        lex_sorted_suffixes_for_recursive_string_filenames, total_io_volume);
    else
      compute_sa<recursive_char_type, text_offset_type>(new_text_length,
        ram_use, n_names, block_count, tempfile_basename, recursive_text_filename,
        lex_sorted_suffixes_for_recursive_string_block_ids_filename,
        lex_sorted_suffixes_for_recursive_string_filenames, total_io_volume);
  }

  // Note: text sorted minus star substrings for normal text is
  // the same as text sorted minus star suffixes for normal text.
  std::vector<std::uint64_t> next_block_leftmost_minus_star_plus_rank(n_blocks, std::numeric_limits<std::uint64_t>::max());
  std::vector<std::string> lex_sorted_minus_star_suffixes_for_normal_string_filenames(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
    lex_sorted_minus_star_suffixes_for_normal_string_filenames[block_id] = tempfile_basename + "tmp." + utils::random_string_hash();
  std::string lex_sorted_minus_star_suffixes_for_normal_string_block_ids_filename = tempfile_basename + "tmp." + utils::random_string_hash();
  permute_minus_star_suffixes_for_normal_string_from_text_to_lex_order<text_offset_type>(
      text_length, max_block_size, max_permute_block_size,
      next_block_leftmost_minus_star_plus_rank, tempfile_basename,
      lex_sorted_suffixes_for_recursive_string_filenames,
      lex_sorted_suffixes_for_recursive_string_block_ids_filename,
      text_sorted_minus_star_substrings_for_normal_string_filenames,
      lex_sorted_minus_star_suffixes_for_normal_string_filenames,
      lex_sorted_minus_star_suffixes_for_normal_string_block_ids_filename,
      total_io_volume);

  // Compute the write the final SA to disk.
  induce_minus_and_plus_suffixes<char_type, text_offset_type>(text_alphabet_size,
      text_length, max_block_size, ram_use, next_block_leftmost_minus_star_plus_rank,
      tempfile_basename, text_filename, lex_sorted_minus_star_suffixes_for_normal_string_block_ids_filename,
      minus_star_suffixes_count_filename, lex_sorted_minus_star_suffixes_for_normal_string_filenames,
      input_block_count, input_lex_sorted_suffixes_block_ids_filename,
      input_lex_sorted_suffixes_filenames, total_io_volume);
}

template<typename char_type,
  typename text_offset_type>
void em_compute_sa(
    std::uint64_t text_length,
    std::uint64_t ram_use,
    std::uint64_t text_alphabet_size,
    std::string text_filename,
    std::string output_filename,
    std::uint64_t &total_io_volume) {
#if 0
  std::uint64_t max_permute_block_size = std::max(1UL, (std::uint64_t)(ram_use / (sizeof(text_offset_type) + 0.125L)));
  std::uint64_t n_permute_blocks = (text_length + max_permute_block_size - 1) / max_permute_block_size;
  std::uint64_t mbs_temp = ram_use - text_alphabet_size * sizeof(text_offset_type);
  std::uint64_t max_block_size = std::max(1UL, (std::uint64_t)(mbs_temp / (sizeof(text_offset_type) + sizeof(char_type) + 0.25L)));
  std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;
#else
  // Testing:
  std::uint64_t max_permute_block_size = 0;
  std::uint64_t n_permute_blocks = 0;
  std::uint64_t max_block_size = 0;
  std::uint64_t n_blocks = 0;
  do {
    max_permute_block_size = utils::random_int64(1L, text_length);
    n_permute_blocks = (text_length + max_permute_block_size - 1) / max_permute_block_size;
  } while (n_permute_blocks > (1UL << 8));
  do {
    max_block_size = utils::random_int64(1L, text_length);
    n_blocks = (text_length + max_block_size - 1) / max_block_size;
  } while (n_blocks > (1UL << 8));
#endif

  // Induce minus star substrings of the normal text.
  std::vector<std::string> lex_sorted_minus_star_substrings_for_normal_string_filenames(n_permute_blocks);
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
    lex_sorted_minus_star_substrings_for_normal_string_filenames[permute_block_id] = output_filename + "tmp." + utils::random_string_hash();
  std::string minus_star_suffixes_count_filename = output_filename + "tmp." + utils::random_string_hash();
  std::uint64_t n_names = em_induce_minus_star_substrings<char_type, text_offset_type>(text_length, text_alphabet_size,
      ram_use, max_permute_block_size, text_filename, output_filename, minus_star_suffixes_count_filename,
      lex_sorted_minus_star_substrings_for_normal_string_filenames, total_io_volume);

  std::vector<std::uint64_t> block_count(n_permute_blocks);
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id) {
    std::string filename = lex_sorted_minus_star_substrings_for_normal_string_filenames[permute_block_id];
    block_count[permute_block_id] = utils::file_size(filename) / (2UL * sizeof(text_offset_type));
  }

  std::vector<std::string> text_sorted_minus_star_substrings_for_normal_string_filenames(n_permute_blocks);
  std::vector<std::string> lex_sorted_suffixes_for_recursive_string_filenames(n_permute_blocks);
  std::string lex_sorted_suffixes_for_recursive_string_block_ids_filename = output_filename + "tmp." + utils::random_string_hash();
  for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id) {
    text_sorted_minus_star_substrings_for_normal_string_filenames[permute_block_id] = output_filename + "tmp." + utils::random_string_hash();
    lex_sorted_suffixes_for_recursive_string_filenames[permute_block_id] = output_filename + "tmp." + utils::random_string_hash();
  }

  //if (n_names < (1UL << 16))
  {
    //typedef std::uint64_t recursive_char_type;
    typedef std::uint32_t recursive_char_type;

    // Permute substrings of the normal string from lex to text order
    // and at the same time create the recursive string
    std::string recursive_text_filename = output_filename + "tmp." + utils::random_string_hash();
    std::uint64_t new_text_length = create_recursive_text<recursive_char_type, text_offset_type>(
        text_length, max_permute_block_size, lex_sorted_minus_star_substrings_for_normal_string_filenames,
        text_sorted_minus_star_substrings_for_normal_string_filenames, recursive_text_filename, total_io_volume);

    // Sort suffixes of the recursive string. The output is distributed
    // into permute blocks and also a sequence of permute block IDs
    // This function should delete the recursive_text upon exit.
    if (new_text_length <= 1)
      temp_compute_sa<recursive_char_type, text_offset_type>(new_text_length,
        ram_use, n_names, block_count, output_filename, recursive_text_filename,
        lex_sorted_suffixes_for_recursive_string_block_ids_filename,
        lex_sorted_suffixes_for_recursive_string_filenames, total_io_volume);
    else
      compute_sa<recursive_char_type, text_offset_type>(new_text_length,
        ram_use, n_names, block_count, output_filename, recursive_text_filename,
        lex_sorted_suffixes_for_recursive_string_block_ids_filename,
        lex_sorted_suffixes_for_recursive_string_filenames, total_io_volume);
  }

  // Note: text sorted minus star substrings for normal text is
  // the same as text sorted minus star suffixes for normal text.
  std::vector<std::uint64_t> next_block_leftmost_minus_star_plus_rank(n_blocks, std::numeric_limits<std::uint64_t>::max());
  std::vector<std::string> lex_sorted_minus_star_suffixes_for_normal_string_filenames(n_blocks);
  for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
    lex_sorted_minus_star_suffixes_for_normal_string_filenames[block_id] = output_filename + "tmp." + utils::random_string_hash();
  std::string lex_sorted_minus_star_suffixes_for_normal_string_block_ids_filename = output_filename + "tmp." + utils::random_string_hash();
  permute_minus_star_suffixes_for_normal_string_from_text_to_lex_order<text_offset_type>(
      text_length, max_block_size, max_permute_block_size,
      next_block_leftmost_minus_star_plus_rank, output_filename,
      lex_sorted_suffixes_for_recursive_string_filenames,
      lex_sorted_suffixes_for_recursive_string_block_ids_filename,
      text_sorted_minus_star_substrings_for_normal_string_filenames,
      lex_sorted_minus_star_suffixes_for_normal_string_filenames,
      lex_sorted_minus_star_suffixes_for_normal_string_block_ids_filename,
      total_io_volume);

  // Compute the write the final SA to disk.
  induce_minus_and_plus_suffixes<char_type, text_offset_type>(text_alphabet_size,
      text_length, max_block_size, ram_use, next_block_leftmost_minus_star_plus_rank,
      text_filename, lex_sorted_minus_star_suffixes_for_normal_string_block_ids_filename,
      minus_star_suffixes_count_filename, output_filename,
      lex_sorted_minus_star_suffixes_for_normal_string_filenames,
      total_io_volume);
}

#endif  // __EM_COMPUTE_SA_HPP_INCULUDED
