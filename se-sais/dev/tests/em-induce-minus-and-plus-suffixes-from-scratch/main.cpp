#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "im_induce_suffixes.hpp"
#include "em_induce_plus_suffixes.hpp"
#include "em_induce_minus_and_plus_suffixes.hpp"

#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_writer.hpp"

#include "utils.hpp"
#include "uint40.hpp"
#include "uint48.hpp"
#include "divsufsort.h"


void test(std::uint64_t n_testcases, std::uint64_t max_length) {
  fprintf(stderr, "TEST, n_testcases=%lu, max_length=%lu\n", n_testcases, max_length);

  typedef std::uint8_t char_type;
  typedef std::uint32_t text_offset_type;

  char_type *text = new char_type[max_length];
  text_offset_type *sa = new text_offset_type[max_length];
  bool *suf_type = new bool[max_length];

  for (std::uint64_t testid = 0; testid < n_testcases; ++testid) {
    if (testid % 10 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * testid) / n_testcases);
    std::uint64_t text_length = utils::random_int64(1L, (std::int64_t)max_length);
    if (utils::random_int64(0L, 1L)) {
      for (std::uint64_t j = 0; j < text_length; ++j)
        text[j] = utils::random_int64(0L, 255L);
    } else {
      for (std::uint64_t j = 0; j < text_length; ++j)
        text[j] = 'a' + utils::random_int64(0L, 5L);
    }

    divsufsort(text, (std::int32_t *)sa, text_length);
    std::uint64_t max_block_size = 0;
    std::uint64_t n_blocks = 0;
    do {
      max_block_size = utils::random_int64(1L, (std::int64_t)text_length);
      n_blocks = (text_length + max_block_size - 1) / max_block_size;
    } while (n_blocks > 256);

    typedef std::uint8_t block_id_type;
    typedef std::uint32_t block_offset_type;

    std::uint64_t total_io_volume = 0;
    std::uint64_t ram_use = utils::random_int64(1L, 1024L);
    std::uint64_t text_alphabet_size = (std::uint64_t)(*std::max_element(text, text + text_length)) + 1;
    char_type last_text_symbol = text[text_length - 1];

    for (std::uint64_t i = text_length; i > 0; --i) {
      if (i == text_length) suf_type[i - 1] = 0;              // minus
      else {
        if (text[i - 1] > text[i]) suf_type[i - 1] = 0;       // minus
        else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;  // plus
        else suf_type[i - 1] = suf_type[i];
      }
    }

    std::string text_filename = "tmp." + utils::random_string_hash();
    utils::write_to_file(text, text_length, text_filename);





    std::string output_filename = "tmp." + utils::random_string_hash();





 
    // Input that comes from outside.
    std::vector<std::string> init_minus_pos_filenames(n_blocks);
    std::string minus_pos_filename = "tmp." + utils::random_string_hash();
    std::string minus_count_filename = "tmp." + utils::random_string_hash();
    std::vector<std::uint64_t> next_block_leftmost_minus_star_plus_rank(n_blocks, std::numeric_limits<std::uint64_t>::max());
    {
      for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
        init_minus_pos_filenames[block_id] = "tmp." + utils::random_string_hash();
      typedef async_stream_writer<block_offset_type> writer_type;
      writer_type **writers = new writer_type*[n_blocks];
      for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
        writers[block_id] = new writer_type(init_minus_pos_filenames[block_id]);
      std::vector<std::uint64_t> leftmost_minus_star_in_a_block(n_blocks, std::numeric_limits<std::uint64_t>::max());
      for (std::uint64_t i = 0; i < text_length; ++i) {
        if (i > 0 && suf_type[i] == 0 && suf_type[i - 1] == 1) {
          std::uint64_t block_id = i / max_block_size;
          std::uint64_t block_beg = block_id * max_block_size;
          leftmost_minus_star_in_a_block[block_id] = std::min(leftmost_minus_star_in_a_block[block_id], i - block_beg);
        }
      }
      std::vector<std::uint64_t> items_written_for_block(n_blocks, 0UL);
      for (std::uint64_t i = 0; i < text_length; ++i) {
        std::uint64_t s = sa[i];
        if (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1) {
          std::uint64_t block_id = s / max_block_size;
          std::uint64_t block_beg = block_id * max_block_size;
          std::uint64_t block_offset = s - block_beg;
          writers[block_id]->write(block_offset);
          ++items_written_for_block[block_id];
          if (block_id > 0 && leftmost_minus_star_in_a_block[block_id] == block_offset)
            next_block_leftmost_minus_star_plus_rank[block_id - 1] = items_written_for_block[block_id - 1];
        }
      }
      for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) delete writers[block_id];
      delete[] writers;
    }
    {
      typedef async_stream_writer<text_offset_type> writer_type;
      writer_type *writer = new writer_type(minus_count_filename);
      std::uint64_t beg = 0;
      for (std::uint64_t ch = 0; ch < 256; ++ch) {
        std::uint64_t minus_count = 0;
        std::uint64_t end = beg;
        while (end < text_length && text[sa[end]] == ch) {
          if (suf_type[sa[end]] == 0 && sa[end] > 0 && suf_type[sa[end] - 1] == 1)
            ++minus_count;
          ++end;
        }
        writer->write(minus_count);
        beg = end;
      }
      delete writer;
    }  
    {
      typedef async_stream_writer<block_id_type> writer_type;
      writer_type *writer = new writer_type(minus_pos_filename);
      for (std::uint64_t i = 0; i < text_length; ++i) {
        std::uint64_t s = sa[i];
        if (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1) {
          std::uint64_t block_id = s / max_block_size;
          writer->write(block_id);
        }
      }
      delete writer;
    }
















    //=======================================================================================================================================================
    std::vector<std::string> plus_type_filenames(n_blocks);
    std::vector<std::string> minus_type_filenames(n_blocks);
    std::vector<std::string> plus_symbols_filenames(n_blocks);
    std::vector<std::string> minus_symbols_filenames(n_blocks);
    std::vector<std::string> plus_pos_filenames(n_blocks);
    std::vector<std::string> minus_pos_filenames(n_blocks);
    std::vector<std::uint64_t> block_count_target(n_blocks, std::numeric_limits<std::uint64_t>::max());
    for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
      plus_pos_filenames[block_id] = "tmp." + utils::random_string_hash();
      plus_symbols_filenames[block_id] = "tmp." + utils::random_string_hash();
      plus_type_filenames[block_id] = "tmp." + utils::random_string_hash();
      minus_pos_filenames[block_id] = "tmp." + utils::random_string_hash();
      minus_type_filenames[block_id] = "tmp." + utils::random_string_hash();
      minus_symbols_filenames[block_id] = "tmp." + utils::random_string_hash();
    }

    // Close stderr.
    int stderr_backup = 0;
    std::fflush(stderr);
    stderr_backup = dup(2);
    int stderr_temp = open("/dev/null", O_WRONLY);
    dup2(stderr_temp, 2);
    close(stderr_temp);

    im_induce_suffixes_small_alphabet<
      char_type,
      text_offset_type,
      block_offset_type>(
          text_alphabet_size,
          text_length,
          max_block_size,
          next_block_leftmost_minus_star_plus_rank,
          text_filename,
          init_minus_pos_filenames,
          plus_pos_filenames,
          plus_symbols_filenames,
          plus_type_filenames,
          minus_pos_filenames,
          minus_type_filenames,
          minus_symbols_filenames,
          block_count_target,
          total_io_volume);

    std::string plus_type_filename = "tmp." + utils::random_string_hash();
    std::string plus_count_filename = "tmp." + utils::random_string_hash();
    std::string plus_pos_filename = "tmp." + utils::random_string_hash();

    em_induce_plus_suffixes<
      char_type,
      text_offset_type,
      block_offset_type,
      block_id_type>(
          text_length,
          max_block_size,
          ram_use,
          block_count_target,
          plus_pos_filename,
          plus_type_filename,
          plus_count_filename,
          minus_pos_filename,
          minus_count_filename,
          plus_type_filenames,
          plus_pos_filenames,
          plus_symbols_filenames,
          total_io_volume);

    utils::file_delete(minus_pos_filename);
    utils::file_delete(minus_count_filename);
    for (std::uint64_t i = 0; i < n_blocks; ++i) {
      if (utils::file_exists(plus_symbols_filenames[i])) utils::file_delete(plus_symbols_filenames[i]);
      if (utils::file_exists(plus_type_filenames[i])) utils::file_delete(plus_type_filenames[i]);
      if (utils::file_exists(plus_pos_filenames[i])) utils::file_delete(plus_pos_filenames[i]);
    }

    em_induce_minus_and_plus_suffixes<
      char_type,
      text_offset_type,
      block_offset_type,
      block_id_type>(
          text_length,
          max_block_size,
          ram_use,
          last_text_symbol,
          output_filename,
          plus_pos_filename,
          plus_type_filename,
          plus_count_filename,
          minus_type_filenames,
          minus_pos_filenames,
          minus_symbols_filenames,
          total_io_volume);

    utils::file_delete(plus_pos_filename);
    utils::file_delete(plus_type_filename);
    utils::file_delete(plus_count_filename);
    for (std::uint64_t j = 0; j < n_blocks; ++j) {
      if (utils::file_exists(minus_symbols_filenames[j])) utils::file_delete(minus_symbols_filenames[j]);
      if (utils::file_exists(minus_type_filenames[j])) utils::file_delete(minus_type_filenames[j]);
      if (utils::file_exists(minus_pos_filenames[j])) utils::file_delete(minus_pos_filenames[j]);
    }

    // Restore stderr.
    std::fflush(stderr);
    dup2(stderr_backup, 2);
    close(stderr_backup);
    //=======================================================================================================================================================

    
    
    
    
    
    


   
    // Compare answer.
    {
      std::vector<text_offset_type> v;
      for (std::uint64_t i = 0; i < text_length; ++i) {
        std::uint64_t s = sa[i];
        v.push_back(s);
      }
      std::vector<text_offset_type> v_computed;
      {
        typedef async_stream_reader<text_offset_type> reader_type;
        reader_type *reader = new reader_type(output_filename);
        while (!reader->empty())
          v_computed.push_back(reader->read());
        delete reader;
      }
      utils::file_delete(output_filename);
      bool ok = true;
      if (v.size() != v_computed.size()) ok = false;
      else if (!std::equal(v.begin(), v.end(), v_computed.begin())) ok = false;
      if (!ok) {
        fprintf(stderr, "Error:\n");
        fprintf(stderr, "  text: ");
        for (std::uint64_t i = 0; i < text_length; ++i)
          fprintf(stderr, "%c", text[i]);
        fprintf(stderr, "\n");
        fprintf(stderr, "  SA: ");
        for (std::uint64_t i = 0; i < text_length; ++i)
          fprintf(stderr, "%lu ", (std::uint64_t)sa[i]);
        fprintf(stderr, "\n");
        fprintf(stderr, "  computed result: ");
        for (std::uint64_t i = 0; i < v_computed.size(); ++i)
          fprintf(stderr, "%lu ", (std::uint64_t)v_computed[i]);
        fprintf(stderr, "\n");
        fprintf(stderr, "  correct result: ");
        for (std::uint64_t i = 0; i < v.size(); ++i)
          fprintf(stderr, "%lu ", (std::uint64_t)v[i]);
        fprintf(stderr, "\n");
        std::exit(EXIT_FAILURE);
      }
    }





    utils::file_delete(text_filename);
  }

  delete[] text;
  delete[] sa;
  delete[] suf_type;
}

int main() {
  srand(time(0) + getpid());
  for (std::uint64_t max_length = 1; max_length <= (1L << 14); max_length *= 2)
    test(1000, max_length);
  fprintf(stderr, "All tests passed.\n");
}
