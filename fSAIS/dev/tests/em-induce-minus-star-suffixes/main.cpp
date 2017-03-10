#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "em_induce_minus_star_suffixes.hpp"
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
    for (std::uint64_t j = 0; j < text_length; ++j)
      text[j] = utils::random_int64(0L, 255L);
    divsufsort(text, (std::int32_t *)sa, text_length);
    std::uint64_t max_block_size = 0;
    std::uint64_t n_blocks = 0;
    do {
      max_block_size = utils::random_int64(1L, (std::int64_t)text_length);
      n_blocks = (text_length + max_block_size - 1) / max_block_size;
    } while (n_blocks > 256);

    std::uint64_t ram_use = utils::random_int64(1L, 1024L);
    std::uint64_t total_io_volume = 0;
    typedef std::uint32_t block_offset_type;
    typedef std::uint8_t block_id_type;








    for (std::uint64_t i = text_length; i > 0; --i) {
      if (i == text_length) suf_type[i - 1] = 0;              // minus
      else {
        if (text[i - 1] > text[i]) suf_type[i - 1] = 0;       // minus
        else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;  // plus
        else suf_type[i - 1] = suf_type[i];
      }
    }










    std::string plus_count_filename = "tmp." + utils::random_string_hash();
    std::string plus_pos_filename = "tmp." + utils::random_string_hash();
    {
#if 1
      {
        typedef async_stream_writer<block_id_type> writer_type;
        writer_type *writer = new writer_type(plus_pos_filename);
        for (std::uint64_t iplus = text_length; iplus > 0; --iplus) {
          std::uint64_t i = iplus - 1;
          std::uint64_t s = sa[i];
          if (s > 0 && suf_type[s] == 1 && suf_type[s - 1] == 0)
            writer->write(s / max_block_size);
        }
        delete writer;
      }
      {
        typedef async_stream_writer<text_offset_type> writer_type;
        writer_type *writer = new writer_type(plus_count_filename);
        std::uint64_t end = text_length;
        for (std::uint64_t ch = 256; ch > 0; --ch) {
          std::uint64_t plus_count = 0;
          std::uint64_t beg = end;
          while (beg > 0 && text[sa[beg - 1]] == ch - 1) {
            if (suf_type[sa[beg - 1]] == 1 && sa[beg - 1] > 0 && suf_type[sa[beg - 1] - 1] == 0) ++plus_count;
            --beg;
          }
          writer->write(plus_count);
          end = beg;
        }
        delete writer;
      }
#else
      // XXX
#endif
    }













    std::vector<std::uint64_t> target_block_count(n_blocks, std::numeric_limits<std::uint64_t>::max());
    std::vector<std::string> symbols_filenames;
    std::vector<std::string> minus_type_filenames;
    std::vector<std::string> minus_pos_filenames;
    { 
      {
        std::vector<std::uint64_t> block_count(n_blocks, 0UL);
        for (std::uint64_t j = 0; j < text_length; ++j) {
          std::uint64_t s = sa[j];
          if (suf_type[s] == 0 || (s > 0 && suf_type[s - 1] == 0)) {
            std::uint64_t block_id = s / max_block_size;
            ++block_count[block_id];
            bool is_at_block_beg = (block_id * max_block_size == s);
            if (is_at_block_beg) target_block_count[block_id] = block_count[block_id];
          }
        }
      }
      {
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          symbols_filenames.push_back(std::string("tmp.") + utils::random_string_hash());
        typedef async_stream_writer<char_type> writer_type;
        writer_type **writers = new writer_type*[n_blocks];
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          writers[j] = new writer_type(symbols_filenames[j]);
        for (std::uint64_t j = 0; j < text_length; ++j) {
          std::uint64_t s = sa[j];
          if (s > 0 && suf_type[s - 1] == 0) writers[s / max_block_size]->write(text[s - 1]);
        }
        for (std::uint64_t j = 0; j < n_blocks; ++j) delete writers[j];
        delete[] writers;
      }
      {
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          minus_type_filenames.push_back(std::string("tmp.") + utils::random_string_hash());
        typedef async_bit_stream_writer writer_type;
        writer_type **writers = new writer_type*[n_blocks];
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          writers[j] = new writer_type(minus_type_filenames[j]);
        for (std::uint64_t j = 0; j < text_length; ++j) {
          std::uint64_t s = sa[j];
          if (suf_type[s] == 0) writers[s / max_block_size]->write(s > 0 && suf_type[s - 1] == 1);
        }
        for (std::uint64_t j = 0; j < n_blocks; ++j) delete writers[j];
        delete[] writers;
      }
      {
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          minus_pos_filenames.push_back(std::string("tmp.") + utils::random_string_hash());
        typedef async_stream_writer<block_offset_type> writer_type;
        writer_type **writers = new writer_type*[n_blocks];
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          writers[j] = new writer_type(minus_pos_filenames[j]);
        for (std::uint64_t j = 0; j < text_length; ++j) {
          std::uint64_t s = sa[j];
          if (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1) {
            std::uint64_t block_id = s / max_block_size;
            std::uint64_t block_beg = block_id * max_block_size;
            writers[block_id]->write(s - block_beg);
          }
        }
        for (std::uint64_t j = 0; j < n_blocks; ++j) delete writers[j];
        delete[] writers;
      }
    }










    // Run tested algorithm.
    std::string output_filename = "tmp." + utils::random_string_hash();
    char_type last_text_symbol = text[text_length - 1];
    em_induce_minus_star_suffixes<
      char_type,
      text_offset_type,
      block_offset_type,
      block_id_type>(
        text_length,
        max_block_size,
        ram_use,
        target_block_count,
        last_text_symbol,
        output_filename,
        plus_pos_filename,
        plus_count_filename,
        minus_type_filenames,
        minus_pos_filenames,
        symbols_filenames,
        total_io_volume);










    // Delete input files.
    utils::file_delete(plus_pos_filename);
    utils::file_delete(plus_count_filename);
    for (std::uint64_t j = 0; j < n_blocks; ++j) {
      if (utils::file_exists(symbols_filenames[j])) utils::file_delete(symbols_filenames[j]);
      if (utils::file_exists(minus_type_filenames[j])) utils::file_delete(minus_type_filenames[j]);
      if (utils::file_exists(minus_pos_filenames[j])) utils::file_delete(minus_pos_filenames[j]);
    }









    // Compare answers.
    {
      std::vector<text_offset_type> v;
      for (std::uint64_t i = 0; i < text_length; ++i) {
        std::uint64_t s = sa[i];
        if (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1)
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

