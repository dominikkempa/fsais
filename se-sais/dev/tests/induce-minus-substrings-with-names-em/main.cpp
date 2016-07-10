#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "induce_minus_substrings.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_writer.hpp"

#include "utils.hpp"
#include "uint40.hpp"
#include "uint48.hpp"
#include "divsufsort.h"


struct substring {
  std::uint64_t m_beg;
  std::string m_str;

  substring() {}
  substring(std::uint64_t beg, std::string str) {
    m_beg = beg;
    m_str = str;
  }
};

struct substring_cmp {
  inline bool operator() (const substring &a, const substring &b) const {
    return (a.m_str == b.m_str) ? (a.m_beg < b.m_beg) : (a.m_str < b.m_str);
  }
};


void test(std::uint64_t n_testcases, std::uint64_t max_length, std::uint64_t radix_heap_bufsize, std::uint64_t radix_log) {
  fprintf(stderr, "TEST, n_testcases=%lu, max_length=%lu, buffer_size=%lu, radix_log=%lu\n", n_testcases, max_length, radix_heap_bufsize, radix_log);

  typedef std::uint8_t chr_t;
  typedef std::uint32_t saidx_tt;

  chr_t *text = new chr_t[max_length];
  saidx_tt *sa = new saidx_tt[max_length];
  bool *suf_type = new bool[max_length];

  for (std::uint64_t testid = 0; testid < n_testcases; ++testid) {
    if (testid % 100 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * testid) / n_testcases);
    std::uint64_t text_length = utils::random_int64(1L, (std::int64_t)max_length);
    for (std::uint64_t j = 0; j < text_length; ++j)
      text[j] = utils::random_int64(0L, 255L);
    divsufsort(text, (std::int32_t *)sa, text_length);
    std::uint64_t max_block_size = utils::random_int64(1L, (std::int64_t)text_length);
    std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;

    for (std::uint64_t i = text_length; i > 0; --i) {
      if (i == text_length) suf_type[i - 1] = 0;
      else {
        if (text[i - 1] > text[i]) suf_type[i - 1] = 0;
        else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;
        else suf_type[i - 1] = suf_type[i];
      }
    }

    std::vector<bool> is_leftmost_suffix(text_length, false);
    for (std::uint64_t j = 0; j < text_length; ++j)
      if (j > 0 && suf_type[j] != suf_type[j - 1])
        is_leftmost_suffix[j] = true;

    std::string plus_count_filename = "tmp." + utils::random_string_hash();
    {
      typedef async_stream_writer<saidx_t> writer_type;
      writer_type *writer = new writer_type(plus_count_filename);
      std::uint64_t end = text_length;
      for (std::uint64_t ch = 256; ch > 0; --ch) {
        std::uint64_t plus_star_count = 0;
        std::uint64_t beg = end;
        while (beg > 0 && text[sa[beg - 1]] == ch - 1) {
          if (sa[beg - 1] > 0 && suf_type[sa[beg - 1]] == 1 && suf_type[sa[beg - 1] - 1] == 0) ++plus_star_count;
          --beg;
        }
        writer->write(plus_star_count);
        end = beg;
      }
      delete writer;
    }

    std::vector<std::string> minus_pos_filenames;
    {
      for (std::uint64_t j = 0; j < n_blocks; ++j) {
        std::string filename = "tmp." + utils::random_string_hash();
        minus_pos_filenames.push_back(filename);
      }
      typedef async_stream_writer<saidx_tt> writer_type;
      writer_type **writers = new writer_type*[n_blocks];
      for (std::uint64_t j = 0; j < n_blocks; ++j)
        writers[j] = new writer_type(minus_pos_filenames[j]);

      // Create a list of minus substrings.
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 0) {
          std::string s;
          s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 0)
            s += text[end++];
          while (end < text_length && suf_type[end] == 1)
            s += text[end++];
          if (end < text_length) 
            s += text[end++];
          substrings.push_back(substring(j, s));
        }
      }

      // Sort the list.
      {
        substring_cmp cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
      }

      // Write the list to files.
      for (std::uint64_t j = 0; j < substrings.size(); ++j) {
        std::uint64_t s = substrings[j].m_beg;
        std::uint64_t block_id = s / max_block_size;
        writers[block_id]->write((saidx_tt)s);
      }

      // Clean up.
      for (std::uint64_t j = 0; j < n_blocks; ++j)
        delete writers[j];
      delete[] writers;
    }

    std::vector<std::string> minus_symbols_filenames;
    {
      for (std::uint64_t j = 0; j < n_blocks; ++j) {
        std::string filename = "tmp." + utils::random_string_hash();
        minus_symbols_filenames.push_back(filename);
      }
      typedef async_stream_writer<chr_t> writer_type;
      writer_type **writers = new writer_type*[n_blocks];
      for (std::uint64_t j = 0; j < n_blocks; ++j)
        writers[j] = new writer_type(minus_symbols_filenames[j]);

      // Create a list of minus substrings.
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 0) {
          std::string s;
          s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 0)
            s += text[end++];
          while (end < text_length && suf_type[end] == 1)
            s += text[end++];
          if (end < text_length) 
            s += text[end++];
          substrings.push_back(substring(j, s));
        }
      }

      // Sort the list.
      {
        substring_cmp cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
      }

      // Write the list to files.
      for (std::uint64_t j = 0; j < substrings.size(); ++j) {
        std::uint64_t s = substrings[j].m_beg;
        std::uint64_t block_id = s / max_block_size;
        std::uint8_t is_star = (s > 0 && suf_type[s - 1] == 1);
        if (s > 0 && !is_star)
          writers[block_id]->write(text[s - 1]);
      }

      // Clean up.
      for (std::uint64_t j = 0; j < n_blocks; ++j)
        delete writers[j];
      delete[] writers;
    }

    std::vector<std::string> minus_type_filenames;
    {
      for (std::uint64_t j = 0; j < n_blocks; ++j) {
        std::string filename = "tmp." + utils::random_string_hash();
        minus_type_filenames.push_back(filename);
      }
      typedef async_bit_stream_writer writer_type;
      writer_type **writers = new writer_type*[n_blocks];
      for (std::uint64_t j = 0; j < n_blocks; ++j)
        writers[j] = new writer_type(minus_type_filenames[j]);

      // Create a list of minus substrings.
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 0) {
          std::string s;
          s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 0)
            s += text[end++];
          while (end < text_length && suf_type[end] == 1)
            s += text[end++];
          if (end < text_length) 
            s += text[end++];
          substrings.push_back(substring(j, s));
        }
      }

      // Sort the list.
      {
        substring_cmp cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
      }

      // Write the list to files.
      for (std::uint64_t j = 0; j < substrings.size(); ++j) {
        std::uint64_t s = substrings[j].m_beg;
        std::uint64_t block_id = s / max_block_size;
        std::uint8_t is_star = (s > 0 && suf_type[s - 1] == 1);
        writers[block_id]->write(is_star);
      }

      // Clean up.
      for (std::uint64_t j = 0; j < n_blocks; ++j)
        delete writers[j];
      delete[] writers;
    }

    std::string plus_symbols_filename = "tmp." + utils::random_string_hash();
    {
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (j > 0 && suf_type[j] == 1 && is_leftmost_suffix[j]) {
          std::string s;
          s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 1)
            s += text[end++];
          if (end < text_length) 
            s += text[end++];
          substrings.push_back(substring(j, s));
        }
      }

      // Sort the list.
      {
        substring_cmp cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
      }

      // Write the list to file.
      {
        typedef async_stream_writer<chr_t> writer_type;
        writer_type *writer = new writer_type(plus_symbols_filename);
        for (std::uint64_t j = substrings.size(); j > 0; --j)
          writer->write(text[substrings[j - 1].m_beg - 1]);
        delete writer;
      }
    }

    std::string plus_substrings_filename = "tmp." + utils::random_string_hash();
    {
      // Create a list of plus-substrings.
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
//        if (/*j > 0 &&*/ suf_type[j] == 1 /*&& is_leftmost_suffix[j]*/) {  // the check for is_leftmost_suffix is optional
        if (j > 0 && suf_type[j] == 1 && is_leftmost_suffix[j]) {  // the check for is_leftmost_suffix is optional
          std::string s;
          s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 1)
            s += text[end++];
          if (end < text_length) 
            s += text[end++];
          substrings.push_back(substring(j, s));
        }
      }

      // Sort the list.
      {
        substring_cmp cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
      }

      // Write the list to file.
      {
        typedef async_stream_writer<saidx_tt> writer_type;
        writer_type *writer = new writer_type(plus_substrings_filename);
        std::uint64_t diff_items_counter = 0;
        for (std::uint64_t j = 0; j < substrings.size(); ++j) {
          if (j == 0 || substrings[j].m_str != substrings[j - 1].m_str)
            ++diff_items_counter;

          writer->write((saidx_tt)substrings[j].m_beg);
          writer->write((saidx_tt)(diff_items_counter - 1));
        }
        delete writer;
      }
    }

    // Create a vector with all minus-substring
    // sorter, i.e., the correct answer.
    std::vector<saidx_tt> v_correct;
    std::vector<saidx_tt> v_correct_names;
    std::vector<substring> substrings;
    {
      // Create a list of minus-substrings.
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 0) {
          std::string s;
          s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 0)
            s += text[end++];
          while (end < text_length && suf_type[end] == 1)
            s += text[end++];
          if (end < text_length)
            s += text[end++];
          substrings.push_back(substring(j, s));
        }
      }

      // Sort the list.
      {
        substring_cmp cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
      }

      std::uint64_t diff_items_counter = 0;
      for (std::uint64_t j = 0; j < substrings.size(); ++j) {
        if (j == 0 || substrings[j].m_str != substrings[j - 1].m_str)
          ++diff_items_counter;
        v_correct.push_back(substrings[j].m_beg);
        v_correct_names.push_back((saidx_tt)(diff_items_counter - 1));
      }
    }

    // Run the tested algorithm.
    std::string minus_substrings_filename = "tmp." + utils::random_string_hash();
    std::uint64_t total_io_volume = 0;
    induce_minus_substrings<chr_t, saidx_tt>(text_length,
        plus_substrings_filename, minus_substrings_filename,
        plus_count_filename,
        plus_symbols_filename,
        minus_type_filenames,
        minus_symbols_filenames,
        minus_pos_filenames,
        total_io_volume, radix_heap_bufsize, radix_log,
        max_block_size, text[text_length - 1]);

    // Delete input files.
    utils::file_delete(plus_substrings_filename);
    utils::file_delete(plus_count_filename);
    utils::file_delete(plus_symbols_filename);
    for (std::uint64_t j = 0; j < n_blocks; ++j) {
      if (utils::file_exists(minus_type_filenames[j])) utils::file_delete(minus_type_filenames[j]);
      if (utils::file_exists(minus_symbols_filenames[j])) utils::file_delete(minus_symbols_filenames[j]);
      if (utils::file_exists(minus_pos_filenames[j])) utils::file_delete(minus_pos_filenames[j]);
    }
    
    // Read the computed output into vector.
    std::vector<saidx_tt> v_computed;
    std::vector<saidx_tt> v_computed_names;
    {
      typedef async_stream_reader<saidx_tt> reader_type;
      reader_type *reader = new reader_type(minus_substrings_filename);
      while (!reader->empty()) {
        v_computed.push_back(reader->read());
        v_computed_names.push_back(reader->read());
      }
      delete reader;
    }

    // Delete output file.
    utils::file_delete(minus_substrings_filename);

    // Compare answer.
    bool ok = true;
    if (v_correct.size() != v_computed.size()) ok = false;
    else if (!std::equal(v_correct.begin(), v_correct.end(), v_computed.begin()) || 
        !std::equal(v_correct_names.begin(), v_correct_names.end(), v_computed_names.begin())) ok = false;
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
      fprintf(stderr, "  computed names: ");
      for (std::uint64_t i = 0; i < v_computed_names.size(); ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)v_computed_names[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  correct result: ");
      for (std::uint64_t i = 0; i < v_correct.size(); ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)v_correct[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  correct names: ");
      for (std::uint64_t i = 0; i < v_correct_names.size(); ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)v_correct_names[i]);
      fprintf(stderr, "\n");
      std::exit(EXIT_FAILURE);
    }
  }

  delete[] text;
  delete[] sa;
  delete[] suf_type;
}

int main() {
  srand(time(0) + getpid());

  for (std::uint64_t max_length = 1; max_length <= (1L << 14); max_length *= 2)
    for (std::uint64_t buffer_size = 1; buffer_size <= (1L << 10); buffer_size *= 2)
      for (std::uint64_t radix_log = 1; radix_log <= 5; ++radix_log)
        test(50, max_length, buffer_size, radix_log);

  fprintf(stderr, "All tests passed.\n");
}
