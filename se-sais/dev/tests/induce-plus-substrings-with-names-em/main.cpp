#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "induce_plus_substrings.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
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
    return (a.m_str == b.m_str) ? (a.m_beg > b.m_beg) : (a.m_str < b.m_str);
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
      // text[j] = 'a' + utils::random_int64(0L, 25L);
    divsufsort(text, (std::int32_t *)sa, text_length);

    for (std::uint64_t i = text_length; i > 0; --i) {
      if (i == text_length) suf_type[i - 1] = 0;              // minus
      else {
        if (text[i - 1] > text[i]) suf_type[i - 1] = 0;       // minus
        else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;  // plus
        else suf_type[i - 1] = suf_type[i];
      }
    }

    // Write starting positions of all minus-star-substrings to file.
    std::string minus_star_positions_filename = "tmp." + utils::random_string_hash();
    {
      typedef async_stream_writer<saidx_tt> writer_type;
      writer_type *writer = new writer_type(minus_star_positions_filename);
      for (std::uint64_t i = 0; i < text_length; ++i)
        if (i > 0 && suf_type[i] == 0 && suf_type[i - 1] == 1)
          writer->write((saidx_tt)i);
      delete writer;
    }

    std::string minus_symbols_filename = "tmp." + utils::random_string_hash();
    {
      typedef async_stream_writer<chr_t> writer_type;
      writer_type *writer = new writer_type(minus_symbols_filename);
      for (std::uint64_t i = 0; i < text_length; ++i)
        if (i > 0 && suf_type[i] == 0 && suf_type[i - 1] == 1)
          writer->write(text[i]);
      delete writer;
    }

#if 0
    std::vector<std::string> plus_symbols_filenames;
    {
      for (std::uint64_t j = 0; j < n_blocks; ++j) {
        std::string filename = "tmp." + utils::random_string_hash();
        plus_symbols_filenames.push_back(filename);
      }
      typedef async_stream_writer<chr_t> writer_type;
      writer_type **writers = new writer_type*[n_blocks];
      for (std::uint64_t j = 0; j < n_blocks; ++j)
        writers[j] = new writer_type(plus_symbols_filenames[j]);

      // Create a list of minus substrings.
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 1) {
          std::string s;
          s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 1)
            s += text[end++];
          while (end < text_length && suf_type[end] == 0)
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
        std::uint64_t s = (substrings[j].m_beg + 1);
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
#endif
    
    // Create a vector with all minus-substring
    // sorter, i.e., the correct answer.
    std::vector<saidx_tt> v_correct;
    std::vector<substring> substrings;
    {
      // Create a list of minus-substrings.
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 1) {
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

      for (std::uint64_t j = 0; j < substrings.size(); ++j)
        v_correct.push_back(substrings[j].m_beg);
    }

    // Run the tested algorithm.
    std::string plus_substrings_filename = "tmp." + utils::random_string_hash();
    std::uint64_t total_io_volume = 0;
    induce_plus_substrings<chr_t, saidx_tt>(text, text_length,
        minus_star_positions_filename, minus_symbols_filename,
        plus_substrings_filename,
        total_io_volume, radix_heap_bufsize, radix_log);

    // Delete input files.
    utils::file_delete(minus_star_positions_filename);
    utils::file_delete(minus_symbols_filename);
    
    // Read the computed output into vector.
    std::vector<saidx_tt> v_computed;
    std::vector<saidx_tt> v_computed_names;
    {
      typedef async_backward_stream_reader<saidx_tt> reader_type;
      reader_type *reader = new reader_type(plus_substrings_filename);
      while (!reader->empty()) {
        v_computed_names.push_back(reader->read());
        v_computed.push_back(reader->read());
      }
      delete reader;
    }

    // At this point the names of susbtrings are inverted (since we
    // induced from the largest), so now we invert them back.
    if (!v_computed_names.empty()) {
      std::uint64_t max_name = (std::uint64_t)v_computed_names.front();
      for (std::uint64_t j = 0; j < v_computed_names.size(); ++j)
        v_computed_names[j] = (saidx_t)(max_name - (std::uint64_t)v_computed_names[j]);
    }

    // Delete output file.
    utils::file_delete(plus_substrings_filename);

    // Compare answer.
    bool ok = true;
    if (v_correct.size() != v_computed.size()) ok = false;
    else {
      std::uint64_t beg = 0;
      std::uint64_t group_count = 0;
      while (beg < v_correct.size()) {
        std::uint64_t end = beg + 1;
        while (end < v_correct.size() && substrings[end].m_str == substrings[end - 1].m_str)
          ++end;
        std::sort(v_correct.begin() + beg, v_correct.begin() + end);
        std::sort(v_computed.begin() + beg, v_computed.begin() + end);

        for (std::uint64_t j = beg; j < end; ++j)
          if ((std::uint64_t)v_computed_names[j] != group_count)
            ok = false;

        beg = end;
        ++group_count;
      }

      if (!std::equal(v_correct.begin(), v_correct.end(), v_computed.begin()))
        ok = false;
    }

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
        fprintf(stderr, "%ld ", (std::uint64_t)v_computed_names[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  correct result: ");
      for (std::uint64_t i = 0; i < v_correct.size(); ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)v_correct[i]);
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

  for (std::uint64_t max_length = 1; max_length <= (1L << 15); max_length *= 2)
    for (std::uint64_t buffer_size = 1; buffer_size <= (1L << 10); buffer_size *= 2)
      for (std::uint64_t radix_log = 1; radix_log <= 5; ++radix_log)
        test(50, max_length, buffer_size, radix_log);

  fprintf(stderr, "All tests passed.\n");
}
