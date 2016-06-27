#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "induce_plus_star_substrings.hpp"
#include "induce_minus_star_substrings.hpp"
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

void test(std::uint64_t n_testcases, std::uint64_t max_length, std::uint64_t ram_use, std::uint64_t buffer_size) {
  fprintf(stderr, "TEST, n_testcases=%lu, max_length=%lu, ram_use=%lu, buffer_size=%lu\n", n_testcases, max_length, ram_use, buffer_size);

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
    
    // Create a vector with all minus-substring
    // sorter, i.e., the correct answer.
    std::vector<saidx_tt> v_correct;
    std::vector<substring> substrings;
    {
      // Create a list of minus-star-substrings.
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (j > 0 && suf_type[j] == 0 && suf_type[j - 1] == 1) {
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

      for (std::uint64_t j = 0; j < substrings.size(); ++j)
        v_correct.push_back(substrings[j].m_beg);
    }

    // Run the tested algorithms, i.e., compute
    // the order of all minus-substrings from scratch.
    std::uint64_t total_io_volume = 0;
    std::uint64_t ramuse = text_length * sizeof(chr_t) + ram_use;
    std::string plus_star_substrings_filename = "tmp." + utils::random_string_hash();
    std::string minus_star_substrings_filename = "tmp." + utils::random_string_hash();
    induce_plus_star_substrings<chr_t, saidx_tt> (text, text_length, ramuse,
        minus_star_positions_filename, plus_star_substrings_filename,  total_io_volume, buffer_size);
    utils::file_delete(minus_star_positions_filename);
    induce_minus_star_substrings<chr_t, saidx_tt>(text, text_length, ramuse,
        plus_star_substrings_filename, minus_star_substrings_filename, total_io_volume, buffer_size);
    utils::file_delete(plus_star_substrings_filename);

    // Read the computed output into vector.
    std::vector<saidx_tt> v_computed;
    {
      typedef async_stream_reader<saidx_tt> reader_type;
      reader_type *reader = new reader_type(minus_star_substrings_filename);
      while (!reader->empty())
        v_computed.push_back(reader->read());
      delete reader;
    }

    // Delete output file.
    utils::file_delete(minus_star_substrings_filename);

    // Compare answer.
    bool ok = true;
    if (v_correct.size() != v_computed.size()) ok = false;
    else {
      std::uint64_t beg = 0;
      while (beg < v_correct.size()) {
        std::uint64_t end = beg + 1;
        while (end < v_correct.size() && substrings[end].m_str == substrings[end - 1].m_str)
          ++end;
        std::sort(v_correct.begin() + beg, v_correct.begin() + end);
        std::sort(v_computed.begin() + beg, v_computed.begin() + end);
        beg = end;
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

  for (std::uint64_t max_length = 1; max_length <= (1UL << 15); max_length *= 2)
    for (std::uint64_t ram_use = 1; ram_use <= (1UL << 10); ram_use = std::max(ram_use + 1, (ram_use * 5 + 3) / 4))
      for (std::uint64_t buffer_size = 1; buffer_size <= (1UL << 10); buffer_size = std::max(buffer_size + 1, (buffer_size * 5 + 3) / 4))
        test(20, max_length, ram_use, buffer_size);

  fprintf(stderr, "All tests passed.\n");
}
