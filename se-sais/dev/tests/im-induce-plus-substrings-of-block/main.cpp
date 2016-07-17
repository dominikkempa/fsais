#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "im_induce_plus_star_substrings.hpp"
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
    if (utils::random_int64(0L, 1L)) {
      for (std::uint64_t j = 0; j < text_length; ++j)
        text[j] = utils::random_int64(0L, 255L);
    } else {
      for (std::uint64_t j = 0; j < text_length; ++j)
        text[j] = 'a' + utils::random_int64(0L, 5L);
    }
    divsufsort(text, (std::int32_t *)sa, text_length);

    for (std::uint64_t i = text_length; i > 0; --i) {
      if (i == text_length) suf_type[i - 1] = 0;              // minus
      else {
        if (text[i - 1] > text[i]) suf_type[i - 1] = 0;       // minus
        else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;  // plus
        else suf_type[i - 1] = suf_type[i];
      }
    }

    std::uint64_t max_block_size = 0;
    std::uint64_t n_blocks = 0;
    do {
      max_block_size = utils::random_int64((std::int64_t)1, (std::int64_t)text_length);
      n_blocks = (text_length + max_block_size - 1) / max_block_size;
    } while (n_blocks > 256);

    std::uint64_t block_beg = utils::random_int64((std::int64_t)0, (std::int64_t)text_length - 1);
    std::uint64_t block_end = std::min(text_length, block_beg + max_block_size);
    std::uint64_t block_size = block_end - block_beg;

    std::uint64_t extract_count_target = text_length;
    bool is_last_suf_minus = (!suf_type[block_end - 1]);

    // Compute extract_count_target (only if
    // type of tail is minus star or plus).
    {
      std::vector<substring> tmp_substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 1) {
          std::string s;
          s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 1)
            s += text[end++];
          if (end < text_length)
            s += text[end++];
          if (block_beg <= j && j < block_end) {
            tmp_substrings.push_back(substring(j, s));
          }
        } else if (j > 0 && suf_type[j - 1] == 1) {
          std::string s;
          s = text[j];
          if (block_beg < j && j < block_end)
            tmp_substrings.push_back(substring(j, s));
        }
      }

      // Sort the list.
      {
        substring_cmp cmp;
        std::sort(tmp_substrings.begin(), tmp_substrings.end(), cmp);
      }

      for (std::uint64_t j = tmp_substrings.size(); j > 0; --j)
        if (!is_last_suf_minus && tmp_substrings[j - 1].m_beg == block_end - 1)
          extract_count_target = tmp_substrings.size() - j;
    }

    // Create a vector with all minus-substring
    // sorter, i.e., the correct answer.
    std::vector<saidx_tt> v_correct;
    std::vector<substring> substrings;
    std::vector<substring> substring_subseq;
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
        if (block_beg <= (std::uint64_t)substrings[j].m_beg && (std::uint64_t)substrings[j].m_beg < block_end) {
          v_correct.push_back(substrings[j].m_beg);
          substring_subseq.push_back(substrings[j]);
        }
    }

    // Run the tested algorithm.
    std::string output_filename = "tmp." + utils::random_string_hash();
    chr_t *block = new chr_t[block_size];
    std::copy(text + block_beg, text + block_end, block);
    im_induce_plus_star_substrings<chr_t, saidx_tt>(block,
        text_length, max_block_size, block_beg, extract_count_target,
        is_last_suf_minus, output_filename);
    delete[] block;

    // Read the computed output into vector.
    std::vector<saidx_tt> v_computed;
    {
      typedef async_backward_stream_reader<saidx_tt> reader_type;
      reader_type *reader = new reader_type(output_filename);
      while (!reader->empty())
        v_computed.push_back(reader->read());
      delete reader;
    }

    // Delete output file.
    utils::file_delete(output_filename);

    // Compare answer.
    bool ok = true;
    if (v_correct.size() != v_computed.size()) ok = false;
    else {
      std::uint64_t beg = 0;
      while (beg < v_correct.size()) {
        std::uint64_t end = beg + 1;
        while (end < v_correct.size() && substring_subseq[end].m_str == substring_subseq[end - 1].m_str)
          ++end;
        std::sort(v_correct.begin() + beg, v_correct.begin() + end);
        std::sort(v_computed.begin() + beg, v_computed.begin() + end);

        //fprintf(stderr, "  range [beg..end) = [%lu..%lu)\n", beg, end);
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
      fprintf(stderr, "  block_beg = %lu, block_end = %lu\n", block_beg, block_end);
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
    for (std::uint64_t buffer_size = 1; buffer_size <= /*(1L << 10)*/1; buffer_size *= 2)
      for (std::uint64_t radix_log = 1; radix_log <= /*5*/1; ++radix_log)
        test(30000, max_length, buffer_size, radix_log);

  fprintf(stderr, "All tests passed.\n");
}
