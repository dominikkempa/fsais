#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <map>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "em_induce_minus_star_substrings.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_writer.hpp"

#include "utils.hpp"
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

void test(std::uint64_t n_testcases, std::uint64_t max_length) {
  fprintf(stderr, "TEST, n_testcases=%lu, max_length=%lu\n", n_testcases, max_length);

  typedef std::uint32_t text_offset_type;
  typedef std::uint8_t char_type;

  char_type *text = new char_type[max_length];
  text_offset_type *sa = new text_offset_type[max_length];
  bool *suf_type = new bool[max_length];

  for (std::uint64_t testid = 0; testid < n_testcases; ++testid) {
    if (testid % 100 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * testid) / n_testcases);
    std::uint64_t text_length = utils::random_int64(1L, (std::int64_t)max_length);
    std::uint64_t text_alphabet_size = utils::random_int64(1L, 256L);
    for (std::uint64_t j = 0; j < text_length; ++j)
      text[j] = utils::random_int64(0L, (std::int64_t)text_alphabet_size - 1);
    divsufsort(text, (std::int32_t *)sa, text_length);

    std::string text_filename = "tmp." + utils::random_string_hash();
    utils::write_to_file(text, text_length, text_filename);

    std::uint64_t max_block_size = 0;
    std::uint64_t n_blocks = 0;
    do {
      max_block_size = utils::random_int64(1L, (std::int64_t)text_length);
      n_blocks = (text_length + max_block_size - 1) / max_block_size;
    } while (n_blocks > (1UL << 16));

    std::string output_filename = "tmp." + utils::random_string_hash();
    std::uint64_t ram_use = utils::random_int64(1L, 1024L);
    std::uint64_t io_volume = 0;

    // Close stderr.
    int stderr_backup = 0;
    std::fflush(stderr);
    stderr_backup = dup(2);
    int stderr_temp = open("/dev/null", O_WRONLY);
    dup2(stderr_temp, 2);
    close(stderr_temp);

    em_induce_minus_star_substrings<
      char_type,
      text_offset_type>(
          text_length,
          text_alphabet_size,
          max_block_size,
          ram_use,
          text_filename,
          output_filename,
          io_volume);

    // Restore stderr.
    std::fflush(stderr);
    dup2(stderr_backup, 2);
    close(stderr_backup);

    // Check the answer.
    {
      for (std::uint64_t i = text_length; i > 0; --i) {
        if (i == text_length) suf_type[i - 1] = 0;
        else {
          if (text[i - 1] > text[i]) suf_type[i - 1] = 0;
          else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;
          else suf_type[i - 1] = suf_type[i];
        }
      }
      {
        std::vector<text_offset_type> v_correct;
        std::vector<text_offset_type> v_correct_names;
        std::vector<substring> substrings;
        {
          for (std::uint64_t j = 0; j < text_length; ++j) {
            if (j > 0 && suf_type[j] == 0 && suf_type[j - 1] == 1) {
              std::string s; s = text[j];
              std::uint64_t end = j + 1;
              while (end < text_length && suf_type[end] == 0) s += text[end++];
              while (end < text_length && suf_type[end] == 1) s += text[end++];
              if (end < text_length) s += text[end++];
              substrings.push_back(substring(j, s));
            }
          }
          substring_cmp cmp;
          std::sort(substrings.begin(), substrings.end(), cmp);
          std::uint64_t diff_items_counter = 0;
          for (std::uint64_t j = 0; j < substrings.size(); ++j) {
            if (j == 0 || substrings[j].m_str != substrings[j - 1].m_str)
              ++diff_items_counter;
            v_correct.push_back(substrings[j].m_beg);
            v_correct_names.push_back(diff_items_counter - 1);
          }
        }
        std::vector<text_offset_type> v_computed;
        std::vector<text_offset_type> v_computed_names;
        {
          typedef async_stream_reader<text_offset_type> reader_type;
          reader_type *reader = new reader_type(output_filename);
          while (!reader->empty()) {
            v_computed.push_back(reader->read());
            v_computed_names.push_back(reader->read());
          }
          delete reader;
        }
        bool ok = true;
        if (v_correct.size() != v_computed.size()) ok = false;
        else if (!std::equal(v_correct.begin(), v_correct.end(), v_computed.begin()) || 
            !std::equal(v_correct_names.begin(), v_correct_names.end(), v_computed_names.begin())) ok = false;
        if (!ok) {
          fprintf(stderr, "\nError:\n");
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
    }

    utils::file_delete(output_filename);
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