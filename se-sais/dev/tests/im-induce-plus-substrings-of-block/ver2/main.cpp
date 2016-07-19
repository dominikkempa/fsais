#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "im_induce_substrings.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_reader.hpp"
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

struct substring_cmp_2 {
  inline bool operator() (const substring &a, const substring &b) const {
    return (a.m_str == b.m_str) ? (a.m_beg < b.m_beg) : (a.m_str < b.m_str);
  }
};


void test(std::uint64_t n_testcases, std::uint64_t max_length, std::uint64_t radix_heap_bufsize, std::uint64_t radix_log) {
  fprintf(stderr, "TEST, n_testcases=%lu, max_length=%lu, buffer_size=%lu, radix_log=%lu\n", n_testcases, max_length, radix_heap_bufsize, radix_log);

  typedef std::uint8_t char_type;
  typedef std::uint32_t text_offset_type;

  char_type *text = new char_type[max_length];
  text_offset_type *sa = new text_offset_type[max_length];
  bool *suf_type = new bool[max_length];

  for (std::uint64_t testid = 0; testid < n_testcases; ++testid) {
//    fprintf(stderr, "\nNEWTEST:\n");
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

    std::string text_filename = "tmp." + utils::random_string_hash();
    utils::write_to_file(text, text_length, text_filename);

    typedef std::uint32_t ext_block_offset_type;
    typedef std::uint32_t block_offset_type;

    std::uint64_t max_block_size = 0;
    std::uint64_t n_blocks = 0;
    do {
      max_block_size = utils::random_int64((std::int64_t)1, (std::int64_t)text_length);
      n_blocks = (text_length + max_block_size - 1) / max_block_size;
    } while (n_blocks > 256);

    std::uint64_t block_beg = utils::random_int64((std::int64_t)0, (std::int64_t)text_length - 1);
    std::uint64_t block_end = std::min(text_length, block_beg + max_block_size);
    std::uint64_t block_size = block_end - block_beg;
    std::uint64_t next_block_size = std::min(max_block_size, text_length - block_end);
    std::uint64_t next_block_end = block_end + next_block_size;
    bool is_last_minus = (!suf_type[next_block_end - 1]);






    std::vector<char_type> plus_symbols_correct;
    {
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (j > 0 && suf_type[j - 1]) {
          if (suf_type[j] == 1) {
            std::string s; s = text[j];
            std::uint64_t end = j + 1;
            while (end < text_length && suf_type[end] == 1) s += text[end++];
            if (end < text_length) s += text[end++];
            substrings.push_back(substring(j, s));
          } else {
            std::string s; s = text[j];
            substrings.push_back(substring(j, s));
          }
        }
      }
      substring_cmp_2 cmp;
      std::sort(substrings.begin(), substrings.end(), cmp);
      for (std::uint64_t jplus = substrings.size(); jplus > 0; --jplus) {
        std::uint64_t j = jplus - 1;
        std::uint64_t s = substrings[j].m_beg;
        if (block_beg <= s && s < block_end)
          plus_symbols_correct.push_back(text[s - 1]);
      }
    }



    std::vector<char_type> minus_symbols_correct;
    {
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 0) {
          std::string s; s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 0) s += text[end++];
          while (end < text_length && suf_type[end] == 1) s += text[end++];
          if (end < text_length)  s += text[end++];
          substrings.push_back(substring(j, s));
        } else if (j > 0 && suf_type[j - 1] == 0) {
          std::string s; s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 1) s += text[end++];
          if (end < text_length) s += text[end++];
          substrings.push_back(substring(j, s));
        }
      }
      substring_cmp_2 cmp;
      std::sort(substrings.begin(), substrings.end(), cmp);
      for (std::uint64_t j = 0; j < substrings.size(); ++j) {
        std::uint64_t s = substrings[j].m_beg;
        std::uint8_t is_minus_star = (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1);
        std::uint8_t is_plus_star  = (s > 0 && suf_type[s] == 1 && suf_type[s - 1] == 0);
        if (s > 0 && block_beg <= s && s < block_end && (!is_minus_star || is_plus_star))
          minus_symbols_correct.push_back(text[s - 1]);
      }
    }






    std::vector<bool> plus_type_correct;
    {
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 1) {
          std::string s; s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 1) s += text[end++];
          if (end < text_length) s += text[end++];
          substrings.push_back(substring(j, s));
        }
      }
      substring_cmp_2 cmp;
      std::sort(substrings.begin(), substrings.end(), cmp);
      for (std::uint64_t jplus = substrings.size(); jplus > 0; --jplus) {
        std::uint64_t j = jplus - 1;
        std::uint64_t s = substrings[j].m_beg;
        if (block_beg <= s && s < block_end) {
          std::uint8_t is_star = (s > 0 && suf_type[s - 1] == 0);
          plus_type_correct.push_back(is_star);
        }
      }
    }





    std::vector<bool> minus_type_correct;
    {
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 0) {
          std::string s; s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 0) s += text[end++];
          while (end < text_length && suf_type[end] == 1) s += text[end++];
          if (end < text_length) s += text[end++];
          substrings.push_back(substring(j, s));
        }
      }
      substring_cmp_2 cmp;
      std::sort(substrings.begin(), substrings.end(), cmp);
      for (std::uint64_t j = 0; j < substrings.size(); ++j) {
        std::uint64_t s = substrings[j].m_beg;
        if (block_beg <= s && s < block_end) {
          std::uint8_t is_star = (s > 0 && suf_type[s - 1] == 1);
          minus_type_correct.push_back(is_star);
        }
      }
    }






    std::vector<text_offset_type> plus_pos_correct;
    {
      std::vector<substring> substrings;
      for (std::uint64_t j = 0; j < text_length; ++j) {
        if (suf_type[j] == 1) {
          std::string s; s = text[j];
          std::uint64_t end = j + 1;
          while (end < text_length && suf_type[end] == 1) s += text[end++];
          if (end < text_length) s += text[end++];
          substrings.push_back(substring(j, s));
        }
      }
      substring_cmp_2 cmp;
      std::sort(substrings.begin(), substrings.end(), cmp);
      for (std::uint64_t j = 0; j < substrings.size(); ++j)
        if (block_beg <= substrings[j].m_beg && substrings[j].m_beg < block_end)
          plus_pos_correct.push_back(substrings[j].m_beg - block_beg);
      std::reverse(plus_pos_correct.begin(), plus_pos_correct.end());
    }








    std::vector<text_offset_type> minus_pos_correct;
    {
      std::vector<substring> substrings;
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
      substring_cmp_2 cmp;
      std::sort(substrings.begin(), substrings.end(), cmp);
      for (std::uint64_t j = 0; j < substrings.size(); ++j)
        if (block_beg <= substrings[j].m_beg && substrings[j].m_beg < block_end)
          minus_pos_correct.push_back(substrings[j].m_beg - block_beg);
    }







    // Run the tested algorithm.
    std::string output_plus_pos_filename = "tmp." + utils::random_string_hash();
    std::string output_plus_symbols_filename = "tmp." + utils::random_string_hash();
    std::string output_plus_type_filename = "tmp." + utils::random_string_hash();
    std::string output_minus_pos_filename = "tmp." + utils::random_string_hash();
    std::string output_minus_type_filename = "tmp." + utils::random_string_hash();
    std::string output_minus_symbols_filename = "tmp." + utils::random_string_hash();
    char_type *block = new char_type[block_size];
    std::copy(text + block_beg, text + block_end, block);
    std::uint64_t text_alphabet_size = (std::uint64_t)(*std::max_element(text, text + text_length)) + 1;
    im_induce_substrings<char_type,
      text_offset_type,
      block_offset_type,
      ext_block_offset_type>(
          block,
          text_alphabet_size,
          text_length,
          max_block_size,
          block_beg,
          is_last_minus,
          text_filename,
          output_plus_pos_filename,
          output_plus_symbols_filename,
          output_plus_type_filename,
          output_minus_pos_filename,
          output_minus_type_filename,
          output_minus_symbols_filename);
    delete[] block;
    utils::file_delete(text_filename);







    std::vector<char_type> plus_symbols_computed;
    {
      typedef async_stream_reader<char_type> reader_type;
      reader_type *reader = new reader_type(output_plus_symbols_filename);
      while (!reader->empty())
        plus_symbols_computed.push_back(reader->read());
      delete reader;
    }
    utils::file_delete(output_plus_symbols_filename);
    if (plus_symbols_correct.size() != plus_symbols_computed.size() ||
        std::equal(plus_symbols_correct.begin(), plus_symbols_correct.end(), plus_symbols_computed.begin()) == false) {
      fprintf(stderr, "Error: plus symbols not equal!\n");
      std::exit(EXIT_FAILURE);
    }






    std::vector<char_type> minus_symbols_computed;
    {
      typedef async_stream_reader<char_type> reader_type;
      reader_type *reader = new reader_type(output_minus_symbols_filename);
      while (!reader->empty())
        minus_symbols_computed.push_back(reader->read());
      delete reader;
    }
    utils::file_delete(output_minus_symbols_filename);
    if (minus_symbols_correct.size() != minus_symbols_computed.size() ||
        std::equal(minus_symbols_correct.begin(), minus_symbols_correct.end(), minus_symbols_computed.begin()) == false) {
      fprintf(stderr, "Error: minus symbols not equal!\n");
      fprintf(stderr, "  text: ");
      for (std::uint64_t i = 0; i < text_length; ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)text[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  computed result: ");
      for (std::uint64_t i = 0; i < minus_symbols_computed.size(); ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)minus_symbols_computed[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  correct result: ");
      for (std::uint64_t i = 0; i < minus_symbols_correct.size(); ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)minus_symbols_correct[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  block_beg = %lu, block_end = %lu\n", block_beg, block_end);
      std::exit(EXIT_FAILURE);
    }






    std::vector<bool> plus_type_computed;
    {
      typedef async_bit_stream_reader reader_type;
      reader_type *reader = new reader_type(output_plus_type_filename);
      for (std::uint64_t j = 0; j < plus_type_correct.size(); ++j)
        plus_type_computed.push_back(reader->read());
      delete reader;
    }
    utils::file_delete(output_plus_type_filename);
    if (plus_type_correct.size() != plus_type_computed.size() ||
        std::equal(plus_type_correct.begin(), plus_type_correct.end(), plus_type_computed.begin()) == false) {
      fprintf(stderr, "Error: plus bits are not correct!\n");
      std::exit(EXIT_FAILURE);
    }



    std::vector<bool> minus_type_computed;
    {
      typedef async_bit_stream_reader reader_type;
      reader_type *reader = new reader_type(output_minus_type_filename);
      for (std::uint64_t j = 0; j < minus_type_correct.size(); ++j)
        minus_type_computed.push_back(reader->read());
      delete reader;
    }
    utils::file_delete(output_minus_type_filename);
    if (minus_type_correct.size() != minus_type_computed.size() ||
        std::equal(minus_type_correct.begin(), minus_type_correct.end(), minus_type_computed.begin()) == false) {
      fprintf(stderr, "Error: minus bits are not correct!\n");
      std::exit(EXIT_FAILURE);
    }






    std::vector<text_offset_type> minus_pos_computed;
    {
      typedef async_stream_reader<block_offset_type> reader_type;
      reader_type *reader = new reader_type(output_minus_pos_filename);
      while (!reader->empty())
        minus_pos_computed.push_back(reader->read());
      delete reader;
    }
    utils::file_delete(output_minus_pos_filename);
    if (minus_pos_correct.size() != minus_pos_computed.size() ||
        !std::equal(minus_pos_correct.begin(), minus_pos_correct.end(), minus_pos_computed.begin())) {
      fprintf(stderr, "Error: minus pos not correct\n");
      fprintf(stderr, "  text: ");
      for (std::uint64_t i = 0; i < text_length; ++i)
        fprintf(stderr, "%c", text[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  computed result: ");
      for (std::uint64_t i = 0; i < minus_pos_computed.size(); ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)minus_pos_computed[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  correct result: ");
      for (std::uint64_t i = 0; i < minus_pos_correct.size(); ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)minus_pos_correct[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  block_beg = %lu, block_end = %lu\n", block_beg, block_end);
      std::exit(EXIT_FAILURE);
    }






    std::vector<text_offset_type> plus_pos_computed;
    {
      typedef async_stream_reader<block_offset_type> reader_type;
      reader_type *reader = new reader_type(output_plus_pos_filename);
      while (!reader->empty())
        plus_pos_computed.push_back(reader->read());
      delete reader;
    }
    utils::file_delete(output_plus_pos_filename);
    if (plus_pos_correct.size() != plus_pos_computed.size() ||
        !std::equal(plus_pos_correct.begin(), plus_pos_correct.end(), plus_pos_computed.begin())) {
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
      for (std::uint64_t i = 0; i < plus_pos_computed.size(); ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)plus_pos_computed[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  correct result: ");
      for (std::uint64_t i = 0; i < plus_pos_correct.size(); ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)plus_pos_correct[i]);
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
        test(5000, max_length, buffer_size, radix_log);

  fprintf(stderr, "All tests passed.\n");
}
