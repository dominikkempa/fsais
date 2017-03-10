#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <map>
#include <ctime>
#include <unistd.h>

#include "em_induce_plus_star_substrings.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_backward_bit_stream_reader.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_writer.hpp"
#include "io/async_bit_stream_reader.hpp"
#include "packed_pair.hpp"
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









    typedef std::uint8_t blockidx_t;
    typedef std::uint16_t extext_blockidx_t;
    std::uint64_t max_block_size = 0;
    std::uint64_t n_blocks = 0;
    do {
      max_block_size = utils::random_int64(1L, (std::int64_t)text_length);
      n_blocks = (text_length + max_block_size - 1) / max_block_size;
    } while (n_blocks > 256);









    for (std::uint64_t i = text_length; i > 0; --i) {
      if (i == text_length) suf_type[i - 1] = 0;              // minus
      else {
        if (text[i - 1] > text[i]) suf_type[i - 1] = 0;       // minus
        else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;  // plus
        else suf_type[i - 1] = suf_type[i];
      }
    }








    // Input.
    std::string minus_data_filename = "tmp." + utils::random_string_hash();
    {
      typedef packed_pair<blockidx_t, chr_t> pair_type;
      typedef async_stream_writer<pair_type> writer_type;
      writer_type *writer = new writer_type(minus_data_filename);
      for (std::uint64_t i = text_length; i > 0; --i) {
        std::uint64_t s = i - 1;
        if (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1) {
          std::uint64_t block_id = s / max_block_size;
          pair_type p((blockidx_t)block_id, text[s]);
          writer->write(p);
        }
      }
      delete writer;
    }










    // Input from in-RAM inducing.
    std::vector<std::uint64_t> block_count_target(n_blocks, 0UL);
    std::vector<std::string> pos_filenames;
    std::vector<std::string> symbols_filenames;
    std::vector<std::string> plus_type_filenames;
    {
      {
        std::vector<std::uint64_t> block_count(n_blocks, 0UL);
        std::vector<substring> substrings;
        for (std::uint64_t j = 0; j < text_length; ++j) {
          if (suf_type[j] == 1) {
            std::string s; s = text[j];
            std::uint64_t end = j + 1;
            while (end < text_length && suf_type[end] == 1) s += text[end++];
            if (end < text_length)  s += text[end++];
            substrings.push_back(substring(j, s));
          } else if (j > 0 && suf_type[j - 1] == 1) {
            std::string s; s = text[j];
            substrings.push_back(substring(j, s));
          }
        }
        substring_cmp_2 cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
        for (std::uint64_t j = substrings.size(); j > 0; --j) {
          std::uint64_t s = substrings[j - 1].m_beg;
          std::uint64_t block_id = s / max_block_size;
          std::uint8_t is_block_beg = (block_id * max_block_size == s);
          ++block_count[block_id];
          if (is_block_beg)
            block_count_target[block_id] = block_count[block_id];
        }
      }
      {
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          pos_filenames.push_back(std::string("tmp.") + utils::random_string_hash());
        typedef async_stream_writer<saidx_tt> writer_type;
        writer_type **writers = new writer_type*[n_blocks];
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          writers[j] = new writer_type(pos_filenames[j]);
        std::vector<substring> substrings;
        for (std::uint64_t j = 0; j < text_length; ++j) {
          if (suf_type[j] == 1 && j > 0 && suf_type[j - 1] == 0) {
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
          std::uint64_t block_id = s / max_block_size;
          writers[block_id]->write((saidx_tt)s);
        }
        for (std::uint64_t j = 0; j < n_blocks; ++j) delete writers[j];
        delete[] writers;
      }
      {
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          symbols_filenames.push_back(std::string("tmp.") + utils::random_string_hash());
        typedef async_stream_writer<chr_t> writer_type;
        writer_type **writers = new writer_type*[n_blocks];
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          writers[j] = new writer_type(symbols_filenames[j]);
        std::vector<substring> substrings;
        for (std::uint64_t j = 0; j < text_length; ++j) {
          if (j > 0) {
            if (suf_type[j] == 1) {
              if (suf_type[j - 1] == 1) {
                std::string s; s = text[j];
                std::uint64_t end = j + 1;
                while (end < text_length && suf_type[end] == 1) s += text[end++];
                if (end < text_length) s += text[end++];
                substrings.push_back(substring(j, s));
              }
            } else if (suf_type[j - 1] == 1) {
              std::string s; s = text[j];
              substrings.push_back(substring(j, s));
            }
          }
        }
        substring_cmp_2 cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
        for (std::uint64_t j = 0; j < substrings.size(); ++j) {
          std::uint64_t s = substrings[j].m_beg;
          std::uint64_t block_id = s / max_block_size;
          writers[block_id]->write(text[s - 1]);
        }
        for (std::uint64_t j = 0; j < n_blocks; ++j) delete writers[j];
        delete[] writers;
      }
      {
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          plus_type_filenames.push_back(std::string("tmp.") + utils::random_string_hash());
        typedef async_bit_stream_writer writer_type;
        writer_type **writers = new writer_type*[n_blocks];
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          writers[j] = new writer_type(plus_type_filenames[j]);
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
        for (std::uint64_t j = 0; j < substrings.size(); ++j) {
          std::uint64_t s = substrings[j].m_beg;
          std::uint64_t block_id = s / max_block_size;
          std::uint8_t is_star = (s > 0 && suf_type[s - 1] == 0);
          writers[block_id]->write(is_star);
        }
        for (std::uint64_t j = 0; j < n_blocks; ++j) delete writers[j];
        delete[] writers;
      }
    }








    // Run the tested algorithm.
    std::uint64_t total_io_volume = 0;
    std::string output_pos_filename = "tmp." + utils::random_string_hash();
    std::string output_diff_filename = "tmp." + utils::random_string_hash();
    std::string output_count_filename = "tmp." + utils::random_string_hash();
    em_induce_plus_star_substrings<chr_t, saidx_tt, blockidx_t, extext_blockidx_t>(
        text_length,
        radix_heap_bufsize,
        radix_log,
        max_block_size,
        block_count_target,
        minus_data_filename,
        output_pos_filename,
        output_diff_filename,
        output_count_filename,
        plus_type_filenames,
        symbols_filenames,
        pos_filenames,
        total_io_volume);






    // Delete input files.
    utils::file_delete(minus_data_filename);
    for (std::uint64_t j = 0; j < n_blocks; ++j) {
      if (utils::file_exists(plus_type_filenames[j])) utils::file_delete(plus_type_filenames[j]);
      if (utils::file_exists(symbols_filenames[j])) utils::file_delete(symbols_filenames[j]);
      if (utils::file_exists(pos_filenames[j])) utils::file_delete(pos_filenames[j]);
    }









    // Check the answer.
    {
      std::vector<saidx_tt> v_correct;
      std::vector<substring> substrings;
      {
        for (std::uint64_t j = 0; j < text_length; ++j) {
          if (j > 0 && suf_type[j] == 1 && suf_type[j - 1] == 0) {
            std::string s; s = text[j];
            std::uint64_t end = j + 1;
            while (end < text_length && suf_type[end] == 1) s += text[end++];
            if (end < text_length) s += text[end++];
            substrings.push_back(substring(j, s));
          }
        }
        substring_cmp cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
        for (std::uint64_t j = 0; j < substrings.size(); ++j)
          v_correct.push_back(substrings[j].m_beg);
      }
      std::vector<saidx_tt> v_computed;
      std::vector<saidx_tt> v_computed_names;
      {
        typedef async_backward_stream_reader<saidx_tt> reader_type;
        reader_type *reader = new reader_type(output_pos_filename);
        while (!reader->empty())
          v_computed.push_back(reader->read());
        delete reader;
      }
      {
        typedef async_backward_bit_stream_reader reader_type;
        reader_type *reader = new reader_type(output_diff_filename);
        std::uint64_t diff_names = 1;
        v_computed_names.push_back(0);
        for (std::uint64_t j = 0; j + 1 < v_computed.size(); ++j) {
          std::uint8_t bit = reader->read();
          if (bit) ++diff_names;
          v_computed_names.push_back(diff_names - 1);
        }
        delete reader;
      }
      std::vector<std::pair<chr_t, std::uint64_t> > v_computed_count;
      {
        typedef async_backward_stream_reader<saidx_t> reader_type;
        reader_type *reader = new reader_type(output_count_filename);
        chr_t cur_sym = 0;
        while (reader->empty() == false) {
          std::uint64_t count = reader->read();
          if (count > 0)
            v_computed_count.push_back(std::make_pair(cur_sym, count));
          ++cur_sym;
        }
        delete reader;
      }
      std::vector<std::pair<chr_t, std::uint64_t> > v_correct_count;
      {
        std::map<chr_t, std::uint64_t> m;
        for (std::uint64_t j = 0; j < text_length; ++j) {
          std::uint64_t s = sa[j];
          if (s > 0 && suf_type[s] == 1 && suf_type[s - 1] == 0)
            m[text[s]] += 1;
        }
        for (std::map<chr_t, std::uint64_t>::iterator it = m.begin(); it != m.end(); ++it)
          v_correct_count.push_back(std::make_pair(it->first, it->second));
      }
      if (v_computed_count.size() != v_correct_count.size() ||
          std::equal(v_computed_count.begin(), v_computed_count.end(), v_correct_count.begin()) == false) {
        fprintf(stderr, "Error: counts do not match!\n");
        std::exit(EXIT_FAILURE);
      }
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









    // Delete output file.
    utils::file_delete(output_pos_filename);
    utils::file_delete(output_diff_filename);
    utils::file_delete(output_count_filename);

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
        test(2000, max_length, buffer_size, radix_log);

  fprintf(stderr, "All tests passed.\n");
}
