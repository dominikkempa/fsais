#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <map>
#include <unistd.h>

#include "em_induce_minus_star_substrings.hpp"
#include "em_induce_plus_star_substrings.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_writer.hpp"

#include "utils.hpp"
#include "uint40.hpp"
#include "uint48.hpp"
#include "divsufsort.h"
#include "packed_pair.hpp"


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

struct substring_cmp_3 {
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
    typedef std::uint16_t ext_blockidx_t;
    std::uint64_t max_block_size = 0;
    std::uint64_t n_blocks = 0;
    do {
      max_block_size = utils::random_int64(1L, (std::int64_t)text_length);
      n_blocks = (text_length + max_block_size - 1) / max_block_size;
    } while (n_blocks > 256);







    for (std::uint64_t i = text_length; i > 0; --i) {
      if (i == text_length) suf_type[i - 1] = 0;
      else {
        if (text[i - 1] > text[i]) suf_type[i - 1] = 0;
        else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;
        else suf_type[i - 1] = suf_type[i];
      }
    }











    // Input.
    std::string plus_count_filename = "tmp." + utils::random_string_hash();
    std::string plus_pos_filename = "tmp." + utils::random_string_hash();
    std::string plus_diff_filename = "tmp." + utils::random_string_hash();
    {
#if 0
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
      {
        std::vector<substring> substrings;
        for (std::uint64_t j = 0; j < text_length; ++j) {
          if (j > 0 && suf_type[j] == 1 && suf_type[j - 1] == 0) {
            std::string s; s = text[j];
            std::uint64_t end = j + 1;
            while (end < text_length && suf_type[end] == 1) s += text[end++];
            if (end < text_length) s += text[end++];
            substrings.push_back(substring(j, s));
          }
        }
        substring_cmp_3 cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
        {
          typedef async_stream_writer<blockidx_t> writer_1_type;
          typedef async_bit_stream_writer writer_2_type;
          writer_1_type *writer_1 = new writer_1_type(plus_pos_filename);
          writer_2_type *writer_2 = new writer_2_type(plus_diff_filename);
          std::vector<std::uint8_t> v_aux;
          for (std::uint64_t j = 0; j < substrings.size(); ++j) {
            std::uint8_t is_diff = 0;
            if (j == 0 || substrings[j].m_str != substrings[j - 1].m_str) is_diff = 1;
            if (j > 0) v_aux.push_back(is_diff);
          }
          for (std::uint64_t j = v_aux.size(); j > 0; --j) writer_2->write(v_aux[j - 1]);
          for (std::uint64_t j = substrings.size(); j > 0; --j) writer_1->write((blockidx_t)(substrings[j - 1].m_beg / max_block_size));
          delete writer_1;
          delete writer_2;
        }
      }
#else
      // Proper input.
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
            symbols_filenames.push_back(std::string("tmp.") + utils::random_string_hash());
          typedef async_stream_writer<chr_t> writer_type;
          writer_type **writers = new writer_type*[n_blocks];
          for (std::uint64_t j = 0; j < n_blocks; ++j)
            writers[j] = new writer_type(symbols_filenames[j]);
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
          for (std::uint64_t jplus = substrings.size(); jplus > 0; --jplus) {
            std::uint64_t j = jplus - 1;
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
      typedef std::uint16_t extext_blockidx_t;
      em_induce_plus_star_substrings<chr_t, saidx_tt, blockidx_t, extext_blockidx_t>(
          text_length,
          radix_heap_bufsize,
          radix_log,
          max_block_size,
          255,
          block_count_target,
          minus_data_filename,
          plus_pos_filename,
          plus_diff_filename,
          plus_count_filename,
          plus_type_filenames,
          symbols_filenames,
          total_io_volume);

      // Delete input files.
      utils::file_delete(minus_data_filename);
      for (std::uint64_t j = 0; j < n_blocks; ++j) {
        if (utils::file_exists(plus_type_filenames[j])) utils::file_delete(plus_type_filenames[j]);
        if (utils::file_exists(symbols_filenames[j])) utils::file_delete(symbols_filenames[j]);
      }
#endif
    }
















    // Input from in-RAM processing.
    std::vector<std::string> minus_pos_filenames;
    std::vector<std::string> symbols_filenames;
    std::vector<std::string> minus_type_filenames;
    std::vector<std::uint64_t> block_beg_target(n_blocks, 0UL);
    {
      {
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          minus_pos_filenames.push_back(std::string("tmp.") + utils::random_string_hash());
        typedef async_stream_writer<saidx_tt> writer_type;
        writer_type **writers = new writer_type*[n_blocks];
        for (std::uint64_t j = 0; j < n_blocks; ++j)
          writers[j] = new writer_type(minus_pos_filenames[j]);
        std::vector<substring> substrings;
        for (std::uint64_t j = 0; j < text_length; ++j) {
          if (suf_type[j] == 0 && j > 0 && suf_type[j - 1] == 1) {
            std::string s; s = text[j];
            std::uint64_t end = j + 1;
            while (end < text_length && suf_type[end] == 0) s += text[end++];
            while (end < text_length && suf_type[end] == 1) s += text[end++];
            if (end < text_length)  s += text[end++];
            substrings.push_back(substring(j, s));
          }
        }
        substring_cmp_3 cmp;
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
        substring_cmp_3 cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
        for (std::uint64_t j = 0; j < substrings.size(); ++j) {
          std::uint64_t s = substrings[j].m_beg;
          std::uint64_t block_id = s / max_block_size;
          std::uint8_t is_minus_star = (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1);
          std::uint8_t is_plus_star  = (s > 0 && suf_type[s] == 1 && suf_type[s - 1] == 0);
          if (s > 0 && (!is_minus_star || is_plus_star))
            writers[block_id]->write(text[s - 1]);
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
        std::vector<substring> substrings;
        for (std::uint64_t j = 0; j < text_length; ++j) {
          if (suf_type[j] == 0) {
            std::string s; s = text[j];
            std::uint64_t end = j + 1;
            while (end < text_length && suf_type[end] == 0) s += text[end++];
            while (end < text_length && suf_type[end] == 1) s += text[end++];
            if (end < text_length)  s += text[end++];
            substrings.push_back(substring(j, s));
          }
        }
        substring_cmp_3 cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
        for (std::uint64_t j = 0; j < substrings.size(); ++j) {
          std::uint64_t s = substrings[j].m_beg;
          std::uint64_t block_id = s / max_block_size;
          std::uint8_t is_star = (s > 0 && suf_type[s - 1] == 1);
          writers[block_id]->write(is_star);
        }
        for (std::uint64_t j = 0; j < n_blocks; ++j) delete writers[j];
        delete[] writers;
      }
      {
        std::vector<std::uint64_t> block_beg_counter(n_blocks, 0UL);
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
        substring_cmp_3 cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
        for (std::uint64_t j = 0; j < substrings.size(); ++j) {
          std::uint64_t s = substrings[j].m_beg;
          std::uint64_t block_id = s / max_block_size;
          std::uint8_t is_block_beg = (block_id * max_block_size == substrings[j].m_beg);
          ++block_beg_counter[block_id];
          if (is_block_beg) block_beg_target[block_id] = block_beg_counter[block_id];
        }
      }
    }










    // Run the tested algorithm.
    std::string output_filename = "tmp." + utils::random_string_hash();
    std::string output_count_filename = "tmp." + utils::random_string_hash();
    std::uint64_t total_io_volume = 0;
    em_induce_minus_star_substrings<chr_t, saidx_tt, blockidx_t, ext_blockidx_t>(
        text_length,
        radix_heap_bufsize,
        radix_log,
        max_block_size,
        255,
        text[text_length - 1],
        block_beg_target,
        output_filename,
        output_count_filename,
        plus_pos_filename,
        plus_count_filename,
        plus_diff_filename,
        minus_type_filenames,
        minus_pos_filenames,
        symbols_filenames,
        total_io_volume);









    // Delete input files.
    utils::file_delete(plus_pos_filename);
    utils::file_delete(plus_count_filename);
    utils::file_delete(plus_diff_filename);
    for (std::uint64_t j = 0; j < n_blocks; ++j) {
      if (utils::file_exists(minus_type_filenames[j])) utils::file_delete(minus_type_filenames[j]);
      if (utils::file_exists(symbols_filenames[j])) utils::file_delete(symbols_filenames[j]);
      if (utils::file_exists(minus_pos_filenames[j])) utils::file_delete(minus_pos_filenames[j]);
    }
    







    // Check the answer.
    {
      std::vector<saidx_tt> v_correct;
      std::vector<saidx_tt> v_correct_names;
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
        substring_cmp_3 cmp;
        std::sort(substrings.begin(), substrings.end(), cmp);
        std::uint64_t diff_items_counter = 0;
        for (std::uint64_t j = 0; j < substrings.size(); ++j) {
          if (j == 0 || substrings[j].m_str != substrings[j - 1].m_str)
            ++diff_items_counter;
          v_correct.push_back(substrings[j].m_beg);
          v_correct_names.push_back((saidx_tt)(diff_items_counter - 1));
        }
      }
      std::vector<saidx_tt> v_computed;
      std::vector<saidx_tt> v_computed_names;
      {
        typedef async_stream_reader<saidx_tt> reader_type;
        reader_type *reader = new reader_type(output_filename);
        while (!reader->empty()) {
          v_computed.push_back(reader->read());
          v_computed_names.push_back(reader->read());
        }
        delete reader;
      }
      std::vector<std::pair<chr_t, std::uint64_t> > v_computed_count;
      {
        typedef async_stream_reader<saidx_t> reader_type;
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
          if (suf_type[s] == 0 && s > 0 && suf_type[s - 1] == 1)
            m[text[s]] += 1;
        }
        for (std::map<chr_t, std::uint64_t>::iterator it = m.begin(); it != m.end(); ++it)
          v_correct_count.push_back(std::make_pair(it->first, it->second));
      }
      if (v_computed_count.size() != v_correct_count.size() ||
          std::equal(v_computed_count.begin(), v_computed_count.end(), v_correct_count.begin()) == false) {
        fprintf(stderr, "Error: counts do not match!\n");
        fprintf(stderr, "  v_computed_count: ");
        for (std::uint64_t j = 0; j < v_computed_count.size(); ++j)
          fprintf(stderr, "(%lu, %lu) ", (std::uint64_t)v_computed_count[j].first, (std::uint64_t)v_computed_count[j].second);
        fprintf(stderr, "\n");
        fprintf(stderr, "  v_correct_count: ");
        for (std::uint64_t j = 0; j < v_correct_count.size(); ++j)
          fprintf(stderr, "(%lu, %lu) ", (std::uint64_t)v_correct_count[j].first, (std::uint64_t)v_correct_count[j].second);
        fprintf(stderr, "\n");
        std::exit(EXIT_FAILURE);
      }
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









    // Delete output files.
    utils::file_delete(output_filename);
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
        test(/*50*/2000, max_length, buffer_size, radix_log);

  fprintf(stderr, "All tests passed.\n");
}
