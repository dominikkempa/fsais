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

#include "uint24.hpp"
#include "uint40.hpp"
#include "uint48.hpp"
#include "utils.hpp"
#include "divsufsort.h"


template<typename char_type>
class substr {
  public:
    typedef substr<char_type> substr_type;

    substr() {}
    substr(const char_type *text, std::uint64_t beg, std::uint64_t length, std::uint64_t text_length) {
      m_beg = beg;
      m_text_length = text_length;
      for (std::uint64_t j = 0; j < length; ++j)
        m_data.push_back(text[beg + j]);
    }

    inline bool operator < (const substr_type &s) const {
      std::uint64_t lcp = 0;
      while (m_beg + lcp < m_text_length && s.m_beg + lcp < m_text_length && m_data[lcp] == s.m_data[lcp]) ++lcp;
      return (m_beg + lcp == m_text_length || (s.m_beg + lcp < m_text_length && (std::uint64_t)m_data[lcp] < (std::uint64_t)s.m_data[lcp]));
    }

    std::uint64_t m_beg;
    std::uint64_t m_text_length;
    std::vector<char_type> m_data;
};

template<typename char_type,
  typename text_offset_type>
void naive_sufsort(const char_type *text,
    std::uint64_t text_length,
    text_offset_type *sa) {
  typedef substr<char_type> substr_type;
  std::vector<substr_type> substrings;
  for (std::uint64_t i = 0; i < text_length; ++i)
    substrings.push_back(substr_type(text, i, text_length - i, text_length));
  std::sort(substrings.begin(), substrings.end());
  for (std::uint64_t i = 0; i < text_length; ++i)
    sa[i] = substrings[i].m_beg;
}

template<typename char_type>
struct substring {
  std::uint64_t m_beg;
  std::vector<char_type> m_str;

  substring() {}
  substring(std::uint64_t beg, std::vector<char_type> &str) {
    m_beg = beg;
    m_str = str;
  }
};

template<typename char_type>
struct substring_cmp {
  inline bool operator() (const substring<char_type> &a, const substring<char_type> &b) const {
    std::uint64_t lcp = 0;
    while (lcp < a.m_str.size() && lcp < b.m_str.size() && a.m_str[lcp] == b.m_str[lcp])
      ++lcp;
    if (lcp == a.m_str.size() && lcp == b.m_str.size()) return a.m_beg < b.m_beg;
    else return (lcp == a.m_str.size() || (lcp < b.m_str.size() && (std::uint64_t)a.m_str[lcp] < (std::uint64_t)b.m_str[lcp]));
  }
};

void test(std::uint64_t n_testcases, std::uint64_t max_length) {
  fprintf(stderr, "TEST, n_testcases=%lu, max_length=%lu\n", n_testcases, max_length);

  typedef uint24 char_type;
  typedef uint40 text_offset_type;

  char_type *text = new char_type[max_length];
  text_offset_type *sa = new text_offset_type[max_length];
  bool *suf_type = new bool[max_length];

  for (std::uint64_t testid = 0; testid < n_testcases; ++testid) {
    fprintf(stderr, "%.2Lf%%\r", (100.L * testid) / n_testcases);
    std::uint64_t text_length = utils::random_int64(1L, (std::int64_t)max_length);
    std::uint64_t text_alphabet_size = utils::random_int64(1L, (1L << 18));
    for (std::uint64_t j = 0; j < text_length; ++j)
      text[j] = utils::random_int64(0L, (std::int64_t)text_alphabet_size - 1);
    naive_sufsort(text, text_length, sa);

    std::string text_filename = "tmp." + utils::random_string_hash();
    utils::write_to_file(text, text_length, text_filename);

    std::uint64_t max_block_size = 0;
    std::uint64_t n_blocks = 0;
    do {
      max_block_size = utils::random_int64(1L, (std::int64_t)text_length);
      n_blocks = (text_length + max_block_size - 1) / max_block_size;
    } while (n_blocks > (1UL << 16));


    std::uint64_t max_permute_block_size = 0;
    std::uint64_t n_permute_blocks = 0;
    do {
      max_permute_block_size = utils::random_int64(1L, (std::int64_t)text_length);
      n_permute_blocks = (text_length + max_permute_block_size - 1) / max_permute_block_size;
    } while (n_permute_blocks > (1UL << 16));


    std::vector<std::string> output_pos_filenames(n_permute_blocks);
    for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
      output_pos_filenames[permute_block_id] = std::string("tmp.") + utils::random_string_hash();
    std::string tempfile_basename = "tmp";
    std::string output_count_filename = "tmp." + utils::random_string_hash();

    std::uint64_t ram_use = utils::random_int64(1L, 1024L);
    std::uint64_t io_volume = 0;

    // Close stderr.
    int stderr_backup = 0;
    std::fflush(stderr);
    stderr_backup = dup(2);
    int stderr_temp = open("/dev/null", O_WRONLY);
    dup2(stderr_temp, 2);
    close(stderr_temp);

    std::uint64_t n_names = em_induce_minus_star_substrings<
      char_type,
      text_offset_type>(
          text_length,
          text_alphabet_size,
          max_block_size,
          ram_use,
          max_permute_block_size,
          text_filename,
          tempfile_basename,
          output_count_filename,
          output_pos_filenames,
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
        typedef std::pair<text_offset_type, text_offset_type> pair_type;
        typedef std::vector<pair_type> vector_type;
        vector_type **v_correct = new vector_type*[n_permute_blocks];
        for (std::uint64_t j = 0; j < n_permute_blocks; ++j)
          v_correct[j] = new vector_type();

        typedef substring<char_type> substring_type;
        std::vector<substring_type> substrings;
        std::uint64_t diff_items_counter = 0;
        {
          for (std::uint64_t j = 0; j < text_length; ++j) {
            if (j > 0 && suf_type[j] == 0 && suf_type[j - 1] == 1) {
              std::vector<char_type> s;
              s.push_back(text[j]);
              std::uint64_t end = j + 1;
              while (end < text_length && suf_type[end] == 0) s.push_back(text[end++]);
              while (end < text_length && suf_type[end] == 1) s.push_back(text[end++]);
              if (end < text_length) s.push_back(text[end++]);
              substrings.push_back(substring_type(j, s));
            }
          }
          substring_cmp<char_type> cmp;
          std::sort(substrings.begin(), substrings.end(), cmp);
          for (std::uint64_t j = 0; j < substrings.size(); ++j) {
            if (j == 0 || substrings[j].m_str != substrings[j - 1].m_str)
              ++diff_items_counter;

            std::uint64_t permute_block_id = substrings[j].m_beg / max_permute_block_size;
            v_correct[permute_block_id]->push_back(std::make_pair(substrings[j].m_beg, diff_items_counter - 1));
          }
        }

        // Compare answers.
        if (diff_items_counter != n_names) {
          fprintf(stderr, "\nError:\n");
          fprintf(stderr, "  n_names(correct) = %lu\n", diff_items_counter);
          fprintf(stderr, "  n_names(computed) = %lu\n", n_names);
          std::exit(EXIT_FAILURE);
        }
        
        std::vector<std::pair<std::uint64_t, std::uint64_t> > v_computed_count;
        {
          typedef async_stream_reader<text_offset_type> reader_type;
          reader_type *reader = new reader_type(output_count_filename);
          std::uint64_t cur_sym = 0;
          while (reader->empty() == false) {
            std::uint64_t count = reader->read();
            if (count > 0)
              v_computed_count.push_back(std::make_pair(cur_sym, count));
            ++cur_sym;
          }
          delete reader;
        }

        std::vector<std::pair<std::uint64_t, std::uint64_t> > v_correct_count;
        {
          std::map<std::uint64_t, std::uint64_t> m;
          for (std::uint64_t j = 0; j < text_length; ++j) {
            std::uint64_t s = sa[j];
            if (suf_type[s] == 0 && s > 0 && suf_type[s - 1] == 1)
              m[text[s]] += 1;
          }
          for (std::map<std::uint64_t, std::uint64_t>::iterator it = m.begin(); it != m.end(); ++it)
            v_correct_count.push_back(std::make_pair(it->first, it->second));
        }

        if (v_computed_count.size() != v_correct_count.size() ||
            std::equal(v_computed_count.begin(), v_computed_count.end(), v_correct_count.begin()) == false) {
          fprintf(stderr, "Error: counts do not match!\n");
          fprintf(stderr, "  text = ");
          for (std::uint64_t j = 0; j < text_length; ++j)
            fprintf(stderr, "%lu ", (std::uint64_t)text[j]);
          fprintf(stderr, "\n");
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

        for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id) {
          std::vector<pair_type> v_computed;
          {
            typedef async_stream_reader<text_offset_type> reader_type;
            reader_type *reader = new reader_type(output_pos_filenames[permute_block_id]);
            while (!reader->empty()) {
              text_offset_type pos = reader->read();
              text_offset_type name = reader->read();
              v_computed.push_back(std::make_pair(pos, name));
            }
            delete reader;
          }

          if (v_computed.size() != v_correct[permute_block_id]->size() ||
              !std::equal(v_computed.begin(), v_computed.end(), v_correct[permute_block_id]->begin())) {
            fprintf(stderr, "\nError: incorrect names!\n");
            std::exit(EXIT_FAILURE);
          }
        }

        // Clean up.
        for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
          delete v_correct[permute_block_id];
        delete[] v_correct;
      }
    }

    for (std::uint64_t permute_block_id = 0; permute_block_id < n_permute_blocks; ++permute_block_id)
      utils::file_delete(output_pos_filenames[permute_block_id]);
    utils::file_delete(text_filename);
    utils::file_delete(output_count_filename);
  }

  delete[] text;
  delete[] sa;
  delete[] suf_type;
}

int main() {
  srand(time(0) + getpid());
  for (std::uint64_t max_length = 1; max_length <= (1L << 14); max_length *= 2)
    test(100, max_length);
  fprintf(stderr, "All tests passed.\n");
}
