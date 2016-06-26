#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "utils.hpp"
#include "divsufsort.h"


namespace large_alphabet_sufsort {

template<typename saidx_t>
struct substring {
  const saidx_t *m_text_ptr;
  saidx_t m_text_length;
  saidx_t m_beg;
  typedef substring<saidx_t> substring_type;

  substring() {}
  substring(const saidx_t *text_ptr, saidx_t text_length, saidx_t beg) {
    m_text_ptr = text_ptr;
    m_text_length = text_length;
    m_beg = beg;
  }

  inline bool operator < (const substring_type &s) const {
    if (m_beg == s.m_beg) return false;
    std::uint64_t lcp = 0;
    while (m_beg + lcp < m_text_length && s.m_beg + lcp < m_text_length &&
        m_text_ptr[m_beg + lcp] == m_text_ptr[s.m_beg + lcp]) ++lcp;
    return (m_beg + lcp == m_text_length) ||
      (s.m_beg + lcp < m_text_length && m_text_ptr[m_beg + lcp] < m_text_ptr[s.m_beg + lcp]);
  }
};

template<typename saidx_t>
void sort(const saidx_t *text, saidx_t *SA, std::uint64_t text_length) {
  typedef substring<saidx_t> substring_type;

  std::vector<substring_type> v(text_length);
  for (std::uint64_t j = 0; j < text_length; ++j)
    v[j] = substring_type(text, text_length, j);

  std::sort(v.begin(), v.end());
  for (std::uint64_t j = 0; j < text_length; ++j)
    SA[j] = v[j].m_beg;
}

}


struct substring {
  std::string m_substr;
  std::uint64_t m_beg;
  std::uint64_t m_length;
  std::uint64_t m_name;

  substring(std::string substr, std::uint64_t beg, std::uint64_t length) {
    m_substr = substr;
    m_beg = beg;
    m_length = length;
  }

  inline bool operator < (const substring &s) const {
    return m_substr < s.m_substr;
  }
};

struct cmp_by_subtr {
  inline bool operator () (const substring &s1, const substring &s2) const {
    return s1.m_substr < s2.m_substr;
  }
};

struct cmp_by_beg {
  inline bool operator () (const substring &s1, const substring &s2) const {
    return s1.m_beg < s2.m_beg;
  }
};

template<typename chr_t, typename saidx_t>
void sort_minus_star_substrings(const chr_t *text, std::uint64_t text_length, std::vector<saidx_t> &result) {
  std::vector<bool> suf_type(text_length);
  for (std::uint64_t i = text_length; i > 0; --i) {
    if (i == text_length) suf_type[i - 1] = 0;
    else {
      if (text[i - 1] > text[i]) suf_type[i - 1] = 0;
      else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;
      else suf_type[i - 1] = suf_type[i];
    }
  }

  // Create vector with minus-star-substrings.
  std::vector<substring> minus_star_substrings;
  for (std::uint64_t j = 0; j < text_length; ++j) {
    if (j > 0 && suf_type[j] == 0 && suf_type[j - 1] == 1) {
      std::string s;
      s += text[j];
      std::uint64_t end = j + 1;
      while (end < text_length && suf_type[end] == 0)
        s += text[end++];
      while (end < text_length && suf_type[end] == 1)
        s += text[end++];
      if (end < text_length)
        s += text[end++];
      minus_star_substrings.push_back(substring(s, j, end));
    }
  }

  if (minus_star_substrings.empty())
    return;

  // Sort minus-star-substrings by substr.
  {
    cmp_by_subtr cmp;
    std::sort(minus_star_substrings.begin(), minus_star_substrings.end(), cmp);
  }

  //
//  fprintf(stderr, "sorted by substr: ");
//  for (std::uint64_t j = 0; j < minus_star_substrings.size(); ++j)
//    fprintf(stderr, "%s ", minus_star_substrings[j].m_substr.c_str());
//  fprintf(stderr, "\n");
  //

  // Assign names to substrings.
  minus_star_substrings[0].m_name = 0;
  for (std::uint64_t j = 1; j < minus_star_substrings.size(); ++j)
    if (minus_star_substrings[j].m_substr == minus_star_substrings[j - 1].m_substr)
      minus_star_substrings[j].m_name = minus_star_substrings[j - 1].m_name;
    else minus_star_substrings[j].m_name = minus_star_substrings[j - 1].m_name + 1;

  //
//  fprintf(stderr, "Names: ");
//  for (std::uint64_t j = 0; j < minus_star_substrings.size(); ++j)
//    fprintf(stderr, "%lu ", minus_star_substrings[j].m_name);
//  fprintf(stderr, "\n");
  //

  // Sort minus-star-substring by beg.
  {
    cmp_by_beg cmp;
    std::sort(minus_star_substrings.begin(), minus_star_substrings.end(), cmp);
  }

  // Create new string from names of minus-star-substrings.
  std::uint64_t text_length2 = minus_star_substrings.size();
  saidx_t *text2 = new saidx_t[text_length2];
  for (std::uint64_t j = 0; j < text_length2; ++j)
    text2[j] = minus_star_substrings[j].m_name;
  saidx_t *SA = new saidx_t[text_length2];
  large_alphabet_sufsort::sort<saidx_t>(text2, SA, text_length2);

  // Compute ISA.
  saidx_t *ISA = new saidx_t[text_length2];
  for (std::uint64_t j = 0; j < text_length2; ++j)
    ISA[(std::uint64_t)SA[j]] = (saidx_t)j;

  // Permute begs of minus-star-suffixes with the help of ISA.
  std::vector<std::pair<saidx_t, saidx_t> > w(text_length2);
  for (std::uint64_t j = 0; j < text_length2; ++j)
    w[j] = std::make_pair((saidx_t)ISA[j], (saidx_t)minus_star_substrings[j].m_beg);

  // Sort substring by ISA, i.e., into lex order.
  std::sort(w.begin(), w.end());

  // Write the output.
  result.resize(text_length2);
  for (std::uint64_t j = 0; j < text_length2; ++j)
    result[j] = w[j].second;

  // Clean up.
  delete[] text2;
  delete[] SA;
  delete[] ISA;
}

void test(std::uint64_t n_testcases, std::uint64_t max_length) {
  fprintf(stderr, "TEST, n_testcases=%lu, max_length=%lu\n", n_testcases, max_length);

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
      //text[j] = 'a' + utils::random_int64(0L, 25L);
//    std::uint64_t text_length = 8;
//    text[0] = 'h'; text[1] = 'm'; text[2] = 'm'; text[3] = 'l';
//    text[4] = 'm'; text[5] = 'm'; text[6] = 'm'; text[7] = 'm';

    divsufsort(text, (std::int32_t *)sa, text_length);

    for (std::uint64_t i = text_length; i > 0; --i) {
      if (i == text_length) suf_type[i - 1] = 0;              // minus
      else {
        if (text[i - 1] > text[i]) suf_type[i - 1] = 0;       // minus
        else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;  // plus
        else suf_type[i - 1] = suf_type[i];
      }
    }

    // Write all lex-sorted star suffixes to file.
    std::vector<saidx_tt> v;
    for (std::uint64_t i = 0; i < text_length; ++i) {
      std::uint64_t s = sa[i];
      if (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1)
        v.push_back((saidx_tt)s);
    }

    std::vector<saidx_tt> v_computed;
    sort_minus_star_substrings<chr_t, saidx_tt>(text, text_length, v_computed);

    // Compare answer.
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

  delete[] text;
  delete[] sa;
  delete[] suf_type;
}

int main() {
  srand(time(0) + getpid());

  for (std::uint64_t max_length = 1; max_length <= (1UL << 15); max_length = std::max(max_length + 1, (max_length * 10 + 8) / 9))
        test(100000, max_length);

  fprintf(stderr, "All tests passed.\n");
}
