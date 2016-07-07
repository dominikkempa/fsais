#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>

#include "induce_minus_suffixes.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_writer.hpp"
#include "utils.hpp"
#include "uint40.hpp"
#include "uint48.hpp"
#include "divsufsort.h"


template<typename T, typename stream_type>
void encode_vbyte_to_stream(T x, stream_type *stream) {
  std::uint64_t y = x;
  while (y > 127) {
    stream->write((std::uint8_t)((y & 0x7f) | 0x80));
    y >>= 7;
  }
  stream->write((std::uint8_t)y);
}

void test(std::uint64_t n_testcases, std::uint64_t max_length, std::uint64_t radix_heap_bufsize, std::uint64_t radix_log) {
  fprintf(stderr, "TEST, n_testcases=%lu, max_length=%lu, buffer_size=%lu, radix_log=%lu\n", n_testcases, max_length, radix_heap_bufsize, radix_log);

  typedef std::uint8_t chr_t;
  typedef std::uint32_t saidx_tt;

  chr_t *text = new chr_t[max_length];
  saidx_tt *sa = new saidx_tt[max_length];
  bool *suf_type = new bool[max_length];

  for (std::uint64_t testid = 0; testid < n_testcases; ++testid) {
    std::uint64_t text_length = utils::random_int64(1L, (std::int64_t)max_length);
    for (std::uint64_t j = 0; j < text_length; ++j)
      // text[j] = utils::random_int64(0L, 255L);
      text[j] = 'a' + utils::random_int64(0L, 25L);
    divsufsort(text, (std::int32_t *)sa, text_length);
    std::uint64_t max_block_size = utils::random_int64(1L, (std::int64_t)text_length);
    std::uint64_t n_blocks = (text_length + max_block_size - 1) / max_block_size;

/*    fprintf(stderr, "text = ");
    for (std::uint64_t j = 0; j < text_length; ++j)
      fprintf(stderr, "%c", text[j]);
    fprintf(stderr, "\n");
    fprintf(stderr, "max_block_size = %lu, n_blocks = %lu\n", max_block_size, n_blocks);*/

    for (std::uint64_t i = text_length; i > 0; --i) {
      if (i == text_length) suf_type[i - 1] = 0;              // minus
      else {
        if (text[i - 1] > text[i]) suf_type[i - 1] = 0;       // minus
        else if (text[i - 1] < text[i]) suf_type[i - 1] = 1;  // plus
        else suf_type[i - 1] = suf_type[i];
      }
    }

/*    fprintf(stderr, "T B SA:\n");
    for (std::uint64_t j = 0; j < text_length; ++j) {
      std::uint64_t s = sa[j];
      fprintf(stderr, "%c %c %lu ", (suf_type[s] == 0 ? '-' : '+'), (s == 0 ? '.' : text[s - 1]), (std::uint64_t)sa[j]);
      for (std::uint64_t t = s; t < text_length; ++t)
        fprintf(stderr, "%c", text[t]);
      fprintf(stderr, "\n");
    }*/

//    fprintf(stderr, "plus_type_filename: ");
    std::string plus_type_filename = "tmp." + utils::random_string_hash();
    {
      typedef async_bit_stream_writer bit_writer_type;
      bit_writer_type *bit_writer = new bit_writer_type(plus_type_filename);
      for (std::uint64_t j = 0; j < text_length; ++j) {
        std::uint64_t s = sa[j];
        if (suf_type[s] == 1) {
          bit_writer->write((s > 0) && (suf_type[s - 1] == 0));
//          fprintf(stderr, "%lu ", (std::uint64_t)((s > 0) && (suf_type[s - 1] == 0)));
        }
      }
      delete bit_writer;
    }
//    fprintf(stderr, "\n");

//    fprintf(stderr, "plus_count_filename: ");
    std::string plus_count_filename = "tmp." + utils::random_string_hash();
    {
      typedef async_stream_writer<std::uint8_t> writer_type;
      writer_type *writer = new writer_type(plus_count_filename);
      std::uint64_t beg = 0;
      for (std::uint64_t ch = 0; ch < 256; ++ch) {
        std::uint64_t plus_count = 0;
        std::uint64_t end = beg;
        while (end < text_length && text[sa[end]] == ch) {
          if (suf_type[sa[end]] == 1) ++plus_count;
          ++end;
        }
//        fprintf(stderr, "%lu ", plus_count);
        encode_vbyte_to_stream<std::uint64_t, writer_type>(plus_count, writer);
        beg = end;
      }
      delete writer;
    }
//    fprintf(stderr, "\n");

//    fprintf(stderr, "plus_symbols_filename: ");
    std::string plus_symbols_filename = "tmp." + utils::random_string_hash();
    {
      typedef async_stream_writer<chr_t> writer_type;
      writer_type *writer = new writer_type(plus_symbols_filename);
      for (std::uint64_t j = 0; j < text_length; ++j) {
        std::uint64_t s = sa[j];
        if (suf_type[s] == 1 && s > 0 && suf_type[s - 1] == 0) {
          writer->write(text[s - 1]);
//          fprintf(stderr, "%c", text[s - 1]);
        }
      }
      delete writer;
    }
//    fprintf(stderr, "\n");

//    fprintf(stderr, "minus_symbols_filenames:\n");
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
      for (std::uint64_t j = 0; j < text_length; ++j) {
        std::uint64_t s = sa[j];
        if (suf_type[s] == 0 && s > 0 && suf_type[s - 1] == 0) {
          std::uint64_t block_id = s / max_block_size;
          writers[block_id]->write(text[s - 1]);
//          fprintf(stderr, "  %lu -> %c\n", block_id, text[s - 1]);
        }
      }
      for (std::uint64_t j = 0; j < n_blocks; ++j)
        delete writers[j];
      delete[] writers;
    }

//    fprintf(stderr, "minus_type_filenames:\n");
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
      for (std::uint64_t j = 0; j < text_length; ++j) {
        std::uint64_t s = sa[j];
        if (suf_type[s] == 0) {
          std::uint64_t block_id = s / max_block_size;
          std::uint8_t is_star = (s > 0 && suf_type[s - 1] == 1);
          writers[block_id]->write(is_star);
//          fprintf(stderr, "  %lu -> %lu\n", block_id, (std::uint64_t)is_star);
        }
      }
      for (std::uint64_t j = 0; j < n_blocks; ++j)
        delete writers[j];
      delete[] writers;
    }

//    fprintf(stderr, "minus_pos_filenames:\n");
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
      for (std::uint64_t j = 0; j < text_length; ++j) {
        std::uint64_t s = sa[j];
        if (suf_type[s] == 0) {
          std::uint64_t block_id = s / max_block_size;
          writers[block_id]->write((saidx_tt)s);
//          fprintf(stderr, "  %lu -> %lu\n", block_id, (std::uint64_t)s);
        }
      }
      for (std::uint64_t j = 0; j < n_blocks; ++j)
        delete writers[j];
      delete[] writers;
    }

    // Write all lex-sorted star suffixes to file.
//    fprintf(stderr, "plus_pos_filename: ");
    std::string plus_pos_filename = "tmp." + utils::random_string_hash();
    {
      typedef async_stream_writer<saidx_tt> writer_type;
      writer_type *writer = new writer_type(plus_pos_filename);
      for (std::uint64_t i = 0; i < text_length; ++i) {
        std::uint64_t s = sa[i];
        // BOTH WORK:
        // if (s > 0 && suf_type[s] == 1 && suf_type[s - 1] == 0)
        if (suf_type[s] == 1) {
          writer->write((saidx_tt)s);
//          fprintf(stderr, "%lu ", (std::uint64_t)s);
        }
      }
      delete writer;
    }
//    fprintf(stderr, "\n");

    // Create a vector with all minus-suffixes (i.e., correct answer).
    std::vector<saidx_tt> v;
    for (std::uint64_t i = 0; i < text_length; ++i) {
      std::uint64_t s = sa[i];
      if (suf_type[s] == 0)
        v.push_back((saidx_tt)s);
    }

    // Run the tested algorithm.
    std::string output_filename = "tmp." + utils::random_string_hash();
    std::uint64_t total_io_volume = 0;
    chr_t last_text_symbol = text[text_length - 1];
    induce_minus_suffixes<chr_t, saidx_tt>(text_length,
        plus_pos_filename, output_filename, total_io_volume,
        radix_heap_bufsize, radix_log, last_text_symbol, max_block_size,
        plus_type_filename, plus_count_filename, plus_symbols_filename,
        minus_symbols_filenames, minus_type_filenames, minus_pos_filenames);

    // Delete input files.
    utils::file_delete(plus_pos_filename);
    utils::file_delete(plus_type_filename);
    utils::file_delete(plus_count_filename);
    utils::file_delete(plus_symbols_filename);
    for (std::uint64_t j = 0; j < n_blocks; ++j) {
      if (utils::file_exists(minus_symbols_filenames[j])) utils::file_delete(minus_symbols_filenames[j]);
      if (utils::file_exists(minus_type_filenames[j])) utils::file_delete(minus_type_filenames[j]);
      if (utils::file_exists(minus_pos_filenames[j])) utils::file_delete(minus_pos_filenames[j]);
    }
    
    // Read the computed output into vector.
    std::vector<saidx_tt> v_computed;
    {
      typedef async_stream_reader<saidx_tt> reader_type;
      reader_type *reader = new reader_type(output_filename);
      while (!reader->empty())
        v_computed.push_back(reader->read());
      delete reader;
    }

    // Delete output file.
    utils::file_delete(output_filename);

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

  for (std::uint64_t max_length = 1; max_length <= (1L << 14); max_length *= 2)
    for (std::uint64_t buffer_size = 1; buffer_size <= (1L << 10); buffer_size *= 2)
      for (std::uint64_t radix_log = 1; radix_log <= 5; ++radix_log)
        test(200, max_length, buffer_size, radix_log);

  fprintf(stderr, "All tests passed.\n");
}
