#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <vector>
#include <string>
#include <algorithm>
#include <ctime>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "im_induce_suffixes.hpp"
#include "io/async_backward_stream_reader.hpp"
#include "io/async_stream_reader.hpp"
#include "io/async_stream_writer.hpp"
#include "io/async_bit_stream_reader.hpp"

#include "utils.hpp"
#include "uint40.hpp"
#include "uint48.hpp"
#include "divsufsort.h"


void test(std::uint64_t n_testcases, std::uint64_t max_length) {
  fprintf(stderr, "TEST, n_testcases=%lu, max_length=%lu\n", n_testcases, max_length);

  typedef std::uint8_t char_type;
  typedef std::uint32_t text_offset_type;

  char_type *text = new char_type[max_length];
  text_offset_type *sa = new text_offset_type[max_length];
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

    std::string text_filename = "tmp." + utils::random_string_hash();
    utils::write_to_file(text, text_length, text_filename);

    typedef std::uint32_t block_offset_type;

    std::uint64_t max_block_size = 0;
    std::uint64_t n_blocks = 0;
    do {
      max_block_size = utils::random_int64((std::int64_t)1, (std::int64_t)text_length);
      n_blocks = (text_length + max_block_size - 1) / max_block_size;
    } while (n_blocks > 256);

    std::uint64_t text_alphabet_size = (std::uint64_t)(*std::max_element(text, text + text_length)) + 1;
    std::uint64_t total_io_volume = 0;















    std::vector<std::string> output_plus_type_filenames(n_blocks);
    std::vector<std::string> output_minus_type_filenames(n_blocks);
    std::vector<std::string> output_plus_symbols_filenames(n_blocks);
    std::vector<std::string> output_minus_symbols_filenames(n_blocks);
    std::vector<std::string> output_plus_pos_filenames(n_blocks);
    std::vector<std::string> output_minus_pos_filenames(n_blocks);

    for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) {
      output_plus_pos_filenames[block_id] = "tmp." + utils::random_string_hash();
      output_plus_symbols_filenames[block_id] = "tmp." + utils::random_string_hash();
      output_plus_type_filenames[block_id] = "tmp." + utils::random_string_hash();
      output_minus_pos_filenames[block_id] = "tmp." + utils::random_string_hash();
      output_minus_type_filenames[block_id] = "tmp." + utils::random_string_hash();
      output_minus_symbols_filenames[block_id] = "tmp." + utils::random_string_hash();
    }
    std::vector<std::uint64_t> minus_block_count_target_computed(n_blocks, std::numeric_limits<std::uint64_t>::max());
    std::vector<std::uint64_t> plus_block_count_target_computed(n_blocks, std::numeric_limits<std::uint64_t>::max());
    {
      // Inpute to in-RAM inducing of all suffixes.
      std::vector<std::string> minus_pos_filenames(n_blocks);
      std::vector<std::uint64_t> next_block_leftmost_minus_star_plus_rank(n_blocks, std::numeric_limits<std::uint64_t>::max());
      {
        for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
          minus_pos_filenames[block_id] = "tmp." + utils::random_string_hash();
        typedef async_stream_writer<block_offset_type> writer_type;
        writer_type **writers = new writer_type*[n_blocks];
        for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id)
          writers[block_id] = new writer_type(minus_pos_filenames[block_id]);
        std::vector<std::uint64_t> leftmost_minus_star_in_a_block(n_blocks, std::numeric_limits<std::uint64_t>::max());
        for (std::uint64_t i = 0; i < text_length; ++i) {
          if (i > 0 && suf_type[i] == 0 && suf_type[i - 1] == 1) {
            std::uint64_t block_id = i / max_block_size;
            std::uint64_t block_beg = block_id * max_block_size;
            leftmost_minus_star_in_a_block[block_id] = std::min(leftmost_minus_star_in_a_block[block_id], i - block_beg);
          }
        }
        std::vector<std::uint64_t> items_written_for_block(n_blocks, 0UL);
        for (std::uint64_t i = 0; i < text_length; ++i) {
          std::uint64_t s = sa[i];
          if (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1) {
            std::uint64_t block_id = s / max_block_size;
            std::uint64_t block_beg = block_id * max_block_size;
            std::uint64_t block_offset = s - block_beg;
            writers[block_id]->write(block_offset);
            ++items_written_for_block[block_id];
            if (block_id > 0 && leftmost_minus_star_in_a_block[block_id] == block_offset)
              next_block_leftmost_minus_star_plus_rank[block_id - 1] = items_written_for_block[block_id - 1];
          }
        }
        for (std::uint64_t block_id = 0; block_id < n_blocks; ++block_id) delete writers[block_id];
        delete[] writers;
      }

      // Close stderr.
      int stderr_backup = 0; std::fflush(stderr); stderr_backup = dup(2);
      int stderr_temp = open("/dev/null", O_WRONLY); dup2(stderr_temp, 2); close(stderr_temp);
      im_induce_suffixes_small_alphabet<char_type,
                    text_offset_type,
                    block_offset_type>(
                        text_alphabet_size,
                        text_length,
                        max_block_size,
                        next_block_leftmost_minus_star_plus_rank,
                        text_filename,
                        minus_pos_filenames,
                        output_plus_pos_filenames,
                        output_plus_symbols_filenames,
                        output_plus_type_filenames,
                        output_minus_pos_filenames,
                        output_minus_type_filenames,
                        output_minus_symbols_filenames,
                        plus_block_count_target_computed,
                        minus_block_count_target_computed,
                        total_io_volume);
      // Restore stderr.
      std::fflush(stderr); dup2(stderr_backup, 2); close(stderr_backup);
    }














    // Compare answers.
    {
      for (std::uint64_t block_id_plus = n_blocks; block_id_plus > 0; --block_id_plus) {
        std::uint64_t block_id = block_id_plus - 1;
        std::uint64_t block_beg = block_id * max_block_size;
        std::uint64_t block_end = std::min(text_length, block_beg + max_block_size);
        std::vector<char_type> plus_symbols_correct;
        {
          for (std::uint64_t iplus = text_length; iplus > 0; --iplus) {
            std::uint64_t i = iplus - 1;
            std::uint64_t s = sa[i];
            if (s > 0 && suf_type[s - 1] == 1 && block_beg <= s && s < block_end)
              plus_symbols_correct.push_back(text[s - 1]);
          }
        }
        std::vector<char_type> minus_symbols_correct;
        {
          for (std::uint64_t j = 0; j < text_length; ++j) {
            std::uint64_t s = sa[j];
            if (s > 0 && suf_type[s - 1] == 0 && block_beg <= s && s < block_end)
              minus_symbols_correct.push_back(text[s - 1]);
          }
        }
        std::vector<bool> plus_type_correct;
        {
          for (std::uint64_t iplus = text_length; iplus > 0; --iplus) {
            std::uint64_t i = iplus - 1;
            std::uint64_t s = sa[i];
            if (suf_type[s] == 1 && block_beg <= s && s < block_end) {
              bool is_star = (s > 0 && suf_type[s - 1] == 0);
              plus_type_correct.push_back(is_star);
            }
          }
        }
        std::vector<bool> minus_type_correct;
        {
          for (std::uint64_t j = 0; j < text_length; ++j) {
            std::uint64_t s = sa[j];
            if (suf_type[s] == 0 && block_beg <= s && s < block_end) {
              std::uint8_t is_star = (s > 0 && suf_type[s - 1] == 1);
              minus_type_correct.push_back(is_star);
            }
          }
        }
        std::uint64_t plus_block_count_target_correct = std::numeric_limits<std::uint64_t>::max();
        {
          std::uint64_t cur_block_count = 0;
          for (std::uint64_t iplus = text_length; iplus > 0; --iplus) {
            std::uint64_t i = iplus - 1;
            std::uint64_t s = sa[i];

            if (block_beg <= s && s < block_end &&
              ((suf_type[s] == 1 && (s == 0 || suf_type[s - 1] == 1)) || (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1))) {
              ++cur_block_count;
              if (s == block_beg) {
                plus_block_count_target_correct = cur_block_count;
                break;
              }
            }
          }
        }
        std::uint64_t minus_block_count_target_correct = std::numeric_limits<std::uint64_t>::max();
        {
          std::uint64_t cur_block_count = 0;
          for (std::uint64_t i = 0; i < text_length; ++i) {
            std::uint64_t s = sa[i];
//                (suf_type[s] == 0 || (s > 0 && suf_type[s - 1] == 0))) {
            if (block_beg <= s && s < block_end && (s > 0 && suf_type[s] == 0 && suf_type[s - 1] == 1)) {
              ++cur_block_count;
              if (s == block_beg) {
                minus_block_count_target_correct = cur_block_count;
                break;
              }
            }
          }
        }
        std::vector<block_offset_type> minus_pos_correct;
        {
          for (std::uint64_t i = 0; i < text_length; ++i) {
            std::uint64_t s = sa[i];
            if (suf_type[s] == 0 && block_beg <= s && s < block_end) {
              std::uint64_t block_offset = s - block_beg;
              minus_pos_correct.push_back(block_offset);
            }
          }
        }
        std::vector<block_offset_type> plus_pos_correct;
        {
          for (std::uint64_t iplus = text_length; iplus > 0; --iplus) {
            std::uint64_t i = iplus - 1;
            std::uint64_t s = sa[i];
            if (suf_type[s] == 1 && block_beg <= s && s < block_end) {
              std::uint64_t block_offset = s - block_beg;
              plus_pos_correct.push_back(block_offset);
            }
          }
        }
        if (plus_block_count_target_correct != plus_block_count_target_computed[block_id]) {
          fprintf(stderr, "Error: plus target value was computed incorrectly!\n");
          fprintf(stderr, "  text: ");
          for (std::uint64_t i = 0; i < text_length; ++i)
            fprintf(stderr, "%lu ", (std::uint64_t)text[i]);
          fprintf(stderr, "\n");
          fprintf(stderr, "  max_block_size = %lu, block_beg = %lu, block_end = %lu\n", max_block_size, block_beg, block_end);
          fprintf(stderr, "  plus_block_count_target_computed = %lu\n", plus_block_count_target_computed[block_id]);
          fprintf(stderr, "  plus_block_count_target_correct = %lu\n", plus_block_count_target_correct);
          std::exit(EXIT_FAILURE);
        }
        if (minus_block_count_target_correct != minus_block_count_target_computed[block_id]) {
          fprintf(stderr, "Error: minus target value was computed incorrectly!\n");
          std::exit(EXIT_FAILURE);
        }
        std::vector<char_type> plus_symbols_computed;
        {
          typedef async_stream_reader<char_type> reader_type;
          reader_type *reader = new reader_type(output_plus_symbols_filenames[block_id]);
          while (!reader->empty())
            plus_symbols_computed.push_back(reader->read());
          delete reader;
        }
        utils::file_delete(output_plus_symbols_filenames[block_id]);
        if (plus_symbols_correct.size() != plus_symbols_computed.size() ||
            std::equal(plus_symbols_correct.begin(), plus_symbols_correct.end(), plus_symbols_computed.begin()) == false) {
          fprintf(stderr, "Error: plus symbols not equal!\n");
          std::exit(EXIT_FAILURE);
        }
        std::vector<char_type> minus_symbols_computed;
        {
          typedef async_stream_reader<char_type> reader_type;
          reader_type *reader = new reader_type(output_minus_symbols_filenames[block_id]);
          while (!reader->empty())
            minus_symbols_computed.push_back(reader->read());
          delete reader;
        }
        utils::file_delete(output_minus_symbols_filenames[block_id]);
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
          reader_type *reader = new reader_type(output_plus_type_filenames[block_id]);
          for (std::uint64_t j = 0; j < plus_type_correct.size(); ++j)
            plus_type_computed.push_back(reader->read());
          delete reader;
        }
        utils::file_delete(output_plus_type_filenames[block_id]);
        if (plus_type_correct.size() != plus_type_computed.size() ||
            std::equal(plus_type_correct.begin(), plus_type_correct.end(), plus_type_computed.begin()) == false) {
          fprintf(stderr, "Error: plus bits are not correct!\n");
          std::exit(EXIT_FAILURE);
        }
        std::vector<bool> minus_type_computed;
        {
          typedef async_bit_stream_reader reader_type;
          reader_type *reader = new reader_type(output_minus_type_filenames[block_id]);
          for (std::uint64_t j = 0; j < minus_type_correct.size(); ++j)
            minus_type_computed.push_back(reader->read());
          delete reader;
        }
        utils::file_delete(output_minus_type_filenames[block_id]);
        if (minus_type_correct.size() != minus_type_computed.size() ||
            std::equal(minus_type_correct.begin(), minus_type_correct.end(), minus_type_computed.begin()) == false) {
          fprintf(stderr, "Error: minus bits are not correct!\n");
          std::exit(EXIT_FAILURE);
        }
        std::vector<block_offset_type> minus_pos_computed;
        {
          typedef async_stream_reader<block_offset_type> reader_type;
          reader_type *reader = new reader_type(output_minus_pos_filenames[block_id]);
          while (!reader->empty())
            minus_pos_computed.push_back(reader->read());
          delete reader;
        }
        utils::file_delete(output_minus_pos_filenames[block_id]);
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
        std::vector<block_offset_type> plus_pos_computed;
        {
          typedef async_stream_reader<block_offset_type> reader_type;
          reader_type *reader = new reader_type(output_plus_pos_filenames[block_id]);
          while (!reader->empty())
            plus_pos_computed.push_back(reader->read());
          delete reader;
        }
        utils::file_delete(output_plus_pos_filenames[block_id]);
        if (plus_pos_correct.size() != plus_pos_computed.size() ||
           !std::equal(plus_pos_correct.begin(), plus_pos_correct.end(), plus_pos_computed.begin())) {
           fprintf(stderr, "Error: plus pos not correct\n");
        }
      }
    }











    utils::file_delete(text_filename);
  }

  delete[] text;
  delete[] sa;
  delete[] suf_type;
}

int main() {
  srand(time(0) + getpid());
  for (std::uint64_t max_length = 1; max_length <= (1L << 14); max_length *= 2)
    test(10000, max_length);
  fprintf(stderr, "All tests passed.\n");
}
