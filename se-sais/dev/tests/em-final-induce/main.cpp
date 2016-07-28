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

#include "em_compute_sa.hpp"
#include "naive_compute_sa.hpp"
#include "utils.hpp"
#include "uint24.hpp"
#include "uint40.hpp"
#include "uint48.hpp"


void test(std::uint64_t n_testcases, std::uint64_t max_length) {
  fprintf(stderr, "TEST, n_testcases=%lu, max_length=%lu\n", n_testcases, max_length);

  typedef std::uint8_t char_type;
  typedef uint40 text_offset_type;

  char_type *text = new char_type[max_length];
  text_offset_type *sa = new text_offset_type[max_length];

  for (std::uint64_t testid = 0; testid < n_testcases; ++testid) {
    if (testid % 10 == 0)
      fprintf(stderr, "%.2Lf%%\r", (100.L * testid) / n_testcases);
    std::uint64_t text_length = utils::random_int64(1L, (std::int64_t)max_length);

    // Generate text.
#if 1
    if (utils::random_int64(0L, 1L)) {
      for (std::uint64_t j = 0; j < text_length; ++j)
        text[j] = utils::random_int64(0L, 255L);
    } else {
      for (std::uint64_t j = 0; j < text_length; ++j)
        text[j] = 'a' + utils::random_int64(0L, 5L);
    }
    std::uint64_t text_alphabet_size = (std::uint64_t)(*std::max_element(text, text + text_length)) + 1;
#else
    std::uint64_t text_alphabet_size = utils::random_int64(1L, (1UL << 8));
    for (std::uint64_t j = 0; j < text_length; ++j)
      text[j] = utils::random_int64(0L, (std::int64_t)text_alphabet_size - 1);
#endif

    std::uint64_t total_io_volume = 0;
    std::uint64_t ram_use = utils::random_int64(1L, 1024L);

    naive_compute_sa::naive_compute_sa<char_type, text_offset_type>(text, text_length, sa);

    std::string text_filename = "tmp." + utils::random_string_hash();
    utils::write_to_file(text, text_length, text_filename);
    std::string output_filename = "tmp." + utils::random_string_hash();

    // Close stderr.
    int stderr_backup = 0;
    std::fflush(stderr);
    stderr_backup = dup(2);
    int stderr_temp = open("/dev/null", O_WRONLY);
    dup2(stderr_temp, 2);
    close(stderr_temp);

    em_compute_sa<
      char_type,
      text_offset_type>(
          text_length,
          ram_use,
          text_alphabet_size,
          text_filename,
          output_filename,
          total_io_volume);

    // Restore stderr.
    std::fflush(stderr);
    dup2(stderr_backup, 2);
    close(stderr_backup);

    text_offset_type *computed_sa = new text_offset_type[text_length];
    utils::read_from_file(computed_sa, text_length, output_filename);

    if (!std::equal(sa, sa + text_length, computed_sa)) {
      fprintf(stderr, "\nError:\n");
      fprintf(stderr, "  text = ");
      for (std::uint64_t i = 0; i < text_length; ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)text[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  corect sa: ");
      for (std::uint64_t i = 0; i < text_length; ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)sa[i]);
      fprintf(stderr, "\n");
      fprintf(stderr, "  computed sa: ");
      for (std::uint64_t i = 0; i < text_length; ++i)
        fprintf(stderr, "%lu ", (std::uint64_t)computed_sa[i]);
      fprintf(stderr, "\n");
      std::exit(EXIT_FAILURE);
    }

    delete[] computed_sa;

    utils::file_delete(text_filename);
    utils::file_delete(output_filename);
  }

  delete[] text;
  delete[] sa;
}

int main() {
  srand(time(0) + getpid());
  for (std::uint64_t max_length = 1; max_length <= (1L << 13); max_length *= 2)
    test(30, max_length);
  fprintf(stderr, "All tests passed.\n");
}
