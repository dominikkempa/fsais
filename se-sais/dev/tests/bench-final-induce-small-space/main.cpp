#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "rhsais_src/em_compute_sa.hpp"
#include "rhsais_src/utils.hpp"


int main(int argc, char **argv) {
  if (argc != 2) std::exit(EXIT_FAILURE);

  typedef std::uint8_t/*std::uint32_t*/ char_type;
  typedef uint40 text_offset_type;

  std::string text_filename = argv[1];
  std::string output_filename = text_filename + ".sa5";
  std::uint64_t text_length = rhsais_private::utils::file_size(text_filename) / sizeof(char_type);

  std::uint64_t text_alphabet_size = 256/*53915629*/;
  std::uint64_t ram_use = (1792UL << 20);
  std::uint64_t io_volume = 0;

  rhsais_private::em_compute_sa<
    char_type,
    text_offset_type>(
        text_length,
        ram_use,
        text_alphabet_size,
        text_filename,
        output_filename,
        io_volume);
}
