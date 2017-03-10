#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "fsais_src/em_compute_sa.hpp"
#include "uint40.hpp"


int main(int argc, char **argv) {
  if (argc != 2) std::exit(EXIT_FAILURE);

  typedef std::uint8_t/*std::uint32_t*/ char_type;
  typedef uint40 text_offset_type;

  std::string text_filename = argv[1];
  std::string output_filename = text_filename + ".sa5";

  std::uint64_t text_alphabet_size = 256;
  std::uint64_t ram_use = (3584UL << 20);
  std::uint64_t io_volume = 0;

  fsais_private::em_compute_sa<
    char_type,
    text_offset_type>(
        ram_use,
        text_alphabet_size,
        text_filename,
        output_filename,
        io_volume);
}
