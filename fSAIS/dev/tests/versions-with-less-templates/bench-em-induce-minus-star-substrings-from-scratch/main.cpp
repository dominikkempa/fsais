#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>

#include "em_induce_minus_star_substrings.hpp"
#include "utils.hpp"


int main(int argc, char **argv) {
  if (argc != 2) std::exit(EXIT_FAILURE);

  std::string text_filename = argv[1];
  std::string output_filename = text_filename + ".output";
  std::uint64_t text_length = utils::file_size(text_filename);

#if 1
  typedef std::uint8_t char_type;
  typedef uint40 text_offset_type;
//  typedef std::uint32_t block_offset_type;
//  typedef std::uint8_t block_id_type;

  std::uint64_t text_alphabet_size = 256;
//  std::uint64_t max_block_size = (680UL << 20);
  std::uint64_t ram_use = (3584UL << 20);
  std::uint64_t io_volume = 0;

  em_induce_minus_star_substrings<
    char_type,
    text_offset_type
//    block_offset_type,
//    block_id_type
    >(
        text_length,
        text_alphabet_size,
//        max_block_size,
        ram_use,
        text_filename,
        output_filename,
        io_volume);
#else
  typedef std::uint8_t char_type;
  typedef uint40 text_offset_type;
  typedef std::uint32_t block_offset_type;
  typedef std::uint8_t block_id_type;

  std::uint64_t text_alphabet_size = 256;
  std::uint64_t max_block_size = (682UL << 20);
  std::uint64_t ram_use = (3584UL << 20);
  std::uint64_t io_volume = 0;

  em_induce_minus_star_substrings<
    char_type,
    text_offset_type,
    block_offset_type,
    block_id_type
    >(
        text_length,
        text_alphabet_size,
        max_block_size,
        ram_use,
        text_filename,
        output_filename,
        io_volume);

#endif
}

