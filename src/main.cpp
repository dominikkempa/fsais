/**
 * @file    src/main.cpp
 * @section LICENCE
 *
 * This file is part of fSAIS v0.1.0
 * See: https://github.com/dkempa/fsais
 *
 * Copyright (C) 2016-2020
 *   Dominik Kempa <dominik.kempa (at) gmail.com>
 *   Juha Karkkainen <juha.karkkainen (at) cs.helsinki.fi>
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following
 * conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 **/

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <ctime>
#include <cstring>
#include <string>
#include <sstream>
#include <getopt.h>
#include <unistd.h>

#include "fsais_src/em_compute_sa.hpp"
#include "uint40.hpp"


char *program_name;

void usage(int status) {
  printf(

"Usage: %s [OPTION]... FILE\n"
"Construct the suffix array of text stored in FILE.\n"
"\n"
"Mandatory arguments to long options are mandatory for short options too.\n"
"  -h, --help              display this help and exit\n"
"  -m, --mem=MEM           use MEM bytes of RAM for computation. Metric and IEC\n"
"                          suffixes are recognized, e.g., -l 10k, -l 1Mi, -l 3G\n"
"                          gives MEM = 10^4, 2^20, 3*10^6. Default: 3584Mi\n"
"  -o, --output=OUTFILE    specify output filename. Default: FILE.saX, where\n"
"                          X = integer size used to encode the suffix array\n"
"                          (5 bytes by default)\n",

    program_name);

  std::exit(status);
}

bool file_exists(std::string filename) {
  std::FILE *f = std::fopen(filename.c_str(), "r");
  bool ret = (f != NULL);
  if (f != NULL) std::fclose(f);

  return ret;
}

template<typename int_type>
std::string intToStr(int_type x) {
  std::stringstream ss;
  ss << x;
  return ss.str();
}

template<typename int_type>
bool parse_number(char *str, int_type *ret) {
  *ret = 0;
  std::uint64_t n_digits = 0;
  std::uint64_t str_len = std::strlen(str);
  while (n_digits < str_len && std::isdigit(str[n_digits])) {
    std::uint64_t digit = str[n_digits] - '0';
    *ret = (*ret) * 10 + digit;
    ++n_digits;
  }

  if (n_digits == 0)
    return false;

  std::uint64_t suffix_length = str_len - n_digits;
  if (suffix_length > 0) {
    if (suffix_length > 2)
      return false;

    for (std::uint64_t j = 0; j < suffix_length; ++j)
      str[n_digits + j] = std::tolower(str[n_digits + j]);
    if (suffix_length == 2 && str[n_digits + 1] != 'i')
      return false;

    switch(str[n_digits]) {
      case 'k':
        if (suffix_length == 1)
          *ret *= 1000;
        else
          *ret <<= 10;
        break;
      case 'm':
        if (suffix_length == 1)
          *ret *= 1000000;
        else
          *ret <<= 20;
        break;
      case 'g':
        if (suffix_length == 1)
          *ret *= 1000000000;
        else
          *ret <<= 30;
        break;
      case 't':
        if (suffix_length == 1)
          *ret *= 1000000000000;
        else
          *ret <<= 40;
        break;
      default:
        return false;
    }
  }

  return true;
}

int main(int argc, char **argv) {
  srand(time(0) + getpid());
  program_name = argv[0];

  static struct option long_options[] = {
    {"help",     no_argument,       NULL, 'h'},
    {"mem",      required_argument, NULL, 'm'},
    {"output",   required_argument, NULL, 'o'},
    {NULL,       0,                 NULL, 0}
  };

  std::uint64_t ram_use = ((std::uint64_t)3584 << 20);
  std::string output_filename("");

  // Parse command-line options.
  int c;
  while ((c = getopt_long(argc, argv, "hm:o:",
          long_options, NULL)) != -1) {
    switch(c) {
      case 'h':
        usage(EXIT_FAILURE);
        break;
      case 'm':
        {
          bool ok = parse_number(optarg, &ram_use);
          if (!ok) {
            fprintf(stderr, "Error: parsing RAM limit"
                "limit (%s) failed\n\n", optarg);
            usage(EXIT_FAILURE);
          }
          if (ram_use == 0) {
            fprintf(stderr, "Error: invalid RAM limit (%lu)\n\n", ram_use);
            usage(EXIT_FAILURE);
          }
          break;
        }
      case 'o':
        output_filename = std::string(optarg);
        break;
      default:
        usage(EXIT_FAILURE);
        break;
    }
  }

  if (optind >= argc) {
    fprintf(stderr, "Error: FILE not provided\n\n");
    usage(EXIT_FAILURE);
  }

  // Parse the text filename.
  std::string text_filename = std::string(argv[optind++]);
  if (optind < argc) {
    fprintf(stderr, "Warning: multiple input files provided. "
    "Only the first will be processed.\n");
  }

  // TODO: eliminate hardcoded values/types.
  typedef std::uint8_t/*std::uint32_t*/ char_type;
  typedef uint40 text_offset_type;
  std::uint64_t text_alphabet_size = 256/*100000000*/;

  // Set default output filename (if not provided).
  if (output_filename.empty())
    output_filename = text_filename + ".sa" +
        intToStr(sizeof(text_offset_type));

  // Check for the existence of text.
  if (!file_exists(text_filename)) {
    fprintf(stderr, "Error: input file (%s) does not exist\n\n",
        text_filename.c_str());
    usage(EXIT_FAILURE);
  }

  if (file_exists(output_filename)) {

    // Output file exists, should we proceed?
    char *line = NULL;
    std::uint64_t buflen = 0;
    std::int64_t len = 0L;

    do {
      printf("Output file (%s) exists. Overwrite? [y/n]: ",
          output_filename.c_str());
      if ((len = getline(&line, &buflen, stdin)) == -1) {
        printf("\nError: failed to read answer\n\n");
        std::fflush(stdout);
        usage(EXIT_FAILURE);
      }
    } while (len != 2 || (line[0] != 'y' && line[0] != 'n'));

    if (line[0] == 'n') {
      free(line);
      std::exit(EXIT_FAILURE);
    }
    free(line);
  }

  fsais_private::em_compute_sa<
    char_type,
    text_offset_type>(
        ram_use,
        text_alphabet_size,
        text_filename,
        output_filename);
}
