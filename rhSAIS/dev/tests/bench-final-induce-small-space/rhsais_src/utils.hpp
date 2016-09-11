/**
 * @file    rhsais_src/utils.hpp
 * @section LICENCE
 *
 * This file is part of rhSAIS v0.1.0
 * See: http://www.cs.helsinki.fi/group/pads/
 *
 * Copyright (C) 2017
 *   Juha Karkkainen <juha.karkkainen (at) cs.helsinki.fi>
 *   Dominik Kempa <dominik.kempa (at) gmail.com>
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

#ifndef __RHSAIS_SRC_UTILS_HPP_INCLUDED
#define __RHSAIS_SRC_UTILS_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <sstream>


namespace rhsais_private {
namespace utils {

long double wclock();

extern std::uint64_t current_ram_allocation;
extern std::uint64_t peak_ram_allocation;

void *allocate(std::uint64_t);
void deallocate(void *);
std::uint64_t get_current_ram_allocation();
std::uint64_t get_peak_ram_allocation();

std::FILE *file_open(std::string fname, std::string mode);
std::FILE *file_open_nobuf(std::string fname, std::string mode);
std::uint64_t file_size(std::string fname);
bool file_exists(std::string fname);
void file_delete(std::string fname);
std::string absolute_path(std::string fname);
void drop_disk_pages(std::string filename);

template<typename value_type>
void write_to_file(const value_type *src, std::uint64_t length, std::FILE *f) {
  std::uint64_t fwrite_ret = std::fwrite(src, sizeof(value_type), length, f);
  if (fwrite_ret != length) {
    fprintf(stderr, "\nError: fwrite failed.\n");
    std::exit(EXIT_FAILURE);
  }
}

template<typename value_type>
void write_to_file(const value_type *src, std::uint64_t length, std::string fname) {
  std::FILE *f = file_open_nobuf(fname, "w");
  write_to_file(src, length, f);
  std::fclose(f);
}

template<typename value_type>
void read_from_file(value_type* dest, std::uint64_t length, std::FILE *f) {
  std::uint64_t fread_ret = std::fread(dest, sizeof(value_type), length, f);
  if (fread_ret != length) {
    fprintf(stderr, "\nError: fread failed.\n");
    std::exit(EXIT_FAILURE);
  }
}

template<typename value_type>
void read_from_file(value_type* dest, std::uint64_t length, std::string fname) {
  std::FILE *f = file_open_nobuf(fname, "r");
  read_from_file<value_type>(dest, length, f);
  std::fclose(f);
}

template<typename value_type>
void read_at_offset(value_type *dest, std::uint64_t offset,
    std::uint64_t length, std::FILE *f) {
  std::fseek(f, sizeof(value_type) * offset, SEEK_SET);
  read_from_file(dest, length, f);
}

template<typename value_type>
void read_at_offset(value_type *dest, std::uint64_t offset,
    std::uint64_t length, std::string filename) {
  std::FILE *f = file_open_nobuf(filename, "r");
  read_at_offset(dest, offset, length, f);
  std::fclose(f);
}

std::int32_t random_int32(std::int32_t p, std::int32_t r);
std::int64_t random_int64(std::int64_t p, std::int64_t r);
void fill_random_string(std::uint8_t* &s, std::uint64_t length, std::uint64_t sigma);
void fill_random_letters(std::uint8_t* &s, std::uint64_t length, std::uint64_t sigma);
std::string random_string_hash();

std::uint64_t log2ceil(std::uint64_t x);
std::uint64_t log2floor(std::uint64_t x);

template<typename int_type>
std::string intToStr(int_type x) {
  std::stringstream ss;
  ss << x;
  return ss.str();
}

}  // namespace utils
}  // namespace rhsais_private

#endif  // __RHSAIS_SRC_UTILS_HPP_INCLUDED
