/**
 * @file    fsais_src/utils.hpp
 * @section LICENCE
 *
 * This file is part of fSAIS v0.1.0
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

#ifndef __FSAIS_SRC_UTILS_HPP_INCLUDED
#define __FSAIS_SRC_UTILS_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <mutex>
#include <sstream>


namespace fsais_private {
namespace utils {

extern std::uint64_t current_ram_allocation;
extern std::uint64_t current_io_volume;
extern std::uint64_t current_disk_allocation;
extern std::uint64_t peak_ram_allocation;
extern std::uint64_t peak_disk_allocation;
extern std::mutex io_mutex;

long double wclock();
void sleep(long double);

void *allocate(std::uint64_t);
void *aligned_allocate(std::uint64_t, std::uint64_t);
void deallocate(void *);
void aligned_deallocate(void *);

void initialize_stats();
std::uint64_t get_current_ram_allocation();
std::uint64_t get_peak_ram_allocation();
std::uint64_t get_current_io_volume();
std::uint64_t get_current_disk_allocation();
std::uint64_t get_peak_disk_allocation();

template<typename value_type>
value_type *allocate_array(std::uint64_t size) {
  return (value_type *)allocate(size * sizeof(value_type));
}

template<typename value_type>
value_type *aligned_allocate_array(std::uint64_t size, std::uint64_t align) {
  return (value_type *)aligned_allocate(size * sizeof(value_type), align);
}

std::FILE *file_open(std::string filename, std::string mode);
std::FILE *file_open_nobuf(std::string filename, std::string mode);
std::uint64_t file_size(std::string filename);
bool file_exists(std::string filename);
void file_delete(std::string filename);
std::string absolute_path(std::string filename);
void empty_page_cache(std::string filename);
std::string get_timestamp();

template<typename value_type>
void write_to_file(
    const value_type *src,
    std::uint64_t length,
    std::FILE *f) {

#ifdef MONITOR_DISK_USAGE
  std::lock_guard<std::mutex> lk(io_mutex);
#endif

  std::uint64_t fwrite_ret =
    std::fwrite(src, sizeof(value_type), length, f);

#ifdef MONITOR_DISK_USAGE
  current_io_volume += sizeof(value_type) * length;
  current_disk_allocation += sizeof(value_type) * length;
  peak_disk_allocation =
    std::max(peak_disk_allocation, current_disk_allocation);
#endif

  if (fwrite_ret != length) {
    fprintf(stderr, "\nError: fwrite failed.\n");
    std::exit(EXIT_FAILURE);
  }
}

template<typename value_type>
void write_to_file_inplace(
    const value_type *src,
    std::uint64_t length,
    std::FILE *f) {

#ifdef MONITOR_DISK_USAGE
  std::lock_guard<std::mutex> lk(io_mutex);
#endif

  std::uint64_t fwrite_ret =
    std::fwrite(src, sizeof(value_type), length, f);

#ifdef MONITOR_DISK_USAGE
  current_io_volume += sizeof(value_type) * length;
#endif

  if (fwrite_ret != length) {
    fprintf(stderr, "\nError: fwrite failed.\n");
    std::exit(EXIT_FAILURE);
  }
}

template<typename value_type>
void write_to_file(
    const value_type *src,
    std::uint64_t length,
    std::string filename) {
  std::FILE *f = file_open_nobuf(filename, "w");
  write_to_file(src, length, f);
  std::fclose(f);
}

template<typename value_type>
void overwrite_at_offset(value_type *src, std::uint64_t offset,
    std::uint64_t length, std::FILE *f) {
  std::fseek(f, sizeof(value_type) * offset, SEEK_SET);
  write_to_file_inplace(src, length, f);
}

template<typename value_type>
void read_from_file(
    value_type* dest,
    std::uint64_t length,
    std::FILE *f) {

#ifdef MONITOR_DISK_USAGE
  std::lock_guard<std::mutex> lk(io_mutex);
#endif

  std::uint64_t fread_ret =
    std::fread(dest, sizeof(value_type), length, f);

#ifdef MONITOR_DISK_USAGE
  current_io_volume += sizeof(value_type) * length;
#endif

  if (fread_ret != length) {
    fprintf(stderr, "\nError: fread failed.\n");
    std::exit(EXIT_FAILURE);
  }
}

template<typename value_type>
void read_from_file(
    value_type* dest,
    std::uint64_t length,
    std::string filename) {
  std::FILE *f = file_open_nobuf(filename, "r");
  read_from_file<value_type>(dest, length, f);
  std::fclose(f);
}

template<typename value_type>
void read_from_file(value_type *dest, std::uint64_t max_items,
    std::uint64_t &items_read, std::FILE *f) {

#ifdef MONITOR_DISK_USAGE
  std::lock_guard<std::mutex> lk(io_mutex);
#endif

  items_read = std::fread(dest, sizeof(value_type), max_items, f);

#ifdef MONITOR_DISK_USAGE
  current_io_volume += sizeof(value_type) * items_read;
#endif

  if (std::ferror(f)) {
    fprintf(stderr, "\nError: fread failed.\n");
    std::exit(EXIT_FAILURE);
  }
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
void fill_random_string(std::uint8_t* &s,
    std::uint64_t length, std::uint64_t sigma);
void fill_random_letters(std::uint8_t* &s,
    std::uint64_t length, std::uint64_t sigma);
std::string random_string_hash();

std::uint64_t log2ceil(std::uint64_t x);
std::uint64_t log2floor(std::uint64_t x);

template<typename int_type>
std::string intToStr(int_type x) {
  std::stringstream ss;
  ss << x;
  return ss.str();
}

template<typename int_type>
int_type gcd(int_type a, int_type b) {
  if (b == (int_type)0) return a;
  else return gcd(b, a % b);
}

template<typename int_type>
int_type lcm(int_type a, int_type b) {
  return (a / gcd(a, b)) * b;
}

template<typename value_type>
std::uint64_t disk_block_size(std::uint64_t ram_budget) {
  std::uint64_t opt_block_size
    = lcm((std::uint64_t)BUFSIZ, (std::uint64_t)sizeof(value_type));

  std::uint64_t result = 0;
  if (ram_budget < opt_block_size) {
    result = std::max((std::uint64_t)1,
        (std::uint64_t)(ram_budget / sizeof(value_type)));
  } else {
    std::uint64_t opt_block_count = ram_budget / opt_block_size;
    std::uint64_t opt_blocks_bytes = opt_block_count * opt_block_size;
    result = opt_blocks_bytes / sizeof(value_type);
  }

  return result;
}

}  // namespace utils
}  // namespace fsais_private

#endif  // __FSAIS_SRC_UTILS_HPP_INCLUDED
