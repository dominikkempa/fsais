/**
 * @file    fsais_src/utils.cpp
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
#include <cstring>
#include <cerrno>
#include <ctime>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string>
#include <mutex>
#include <fstream>
#include <algorithm>

#include "utils.hpp"


namespace fsais_private {
namespace utils {

std::mutex io_mutex;
std::mutex allocator_mutex;
std::uint64_t current_ram_allocation;
std::uint64_t current_io_volume;
std::uint64_t current_disk_allocation;
std::uint64_t peak_ram_allocation;
std::uint64_t peak_disk_allocation;

void *allocate(const std::uint64_t bytes) {
  std::lock_guard<std::mutex> lk(allocator_mutex);
  std::uint8_t * const ptr =
    (std::uint8_t *)malloc(bytes + 8);
  std::uint64_t * const ptr64 = (std::uint64_t *)ptr;
  *ptr64 = bytes;
  std::uint8_t * const ret = ptr + 8;
  current_ram_allocation += bytes;
  peak_ram_allocation =
    std::max(peak_ram_allocation,
        current_ram_allocation);
  return (void *)ret;
}

void *aligned_allocate(
    const std::uint64_t bytes,
    const std::uint64_t align) {
  std::uint8_t * const ptr =
    (std::uint8_t *)allocate(bytes + (align - 1) + 8);
  std::uint8_t *ptr2 = ptr + 8;
  const std::uint64_t n_blocks =
    ((std::uint64_t)ptr2 + align - 1) / align;
  ptr2 = (std::uint8_t *)(n_blocks * align);
  std::uint64_t * const ptr64 = (std::uint64_t *)(ptr2 - 8);
  *ptr64 = (std::uint64_t)ptr;
  return (void *)ptr2;
}

void deallocate(const void * const tab) {
  std::lock_guard<std::mutex> lk(allocator_mutex);
  std::uint8_t * const ptr = (std::uint8_t *)tab - 8;
  const std::uint64_t * const ptr64 = (std::uint64_t *)ptr;
  const std::uint64_t bytes = *ptr64;
  current_ram_allocation -= bytes;
  free(ptr);
}

void aligned_deallocate(const void * const tab) {
  const std::uint8_t * const ptr = (std::uint8_t *)tab;
  const std::uint64_t * const ptr64 = (std::uint64_t *)(ptr - 8);
  deallocate((void *)(*ptr64));
}

void initialize_stats() {
  current_ram_allocation = 0;
  current_disk_allocation = 0;
  current_io_volume = 0;
  peak_ram_allocation = 0;
  peak_disk_allocation = 0;
}

std::uint64_t get_current_ram_allocation() {
  return current_ram_allocation;
}

std::uint64_t get_peak_ram_allocation() {
  return peak_ram_allocation;
}

std::uint64_t get_current_io_volume() {
  return current_io_volume;
}

std::uint64_t get_current_disk_allocation() {
  return current_disk_allocation;
}

std::uint64_t get_peak_disk_allocation() {
  return peak_disk_allocation;
}

long double wclock() {
  timeval tim;
  gettimeofday(&tim, NULL);
  return tim.tv_sec + (tim.tv_usec / 1000000.0L);
}

void sleep(const long double duration_sec) {
  const long double timestamp = wclock();
  while (wclock() - timestamp < duration_sec);
}

std::FILE *file_open(
    const std::string filename,
    const std::string mode) {
  std::FILE * const f =
    std::fopen(filename.c_str(), mode.c_str());
  if (f == NULL) {
    std::perror(filename.c_str());
    std::exit(EXIT_FAILURE);
  }
  return f;
}

std::FILE *file_open_nobuf(
    const std::string filename,
    const std::string mode) {
  std::FILE * const f =
    std::fopen(filename.c_str(), mode.c_str());
  if (f == NULL) {
    std::perror(filename.c_str());
    std::exit(EXIT_FAILURE);
  }
  if(std::setvbuf(f, NULL, _IONBF, 0) != 0) {
    perror("setvbuf failed");
    std::exit(EXIT_FAILURE);
  }
  return f;
}

std::uint64_t file_size(const std::string filename) {
  std::FILE * const f = file_open_nobuf(filename, "r");
  std::fseek(f, 0, SEEK_END);
  const long size = std::ftell(f);
  if (size < 0) {
    std::perror(filename.c_str());
    std::exit(EXIT_FAILURE);
  }
  std::fclose(f);
  return (std::uint64_t)size;
}

bool file_exists(const std::string filename) {
  std::FILE * const f = std::fopen(filename.c_str(), "r");
  const bool result = (f != NULL);
  if (f != NULL)
    std::fclose(f);
  return result;
}

void file_delete(const std::string filename) {

#ifdef MONITOR_DISK_USAGE
  std::lock_guard<std::mutex> lk(io_mutex);
  current_disk_allocation -= file_size(filename);
#endif

  const int res = std::remove(filename.c_str());
  if (res != 0) {
    std::perror(filename.c_str());
    std::exit(EXIT_FAILURE);
  }
}

std::string absolute_path(std::string filename) {
  char path[1 << 12];
  bool created = false;
  if (!file_exists(filename)) {
    std::fclose(file_open(filename, "w"));
    created = true;
  }
  if (!realpath(filename.c_str(), path)) {
    std::perror(filename.c_str());
    std::exit(EXIT_FAILURE);
  }
  if (created)
    file_delete(filename);
  return std::string(path);
}

void empty_page_cache(const std::string filename) {
  const int fd = open(filename.c_str(), O_RDWR);
  if (fd == -1) {
    std::perror(filename.c_str());
    std::exit(EXIT_FAILURE);
  }
  const off_t length = lseek(fd, 0, SEEK_END);
  lseek(fd, 0L, SEEK_SET);
  posix_fadvise(fd, 0, length, POSIX_FADV_DONTNEED);
  close(fd);
}

std::string get_timestamp() {
  const std::time_t result = std::time(NULL);
  return std::string(std::ctime(&result));
}

std::int32_t random_int32(
    const std::int32_t p,
    const std::int32_t r) {
  return p + rand() % (r - p + 1);
}

std::int64_t random_int64(
    const std::int64_t p,
    const std::int64_t r) {
  const std::uint64_t r30 = RAND_MAX * rand() + rand();
  const std::uint64_t s30 = RAND_MAX * rand() + rand();
  const std::uint64_t t4  = rand() & 0xf;
  const std::uint64_t r64 = (r30 << 34) + (s30 << 4) + t4;
  return p + r64 % (r - p + 1);
}

void fill_random_string(
    std::uint8_t * const &s,
    const std::uint64_t length,
    const std::uint64_t sigma) {
  for (std::uint64_t i = 0; i < length; ++i)
    s[i] = random_int32(0, sigma - 1);
}

void fill_random_letters(
    std::uint8_t * const &s,
    const std::uint64_t length,
    const std::uint64_t sigma) {
  fill_random_string(s, length, sigma);
  for (std::uint64_t i = 0; i < length; ++i)
    s[i] += 'a';
}

std::string random_string_hash() {
  const uint64_t hash =
    (uint64_t)rand() * RAND_MAX + rand();
  std::stringstream ss;
  ss << hash;
  return ss.str();
}

std::uint64_t log2ceil(const std::uint64_t x) {
  std::uint64_t pow2 = 1;
  std::uint64_t w = 0;
  while (pow2 < x) {
    pow2 <<= 1;
    ++w;
  }
  return w;
}

std::uint64_t log2floor(const std::uint64_t x) {
  std::uint64_t pow2 = 1;
  std::uint64_t w = 0;
  while ((pow2 << 1) <= x) {
    pow2 <<= 1;
    ++w;
  }
  return w;
}

}  // namespace utils
}  // namespace fsais_private
