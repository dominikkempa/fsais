/**
 * @file    fsais_src/io/async_bit_stream_writer.hpp
 * @section LICENCE
 *
 * This file is part of fSAIS v0.1.0
 * See: https://github.com/dominikkempa/fsais
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

#ifndef __FSAIS_SRC_IO_ASYNC_BIT_STREAM_WRITER_HPP_INCLUDED
#define __FSAIS_SRC_IO_ASYNC_BIT_STREAM_WRITER_HPP_INCLUDED

#include <cstdio>
#include <cstdint>
#include <string>
#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "../utils.hpp"


namespace fsais_private {

class async_bit_stream_writer {
  private:
    static void io_thread_code(async_bit_stream_writer *writer) {
      while (true) {

        // Wait until the passive buffer is available.
        std::unique_lock<std::mutex> lk(writer->m_mutex);
        while (!(writer->m_avail) && !(writer->m_finished))
          writer->m_cv.wait(lk);

        if (!(writer->m_avail) && (writer->m_finished)) {

          // We're done, terminate the thread.
          lk.unlock();
          return;
        }
        lk.unlock();

        // Safely write the data to disk.
        utils::write_to_file(writer->m_passive_buf,
            writer->m_passive_buf_filled, writer->m_file);

        // Let the caller know that the I/O thread finished writing.
        lk.lock();
        writer->m_avail = false;
        lk.unlock();
        writer->m_cv.notify_one();
      }
    }

    // Passes on the active buffer (full, unless it's the last one,
    // partially filled, buffer passed from destructor) to the I/O thread.
    void send_active_buf_to_write() {

      // Wait until the I/O thread finishes writing the previous buffer.
      std::unique_lock<std::mutex> lk(m_mutex);
      while (m_avail == true)
        m_cv.wait(lk);

      // Set the new passive buffer.
      std::swap(m_active_buf, m_passive_buf);
      m_passive_buf_filled = m_active_buf_filled;
      m_active_buf_filled = 0;
      m_bit_pos = 0;
      m_active_buf[0] = 0;

      // Let the I/O thread know that the buffer is waiting.
      m_avail = true;
      lk.unlock();
      m_cv.notify_one();
    }

  public:
    async_bit_stream_writer(
        std::string filename,
        std::uint64_t buf_size_bytes,
        std::uint64_t n_buffers) {

      (void)n_buffers;  // unused now.
      m_file = utils::file_open_nobuf(filename.c_str(), "w");

      // Compute optimal buffer size.
      buf_size_bytes = std::max((std::uint64_t)1, buf_size_bytes / 2);
      m_items_per_buf =
        utils::disk_block_size<std::uint64_t>(buf_size_bytes);

      // Allocate buffers.
      m_mem = utils::allocate_array<std::uint64_t>(2UL * m_items_per_buf);
      m_active_buf = m_mem;
      m_passive_buf = m_mem + m_items_per_buf;

      m_active_buf[0] = 0;
      m_bit_pos = 0;
      m_active_buf_filled = 0;
      m_passive_buf_filled = 0;
      m_bits_written = 0;

      // Start the I/O thread.
      m_avail = false;
      m_finished = false;
      m_thread = new std::thread(io_thread_code, this);
    }

    ~async_bit_stream_writer() {

      // Clean up.
      delete m_thread;
      std::fclose(m_file);
      utils::deallocate(m_mem);
    }

    inline void write(std::uint8_t bit) {
      ++m_bits_written;
      m_active_buf[m_active_buf_filled] |= ((std::uint64_t)bit << m_bit_pos);
      ++m_bit_pos;
      if (m_bit_pos == 64) {
        m_bit_pos = 0;
        ++m_active_buf_filled;

        // If the active buffer was full, send it to I/O thread.
        // This function may wait a bit until the I/O thread
        // finishes writing the previous passive buffer.
        if (m_active_buf_filled == m_items_per_buf)
          send_active_buf_to_write();

        // Clear all bits in the current byte.
        m_active_buf[m_active_buf_filled] = 0;
      }
    }

    void stop_writing() {

      // Write the partially filled active buffer to disk.
      std::uint64_t m_bit_pos_backup = m_bit_pos;
      if (m_bit_pos != 0) ++m_active_buf_filled;
      if (m_active_buf_filled > 0L)
        send_active_buf_to_write();

      // Let the I/O thread know that we're done.
      std::unique_lock<std::mutex> lk(m_mutex);
      m_finished = true;
      lk.unlock();
      m_cv.notify_one();

      // Wait for the thread to finish.
      m_thread->join();

      // Append the number of bits in the last 64-bit word to file.
      utils::write_to_file(&m_bit_pos_backup, 1, m_file);
      m_bits_written += 64;
    }

    std::uint64_t bytes_written() const {
      std::uint64_t words_written = (m_bits_written + 63) / 64;
      return (std::uint64_t)8 * words_written;
    }

  private:
    std::uint64_t *m_mem;
    std::uint64_t *m_active_buf;
    std::uint64_t *m_passive_buf;

    std::uint64_t m_items_per_buf;
    std::uint64_t m_bit_pos;
    std::uint64_t m_active_buf_filled;
    std::uint64_t m_passive_buf_filled;
    std::uint64_t m_bits_written;

    // Used for synchronization with the I/O thread.
    bool m_avail;
    bool m_finished;
    std::mutex m_mutex;
    std::condition_variable m_cv;

    std::FILE *m_file;
    std::thread *m_thread;
};

}  // namespace fsais_private

#endif  // __FSAIS_SRC_IO_ASYNC_BIT_STREAM_WRITER_HPP_INCLUDED
