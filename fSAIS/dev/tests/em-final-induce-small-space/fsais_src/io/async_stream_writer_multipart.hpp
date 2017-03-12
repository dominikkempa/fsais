/**
 * @file    fsais_src/io/async_stream_writer_multipart.hpp
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

#ifndef __FSAIS_SRC_IO_ASYNC_STREAM_WRITER_MULTIPART_HPP_INCLUDED
#define __FSAIS_SRC_IO_ASYNC_STREAM_WRITER_MULTIPART_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <cstdint>
#include <string>
#include <algorithm>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "../utils.hpp"


namespace fsais_private {

template<typename value_type>
class async_stream_writer_multipart {
  private:
    template<typename T>
    struct buffer {
      buffer(std::uint64_t size, T* const mem)
        : m_content(mem), m_size(size) {
        m_filled = 0;
      }

      inline bool empty() const { return m_filled == 0; }
      inline bool full() const { return m_filled == m_size; }
      inline std::uint64_t size_in_bytes() const { return sizeof(T) * m_filled; }
      inline std::uint64_t free_space() const { return m_size - m_filled; }

      T* const m_content;
      const std::uint64_t m_size;
      std::uint64_t m_filled;
    };

    template<typename T>
    struct circular_queue {
      private:
        std::uint64_t m_size;
        std::uint64_t m_filled;
        std::uint64_t m_head;
        std::uint64_t m_tail;
        T *m_data;

      public:
        circular_queue()
          : m_size(1),
            m_filled(0),
            m_head(0),
            m_tail(0),
            m_data(new T[m_size]) {}

        inline void push(T x) {
          m_data[m_head++] = x;
          if (m_head == m_size)
            m_head = 0;
          ++m_filled;
          if (m_filled == m_size)
            enlarge();
        }

        inline T &front() const {
          return m_data[m_tail];
        }

        inline void pop() {
          ++m_tail;
          if (m_tail == m_size)
            m_tail = 0;
          --m_filled;
        }

        inline bool empty() const { return (m_filled == 0); }
        inline std::uint64_t size() const { return m_filled; }

        ~circular_queue() {
          delete[] m_data;
        }

      private:
        void enlarge() {
          T *new_data = new T[2 * m_size];
          std::uint64_t left = m_filled;
          m_filled = 0;
          while (left > 0) {
            std::uint64_t tocopy = std::min(left, m_size - m_tail);
            std::copy(m_data + m_tail, m_data + m_tail + tocopy, new_data + m_filled);
            m_tail += tocopy;
            if (m_tail == m_size)
              m_tail = 0;
            left -= tocopy;
            m_filled += tocopy;
          }
          m_head = m_filled;
          m_tail = 0;
          m_size <<= 1;
          std::swap(m_data, new_data);
          delete[] new_data;
        }
    };

    template<typename T>
    struct buffer_queue {
      typedef buffer<T> buffer_type;

      buffer_queue(std::uint64_t n_buffers,
          std::uint64_t items_per_buf, T *mem) {
        m_signal_stop = false;
        for (std::uint64_t i = 0; i < n_buffers; ++i) {
          m_queue.push(new buffer_type(items_per_buf, mem));
          mem += items_per_buf;
        }
      }

      ~buffer_queue() {
        while (!m_queue.empty()) {
          buffer_type *buf = m_queue.front();
          m_queue.pop();
          delete buf;
        }
      }

      buffer_type *pop() {
        buffer_type *ret = m_queue.front();
        m_queue.pop();
        return ret;
      }

      void push(buffer_type *buf) {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_queue.push(buf);
      }

      void send_stop_signal() {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_signal_stop = true;
      }

      inline bool empty() const { return m_queue.empty(); }

      circular_queue<buffer_type*> m_queue;  // Must have FIFO property
      std::condition_variable m_cv;
      std::mutex m_mutex;
      bool m_signal_stop;
    };

  private:
    typedef buffer<value_type> buffer_type;
    typedef buffer_queue<value_type> buffer_queue_type;

    buffer_queue_type *m_empty_buffers;
    buffer_queue_type *m_full_buffers;

  private:
    template<typename T>
    static void io_thread_code(async_stream_writer_multipart<T> *caller) {
      typedef buffer<T> buffer_type;
      while (true) {
        // Wait for the full buffer (or a stop signal).
        std::unique_lock<std::mutex> lk(caller->m_full_buffers->m_mutex);
        while (caller->m_full_buffers->empty() &&
            !(caller->m_full_buffers->m_signal_stop))
          caller->m_full_buffers->m_cv.wait(lk);

        if (caller->m_full_buffers->empty()) {
          // We received the stop signal -- exit.
          lk.unlock();
          break;
        }

        // Extract the buffer from the collection.
        buffer_type *buffer = caller->m_full_buffers->pop();
        lk.unlock();

        // Write the data to disk.
        const T *ptr = buffer->m_content;
        while (buffer->m_filled > 0) {
          if (caller->m_file == NULL || caller->m_cur_part_items_written == caller->m_single_part_max_items) {
            if (caller->m_file != NULL) {
              std::fclose(caller->m_file);
              ++caller->m_cur_part;
            } else caller->m_cur_part = 0;
            std::string cur_part_filename = caller->m_filename +
              ".multipart_file.part" + utils::intToStr(caller->m_cur_part);
            caller->m_file = utils::file_open(cur_part_filename, "w");
            caller->m_cur_part_items_written = 0;
          }

          std::uint64_t cur_part_items_left = caller->m_single_part_max_items - caller->m_cur_part_items_written;
          std::uint64_t towrite = std::min(cur_part_items_left, buffer->m_filled);
          utils::write_to_file(ptr, towrite, caller->m_file);
          caller->m_cur_part_items_written += towrite;
          buffer->m_filled -= towrite;
          ptr += towrite;
        }

        // Add the (now empty) buffer to the collection
        // of empty buffers and notify the waiting thread.
        caller->m_empty_buffers->push(buffer);
        caller->m_empty_buffers->m_cv.notify_one();
      }
    }

    // Get a free buffer from the collection of free buffers.
    buffer_type* get_empty_buffer() {
      std::unique_lock<std::mutex> lk(m_empty_buffers->m_mutex);
      while (m_empty_buffers->empty())
        m_empty_buffers->m_cv.wait(lk);
      buffer_type *ret = m_empty_buffers->pop();
      lk.unlock();
      return ret;
    }

  private:
    std::FILE *m_file;
    std::string m_filename;

    std::uint64_t m_cur_part;
    std::uint64_t m_single_part_max_items;
    std::uint64_t m_cur_part_items_written;
    std::uint64_t m_bytes_written;
    std::uint64_t m_items_per_buf;

    value_type *m_mem;
    buffer_type *m_cur_buffer;
    std::thread *m_io_thread;

  public:
    async_stream_writer_multipart(std::string filename,
        std::uint64_t single_part_max_bytes) {
      init(filename, single_part_max_bytes, (8UL << 20), 4UL);
    }

    async_stream_writer_multipart(std::string filename,
        std::uint64_t single_part_max_bytes,
        std::uint64_t total_buf_size_bytes,
        std::uint64_t n_buffers) {
      init(filename, single_part_max_bytes, total_buf_size_bytes, n_buffers);
    }

    void init(std::string filename,
        std::uint64_t single_part_max_bytes,
        std::uint64_t total_buf_size_bytes,
        std::uint64_t n_buffers) {
      if (n_buffers == 0) {
        fprintf(stderr, "\nError in async_stream_writer_multipart: n_buffers == 0\n");
        std::exit(EXIT_FAILURE);
      }

      m_filename = filename;

      // Initialize basic parameters. Note: if no items are
      // written, this class does not create any files.
      m_single_part_max_items = std::max(1UL, single_part_max_bytes / sizeof(value_type));
      m_file = NULL;

      // Allocate buffers.
      std::uint64_t total_buf_size_items = total_buf_size_bytes / sizeof(value_type);
      m_items_per_buf = std::max(1UL, total_buf_size_items / n_buffers);
      m_mem = utils::allocate_array<value_type>(n_buffers * m_items_per_buf);
      m_empty_buffers = new buffer_queue_type(n_buffers, m_items_per_buf, m_mem);
      m_full_buffers = new buffer_queue_type(0, 0, NULL);

      // Initialize empty buffer.
      m_cur_buffer = get_empty_buffer();
      m_bytes_written = 0;

      // Start the I/O thread.
      m_io_thread = new std::thread(io_thread_code<value_type>, this);
    }

    // It's safe to call if the buffer is not full, though
    // in principle should only be called internally. Calling
    // it too often will lead to poor I/O performance.
    void flush() {
      if (!m_cur_buffer->empty()) {
        m_full_buffers->push(m_cur_buffer);
        m_full_buffers->m_cv.notify_one();
        m_cur_buffer = get_empty_buffer();
      }
    }

    inline std::uint64_t get_parts_count() const {
      std::uint64_t items_written = m_bytes_written / sizeof(value_type);
      std::uint64_t n_parts = (items_written + m_single_part_max_items - 1) / m_single_part_max_items;
      return n_parts;
    }

    ~async_stream_writer_multipart() {
      // Send the last incomplete buffer for writing.
      flush();

      // Let the I/O thread know that we're done.
      m_full_buffers->send_stop_signal();
      m_full_buffers->m_cv.notify_one();

      // Wait for the I/O thread to finish.
      m_io_thread->join();

      // Clean up.
      delete m_empty_buffers;
      delete m_full_buffers;
      delete m_io_thread;
      if (m_file != NULL)
        std::fclose(m_file);
      delete m_cur_buffer;
      utils::deallocate(m_mem);
    }

    inline void write(value_type x) {
      m_bytes_written += sizeof(value_type);
      m_cur_buffer->m_content[m_cur_buffer->m_filled++] = x;
      if (m_cur_buffer->full())
        flush();
    }

    inline void write(const value_type *values, std::uint64_t length) {
      m_bytes_written += length * sizeof(value_type);
      while (length > 0) {
        std::uint64_t tocopy = std::min(length, m_cur_buffer->free_space());
        std::copy(values, values + tocopy, m_cur_buffer->m_content + m_cur_buffer->m_filled);
        m_cur_buffer->m_filled += tocopy;
        values += tocopy;
        length -= tocopy;
        if (m_cur_buffer->full())
          flush();
      }
    }

    inline std::uint64_t bytes_written() const {
      return m_bytes_written;
    }
};

}  // namespace fsais_private

#endif  // __FSAIS_SRC_IO_ASYNC_STREAM_WRITER_MULTIPART_HPP_INCLUDED
