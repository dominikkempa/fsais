/**
 * @file    fsais_src/io/async_backward_stream_writer_multipart.hpp
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

#ifndef __FSAIS_SRC_IO_ASYNC_BACKWARD_STREAM_READER_MULTIPART_HPP_INCLUDED
#define __FSAIS_SRC_IO_ASYNC_BACKWARD_STREAM_READER_MULTIPART_HPP_INCLUDED

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
class async_backward_stream_reader_multipart {
  private:
    template<typename T>
    struct buffer {
      buffer(std::uint64_t size, T* const mem)
        : m_content(mem), m_size(size) {
        m_filled = 0;
      }

      bool read_from_file(std::FILE *f) {
        std::uint64_t filepos = std::ftell(f);
        if (filepos == 0) m_filled = 0;
        else {
          m_filled = std::min(m_size, filepos / sizeof(T));
          std::fseek(f, -1UL * m_filled * sizeof(T), SEEK_CUR);
          utils::read_from_file(m_content, m_filled, f);
          std::fseek(f, -1UL * m_filled * sizeof(T), SEEK_CUR);
        }

        return (filepos == m_filled * sizeof(T));
      }

      std::uint64_t size_in_bytes() const {
        return sizeof(T) * m_filled;
      }

      inline bool empty() const {
        return (m_filled == 0);
      }

      inline void set_empty() {
        m_filled = 0;
      }

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
    static void io_thread_code(async_backward_stream_reader_multipart<T> *caller) {
      typedef buffer<T> buffer_type;
      while (true) {
        // Wait for an empty buffer (or a stop signal).
        std::unique_lock<std::mutex> lk(caller->m_empty_buffers->m_mutex);
        while (caller->m_empty_buffers->empty() &&
            !(caller->m_empty_buffers->m_signal_stop))
          caller->m_empty_buffers->m_cv.wait(lk);

        if (caller->m_empty_buffers->empty()) {
          // We received the stop signal -- exit.
          lk.unlock();
          break;
        }

        // Extract the buffer from the queue.
        buffer_type *buffer = caller->m_empty_buffers->pop();
        lk.unlock();

        if (caller->m_file == NULL) {
          std::string cur_part_filename = caller->m_filename + ".multipart_file.part" + utils::intToStr(caller->m_parts_left - 1);
          caller->m_file = utils::file_open(cur_part_filename, "r");
          std::fseek(caller->m_file, 0, SEEK_END);
        }

        bool no_more_data = buffer->read_from_file(caller->m_file);
        if (buffer->empty()) {
          // Here we assume that any multipart writer produces
          // zero files, if no write operation was called.
          fprintf(stderr, "\nError: empty buffer in async_backward_stream_reader_multipart!\n");
          std::exit(EXIT_FAILURE);
        }

        // Add the buffer to the queue of filled buffers.
        caller->m_full_buffers->push(buffer);
        caller->m_full_buffers->m_cv.notify_one();

        if (no_more_data) {
          // We reached the beginning of file.
          std::fclose(caller->m_file);
          caller->m_file = NULL;
          std::string cur_part_filename = caller->m_filename + ".multipart_file.part" + utils::intToStr(caller->m_parts_left - 1);
          utils::file_delete(cur_part_filename);
          --caller->m_parts_left;
          if (caller->m_parts_left == 0) {
            caller->m_full_buffers->send_stop_signal();
            caller->m_full_buffers->m_cv.notify_one();
            break;
          }
        }
      }
    }

  public:
    void receive_new_buffer() {
      // Push the current buffer back to the poll of empty buffer.
      if (m_cur_buffer != NULL) {
        m_cur_buffer->set_empty();
        m_empty_buffers->push(m_cur_buffer);
        m_empty_buffers->m_cv.notify_one();
        m_cur_buffer = NULL;
      }

      // Extract a filled buffer.
      std::unique_lock<std::mutex> lk(m_full_buffers->m_mutex);
      while (m_full_buffers->empty() && !(m_full_buffers->m_signal_stop))
        m_full_buffers->m_cv.wait(lk);
      if (!m_full_buffers->empty()) {
        m_cur_buffer = m_full_buffers->pop();
        m_cur_buffer_left = m_cur_buffer->m_filled;
      }
      lk.unlock();
    }

  private:
    std::uint64_t m_bytes_read;
    std::uint64_t m_parts_left;
    std::uint64_t m_cur_buffer_left;

    std::FILE *m_file;
    std::string m_filename;

    value_type *m_mem;
    buffer_type *m_cur_buffer;
    std::thread *m_io_thread;

  public:
    async_backward_stream_reader_multipart(std::string filename,
        std::uint64_t parts_count) {
      init(filename, parts_count, (8UL << 20), 4UL);
    }

    async_backward_stream_reader_multipart(std::string filename,
        std::uint64_t parts_count,
        std::uint64_t total_buf_size_bytes,
        std::uint64_t n_buffers) {
      init(filename, parts_count, total_buf_size_bytes, n_buffers);
    }

    void init(std::string filename,
        std::uint64_t parts_count,
        std::uint64_t total_buf_size_bytes,
        std::uint64_t n_buffers) {
      if (n_buffers == 0) {
        fprintf(stderr, "\nError in async_backward_stream_reader_multipart: n_buffers == 0\n");
        std::exit(EXIT_FAILURE);
      }

      // Initialize basic parameters.
      m_bytes_read = 0;
      m_cur_buffer_left = 0;
      m_parts_left = parts_count;
      m_cur_buffer = NULL;
      m_file = NULL;
      m_filename = filename;

      // Allocate buffers.
      std::uint64_t total_buf_size_items = total_buf_size_bytes / sizeof(value_type);
      std::uint64_t items_per_buf = std::max(1UL, total_buf_size_items / n_buffers);
      m_mem = utils::allocate_array<value_type>(n_buffers * items_per_buf);
      m_empty_buffers = new buffer_queue_type(n_buffers, items_per_buf, m_mem);
      m_full_buffers = new buffer_queue_type(0, 0, NULL);

      // Start the I/O thread.
      if (m_parts_left > 0)
        m_io_thread = new std::thread(io_thread_code<value_type>, this);
      else m_io_thread = NULL;
    }

    // Return the next item in the stream.
    inline value_type read() {
      m_bytes_read += sizeof(value_type);
      if (m_cur_buffer_left == 0)
        receive_new_buffer();

      return m_cur_buffer->m_content[--m_cur_buffer_left];
    }

    inline std::uint64_t bytes_read() const {
      return m_bytes_read;
    }

    // Destructor.
    ~async_backward_stream_reader_multipart() {
      if (m_io_thread != NULL) {
        // Let the I/O thread know that we're done.
        m_empty_buffers->send_stop_signal();
        m_empty_buffers->m_cv.notify_one();

        // Wait for the thread to finish.
        m_io_thread->join();
        delete m_io_thread;
      }

      // Clean up.
      delete m_empty_buffers;
      delete m_full_buffers;
      if (m_file != NULL) {
        fprintf(stderr, "\nError: m_file != NULL when destroying multipart backward stream reader!\n");
        fprintf(stderr, "Most likely, not all items were read from the file!\n");
        std::exit(EXIT_FAILURE);
      }

      if (m_cur_buffer != NULL)
        delete m_cur_buffer;

      utils::deallocate(m_mem);
    }
};

}  // namespace fsais_private

#endif  // __FSAIS_SRC_IO_ASYNC_BACKWARD_STREAM_READER_MULTIPART_HPP_INCLUDED
