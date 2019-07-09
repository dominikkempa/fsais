/**
 * @file    fsais_src/io/async_backward_stream_writer.hpp
 * @section LICENCE
 *
 * This file is part of fSAIS v0.1.0
 * See: http://www.cs.helsinki.fi/group/pads/
 *
 * Copyright (C) 2017-2019
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

#ifndef __FSAIS_SRC_IO_ASYNC_BACKWARD_STREAM_READER_HPP_INCLUDED
#define __FSAIS_SRC_IO_ASYNC_BACKWARD_STREAM_READER_HPP_INCLUDED

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
class async_backward_stream_reader {
  private:
    template<typename T>
    struct buffer {
      buffer(std::uint64_t size, T* const mem)
        : m_content(mem), m_size(size) {
        m_filled = 0;
      }

      void read_from_file(std::FILE *f) {
        std::uint64_t filepos = std::ftell(f);
        if (filepos == 0) m_filled = 0;
        else {
          m_filled = std::min(m_size, filepos / sizeof(T));
          std::fseek(f, -1UL * m_filled * sizeof(T), SEEK_CUR);
          utils::read_from_file(m_content, m_filled, f);
          std::fseek(f, -1UL * m_filled * sizeof(T), SEEK_CUR);
        }
      }

      inline std::uint64_t size_in_bytes() const {
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

        inline bool empty() const {
          return (m_filled == 0);
        }

        inline std::uint64_t size() const {
          return m_filled;
        }

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
            std::copy(m_data + m_tail,
                m_data + m_tail + tocopy, new_data + m_filled);

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

      buffer_queue(
          std::uint64_t n_buffers,
          std::uint64_t items_per_buf,
          T *mem) {
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

      inline bool empty() const {
        return m_queue.empty();
      }

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
    static void io_thread_code(
        async_backward_stream_reader<T> *caller) {
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

        // Read the data from disk.
        buffer->read_from_file(caller->m_file);
        if (buffer->empty()) {

          // If we reached the end of file,
          // reinsert the buffer into the queue
          // of empty buffers and exit.
          caller->m_empty_buffers->push(buffer);
          caller->m_full_buffers->send_stop_signal();
          caller->m_full_buffers->m_cv.notify_one();
          break;
        } else {

          // Update the number of bytes read.
          caller->m_bytes_read += buffer->size_in_bytes();

          // Add the buffer to the queue of filled buffers.
          caller->m_full_buffers->push(buffer);
          caller->m_full_buffers->m_cv.notify_one();
        }
      }
    }

  public:
    void receive_new_buffer() {

      // Push the current buffer back to the poll of empty buffers.
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
      if (m_full_buffers->empty()) {
        lk.unlock();
        m_cur_buffer_filled = 0;
      } else {
        m_cur_buffer = m_full_buffers->pop();
        lk.unlock();
        m_cur_buffer_filled = m_cur_buffer->m_filled;
      }
      m_cur_buffer_pos = m_cur_buffer_filled;
    }

  private:
    std::FILE *m_file;
    std::uint64_t m_bytes_read;
    std::uint64_t m_cur_buffer_pos;
    std::uint64_t m_cur_buffer_filled;

    value_type *m_mem;
    buffer_type *m_cur_buffer;
    std::thread *m_io_thread;

  public:

    // Constructor, default buffer sizes, no skip.
    async_backward_stream_reader(std::string filename) {
      init(filename, (8UL << 20), 4UL, 0UL);
    }

    // Constructor, default buffer sizes, given skip.
    async_backward_stream_reader(std::string filename,
        std::uint64_t n_skip_bytes) {
      init(filename, (8UL << 20), 4UL, n_skip_bytes);
    }

    // Constructor, no skip, given buffer sizes.
    async_backward_stream_reader(std::string filename,
        std::uint64_t total_buf_size_items, std::uint64_t n_buffers) {
      init(filename, total_buf_size_items, n_buffers, 0UL);
    }

    // Constructor, given buffer sizes and skip.
    async_backward_stream_reader(std::string filename,
        std::uint64_t total_buf_size_items, std::uint64_t n_buffers,
        std::uint64_t n_skip_bytes) {
      init(filename, total_buf_size_items, n_buffers, n_skip_bytes);
    }

    // Main initializing function.
    void init(std::string filename, std::uint64_t total_buf_size_bytes,
        std::uint64_t n_buffers, std::uint64_t n_skip_bytes) {
      if (n_buffers == 0) {
        fprintf(stderr, "\nError in async_backward_stream_reader: n_buffers == 0\n");
        std::exit(EXIT_FAILURE);
      }

      // Open input file.
      m_file = utils::file_open_nobuf(filename.c_str(), "r");

      // Reposition the file pointer if necessary.
      std::fseek(m_file, 0, SEEK_END);
      if (n_skip_bytes > 0)
        std::fseek(m_file, -1UL * n_skip_bytes, SEEK_CUR);

      // Initialize counters.
      m_bytes_read = 0;
      m_cur_buffer_pos = 0;
      m_cur_buffer_filled = 0;
      m_cur_buffer = NULL;

      // Computer optimal buffer size.
      std::uint64_t buf_size_bytes =
        std::max((std::uint64_t)1, total_buf_size_bytes / n_buffers);
      std::uint64_t items_per_buf =
        utils::disk_block_size<value_type>(buf_size_bytes);

      // Allocate buffers.
      m_mem = utils::allocate_array<value_type>(n_buffers * items_per_buf);
      m_empty_buffers = new buffer_queue_type(n_buffers, items_per_buf, m_mem);
      m_full_buffers = new buffer_queue_type(0, 0, NULL);

      // Start the I/O thread.
      m_io_thread = new std::thread(io_thread_code<value_type>, this);
    }

    // Return the next item in the stream.
    inline value_type read() {
      if (m_cur_buffer_pos == 0)
        receive_new_buffer();

      return m_cur_buffer->m_content[--m_cur_buffer_pos];
    }

    // Read 'howmany' items into 'dest'.
    void read(value_type *dest, std::uint64_t howmany) {
      while (howmany > 0) {
        if (m_cur_buffer_pos == 0)
          receive_new_buffer();

        std::uint64_t cur_buf_left = m_cur_buffer_pos;
        std::uint64_t tocopy = std::min(howmany, cur_buf_left);
        for (std::uint64_t i = 0; i < tocopy; ++i)
          dest[i] = m_cur_buffer->m_content[m_cur_buffer_pos - 1 - i];
        m_cur_buffer_pos -= tocopy;
        dest += tocopy;
        howmany -= tocopy;
      }
    }

    // Return the next item in the stream.
    inline value_type peek() {
      if (m_cur_buffer_pos == 0)
        receive_new_buffer();

      return m_cur_buffer->m_content[m_cur_buffer_pos - 1];
    }

    // True iff there are no more items in the stream.
    inline bool empty() {
      if (m_cur_buffer_pos == 0)
        receive_new_buffer();

      return (m_cur_buffer_pos == 0);
    }

    // Return the performed I/O in bytes. Unlike in the
    // writer classes (where m_bytes_written is updated
    // in the write methods), here m_bytes_read is updated
    // in the I/O thread. This is to correctly account
    // for the read-ahead operations in cases where user
    // did not read the whole file. In those cases, however,
    // the user must call the stop_reading() method before
    // calling bytes_read() to obtain the correct result.
    inline std::uint64_t bytes_read() const {
      return m_bytes_read;
    }

    // Stop the I/O thread, now the user can
    // cafely call the bytes_read() method.
    void stop_reading() {
      if (m_io_thread != NULL) {
        m_empty_buffers->send_stop_signal();
        m_empty_buffers->m_cv.notify_one();
        m_io_thread->join();
        delete m_io_thread;
        m_io_thread = NULL;
      }
    }

    // Destructor.
    ~async_backward_stream_reader() {
      stop_reading();

      // Clean up.
      delete m_empty_buffers;
      delete m_full_buffers;
      if (m_file != stdin)
        std::fclose(m_file);

      if (m_cur_buffer != NULL)
        delete m_cur_buffer;

      utils::deallocate(m_mem);
    }
};

}  // namespace fsais_private

#endif  // __FSAIS_SRC_IO_ASYNC_BACKWARD_STREAM_READER_HPP_INCLUDED
