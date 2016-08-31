
/************************************************************************
* Copyright (C) 2014-2016                                               *
*   Juha Karkkainen <juha.karkkainen (at) cs.helsinki.fi>               *
*   Dominik Kempa <dominik.kempa (at) cs.helsinki.fi or (at) gmail.com> *
************************************************************************/

#ifndef __ASYNC_STREAM_WRITER_HPP_INCLUDED
#define __ASYNC_STREAM_WRITER_HPP_INCLUDED

#include <cstdio>
#include <cstdint>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <string>
#include <algorithm>

#include "utils.hpp"


template<typename value_type>
class async_stream_writer {
  private:
    template<typename T>
    struct buffer {
      buffer(std::uint64_t size) {
        m_size = size;
        m_content = (T *)malloc(m_size * sizeof(T));
        m_filled = 0;
      }

      void write_to_file(std::FILE *f) {
        utils::write_to_file(m_content, m_filled, f);
        m_filled = 0;
      }

      ~buffer() {
        free(m_content);
      }

      inline std::uint64_t size_in_bytes() const { return sizeof(T) * m_filled; }
      inline std::uint64_t free_space() const { return m_size - m_filled; }

      inline bool empty() const { return m_filled == 0; }
      inline bool full() const { return m_filled == m_size; }

      T *m_content;
      std::uint64_t m_size;
      std::uint64_t m_filled;
    };

    template<typename buffer_type>
    struct buffer_queue {
      buffer_queue(std::uint64_t n_buffers = 0, std::uint64_t items_per_buf = 0) {
        m_signal_stop = false;
        for (std::uint64_t i = 0; i < n_buffers; ++i)
          m_queue.push(new buffer_type(items_per_buf));
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

      std::queue<buffer_type*> m_queue;  // Must have FIFO property
      std::condition_variable m_cv;
      std::mutex m_mutex;
      bool m_signal_stop;
    };

  private:
    typedef buffer<value_type> buffer_type;
    typedef buffer_queue<buffer_type> buffer_queue_type;

    buffer_queue_type *m_empty_buffers;
    buffer_queue_type *m_full_buffers;

  private:
    template<typename T>
    static void io_thread_code(async_stream_writer<T> *caller) {
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

        // Safely write the data to disk.
        buffer->write_to_file(caller->m_file);

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

    std::uint64_t m_bytes_written;
    std::uint64_t m_items_per_buf;

    buffer_type *m_cur_buffer;
    std::thread *m_io_thread;

  public:
    async_stream_writer(std::string filename = std::string(""),
        std::uint64_t total_buf_size_bytes = (8UL << 20),
        std::uint64_t n_buffers = 4UL,
        std::string write_mode = std::string("w")) {
      if (filename.empty()) m_file = stdout;
      else m_file = utils::file_open_nobuf(filename.c_str(), write_mode);

      // Allocate buffers.
      std::uint64_t total_buf_size_items = total_buf_size_bytes / sizeof(value_type);
      m_items_per_buf = std::max(1UL, total_buf_size_items / n_buffers);
      m_empty_buffers = new buffer_queue_type(n_buffers, m_items_per_buf);
      m_full_buffers = new buffer_queue_type();

      // Initialize empty buffer.
      m_cur_buffer = get_empty_buffer();
      m_bytes_written = 0;

      // Start the I/O thread.
      m_io_thread = new std::thread(io_thread_code<value_type>, this);
    }

    ~async_stream_writer() {
      // Send the last incomplete buffer for writing.
      if (!(m_cur_buffer->empty())) {
        m_full_buffers->push(m_cur_buffer);
        m_full_buffers->m_cv.notify_one();
        m_cur_buffer = NULL;
      }

      // Let the I/O thread know that we're done.
      m_full_buffers->send_stop_signal();
      m_full_buffers->m_cv.notify_one();

      // Wait for the I/O thread to finish.
      m_io_thread->join();

      // Clean up.
      delete m_empty_buffers;
      delete m_full_buffers;
      delete m_io_thread;
      if (m_file != stdout)
        std::fclose(m_file);
      if (m_cur_buffer != NULL)
        delete m_cur_buffer;
    }

    inline void write(value_type x) {
      m_bytes_written += sizeof(value_type);
      m_cur_buffer->m_content[m_cur_buffer->m_filled++] = x;
      if (m_cur_buffer->full()) {
        m_full_buffers->push(m_cur_buffer);
        m_full_buffers->m_cv.notify_one();
        m_cur_buffer = get_empty_buffer();
      }
    }

    inline void write(const value_type *values, std::uint64_t length) {
      m_bytes_written += length * sizeof(value_type);
      while (length > 0) {
        std::uint64_t tocopy = std::min(length, m_cur_buffer->free_space());
        std::copy(values, values + tocopy, m_cur_buffer->m_content + m_cur_buffer->m_filled);
        m_cur_buffer->m_filled += tocopy;
        values += tocopy;
        length -= tocopy;
        if (m_cur_buffer->full()) {
          m_full_buffers->push(m_cur_buffer);
          m_full_buffers->m_cv.notify_one();
          m_cur_buffer = get_empty_buffer();
        }
      }
    }

    inline std::uint64_t bytes_written() const {
      return m_bytes_written;
    }
};

#endif  // __ASYNC_STREAM_WRITER_HPP_INCLUDED
