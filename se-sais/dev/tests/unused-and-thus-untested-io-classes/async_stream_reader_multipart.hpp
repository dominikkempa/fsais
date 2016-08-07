#ifndef __ASYNC_STREAM_READER_MULTIPART_HPP_INCLUDED
#define __ASYNC_STREAM_READER_MULTIPART_HPP_INCLUDED

#include <cstdio>
#include <cstdint>
#include <thread>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <string>
#include <algorithm>

#include "../utils.hpp"


template<typename value_type>
class async_stream_reader_multipart {
  private:
    template<typename T>
    struct buffer {
      buffer(std::uint64_t size) {
        m_size = size;
        m_content = (T *)malloc(m_size * sizeof(T));
        m_filled = 0;
      }

      void read_from_file(std::FILE *f) {
        m_filled = std::fread(m_content, sizeof(T), m_size, f);
      }

      std::uint64_t size_in_bytes() const {
        return sizeof(T) * m_filled;
      }

      ~buffer() {
        free(m_content);
      }

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

      std::queue<buffer_type*> m_queue;
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
    static void io_thread_code(async_stream_reader_multipart<T> *caller) {
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
        if (caller->m_file == NULL) {
          // Attempt to open and read from the file.
          std::string cur_part_filename = caller->m_filename + ".multipart_file.part" + utils::intToStr(caller->m_cur_part);
          if (utils::file_exists(cur_part_filename)) {
            caller->m_file = utils::file_open(cur_part_filename, "r");
            buffer->read_from_file(caller->m_file);
          } else buffer->m_filled = 0;
        } else {
          buffer->read_from_file(caller->m_file);
          if (buffer->empty()) {
            // Close and delete current file.
            std::fclose(caller->m_file);
            caller->m_file = NULL;
            std::string cur_part_filename = caller->m_filename + ".multipart_file.part" + utils::intToStr(caller->m_cur_part);
            utils::file_delete(cur_part_filename);

            // Attempt to read from the next file.
            ++caller->m_cur_part;
            cur_part_filename = caller->m_filename + ".multipart_file.part" + utils::intToStr(caller->m_cur_part);
            if (utils::file_exists(cur_part_filename)) {
              caller->m_file = utils::file_open(cur_part_filename, "r");
              buffer->read_from_file(caller->m_file);
            } else buffer->m_filled = 0;
          }
        }
        caller->m_bytes_read += buffer->size_in_bytes();

        if (buffer->empty()) {
          // Reinsert the buffer into the queue of empty buffers,
          // notify the full buffers queue, and exit.
          caller->m_empty_buffers->push(buffer);
          caller->m_full_buffers->send_stop_signal();
          caller->m_full_buffers->m_cv.notify_one();
          break;
        } else {
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
        m_empty_buffers->push(m_cur_buffer);
        m_empty_buffers->m_cv.notify_one();
        m_cur_buffer = NULL;
      }

      // Extract a filled buffer.
      std::unique_lock<std::mutex> lk(m_full_buffers->m_mutex);
      while (m_full_buffers->empty() && !(m_full_buffers->m_signal_stop))
        m_full_buffers->m_cv.wait(lk);
      m_cur_buffer_pos = 0;
      if (m_full_buffers->empty()) {
        lk.unlock();
        m_cur_buffer_filled = 0;
      } else {
        m_cur_buffer = m_full_buffers->pop();
        lk.unlock();
        m_cur_buffer_filled = m_cur_buffer->m_filled;
      }
    }

  private:
    std::FILE *m_file;
    std::string m_filename;
    std::uint64_t m_cur_part;
    std::uint64_t m_bytes_read;
    std::uint64_t m_cur_buffer_pos;
    std::uint64_t m_cur_buffer_filled;
    buffer_type *m_cur_buffer;
    std::thread *m_io_thread;

  public:
    async_stream_reader_multipart(std::string filename) {
      init(filename, (8UL << 20), 4UL);
    }

    async_stream_reader_multipart(std::string filename,
        std::uint64_t total_buf_size_bytes,
        std::uint64_t n_buffers) {
      init(filename, total_buf_size_bytes, n_buffers);
    }

    void init(std::string filename,
        std::uint64_t total_buf_size_bytes,
        std::uint64_t n_buffers) {
      // Initialize counters.
      m_cur_part = 0;
      m_bytes_read = 0;
      m_cur_buffer_pos = 0;
      m_cur_buffer_filled = 0;
      m_cur_buffer = NULL;
      m_file = NULL;
      m_filename = filename;

      // Allocate buffers.
      std::uint64_t total_buf_size_items = total_buf_size_bytes / sizeof(value_type);
      std::uint64_t items_per_buf = std::max(1UL, total_buf_size_items / n_buffers);
      m_empty_buffers = new buffer_queue_type(n_buffers, items_per_buf);
      m_full_buffers = new buffer_queue_type();

      // Start the I/O thread.
      m_io_thread = new std::thread(io_thread_code<value_type>, this);
    }

    ~async_stream_reader_multipart() {
      // Let the I/O thread know that we're done.
      m_empty_buffers->send_stop_signal();
      m_empty_buffers->m_cv.notify_one();

      // Wait for the thread to finish.
      m_io_thread->join();

      // Clean up.
      delete m_empty_buffers;
      delete m_full_buffers;
      delete m_io_thread;
      if (m_file != NULL)
        std::fclose(m_file);
      if (m_cur_buffer != NULL)
        delete m_cur_buffer;
    }

    inline value_type read() {
      if (m_cur_buffer_pos == m_cur_buffer_filled)
        receive_new_buffer();

      return m_cur_buffer->m_content[m_cur_buffer_pos++];
    }

    void read(value_type *dest, std::uint64_t howmany) {
      while (howmany > 0) {
        if (m_cur_buffer_pos == m_cur_buffer_filled)
          receive_new_buffer();

        std::uint64_t cur_buf_left = m_cur_buffer_filled - m_cur_buffer_pos;
        std::uint64_t tocopy = std::min(howmany, cur_buf_left);
        for (std::uint64_t i = 0; i < tocopy; ++i)
          dest[i] = m_cur_buffer->m_content[m_cur_buffer_pos + i];
        m_cur_buffer_pos += tocopy;
        dest += tocopy;
        howmany -= tocopy;
      }
    }

    void skip(std::uint64_t howmany) {
      while (howmany > 0) {
        if (m_cur_buffer_pos == m_cur_buffer_filled)
          receive_new_buffer();

        std::uint64_t toskip = std::min(howmany, m_cur_buffer_filled - m_cur_buffer_pos);
        m_cur_buffer_pos += toskip;
        howmany -= toskip;
      }
    }

    inline value_type peek() {
      if (m_cur_buffer_pos == m_cur_buffer_filled)
        receive_new_buffer();

      return m_cur_buffer->m_content[m_cur_buffer_pos];
    }

    inline bool empty() {
      if (m_cur_buffer_pos == m_cur_buffer_filled)
        receive_new_buffer();

      return (m_cur_buffer_pos == m_cur_buffer_filled);
    }

    inline std::uint64_t bytes_read() const {
      return m_bytes_read;
    }
};

#endif  // __ASYNC_STREAM_READER_MULTIPART_HPP_INCLUDED
