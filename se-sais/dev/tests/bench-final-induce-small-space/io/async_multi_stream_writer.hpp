#ifndef __ASYNC_MULTI_STREAM_WRITER_H_INCLUDED
#define __ASYNC_MULTI_STREAM_WRITER_H_INCLUDED

#include <cstdio>
#include <cstdint>
#include <vector>
#include <queue>
#include <string>
#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "../utils.hpp"


template<typename value_type>
class async_multi_stream_writer {
  private:
    template<typename T>
    struct buffer {
      buffer(std::uint64_t size) {
        m_size = size;
        m_content = (T *)malloc(m_size * sizeof(T));
        m_filled = 0;
      }

      void flush_to_file(std::FILE *f) {
        utils::write_to_file(m_content, m_filled, f);
        m_filled = 0;
      }

      ~buffer() {
        free(m_content);
      }

      inline bool full() const { return m_filled == m_size; }
      inline bool empty() const { return m_filled == 0; }
      inline std::uint64_t size_in_bytes() const { return sizeof(T) * m_filled; }

      T *m_content;
      std::uint64_t m_filled;
      std::uint64_t m_size;
    };

    template<typename buffer_type>
    struct request {
      request(buffer_type *buffer, std::uint64_t file_id) {
        m_buffer = buffer;
        m_file_id = file_id;
      }

      buffer_type *m_buffer;
      std::uint64_t m_file_id;
    };

    template<typename request_type>
    struct request_queue {
      request_queue()
        : m_no_more_requests(false) {}

      request_type get() {
        request_type ret = m_requests.front();
        m_requests.pop();
        return ret;
      }

      inline void add(request_type request) {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_requests.push(request);
      }

      inline bool empty() const { return m_requests.empty(); }

      std::queue<request_type> m_requests;  // Must have FIFO property
      std::condition_variable m_cv;
      std::mutex m_mutex;
      bool m_no_more_requests;
    };

    template<typename buffer_type>
    struct buffer_collection {
      // Separate method to allow locking.
      inline void add(buffer_type *buffer) {
        std::lock_guard<std::mutex> lk(m_mutex);
        m_buffers.push_back(buffer);
      }

      buffer_type* get() {
        buffer_type *ret = m_buffers.back();
        m_buffers.pop_back();
        return ret;
      }

      inline bool empty() const { return m_buffers.empty(); }

      std::vector<buffer_type*> m_buffers;
      std::condition_variable m_cv;
      std::mutex m_mutex;
    };

  private:
    template<typename T>
    static void async_io_thread_code(async_multi_stream_writer<T> *caller) {
      typedef buffer<T> buffer_type;
      typedef request<buffer_type> request_type;
      while (true) {
        // Wait for request or until 'no more requests' flag is set.
        std::unique_lock<std::mutex> lk(caller->m_write_requests.m_mutex);
        while (caller->m_write_requests.empty() &&
            !(caller->m_write_requests.m_no_more_requests))
          caller->m_write_requests.m_cv.wait(lk);

        if (caller->m_write_requests.empty() &&
            caller->m_write_requests.m_no_more_requests) {
          // No more requests -- exit.
          lk.unlock();
          break;
        }

        // Extract the buffer from the collection.
        request_type request = caller->m_write_requests.get();
        lk.unlock();

        // Process the request.
        request.m_buffer->flush_to_file(caller->m_files[request.m_file_id]);

        // Add the (now empty) buffer to the collection
        // of empty buffers and notify the waiting thread.
        caller->m_free_buffers.add(request.m_buffer);
        caller->m_free_buffers.m_cv.notify_one();
      }
    }

  private:
    typedef buffer<value_type> buffer_type;
    typedef request<buffer_type> request_type;

    std::uint64_t m_bytes_written;
    std::uint64_t m_items_per_buf;

    std::vector<std::FILE*> m_files;
    std::vector<buffer_type*> m_buffers;
    buffer_collection<buffer_type> m_free_buffers;
    request_queue<request_type> m_write_requests;
    std::thread *m_io_thread;

    // Issue a request to write to buffer.
    void issue_write_request(std::uint64_t file_id) {
      request_type req(m_buffers[file_id], file_id);
      m_buffers[file_id] = NULL;
      m_write_requests.add(req);
      m_write_requests.m_cv.notify_one();
    }

    // Get a free buffer from the collection of free buffers.
    buffer_type* get_free_buffer() {
      std::unique_lock<std::mutex> lk(m_free_buffers.m_mutex);
      while (m_free_buffers.empty())
        m_free_buffers.m_cv.wait(lk);
      buffer_type *ret = m_free_buffers.get();
      lk.unlock();
      return ret;
    }

  public:
    async_multi_stream_writer(std::uint64_t bufsize_per_file_in_bytes = (1UL << 20),
        std::uint64_t n_free_buffers = 4UL) {
      // Initialize basic parameters.
      // Works even with n_free_buffers == 0.
      m_bytes_written = 0;
      m_items_per_buf = std::max(1UL, bufsize_per_file_in_bytes / sizeof(value_type));
      for (std::uint64_t j = 0; j < n_free_buffers; ++j)
        m_free_buffers.add(new buffer_type(m_items_per_buf));
      m_io_thread = new std::thread(async_io_thread_code<value_type>, this);
    }

    // The added file gets the next available ID (file IDs start from 0).
    void add_file(std::string filename, std::string write_mode =
        std::string("w")) {
      m_buffers.push_back(new buffer_type(m_items_per_buf));
      m_files.push_back(utils::file_open_nobuf(filename, write_mode));
    }

    // Write value to i-th file.
    void write_to_ith_file(std::uint64_t i, value_type value) {
      m_bytes_written += sizeof(value_type);
      m_buffers[i]->m_content[m_buffers[i]->m_filled++] = value;
      if (m_buffers[i]->full()) {
        issue_write_request(i);
        m_buffers[i] = get_free_buffer();
      }
    }

    // Write values[0..length) to i-th file.
    void write_to_ith_file(std::uint64_t i, const value_type *values, std::uint64_t length) {
      m_bytes_written += length * sizeof(value_type);
      while (length > 0) {
        std::uint64_t towrite = std::min(length, m_items_per_buf - m_buffers[i]->m_filled);
        std::copy(values, values + towrite, m_buffers[i]->m_content + m_buffers[i]->m_filled);
        m_buffers[i]->m_filled += towrite;
        length -= towrite;
        values += towrite;
        if (m_buffers[i]->full()) {
          issue_write_request(i);
          m_buffers[i] = get_free_buffer();
        }
      }
    }

    inline std::uint64_t bytes_written() const {
      return m_bytes_written;
    }

    ~async_multi_stream_writer() {
      // Flush all buffers.
      std::uint64_t n_buffers = m_buffers.size();
      for (std::uint64_t file_id = 0; file_id < n_buffers; ++file_id) {
        if (!(m_buffers[file_id]->empty()))
          issue_write_request(file_id);
      }

      // Let the I/O thread know that there
      // won't be any more requests.
      std::unique_lock<std::mutex> lk(m_write_requests.m_mutex);
      m_write_requests.m_no_more_requests = true;
      lk.unlock();
      m_write_requests.m_cv.notify_one();

      // Wait for the I/O to finish.
      m_io_thread->join();
      delete m_io_thread;

      // Delete buffers and close files.
      for (std::uint64_t file_id = 0; file_id < n_buffers; ++file_id) {
        delete m_buffers[file_id];  // Can be NULL
        std::fclose(m_files[file_id]);
      }

      // Delete free buffers.
      while (!(m_free_buffers.empty())) {
        buffer_type *buf = m_free_buffers.get();
        delete buf;
      }
    }
};

#endif  // __ASYNC_MULTI_STREAM_WRITER_H_INCLUDED
