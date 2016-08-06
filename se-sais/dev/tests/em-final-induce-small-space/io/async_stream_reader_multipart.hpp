#ifndef __ASYNC_STREAM_READER_MULTIPART_HPP_INCLUDED
#define __ASYNC_STREAM_READER_MULTIPART_HPP_INCLUDED

#include <cstdio>
#include <cstdint>
#include <queue>
#include <string>
#include <algorithm>
#include <condition_variable>
#include <mutex>
#include <thread>

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
        m_is_filled = false;
      }

      void read_from_file(std::FILE *f) {
        m_filled = std::fread(m_content, sizeof(T), m_size, f);
      }

      ~buffer() {
        free(m_content);
      }

      inline std::uint64_t size_in_bytes() const { return sizeof(T) * m_filled; }
      inline bool empty() const { return m_filled == 0; }

      T *m_content;
      std::uint64_t m_filled;
      std::uint64_t m_size;
      bool m_is_filled;
    };

    template<typename buffer_type>
    struct request {
      request(buffer_type *buffer) {
        m_buffer = buffer;
      }

      buffer_type *m_buffer;
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

      std::queue<request_type> m_requests;
      std::condition_variable m_cv;
      std::mutex m_mutex;
      bool m_no_more_requests;
    };

  private:
    template<typename T>
    static void async_io_thread_code(async_stream_reader_multipart<T> *caller) {
      typedef buffer<T> buffer_type;
      typedef request<buffer_type> request_type;
      while (true) {
        // Wait for request or until 'no more requests' flag is set.
        std::unique_lock<std::mutex> lk(caller->m_read_requests.m_mutex);
        while (caller->m_read_requests.empty() &&
            !(caller->m_read_requests.m_no_more_requests))
          caller->m_read_requests.m_cv.wait(lk);

        if (caller->m_read_requests.empty() &&
            caller->m_read_requests.m_no_more_requests) {
          // No more requests -- exit.
          lk.unlock();
          break;
        }

        // Extract the buffer from the collection.
        request_type request = caller->m_read_requests.get();
        lk.unlock();

        // Process the request.
        if (caller->m_file == NULL) {
          // Attempt to open and read from the file.
          std::string cur_part_filename = caller->m_filename + ".multipart_file.part" + utils::intToStr(caller->m_cur_part);
          if (utils::file_exists(cur_part_filename)) {
            caller->m_file = utils::file_open(cur_part_filename, "r");
            request.m_buffer->read_from_file(caller->m_file);
          } else request.m_buffer->m_filled = 0;
        } else {
          request.m_buffer->read_from_file(caller->m_file);
          if (request.m_buffer->empty()) {
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
              request.m_buffer->read_from_file(caller->m_file);
            } else request.m_buffer->m_filled = 0;
          }
        }
        caller->m_bytes_read += request.m_buffer->size_in_bytes();

        // Update the status of the buffer
        // and notify the waiting thread.
        std::unique_lock<std::mutex> lk2(caller->m_mutex);
        request.m_buffer->m_is_filled = true;
        lk2.unlock();
        caller->m_cv.notify_one();
      }
    }

  private:
    typedef buffer<value_type> buffer_type;
    typedef request<buffer_type> request_type;

    std::uint64_t m_bytes_read;
    std::uint64_t m_items_per_buf;
    std::uint64_t n_files;
    std::uint64_t m_files_added;

    std::FILE *m_file;
    std::string m_filename;
    std::uint64_t m_cur_part;

    std::uint64_t m_active_buffer_pos;
    buffer_type *m_active_buffer;
    buffer_type *m_passive_buffer;
    std::mutex m_mutex;
    std::condition_variable m_cv;

    request_queue<request_type> m_read_requests;
    std::thread *m_io_thread;

  private:
    void issue_read_request() {
      request_type req(m_passive_buffer);
      m_read_requests.add(req);
      m_read_requests.m_cv.notify_one();
    }

    void receive_new_buffer() {
      // Wait for the I/O thread to finish reading passive buffer.
      std::unique_lock<std::mutex> lk(m_mutex);
      while (m_passive_buffer->m_is_filled == false)
        m_cv.wait(lk);

      // Swap active and bassive buffers.
      std::swap(m_active_buffer, m_passive_buffer);
      m_active_buffer_pos = 0;
      m_passive_buffer->m_is_filled = false;
      lk.unlock();

      // Issue the read request for the passive buffer.
      issue_read_request();
    }

  public:
    async_stream_reader_multipart(std::string filename,
        std::uint64_t total_buf_size_bytes = (8UL << 20),
        std::uint64_t n_buffers = 4UL) {
      (void)n_buffers;

      // Initialize basic parameters.
      m_bytes_read = 0;
      m_items_per_buf = std::max(1UL, total_buf_size_bytes / (2UL * sizeof(value_type)));
      m_active_buffer_pos = 0;
      m_active_buffer = new buffer_type(m_items_per_buf);
      m_passive_buffer = new buffer_type(m_items_per_buf);

      // Start the I/O thread.
      m_io_thread = new std::thread(async_io_thread_code<value_type>, this);

      // Issue the read request.
      m_filename = filename;
      m_file = NULL;
      m_cur_part = 0;
      issue_read_request();
    }

    // Read from i-th file.
    value_type read() {
      if (m_active_buffer_pos == m_active_buffer->m_filled)
        receive_new_buffer();
      return m_active_buffer->m_content[m_active_buffer_pos++];
    }

    inline std::uint64_t bytes_read() const {
      return m_bytes_read;
    }

    ~async_stream_reader_multipart() {
      // Let the I/O thread know that there
      // won't be any more requests.
      std::unique_lock<std::mutex> lk(m_read_requests.m_mutex);
      m_read_requests.m_no_more_requests = true;
      lk.unlock();
      m_read_requests.m_cv.notify_one();

      // Wait for the I/O to finish.
      m_io_thread->join();
      delete m_io_thread;

      // Delete buffers.
      delete m_active_buffer;
      delete m_passive_buffer;
    }
};

#endif  // __ASYNC_MULTI_STREAM_READER_MULTIPART_HPP_INCLUDED
