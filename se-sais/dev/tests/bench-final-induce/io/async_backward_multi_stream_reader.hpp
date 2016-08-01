#ifndef __ASYNC_BACKWARD_MULTI_STREAM_READER_HPP_INCLUDED
#define __ASYNC_BACKWARD_MULTI_STREAM_READER_HPP_INCLUDED

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
class async_backward_multi_stream_reader {
  private:
    template<typename T>
    struct buffer {
      buffer(std::uint64_t size) {
        m_size = size;
        m_content = (T *)malloc(m_size * sizeof(T));
        m_filled = 0;
        m_is_filled = false;
      }

      ~buffer() {
        free(m_content);
      }

      inline std::uint64_t size_in_bytes() const {
        return sizeof(T) * m_filled;
      }

      inline void set_empty() {
        m_filled = 0;
        m_is_filled = false;
      }

      T *m_content;
      std::uint64_t m_filled;
      std::uint64_t m_size;
      bool m_is_filled;
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

      std::queue<request_type> m_requests;
      std::condition_variable m_cv;
      std::mutex m_mutex;
      bool m_no_more_requests;
    };

  private:
    template<typename T>
    static void async_io_thread_code(async_backward_multi_stream_reader<T> *caller) {
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
        std::uint64_t toread = std::min(request.m_buffer->m_size, std::ftell(caller->m_files[request.m_file_id]) / sizeof(T));
        if (toread > 0) {
          std::fseek(caller->m_files[request.m_file_id], -1UL * toread * sizeof(T), SEEK_CUR);
          request.m_buffer->m_filled = toread;
          utils::read_from_file(request.m_buffer->m_content, toread, caller->m_files[request.m_file_id]);
          std::fseek(caller->m_files[request.m_file_id], -1UL * toread * sizeof(T), SEEK_CUR);
          caller->m_bytes_read += request.m_buffer->size_in_bytes();
        }

        // Update the status of the buffer
        // and notify the waiting thread.
        std::unique_lock<std::mutex> lk2(caller->m_mutexes[request.m_file_id]);
        request.m_buffer->m_is_filled = true;
        lk2.unlock();
        caller->m_cvs[request.m_file_id].notify_one();
      }
    }

  private:
    typedef buffer<value_type> buffer_type;
    typedef request<buffer_type> request_type;

    std::uint64_t m_bytes_read;
    std::uint64_t m_items_per_buf;
    std::uint64_t n_files;
    std::uint64_t m_files_added;

    std::FILE **m_files;
    std::uint64_t *m_active_buffer_pos;
    buffer_type **m_active_buffers;
    buffer_type **m_passive_buffers;
    std::mutex *m_mutexes;
    std::condition_variable *m_cvs;

    request_queue<request_type> m_read_requests;
    std::thread *m_io_thread;

    void issue_read_request(std::uint64_t file_id) {
      request_type req(m_passive_buffers[file_id], file_id);
      m_read_requests.add(req);
      m_read_requests.m_cv.notify_one();
    }

    void receive_new_buffer(std::uint64_t file_id) {
      // Wait for the I/O thread to finish reading passive buffer.
      std::unique_lock<std::mutex> lk(m_mutexes[file_id]);
      while (m_passive_buffers[file_id]->m_is_filled == false)
        m_cvs[file_id].wait(lk);

      // Swap active and bassive buffers.
      std::swap(m_active_buffers[file_id], m_passive_buffers[file_id]);
      m_active_buffer_pos[file_id] = m_active_buffers[file_id]->m_filled;
      m_passive_buffers[file_id]->set_empty();
      lk.unlock();

      // Issue the read request for the passive buffer.
      issue_read_request(file_id);
    }

  public:
    async_backward_multi_stream_reader(std::uint64_t number_of_files,
        std::uint64_t bufsize_per_file_in_bytes = (1UL << 20)) {
      // Initialize basic parameters.
      n_files = number_of_files;
      m_files_added = 0;
      m_bytes_read = 0;
      m_items_per_buf = std::max(1UL, bufsize_per_file_in_bytes / (2UL * sizeof(value_type)));

      m_mutexes = new std::mutex[n_files];
      m_cvs = new std::condition_variable[n_files];
      m_active_buffer_pos = new std::uint64_t[n_files];
      m_files = new std::FILE*[n_files];
      m_active_buffers = new buffer_type*[n_files];
      m_passive_buffers = new buffer_type*[n_files];

      for (std::uint64_t i = 0; i < n_files; ++i) {
        m_active_buffer_pos[i] = 0;
        m_active_buffers[i] = new buffer_type(m_items_per_buf);
        m_passive_buffers[i] = new buffer_type(m_items_per_buf);
      }

      m_io_thread = new std::thread(async_io_thread_code<value_type>, this);
    }

    // The added file gets the next available ID (file IDs start from 0).
    void add_file(std::string filename) {
      m_files[m_files_added] = utils::file_open(filename, "r");
      std::fseek(m_files[m_files_added], 0, SEEK_END);
      issue_read_request(m_files_added);
      ++m_files_added;
    }

    // Read from i-th file.
    value_type read_from_ith_file(std::uint64_t i) {
      if (m_active_buffer_pos[i] == 0)
        receive_new_buffer(i);
      return m_active_buffers[i]->m_content[--m_active_buffer_pos[i]];
    }

    inline std::uint64_t bytes_read() const {
      return m_bytes_read;
    }

    ~async_backward_multi_stream_reader() {
      // Let the I/O thread know that there
      // won't be any more requests.
      std::unique_lock<std::mutex> lk(m_read_requests.m_mutex);
      m_read_requests.m_no_more_requests = true;
      lk.unlock();
      m_read_requests.m_cv.notify_one();

      // Wait for the I/O to finish.
      m_io_thread->join();
      delete m_io_thread;

      // Delete buffers and close files.
      for (std::uint64_t i = 0; i < n_files; ++i) {
        std::fclose(m_files[i]);
        delete m_active_buffers[i];
        delete m_passive_buffers[i];
      }

      // Rest of the cleanup.
      delete[] m_active_buffers;
      delete[] m_passive_buffers;
      delete[] m_mutexes;
      delete[] m_cvs;
      delete[] m_active_buffer_pos;
      delete[] m_files;
    }
};

#endif  // __ASYNC_BACKWARD_MULTI_STREAM_READER_HPP_INCLUDED
