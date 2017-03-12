/**
 * @file    fsais_src/io/async_multi_stream_reader_multipart.hpp
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

#ifndef __FSAIS_SRC_IO_ASYNC_MULTI_STREAM_READER_MULTIPART_HPP_INCLUDED
#define __FSAIS_SRC_IO_ASYNC_MULTI_STREAM_READER_MULTIPART_HPP_INCLUDED

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
class async_multi_stream_reader_multipart {
  private:
    template<typename T>
    struct buffer {
      buffer(std::uint64_t size, T* const mem)
        : m_content(mem), m_size(size) {
        m_filled = 0;
        m_is_filled = false;
      }

      void read_from_file(std::FILE *f) {
        m_filled = std::fread(m_content, sizeof(T), m_size, f);
      }

      inline std::uint64_t size_in_bytes() const { return sizeof(T) * m_filled; }
      inline bool empty() const { return m_filled == 0; }

      T* const m_content;
      const std::uint64_t m_size;

      std::uint64_t m_filled;
      bool m_is_filled;
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

    template<typename buffer_type>
    struct request {
      request() {}
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

      circular_queue<request_type> m_requests;  // Must have FIFO property
      std::condition_variable m_cv;
      std::mutex m_mutex;
      bool m_no_more_requests;
    };

  private:
    template<typename T>
    static void async_io_thread_code(async_multi_stream_reader_multipart<T> *caller) {
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
        if (caller->m_files[request.m_file_id] == NULL) {
          // Attempt to open and read from the file.
          std::string cur_part_filename = caller->m_filenames[request.m_file_id] +
            ".multipart_file.part" + utils::intToStr(caller->m_cur_part[request.m_file_id]);
          if (utils::file_exists(cur_part_filename)) {
            caller->m_files[request.m_file_id] = utils::file_open(cur_part_filename, "r");
            request.m_buffer->read_from_file(caller->m_files[request.m_file_id]);
          } else request.m_buffer->m_filled = 0;
        } else {
          request.m_buffer->read_from_file(caller->m_files[request.m_file_id]);
          if (request.m_buffer->empty()) {
            // Close and delete current file.
            std::fclose(caller->m_files[request.m_file_id]);
            caller->m_files[request.m_file_id] = NULL;
            std::string cur_part_filename = caller->m_filenames[request.m_file_id] +
              ".multipart_file.part" + utils::intToStr(caller->m_cur_part[request.m_file_id]);
            utils::file_delete(cur_part_filename);

            // Attempt to read from the next file.
            ++caller->m_cur_part[request.m_file_id];
            cur_part_filename = caller->m_filenames[request.m_file_id] +
              ".multipart_file.part" + utils::intToStr(caller->m_cur_part[request.m_file_id]);
            if (utils::file_exists(cur_part_filename)) {
              caller->m_files[request.m_file_id] = utils::file_open(cur_part_filename, "r");
              request.m_buffer->read_from_file(caller->m_files[request.m_file_id]);
            } else request.m_buffer->m_filled = 0;
          }
        }
        caller->m_bytes_read += request.m_buffer->size_in_bytes();

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
    std::string *m_filenames;
    std::uint64_t *m_cur_part;

    std::uint64_t *m_active_buffer_pos;
    value_type *m_mem;
    buffer_type **m_active_buffers;
    buffer_type **m_passive_buffers;
    std::mutex *m_mutexes;
    std::condition_variable *m_cvs;

    request_queue<request_type> m_read_requests;
    std::thread *m_io_thread;

  private:
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
      m_active_buffer_pos[file_id] = 0;
      m_passive_buffers[file_id]->m_is_filled = false;
      lk.unlock();

      // Issue the read request for the passive buffer.
      issue_read_request(file_id);
    }

  public:
    async_multi_stream_reader_multipart(std::uint64_t number_of_files,
        std::uint64_t buf_size_bytes = (std::uint64_t)(1 << 19)) {
      if (number_of_files == 0) {
        fprintf(stderr, "\nError in async_multi_stream_reader_multipart: number_of_files == 0\n");
        std::exit(EXIT_FAILURE);
      }

      // Initialize basic parameters.
      n_files = number_of_files;
      m_files_added = 0;
      m_bytes_read = 0;
      m_items_per_buf = std::max(1UL, buf_size_bytes / (2UL * sizeof(value_type)));

      m_mutexes = new std::mutex[n_files];
      m_cvs = new std::condition_variable[n_files];
      m_active_buffer_pos = new std::uint64_t[n_files];
      m_files = new std::FILE*[n_files];
      m_filenames = new std::string[n_files];
      m_cur_part = new std::uint64_t[n_files];
      m_active_buffers = new buffer_type*[n_files];
      m_passive_buffers = new buffer_type*[n_files];

      m_mem = utils::allocate_array<value_type>(2UL * n_files * m_items_per_buf);
      {
        value_type *mem = m_mem;
        for (std::uint64_t i = 0; i < n_files; ++i) {
          m_active_buffer_pos[i] = 0;
          m_active_buffers[i] = new buffer_type(m_items_per_buf, mem);
          mem += m_items_per_buf;
          m_passive_buffers[i] = new buffer_type(m_items_per_buf, mem);
          mem += m_items_per_buf;
        }
      }

      m_io_thread = new std::thread(async_io_thread_code<value_type>, this);
    }

    // The added file gets the next available ID (file IDs start from 0).
    void add_file(std::string filename) {
      m_filenames[m_files_added] = filename;
      m_files[m_files_added] = NULL;
      m_cur_part[m_files_added] = 0;
      issue_read_request(m_files_added);
      ++m_files_added;
    }

    // Read from i-th file.
    value_type read_from_ith_file(std::uint64_t i) {
      if (m_active_buffer_pos[i] == m_active_buffers[i]->m_filled)
        receive_new_buffer(i);
      return m_active_buffers[i]->m_content[m_active_buffer_pos[i]++];
    }

    // Return performed I/O in bytes.
    inline std::uint64_t bytes_read() const {
      return m_bytes_read;
    }

    // Destructor.
    ~async_multi_stream_reader_multipart() {
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
      for (std::uint64_t i = 0; i < n_files; ++i) {
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
      delete[] m_filenames;
      delete[] m_cur_part;
      utils::deallocate(m_mem);
    }
};

}  // namespace fsais_private

#endif  // __FSAIS_SRC_IO_ASYNC_MULTI_STREAM_READER_MULTIPART_HPP_INCLUDED
