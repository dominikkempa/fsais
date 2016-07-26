#ifndef __ASYNC_VBYTE_STREAM_READER_HPP_INCLUDED
#define __ASYNC_VBYTE_STREAM_READER_HPP_INCLUDED

#include <cstdio>
#include <cstdint>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <algorithm>

#include "../utils.hpp"


template<typename value_type>
class async_vbyte_stream_reader {
  private:
    static void io_thread_code(async_vbyte_stream_reader *reader) {
      while (true) {
        // Wait until the passive buffer is available.
        std::unique_lock<std::mutex> lk(reader->m_mutex);
        while (!(reader->m_avail) && !(reader->m_finished))
          reader->m_cv.wait(lk);

        if (!(reader->m_avail) && (reader->m_finished)) {
          // We're done, terminate the thread.
          lk.unlock();
          return;
        }
        lk.unlock();

        // Safely read the data from disk.
        std::uint64_t count = std::fread(reader->m_passive_buf, 1, reader->m_buf_size + 128, reader->m_file);
        if (count > reader->m_buf_size) {
          reader->m_passive_buf_filled = reader->m_buf_size;
          std::fseek(reader->m_file, -(count - reader->m_buf_size), SEEK_CUR);
        } else reader->m_passive_buf_filled = count;

        // Let the caller know that the I/O thread finished reading.
        lk.lock();
        reader->m_avail = false;
        lk.unlock();
        reader->m_cv.notify_one();
      }
    }

    // Check if the reading thread has already prefetched the next
    // buffer (the request should have been issued before), and wait
    // in case the prefetching was not completed yet.
    void receive_new_buffer(std::uint64_t skipped_bytes) {
      // Wait until the I/O thread finishes reading the previous
      // buffer. In most cases, this step is instantaneous.
      std::unique_lock<std::mutex> lk(m_mutex);
      while (m_avail == true)
        m_cv.wait(lk);

      // Set the new active buffer.
      std::swap(m_active_buf, m_passive_buf);
      m_active_buf_filled = m_passive_buf_filled;
      m_active_buf_pos = skipped_bytes;

      // Let the I/O thread know that it can now prefetch
      // another buffer.
      m_avail = true;
      lk.unlock();
      m_cv.notify_one();
    }

  public:
    async_vbyte_stream_reader(std::string filename = std::string(""),
        std::uint64_t pos = 0UL,
        std::uint64_t bufsize = (4UL << 20)) {
      if (filename.empty()) m_file = stdin;
      else {
        m_file = utils::file_open(filename.c_str(), "r");
        if (pos != 0)
          std::fseek(m_file, pos, SEEK_SET);
      }

      // Initialize buffers.
      m_buf_size = std::max(2048UL, bufsize / 2);
      m_bytes_read = 0;
      m_active_buf_filled = 0;
      m_passive_buf_filled = 0;
      m_active_buf_pos = 0;
      m_active_buf = (std::uint8_t *)malloc(m_buf_size + 128);
      m_passive_buf = (std::uint8_t *)malloc(m_buf_size + 128);

      // Start the I/O thread and immediately start reading.
      m_finished = false;
      m_avail = true;
      m_thread = new std::thread(io_thread_code, this);
    }

    ~async_vbyte_stream_reader() {
      // Let the I/O thread know that we're done.
      std::unique_lock<std::mutex> lk(m_mutex);
      m_finished = true;
      lk.unlock();
      m_cv.notify_one();

      // Wait for the thread to finish.
      m_thread->join();

      // Clean up.
      delete m_thread;
      free(m_active_buf);
      free(m_passive_buf);
      if (m_file != stdin)
        std::fclose(m_file);
    }

    inline value_type read() {
      if (m_active_buf_pos >= m_active_buf_filled) {
        // The active buffer run out of data.
        // At this point we need to swap it with the passive
        // buffer. The request to read that passive buffer should
        // have been scheduled long time ago, so hopefully the
        // buffer is now available. We check for that, but we
        // also might wait, if the reading has not yet been finished.
        // At this point we also already schedule the next read.
        receive_new_buffer(m_active_buf_pos - m_active_buf_filled);
      }

      value_type result = 0;
      std::uint64_t offset = 0;
      while (m_active_buf[m_active_buf_pos] & 0x80) {
        result |= (((value_type)m_active_buf[m_active_buf_pos++] & 0x7F) << offset);
        offset += 7;
        ++m_bytes_read;
      }
      result |= ((value_type)m_active_buf[m_active_buf_pos++] << offset);
      ++m_bytes_read;

      return result;
    }

    inline bool empty() {
      if (m_active_buf_pos >= m_active_buf_filled)
        receive_new_buffer(m_active_buf_pos - m_active_buf_filled);

      return (m_active_buf_pos >= m_active_buf_filled);
    }

    inline std::uint64_t bytes_read() const {
      return m_bytes_read;
    }

  private:
    std::uint8_t *m_active_buf;
    std::uint8_t *m_passive_buf;

    std::uint64_t m_buf_size;
    std::uint64_t m_active_buf_pos;
    std::uint64_t m_active_buf_filled;
    std::uint64_t m_passive_buf_filled;
    std::uint64_t m_bytes_read;

    // Used for synchronization with the I/O thread.
    std::mutex m_mutex;
    std::condition_variable m_cv;
    bool m_avail;
    bool m_finished;

    std::FILE *m_file;
    std::thread *m_thread;
};

#endif  // __ASYNC_VBYTE_STREAM_READER_HPP_INCLUDED
