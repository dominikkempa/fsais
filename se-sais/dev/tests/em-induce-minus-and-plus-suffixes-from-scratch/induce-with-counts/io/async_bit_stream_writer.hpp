#ifndef __ASYNC_BIT_STREAM_WRITER_HPP_INCLUDED
#define __ASYNC_BIT_STREAM_WRITER_HPP_INCLUDED

#include <cstdint>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <algorithm>

#include "../utils.hpp"


class async_bit_stream_writer {
  private:
    static void io_thread_code(async_bit_stream_writer *writer) {
      while (true) {
        // Wait until the passive buffer is available.
        std::unique_lock<std::mutex> lk(writer->m_mutex);
        while (!(writer->m_avail) && !(writer->m_finished))
          writer->m_cv.wait(lk);

        if (!(writer->m_avail) && (writer->m_finished)) {
          // We're done, terminate the thread.
          lk.unlock();
          return;
        }
        lk.unlock();

        // Safely write the data to disk.
        utils::write_to_file(writer->m_passive_buf,
            writer->m_passive_buf_filled, writer->m_file);

        // Let the caller know that the I/O thread finished writing.
        lk.lock();
        writer->m_avail = false;
        lk.unlock();
        writer->m_cv.notify_one();
      }
    }

    // Passes on the active buffer (full, unless it's the last one,
    // partially filled, buffer passed from destructor) to the I/O thread.
    void send_active_buf_to_write() {
      // Wait until the I/O thread finishes writing the previous buffer.
      std::unique_lock<std::mutex> lk(m_mutex);
      while (m_avail == true)
        m_cv.wait(lk);

      // Set the new passive buffer.
      std::swap(m_active_buf, m_passive_buf);
      m_passive_buf_filled = m_active_buf_filled;
      m_active_buf_filled = 0;
      m_bit_pos = 0;
      m_active_buf[0] = 0;

      // Let the I/O thread know that the buffer is waiting.
      m_avail = true;
      lk.unlock();
      m_cv.notify_one();
    }

  public:
    async_bit_stream_writer(std::string filename, std::uint64_t bufsize = (4UL << 20)) {
      m_file = utils::file_open(filename.c_str(), "w");

      // Initialize buffers.
      m_buf_size = std::max(1UL, bufsize / (2UL * sizeof(std::uint64_t)));
      m_active_buf = (std::uint64_t *)malloc(m_buf_size * sizeof(std::uint64_t));
      m_passive_buf = (std::uint64_t *)malloc(m_buf_size * sizeof(std::uint64_t));

      m_active_buf[0] = 0;
      m_bit_pos = 0;
      m_active_buf_filled = 0;
      m_passive_buf_filled = 0;
      m_bits_written = 0;

      // Start the I/O thread.
      m_avail = false;
      m_finished = false;
      m_thread = new std::thread(io_thread_code, this);
    }

    ~async_bit_stream_writer() {
      // Write the partially filled active buffer to disk.
      std::uint64_t m_bit_pos_backup = m_bit_pos;
      if (m_bit_pos != 0) ++m_active_buf_filled;
      if (m_active_buf_filled > 0L)
        send_active_buf_to_write();

      // Let the I/O thread know that we're done.
      std::unique_lock<std::mutex> lk(m_mutex);
      m_finished = true;
      lk.unlock();
      m_cv.notify_one();

      // Wait for the thread to finish.
      m_thread->join();

      // Append the number of bits in the last 64-bit word to file.
      utils::write_to_file(&m_bit_pos_backup, 1, m_file);

      // Clean up.
      delete m_thread;
      free(m_active_buf);
      free(m_passive_buf);
      std::fclose(m_file);
    }

    inline void write(std::uint8_t bit) {
      ++m_bits_written;
      m_active_buf[m_active_buf_filled] |= ((std::uint64_t)bit << m_bit_pos);
      ++m_bit_pos;
      if (m_bit_pos == 64) {
        m_bit_pos = 0;
        ++m_active_buf_filled;

        // If the active buffer was full, send it to I/O thread.
        // This function may wait a bit until the I/O thread
        // finishes writing the previous passive buffer.
        if (m_active_buf_filled == m_buf_size)
          send_active_buf_to_write();

        // Clear all bits in the current byte.
        m_active_buf[m_active_buf_filled] = 0;
      }
    }

    std::uint64_t bytes_written() const {
      return (m_bits_written + 7) / 8;
    }

  private:
    std::uint64_t *m_active_buf;
    std::uint64_t *m_passive_buf;

    std::uint64_t m_buf_size;
    std::uint64_t m_bit_pos;
    std::uint64_t m_active_buf_filled;
    std::uint64_t m_passive_buf_filled;
    std::uint64_t m_bits_written;

    // Used for synchronization with the I/O thread.
    bool m_avail;
    bool m_finished;
    std::mutex m_mutex;
    std::condition_variable m_cv;

    std::FILE *m_file;
    std::thread *m_thread;
};

#endif  // __ASYNC_STREAM_WRITER_HPP_INCLUDED
