#ifndef __EM_QUEUE_HPP_INCLUDED
#define __EM_QUEUE_HPP_INCLUDED

#include <cstdio>
#include <cstdlib>
#include <string>
#include <algorithm>

#include "utils.hpp"


template<typename ValueType>
class em_queue {
  public:
    typedef ValueType value_type;

  private:
    value_type *m_temp_buf;
    value_type *m_head_buf;
    value_type *m_tail_buf;

    std::uint64_t m_items_per_buf;
    std::uint64_t m_head_buf_size;
    std::uint64_t m_head_buf_beg;
    std::uint64_t m_tail_buf_size;
    std::uint64_t m_tail_buf_end;

    std::FILE *m_file;
    std::string m_filename;
    std::uint64_t m_file_size;
    std::uint64_t m_file_head;

    std::uint64_t m_size;
    std::uint64_t m_io_volume;

  public:
    em_queue(std::uint64_t ram_use, std::string filename) {
      m_size = 0;
      m_io_volume = 0;

      // Initialize file.
      m_filename = filename;
      m_file = utils::file_open(m_filename, "a+");
      m_file_size = 0;
      m_file_head = 0;

      // Initialize buffers.
      m_items_per_buf = std::max(1UL, ram_use / (2UL * sizeof(value_type)));
      m_tail_buf = new value_type[m_items_per_buf];
      m_temp_buf = new value_type[m_items_per_buf];
      m_head_buf = m_tail_buf;
      m_head_buf_size = 0;
      m_head_buf_beg = 0;
      m_tail_buf_size = 0;
      m_tail_buf_end = 0;
    }

    inline void push(value_type value) {
      // If the tail buffer is full, make room.
      if (m_tail_buf_size == m_items_per_buf) {
        // Try to bypass I/O.
        if (m_file_head == m_file_size) {
          if (m_head_buf == m_tail_buf) {
            // Easy case, tail buffer becomes the head buffer,
            // temp buffer (currently unused) becomes tail buffer.
            std::swap(m_tail_buf, m_temp_buf);
            m_head_buf = m_temp_buf;
            m_head_buf_size = m_tail_buf_size;
            m_head_buf_beg = m_tail_buf_end;
            m_tail_buf_size = 0;
          } else if (m_head_buf_size < m_items_per_buf) {
            if (m_head_buf_size == 0) {
              // Swap head and tail buffers.
              std::swap(m_head_buf, m_tail_buf);
              std::swap(m_head_buf_size, m_tail_buf_size);
              std::swap(m_head_buf_beg, m_tail_buf_end);
            } else if (2 * m_head_buf_size < m_items_per_buf - m_head_buf_size) {
              // Swap all items from the head buffer with the last
              // m_head_buf_size items of the tail buffer and swap buffers.
              std::uint64_t head_buf_end = m_head_buf_beg + m_head_buf_size;
              if (head_buf_end >= m_items_per_buf)
                head_buf_end -= m_items_per_buf;
              for (std::uint64_t j = 0; j < m_head_buf_size; ++j) {
                if (head_buf_end > 0) --head_buf_end;
                else head_buf_end = m_items_per_buf - 1;
                if (m_tail_buf_end > 0) --m_tail_buf_end;
                else m_tail_buf_end = m_items_per_buf - 1;
                std::swap(m_head_buf[head_buf_end], m_tail_buf[m_tail_buf_end]);
              }
              std::swap(m_head_buf, m_tail_buf);
              std::swap(m_head_buf_size, m_tail_buf_size);
              m_head_buf_beg = m_tail_buf_end;
              m_tail_buf_end = head_buf_end + m_tail_buf_size;
              if (m_tail_buf_end >= m_items_per_buf)
                m_tail_buf_end -= m_items_per_buf;
            } else {
              // Move as many items from tail
              // buffer to the head buffer.
              std::uint64_t tail_buf_beg = m_tail_buf_end;
              std::uint64_t head_buf_end = m_head_buf_beg + m_head_buf_size;
              if (head_buf_end >= m_items_per_buf)
                head_buf_end -= m_items_per_buf;
              value_type *tail_buf_beg_ptr = m_tail_buf + tail_buf_beg;
              while (m_head_buf_size < m_items_per_buf) {
                std::uint64_t tomove = m_items_per_buf - std::max(tail_buf_beg, std::max(head_buf_end, m_head_buf_size));
                std::copy(tail_buf_beg_ptr, tail_buf_beg_ptr + tomove, m_head_buf + head_buf_end);
                m_head_buf_size += tomove;
                head_buf_end += tomove;
                if (head_buf_end == m_items_per_buf)
                  head_buf_end = 0;
                m_tail_buf_size -= tomove;
                tail_buf_beg += tomove;
                tail_buf_beg_ptr += tomove;
                if (tail_buf_beg == m_items_per_buf) {
                  tail_buf_beg = 0;
                  tail_buf_beg_ptr = m_tail_buf;
                }
              }
            }
          } else {
            // Flush tail buffer to file.
            std::uint64_t tail_buf_beg = m_tail_buf_end;
            while (m_tail_buf_size > 0) {
              std::uint64_t towrite = std::min(m_items_per_buf - tail_buf_beg, m_tail_buf_size);
              utils::write_to_file(m_tail_buf + tail_buf_beg, towrite, m_file);
              m_io_volume += towrite * sizeof(value_type);
              m_file_size += towrite;
              m_tail_buf_size -= towrite;
              tail_buf_beg += towrite;
              if (tail_buf_beg == m_items_per_buf)
                tail_buf_beg = 0;
            }
          }
        } else {
          // Flush tail buffer to file.
          std::uint64_t tail_buf_beg = m_tail_buf_end;
          while (m_tail_buf_size > 0) {
            std::uint64_t towrite = std::min(m_items_per_buf - tail_buf_beg, m_tail_buf_size);
            utils::write_to_file(m_tail_buf + tail_buf_beg, towrite, m_file);
            m_io_volume += towrite * sizeof(value_type);
            m_file_size += towrite;
            m_tail_buf_size -= towrite;
            tail_buf_beg += towrite;
            if (tail_buf_beg == m_items_per_buf)
              tail_buf_beg = 0;
          }
        }
      }

      // Update total number of items.
      ++m_size;

      // Add item to tail buffer.
      m_tail_buf[m_tail_buf_end++] = value;
      if (m_tail_buf_end == m_items_per_buf)
        m_tail_buf_end = 0;

      // Update the size of tail buffer (and maybe also head buffer).
      ++m_tail_buf_size;
      if (m_head_buf == m_tail_buf)
        ++m_head_buf_size;
    }

    inline value_type& front() {
      if (m_head_buf_size == 0) {
        // Check where to look for the next item.
        if (m_file_head == m_file_size) {
          // The next item is in RAM, combine two buffer into one.
          m_temp_buf = m_head_buf;
          m_head_buf = m_tail_buf;
          m_head_buf_size = m_tail_buf_size;
          m_head_buf_beg = m_tail_buf_end + (m_items_per_buf - m_tail_buf_size);
          if (m_head_buf_beg >= m_items_per_buf)
            m_head_buf_beg -= m_items_per_buf;
        } else {
          // The next item is on disk, refill head buffer.
          m_head_buf_size = std::min(m_file_size - m_file_head, m_items_per_buf);
          std::fseek(m_file, m_file_head * sizeof(value_type), SEEK_SET);
          utils::read_from_file(m_head_buf, m_head_buf_size, m_file);
          m_io_volume += m_head_buf_size * sizeof(value_type);
          m_file_head += m_head_buf_size;
          m_head_buf_beg = 0;
        }
      }

      return m_head_buf[m_head_buf_beg];
    }

    inline void pop() {
      (void) front();
      --m_size;
      --m_head_buf_size;
      ++m_head_buf_beg;
      if (m_head_buf_beg == m_items_per_buf)
        m_head_buf_beg = 0;
      if (m_head_buf == m_tail_buf)
        --m_tail_buf_size;
    }

    inline bool empty() const {
      return m_size == 0;
    }

    inline std::uint64_t size() const {
      return m_size;
    }

    inline std::uint64_t io_volume() const {
      return m_io_volume;
    }

    void reset_file() {
      std::fclose(m_file);
      utils::file_delete(m_filename);
      m_file = utils::file_open(m_filename, "a+");
      m_file_size = 0;
      m_file_head = 0;
    }

    ~em_queue() {
      std::fclose(m_file);
      if (utils::file_exists(m_filename))
        utils::file_delete(m_filename);
      if (m_head_buf == m_tail_buf) {
        delete[] m_head_buf;
        delete[] m_temp_buf;
      } else {
        delete[] m_head_buf;
        delete[] m_tail_buf;
      }
    }
};

#endif  // __EM_QUEUE_HPP_INCLUDED
