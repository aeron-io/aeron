/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>

#if defined(_MSC_VER)
#include <io.h>
#include <windows.h>
#define open _open
#define close _close
#define ftruncate(fd, size) _chsize_s(fd, size)
#define O_RDWR _O_RDWR
#define O_CREAT _O_CREAT
#define S_IRUSR _S_IREAD
#define S_IWUSR _S_IWRITE
#define S_IRGRP 0
#define S_IROTH 0

static int aeron_archive_pwrite(int fd, const void *buf, size_t count, int64_t offset)
{
    if (_lseeki64(fd, offset, SEEK_SET) < 0) { return -1; }
    return _write(fd, buf, (unsigned int)count);
}
#define pwrite(fd, buf, count, offset) aeron_archive_pwrite(fd, buf, count, offset)
#define fsync(fd) _commit(fd)
#define fdatasync(fd) _commit(fd)
typedef int ssize_t;
#else
#include <unistd.h>
#endif

#include <sys/stat.h>

#include "aeron_archive_recording_writer.h"
#include "aeron_alloc.h"
#include "aeron_common.h"
#include "util/aeron_error.h"
#include "protocol/aeron_udp_protocol.h"
#include "util/aeron_bitutil.h"

#define AERON_ARCHIVE_SEGMENT_FILE_NAME_MAX_LENGTH (128)

static int64_t aeron_archive_segment_file_base_position(
    int64_t start_position, int64_t position, int32_t term_length, int32_t segment_length)
{
    const int64_t start_term_base_position = start_position - (start_position & (term_length - 1));
    const int64_t length_from_base = position - start_term_base_position;
    const int64_t segments = length_from_base - (length_from_base & (segment_length - 1));

    return start_term_base_position + segments;
}

static void aeron_archive_segment_file_name(
    char *dst, size_t dst_len, int64_t recording_id, int64_t segment_base_position)
{
    snprintf(dst, dst_len, "%" PRId64 "-%" PRId64 "%s",
        recording_id, segment_base_position, AERON_ARCHIVE_RECORDING_SEGMENT_SUFFIX);
}

static int aeron_archive_recording_writer_open_segment_file(aeron_archive_recording_writer_t *writer)
{
    char segment_file_name[AERON_ARCHIVE_SEGMENT_FILE_NAME_MAX_LENGTH];
    char segment_file_path[AERON_MAX_PATH];

    aeron_archive_segment_file_name(
        segment_file_name, sizeof(segment_file_name),
        writer->recording_id, writer->segment_base_position);

    snprintf(segment_file_path, sizeof(segment_file_path), "%s/%s", writer->archive_dir, segment_file_name);

    int fd = open(segment_file_path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    if (fd < 0)
    {
        AERON_SET_ERR(errno, "failed to open segment file: %s", segment_file_path);
        return -1;
    }

    if (ftruncate(fd, writer->segment_length) < 0)
    {
        AERON_SET_ERR(errno, "failed to set segment file length: %s", segment_file_path);
        close(fd);
        return -1;
    }

    writer->segment_fd = fd;

    if (writer->force_writes)
    {
        if (writer->force_metadata)
        {
            fsync(fd);
        }
        else
        {
            fdatasync(fd);
        }
    }

    return 0;
}

static void aeron_archive_recording_writer_close_segment_file(aeron_archive_recording_writer_t *writer)
{
    if (writer->segment_fd >= 0)
    {
        close(writer->segment_fd);
        writer->segment_fd = -1;
    }
}

static int aeron_archive_recording_writer_on_file_roll_over(aeron_archive_recording_writer_t *writer)
{
    aeron_archive_recording_writer_close_segment_file(writer);

    writer->segment_offset = 0;
    writer->segment_base_position += writer->segment_length;

    char segment_file_name[AERON_ARCHIVE_SEGMENT_FILE_NAME_MAX_LENGTH];
    char segment_file_path[AERON_MAX_PATH];

    aeron_archive_segment_file_name(
        segment_file_name, sizeof(segment_file_name),
        writer->recording_id, writer->segment_base_position);

    snprintf(segment_file_path, sizeof(segment_file_path), "%s/%s", writer->archive_dir, segment_file_name);

    struct stat st;
    if (stat(segment_file_path, &st) == 0)
    {
        AERON_SET_ERR(EEXIST, "segment file already exists: %s", segment_file_path);
        return -1;
    }

    return aeron_archive_recording_writer_open_segment_file(writer);
}

int aeron_archive_recording_writer_create(
    aeron_archive_recording_writer_t **writer,
    int64_t recording_id,
    int64_t start_position,
    int64_t join_position,
    int32_t term_length,
    int32_t segment_length,
    const char *archive_dir,
    bool force_writes,
    bool force_metadata)
{
    if (NULL == writer)
    {
        AERON_SET_ERR(EINVAL, "%s", "writer is NULL");
        return -1;
    }

    aeron_archive_recording_writer_t *_writer = NULL;
    if (aeron_alloc((void **)&_writer, sizeof(aeron_archive_recording_writer_t)) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate recording writer");
        return -1;
    }

    const size_t dir_len = strlen(archive_dir);
    if (aeron_alloc((void **)&_writer->archive_dir, dir_len + 1) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate archive_dir");
        aeron_free(_writer);
        return -1;
    }
    memcpy(_writer->archive_dir, archive_dir, dir_len + 1);

    _writer->recording_id = recording_id;
    _writer->segment_length = segment_length;
    _writer->force_writes = force_writes;
    _writer->force_metadata = force_metadata;
    _writer->is_closed = false;
    _writer->segment_fd = -1;

    _writer->segment_base_position =
        aeron_archive_segment_file_base_position(start_position, join_position, term_length, segment_length);
    _writer->segment_offset = (int32_t)(join_position - _writer->segment_base_position);

    *writer = _writer;
    return 0;
}

int aeron_archive_recording_writer_init(aeron_archive_recording_writer_t *writer)
{
    if (NULL == writer)
    {
        AERON_SET_ERR(EINVAL, "%s", "writer is NULL");
        return -1;
    }

    return aeron_archive_recording_writer_open_segment_file(writer);
}

int aeron_archive_recording_writer_write(
    aeron_archive_recording_writer_t *writer,
    const uint8_t *buffer,
    size_t length,
    bool is_padding_frame)
{
    if (NULL == writer || writer->is_closed)
    {
        AERON_SET_ERR(EINVAL, "%s", "writer is NULL or closed");
        return -1;
    }

    const size_t data_length = is_padding_frame ? AERON_DATA_HEADER_LENGTH : length;

    ssize_t total_written = 0;
    int32_t file_offset = writer->segment_offset;

    while ((size_t)total_written < data_length)
    {
        ssize_t written = pwrite(
            writer->segment_fd,
            buffer + total_written,
            data_length - (size_t)total_written,
            file_offset + total_written);

        if (written < 0)
        {
            AERON_SET_ERR(errno, "failed to write to segment file at offset %d", file_offset);
            writer->is_closed = true;
            aeron_archive_recording_writer_close_segment_file(writer);
            return -1;
        }

        total_written += written;
    }

    if (writer->force_writes)
    {
        if (writer->force_metadata)
        {
            fsync(writer->segment_fd);
        }
        else
        {
            fdatasync(writer->segment_fd);
        }
    }

    writer->segment_offset += (int32_t)length;

    if (writer->segment_offset >= writer->segment_length)
    {
        if (aeron_archive_recording_writer_on_file_roll_over(writer) < 0)
        {
            writer->is_closed = true;
            return -1;
        }
    }

    return 0;
}

int64_t aeron_archive_recording_writer_position(const aeron_archive_recording_writer_t *writer)
{
    if (NULL == writer)
    {
        return -1;
    }

    return writer->segment_base_position + writer->segment_offset;
}

int aeron_archive_recording_writer_close(aeron_archive_recording_writer_t *writer)
{
    if (NULL == writer)
    {
        return 0;
    }

    if (!writer->is_closed)
    {
        writer->is_closed = true;
        aeron_archive_recording_writer_close_segment_file(writer);
    }

    aeron_free(writer->archive_dir);
    aeron_free(writer);

    return 0;
}
