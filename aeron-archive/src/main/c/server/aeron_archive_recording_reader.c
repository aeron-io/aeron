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
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#define open _open
#define close _close
#define O_RDONLY _O_RDONLY
#else
#include <unistd.h>
#include <sys/mman.h>
#endif

#include <sys/stat.h>

#include "aeron_archive_recording_reader.h"
#include "aeron_archive_recording_writer.h"
#include "aeron_alloc.h"
#include "aeron_common.h"
#include "util/aeron_error.h"
#include "util/aeron_bitutil.h"
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_logbuffer_descriptor.h"

#define AERON_ARCHIVE_SEGMENT_FILE_NAME_MAX_LENGTH (128)

static void aeron_archive_reader_segment_file_name(
    char *dst, size_t dst_len, int64_t recording_id, int64_t segment_base_position)
{
    snprintf(dst, dst_len, "%" PRId64 "-%" PRId64 "%s",
        recording_id, segment_base_position, AERON_ARCHIVE_RECORDING_SEGMENT_SUFFIX);
}

static int64_t aeron_archive_reader_segment_file_base_position(
    int64_t start_position, int64_t position, int32_t term_length, int32_t segment_length)
{
    const int64_t start_term_base_position = start_position - (start_position & (term_length - 1));
    const int64_t length_from_base = position - start_term_base_position;
    const int64_t segments = length_from_base - (length_from_base & (segment_length - 1));

    return start_term_base_position + segments;
}

static void aeron_archive_recording_reader_close_segment(aeron_archive_recording_reader_t *reader)
{
    if (NULL != reader->mapped_segment)
    {
#if defined(_MSC_VER)
        UnmapViewOfFile(reader->mapped_segment);
#else
        munmap(reader->mapped_segment, reader->mapped_segment_size);
#endif
        reader->mapped_segment = NULL;
        reader->mapped_segment_size = 0;
    }
}

static int aeron_archive_recording_reader_open_segment(aeron_archive_recording_reader_t *reader)
{
    char segment_file_name[AERON_ARCHIVE_SEGMENT_FILE_NAME_MAX_LENGTH];
    char segment_file_path[AERON_MAX_PATH];

    aeron_archive_reader_segment_file_name(
        segment_file_name, sizeof(segment_file_name),
        reader->recording_id, reader->segment_file_position);

    snprintf(segment_file_path, sizeof(segment_file_path), "%s/%s", reader->archive_dir, segment_file_name);

    int fd = open(segment_file_path, O_RDONLY);
    if (fd < 0)
    {
        AERON_SET_ERR(errno, "failed to open recording segment file: %s", segment_file_path);
        return -1;
    }

#if defined(_MSC_VER)
    HANDLE hmap = CreateFileMapping((HANDLE)_get_osfhandle(fd), NULL, PAGE_READONLY, 0,
        (DWORD)reader->segment_length, NULL);
    if (NULL == hmap)
    {
        AERON_SET_ERR(GetLastError(), "CreateFileMapping recording segment file: %s", segment_file_path);
        close(fd);
        return -1;
    }
    void *mapped = MapViewOfFile(hmap, FILE_MAP_READ, 0, 0, (size_t)reader->segment_length);
    CloseHandle(hmap);
    close(fd);
    if (NULL == mapped)
    {
        AERON_SET_ERR(GetLastError(), "MapViewOfFile recording segment file: %s", segment_file_path);
        return -1;
    }
#else
    void *mapped = mmap(NULL, (size_t)reader->segment_length, PROT_READ, MAP_PRIVATE, fd, 0);
    close(fd);

    if (MAP_FAILED == mapped)
    {
        AERON_SET_ERR(errno, "failed to mmap recording segment file: %s", segment_file_path);
        return -1;
    }
#endif

    reader->mapped_segment = (uint8_t *)mapped;
    reader->mapped_segment_size = (size_t)reader->segment_length;

    return 0;
}

int aeron_archive_recording_reader_create(
    aeron_archive_recording_reader_t **reader,
    int64_t recording_id,
    int64_t start_position,
    int64_t stop_position,
    int64_t from_position,
    int64_t length,
    int32_t term_length,
    int32_t segment_length,
    int32_t initial_term_id,
    int32_t stream_id,
    const char *archive_dir)
{
    if (NULL == reader)
    {
        AERON_SET_ERR(EINVAL, "%s", "reader is NULL");
        return -1;
    }

    if (from_position < AERON_ARCHIVE_NULL_POSITION)
    {
        AERON_SET_ERR(EINVAL, "invalid position: %" PRId64, from_position);
        return -1;
    }

    if (length < AERON_ARCHIVE_NULL_LENGTH)
    {
        AERON_SET_ERR(EINVAL, "invalid length: %" PRId64, length);
        return -1;
    }

    aeron_archive_recording_reader_t *_reader = NULL;
    if (aeron_alloc((void **)&_reader, sizeof(aeron_archive_recording_reader_t)) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate recording reader");
        return -1;
    }

    const size_t dir_len = strlen(archive_dir);
    if (aeron_alloc((void **)&_reader->archive_dir, dir_len + 1) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate archive_dir");
        aeron_free(_reader);
        return -1;
    }
    memcpy(_reader->archive_dir, archive_dir, dir_len + 1);

    _reader->recording_id = recording_id;
    _reader->term_length = term_length;
    _reader->segment_length = segment_length;
    _reader->is_done = false;
    _reader->mapped_segment = NULL;
    _reader->mapped_segment_size = 0;

    const int64_t actual_from = (from_position == AERON_ARCHIVE_NULL_POSITION) ? start_position : from_position;

    const int64_t max_length = (stop_position != AERON_ARCHIVE_NULL_POSITION)
        ? (stop_position - actual_from)
        : (INT64_MAX - actual_from);

    const int64_t replay_length = (length == AERON_ARCHIVE_NULL_LENGTH) ? max_length : (length < max_length ? length : max_length);

    if (replay_length < 0)
    {
        AERON_SET_ERR(EINVAL, "%s", "replay length must be positive");
        aeron_free(_reader->archive_dir);
        aeron_free(_reader);
        return -1;
    }

    const int64_t start_term_base_position = start_position - (start_position & (term_length - 1));
    const int32_t segment_offset =
        (int32_t)((actual_from - start_term_base_position) & (segment_length - 1));
    const int32_t position_bits_to_shift = aeron_number_of_trailing_zeroes((int32_t)term_length);
    const int32_t term_id = ((int32_t)(actual_from >> position_bits_to_shift)) + initial_term_id;

    _reader->segment_file_position =
        aeron_archive_reader_segment_file_base_position(start_position, actual_from, term_length, segment_length);

    if (aeron_archive_recording_reader_open_segment(_reader) < 0)
    {
        aeron_free(_reader->archive_dir);
        aeron_free(_reader);
        return -1;
    }

    _reader->term_offset = (int32_t)(actual_from & (term_length - 1));
    _reader->term_base_segment_offset = segment_offset - _reader->term_offset;

    /* Validate that the from_position aligns with a valid fragment. */
    if (actual_from > start_position)
    {
        const uint8_t *term_base = _reader->mapped_segment + _reader->term_base_segment_offset;
        const aeron_data_header_t *hdr =
            (const aeron_data_header_t *)(term_base + _reader->term_offset);

        if (hdr->term_offset != _reader->term_offset ||
            hdr->term_id != term_id ||
            hdr->stream_id != stream_id)
        {
            aeron_archive_recording_reader_close_segment(_reader);
            AERON_SET_ERR(EINVAL, "position %" PRId64 " not aligned to valid fragment", actual_from);
            aeron_free(_reader->archive_dir);
            aeron_free(_reader);
            return -1;
        }
    }

    _reader->replay_position = actual_from;
    _reader->replay_limit = actual_from + replay_length;

    *reader = _reader;
    return 0;
}

static void aeron_archive_recording_reader_next_term(aeron_archive_recording_reader_t *reader)
{
    reader->term_offset = 0;
    reader->term_base_segment_offset += reader->term_length;

    if (reader->term_base_segment_offset == reader->segment_length)
    {
        aeron_archive_recording_reader_close_segment(reader);
        reader->segment_file_position += reader->segment_length;
        aeron_archive_recording_reader_open_segment(reader);
        reader->term_base_segment_offset = 0;
    }
}

int aeron_archive_recording_reader_poll(
    aeron_archive_recording_reader_t *reader,
    aeron_archive_recording_fragment_handler_t fragment_handler,
    void *clientd,
    int fragment_limit)
{
    if (NULL == reader)
    {
        AERON_SET_ERR(EINVAL, "%s", "reader is NULL");
        return -1;
    }

    int fragments = 0;

    while (reader->replay_position < reader->replay_limit && fragments < fragment_limit)
    {
        if (reader->term_offset == reader->term_length)
        {
            aeron_archive_recording_reader_next_term(reader);

            if (NULL == reader->mapped_segment)
            {
                reader->is_done = true;
                return fragments;
            }
        }

        const uint8_t *term_buffer = reader->mapped_segment + reader->term_base_segment_offset;
        const int32_t frame_offset = reader->term_offset;

        const aeron_frame_header_t *frame_header = (const aeron_frame_header_t *)(term_buffer + frame_offset);
        const int32_t frame_length = frame_header->frame_length;

        if (frame_length <= 0)
        {
            reader->is_done = true;
            aeron_archive_recording_reader_close_segment(reader);
            break;
        }

        const int16_t frame_type = frame_header->type;
        const uint8_t flags = (uint8_t)frame_header->flags;

        const aeron_data_header_t *data_header = (const aeron_data_header_t *)(term_buffer + frame_offset);
        const int64_t reserved_value = data_header->reserved_value;

        const int32_t aligned_length = (int32_t)AERON_ALIGN((uint32_t)frame_length, AERON_LOGBUFFER_FRAME_ALIGNMENT);
        const int32_t data_offset = frame_offset + (int32_t)AERON_DATA_HEADER_LENGTH;
        const int32_t data_length = frame_length - (int32_t)AERON_DATA_HEADER_LENGTH;

        fragment_handler(
            term_buffer + data_offset,
            (size_t)data_length,
            frame_type,
            flags,
            reserved_value,
            clientd);

        reader->replay_position += aligned_length;
        reader->term_offset += aligned_length;
        fragments++;

        if (reader->replay_position >= reader->replay_limit)
        {
            reader->is_done = true;
            aeron_archive_recording_reader_close_segment(reader);
            break;
        }
    }

    return fragments;
}

int64_t aeron_archive_recording_reader_position(const aeron_archive_recording_reader_t *reader)
{
    if (NULL == reader)
    {
        return -1;
    }

    return reader->replay_position;
}

bool aeron_archive_recording_reader_is_done(const aeron_archive_recording_reader_t *reader)
{
    if (NULL == reader)
    {
        return true;
    }

    return reader->is_done;
}

int aeron_archive_recording_reader_close(aeron_archive_recording_reader_t *reader)
{
    if (NULL == reader)
    {
        return 0;
    }

    aeron_archive_recording_reader_close_segment(reader);
    aeron_free(reader->archive_dir);
    aeron_free(reader);

    return 0;
}
