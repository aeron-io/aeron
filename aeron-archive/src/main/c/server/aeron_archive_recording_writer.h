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

#ifndef AERON_ARCHIVE_RECORDING_WRITER_H
#define AERON_ARCHIVE_RECORDING_WRITER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define AERON_ARCHIVE_RECORDING_SEGMENT_SUFFIX ".rec"

typedef struct aeron_archive_recording_writer_stct
{
    int64_t recording_id;
    int64_t segment_base_position;
    int32_t segment_length;
    int32_t segment_offset;
    bool force_writes;
    bool force_metadata;
    bool is_closed;
    int segment_fd;
    char *archive_dir;
}
aeron_archive_recording_writer_t;

/**
 * Create and initialise a recording writer.
 *
 * @param writer        out param for the allocated writer.
 * @param recording_id  the recording id.
 * @param start_position the start position of the recording.
 * @param join_position  the join position of the image being recorded.
 * @param term_length    the term buffer length of the image.
 * @param segment_length the segment file length.
 * @param archive_dir   the archive directory path.
 * @param force_writes  whether to fsync after each write.
 * @param force_metadata whether to fsync metadata as well.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_recording_writer_create(
    aeron_archive_recording_writer_t **writer,
    int64_t recording_id,
    int64_t start_position,
    int64_t join_position,
    int32_t term_length,
    int32_t segment_length,
    const char *archive_dir,
    bool force_writes,
    bool force_metadata);

/**
 * Initialise the writer by opening the first segment file.
 *
 * @param writer the recording writer.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_recording_writer_init(aeron_archive_recording_writer_t *writer);

/**
 * Write a block of data to the recording at the current segment offset.
 * The buffer should contain complete Aeron data frames (including headers).
 * For padding frames, only the header (AERON_DATA_HEADER_LENGTH bytes) is written.
 *
 * @param writer  the recording writer.
 * @param buffer  the buffer containing the data frames.
 * @param length  the total aligned length of the block (used to advance position).
 * @param is_padding_frame true if this is a padding frame.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_recording_writer_write(
    aeron_archive_recording_writer_t *writer,
    const uint8_t *buffer,
    size_t length,
    bool is_padding_frame);

/**
 * Get the current write position.
 *
 * @param writer the recording writer.
 * @return the current absolute position in the recording.
 */
int64_t aeron_archive_recording_writer_position(const aeron_archive_recording_writer_t *writer);

/**
 * Close the recording writer and release resources.
 *
 * @param writer the recording writer. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_recording_writer_close(aeron_archive_recording_writer_t *writer);

#endif /* AERON_ARCHIVE_RECORDING_WRITER_H */
