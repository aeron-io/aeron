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

#ifndef AERON_ARCHIVE_RECORDING_READER_H
#define AERON_ARCHIVE_RECORDING_READER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define AERON_ARCHIVE_NULL_POSITION (-1LL)
#define AERON_ARCHIVE_NULL_LENGTH (-1LL)

/**
 * Callback invoked for each fragment read from the recording.
 *
 * @param buffer      pointer to the data payload (after the data header).
 * @param length      length of the data payload.
 * @param frame_type  the frame type from the header.
 * @param flags       the frame flags from the header.
 * @param reserved_value the reserved value from the data header.
 * @param clientd     user-supplied opaque pointer.
 */
typedef void (*aeron_archive_recording_fragment_handler_t)(
    const uint8_t *buffer,
    size_t length,
    int16_t frame_type,
    uint8_t flags,
    int64_t reserved_value,
    void *clientd);

typedef struct aeron_archive_recording_reader_stct
{
    int64_t recording_id;
    int64_t replay_position;
    int64_t replay_limit;
    int64_t segment_file_position;
    int32_t segment_length;
    int32_t term_length;
    int32_t term_offset;
    int32_t term_base_segment_offset;
    bool is_done;
    uint8_t *mapped_segment;
    size_t mapped_segment_size;
    char *archive_dir;
}
aeron_archive_recording_reader_t;

/**
 * Create and initialise a recording reader.
 *
 * @param reader          out param for the allocated reader.
 * @param recording_id    the recording id.
 * @param start_position  the start position of the recording.
 * @param stop_position   the stop position of the recording (AERON_ARCHIVE_NULL_POSITION if live).
 * @param from_position   the position to start reading from (AERON_ARCHIVE_NULL_POSITION for start).
 * @param length          the length to read (AERON_ARCHIVE_NULL_LENGTH for all available).
 * @param term_length     the term buffer length.
 * @param segment_length  the segment file length.
 * @param initial_term_id the initial term id of the recording.
 * @param stream_id       the stream id of the recording.
 * @param archive_dir     the archive directory path.
 * @return 0 on success, -1 on failure.
 */
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
    const char *archive_dir);

/**
 * Poll fragments from the recording.
 *
 * @param reader           the recording reader.
 * @param fragment_handler callback for each fragment.
 * @param clientd          opaque pointer passed to the callback.
 * @param fragment_limit   maximum number of fragments to read.
 * @return the number of fragments read, or -1 on error.
 */
int aeron_archive_recording_reader_poll(
    aeron_archive_recording_reader_t *reader,
    aeron_archive_recording_fragment_handler_t fragment_handler,
    void *clientd,
    int fragment_limit);

/**
 * Get the current replay position.
 *
 * @param reader the recording reader.
 * @return the current absolute replay position.
 */
int64_t aeron_archive_recording_reader_position(const aeron_archive_recording_reader_t *reader);

/**
 * Check whether replay is complete.
 *
 * @param reader the recording reader.
 * @return true if done.
 */
bool aeron_archive_recording_reader_is_done(const aeron_archive_recording_reader_t *reader);

/**
 * Close the recording reader and release resources.
 *
 * @param reader the recording reader. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_recording_reader_close(aeron_archive_recording_reader_t *reader);

#endif /* AERON_ARCHIVE_RECORDING_READER_H */
