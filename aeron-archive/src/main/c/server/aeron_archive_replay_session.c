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

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "protocol/aeron_udp_protocol.h"
#include "aeron_archive_replay_session.h"

#define AERON_ARCHIVE_SEGMENT_FILE_SUFFIX ".rec"
#define AERON_FRAME_ALIGNMENT (32)

/**
 * Compute the segment file base position for a given replay position.
 * Mirrors AeronArchive.segmentFileBasePosition in Java.
 */
static int64_t segment_file_base_position(
    int64_t start_position, int64_t position, int32_t term_length, int32_t segment_length)
{
    const int64_t start_term_base_position = start_position - (start_position & (term_length - 1));
    const int64_t position_since_base = position - start_term_base_position;
    const int64_t segment_offset = position_since_base - (position_since_base & ((int64_t)segment_length - 1));
    return start_term_base_position + segment_offset;
}

static void format_segment_file_name(char *buf, size_t buf_len, int64_t recording_id, int64_t position)
{
    snprintf(buf, buf_len, "%" PRId64 "-%" PRId64 "%s", recording_id, position, AERON_ARCHIVE_SEGMENT_FILE_SUFFIX);
}

static int64_t min_int64(int64_t a, int64_t b)
{
    return a < b ? a : b;
}

static int32_t min_int32(int32_t a, int32_t b)
{
    return a < b ? a : b;
}

static int32_t align_int32(int32_t value, int32_t alignment)
{
    return (value + (alignment - 1)) & ~(alignment - 1);
}

static int open_segment_file(aeron_archive_replay_session_t *session)
{
    char segment_file_name[256];
    char segment_file_path[1024];

    format_segment_file_name(segment_file_name, sizeof(segment_file_name),
        session->recording_id, session->segment_file_base_position);
    snprintf(segment_file_path, sizeof(segment_file_path), "%s/%s", session->archive_dir, segment_file_name);

    session->segment_fd = open(segment_file_path, O_RDONLY);
    if (session->segment_fd < 0)
    {
        return -1;
    }

    return 0;
}

static void close_segment_file(aeron_archive_replay_session_t *session)
{
    if (session->segment_fd >= 0)
    {
        close(session->segment_fd);
        session->segment_fd = -1;
    }
}

static int segment_file_exists(aeron_archive_replay_session_t *session)
{
    char segment_file_name[256];
    char segment_file_path[1024];

    format_segment_file_name(segment_file_name, sizeof(segment_file_name),
        session->recording_id, session->segment_file_base_position);
    snprintf(segment_file_path, sizeof(segment_file_path), "%s/%s", session->archive_dir, segment_file_name);

    return access(segment_file_path, F_OK) == 0;
}

static void set_state(aeron_archive_replay_session_t *session, aeron_archive_replay_session_state_t new_state)
{
    session->state = new_state;
}

static void raise_error(aeron_archive_replay_session_t *session, const char *message)
{
    session->revoke_publication = true;

    char error_buf[1024];
    char segment_name[256];
    format_segment_file_name(segment_name, sizeof(segment_name),
        session->recording_id, session->segment_file_base_position);

    snprintf(error_buf, sizeof(error_buf),
        "%s, recordingId=%" PRId64 ", replaySessionId=%" PRId64 ", segmentFile=%s",
        message, session->recording_id, session->session_id, segment_name);

    free(session->error_message);
    session->error_message = strdup(error_buf);

    set_state(session, AERON_ARCHIVE_REPLAY_SESSION_STATE_INACTIVE);
}

/**
 * Read data from the current segment file at the given offset.
 *
 * @return the number of bytes read, or -1 on error.
 */
static int32_t read_recording(aeron_archive_replay_session_t *session, int64_t available_replay)
{
    const int32_t limit = min_int32(
        (int32_t)min_int64(available_replay, (int64_t)session->replay_buffer_length),
        session->term_length - session->term_offset);

    if (limit <= 0)
    {
        return 0;
    }

    const int32_t file_position = session->term_base_segment_offset + session->term_offset;
    ssize_t total_read = 0;

    while (total_read < limit)
    {
        const ssize_t bytes_read = pread(
            session->segment_fd,
            session->replay_buffer + total_read,
            (size_t)(limit - total_read),
            file_position + total_read);

        if (bytes_read <= 0)
        {
            break;
        }

        total_read += bytes_read;
    }

    return limit;
}

/**
 * Advance to the next term within the segment or open the next segment file.
 */
static int next_term(aeron_archive_replay_session_t *session)
{
    session->term_offset = 0;
    session->term_base_segment_offset += session->term_length;

    if (session->term_base_segment_offset == session->segment_length)
    {
        close_segment_file(session);
        session->segment_file_base_position += session->segment_length;

        if (open_segment_file(session) < 0)
        {
            raise_error(session, "recording segment not found");
            return -1;
        }

        session->term_base_segment_offset = 0;
    }

    return 0;
}

/**
 * Check whether a bounded replay has been extended by the recording counter.
 *
 * @return true if NOT extended (i.e. still waiting), false if extended.
 */
static bool not_extended(aeron_archive_replay_session_t *session, int64_t replay_position, int64_t old_stop_position)
{
    aeron_counter_t *limit_position = session->limit_position;
    int64_t *counter_addr = aeron_counter_addr(limit_position);
    int64_t current_limit_position = 0;
    int64_t new_stop_position = old_stop_position;

    if (NULL != counter_addr)
    {
        current_limit_position = __atomic_load_n(counter_addr, __ATOMIC_ACQUIRE);
    }

    if (aeron_counter_is_closed(limit_position))
    {
        if (session->replay_limit >= old_stop_position)
        {
            session->replay_limit = old_stop_position;
        }
    }
    else
    {
        new_stop_position = current_limit_position;
    }

    if (replay_position >= session->replay_limit)
    {
        set_state(session, AERON_ARCHIVE_REPLAY_SESSION_STATE_INACTIVE);
    }
    else if (new_stop_position > old_stop_position)
    {
        session->stop_position = new_stop_position;
        return false;
    }

    return true;
}

/**
 * Read frame header fields from the replay buffer at a given offset.
 */
static inline int32_t frame_length_at(const uint8_t *buffer, int32_t offset)
{
    int32_t value;
    memcpy(&value, buffer + offset, sizeof(value));
    return value;
}

static inline int16_t frame_type_at(const uint8_t *buffer, int32_t offset)
{
    int16_t value;
    memcpy(&value, buffer + offset + 6, sizeof(value));
    return value;
}

static bool has_publication_advanced(
    aeron_archive_replay_session_t *session, int64_t position, int32_t aligned_length)
{
    if (position > 0)
    {
        session->term_offset += aligned_length;
        session->replay_position += aligned_length;

        if (session->replay_position >= session->replay_limit)
        {
            set_state(session, AERON_ARCHIVE_REPLAY_SESSION_STATE_INACTIVE);
        }

        return true;
    }
    else if (AERON_PUBLICATION_CLOSED == position || AERON_PUBLICATION_NOT_CONNECTED == position)
    {
        session->revoke_publication = true;
        set_state(session, AERON_ARCHIVE_REPLAY_SESSION_STATE_INACTIVE);
    }

    return false;
}

/**
 * The INIT state handler. Opens the segment file, validates the replay position,
 * and waits for the publication to connect.
 */
static int replay_session_init(aeron_archive_replay_session_t *session, int64_t current_time_ms)
{
    if (session->segment_fd < 0)
    {
        if (!segment_file_exists(session))
        {
            if (current_time_ms > session->connect_deadline_ms)
            {
                raise_error(session, "recording segment file not created");
            }
            return 0;
        }

        if (open_segment_file(session) < 0)
        {
            raise_error(session, "failed to open recording segment file");
            return 0;
        }

        const int64_t start_term_base = session->start_position -
            (session->start_position & (session->term_length - 1));
        const int32_t segment_offset =
            (int32_t)((session->replay_position - start_term_base) & (session->segment_length - 1));

        session->term_offset = (int32_t)(session->replay_position & (session->term_length - 1));
        session->term_base_segment_offset = segment_offset - session->term_offset;
    }

    if (!aeron_exclusive_publication_is_connected(session->publication))
    {
        if (current_time_ms > session->connect_deadline_ms)
        {
            raise_error(session, "no connection established for replay publication");
        }
        return 0;
    }

    set_state(session, AERON_ARCHIVE_REPLAY_SESSION_STATE_REPLAY);

    return 1;
}

/**
 * The REPLAY state handler. Reads data from the segment file and sends it
 * through the exclusive publication.
 */
static int replay_session_replay(aeron_archive_replay_session_t *session)
{
    if (!aeron_exclusive_publication_is_connected(session->publication))
    {
        session->revoke_publication = true;
        set_state(session, AERON_ARCHIVE_REPLAY_SESSION_STATE_INACTIVE);
        return 0;
    }

    if (session->start_position == session->stop_position && 0 == session->replay_limit)
    {
        set_state(session, AERON_ARCHIVE_REPLAY_SESSION_STATE_INACTIVE);
        return 0;
    }

    if (NULL != session->limit_position &&
        session->replay_position >= session->stop_position &&
        not_extended(session, session->replay_position, session->stop_position))
    {
        return 0;
    }

    if (session->term_offset == session->term_length)
    {
        if (next_term(session) < 0)
        {
            return 0;
        }
    }

    int work_count = 0;

    const int32_t bytes_read = read_recording(session, session->stop_position - session->replay_position);
    if (bytes_read > 0)
    {
        aeron_publication_constants_t pub_constants;
        aeron_exclusive_publication_constants(session->publication, &pub_constants);

        const int32_t pub_session_id = pub_constants.session_id;
        const int32_t pub_stream_id = pub_constants.stream_id;

        int64_t remaining = session->replay_limit - session->replay_position;
        if (remaining > (int64_t)(session->term_length))
        {
            remaining = session->term_length;
        }

        int32_t batch_offset = 0;
        int32_t padding_frame_length = 0;

        while (batch_offset < bytes_read && batch_offset < (int32_t)remaining)
        {
            const int32_t frame_length = frame_length_at(session->replay_buffer, batch_offset);
            if (frame_length <= 0)
            {
                raise_error(session, "unexpected end of recording");
                return 0;
            }

            const int16_t frame_type = frame_type_at(session->replay_buffer, batch_offset);
            const int32_t aligned_length = align_int32(frame_length, AERON_FRAME_ALIGNMENT);

            if (AERON_HDR_TYPE_DATA == frame_type)
            {
                if (batch_offset + aligned_length > bytes_read)
                {
                    break;
                }

                /* Rewrite session id and stream id to match the replay publication */
                int32_t session_id_le = pub_session_id;
                int32_t stream_id_le = pub_stream_id;
                memcpy(session->replay_buffer + batch_offset + 12, &session_id_le, sizeof(session_id_le));
                memcpy(session->replay_buffer + batch_offset + 16, &stream_id_le, sizeof(stream_id_le));

                batch_offset += aligned_length;
            }
            else if (AERON_HDR_TYPE_PAD == frame_type)
            {
                padding_frame_length = frame_length;
                break;
            }
            else
            {
                raise_error(session, "unexpected frame type in recording");
                return 0;
            }
        }

        if (batch_offset > 0)
        {
            const int64_t position = aeron_exclusive_publication_offer_block(
                session->publication, session->replay_buffer, (size_t)batch_offset);

            if (has_publication_advanced(session, position, batch_offset))
            {
                work_count++;
            }
            else
            {
                padding_frame_length = 0;
            }
        }

        if (padding_frame_length > 0)
        {
            const int32_t padding_length = padding_frame_length - (int32_t)AERON_DATA_HEADER_LENGTH;
            const int64_t position = aeron_exclusive_publication_append_padding(
                session->publication, (size_t)padding_length);

            if (has_publication_advanced(session, position, align_int32(padding_frame_length, AERON_FRAME_ALIGNMENT)))
            {
                work_count++;
            }
        }
    }

    return work_count;
}

int aeron_archive_replay_session_create(
    aeron_archive_replay_session_t **session,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t replay_position,
    int64_t replay_length,
    int64_t start_position,
    int64_t stop_position,
    int32_t segment_file_length,
    int32_t term_buffer_length,
    int32_t stream_id,
    int64_t replay_session_id,
    int64_t connect_timeout_ms,
    int64_t current_time_ms,
    aeron_exclusive_publication_t *publication,
    aeron_counter_t *limit_position,
    const char *archive_dir,
    size_t replay_buffer_length)
{
    aeron_archive_replay_session_t *_session = NULL;

    if (aeron_alloc((void **)&_session, sizeof(aeron_archive_replay_session_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_replay_session_t");
        return -1;
    }

    _session->correlation_id = correlation_id;
    _session->session_id = replay_session_id;
    _session->recording_id = recording_id;
    _session->start_position = start_position;
    _session->replay_position = replay_position;
    _session->stop_position = stop_position;
    _session->stream_id = stream_id;
    _session->term_length = term_buffer_length;
    _session->segment_length = segment_file_length;
    _session->state = AERON_ARCHIVE_REPLAY_SESSION_STATE_INIT;
    _session->publication = publication;
    _session->limit_position = limit_position;
    _session->segment_fd = -1;
    _session->error_message = NULL;
    _session->revoke_publication = false;
    _session->is_aborted = false;
    _session->term_offset = 0;
    _session->term_base_segment_offset = 0;

    _session->segment_file_base_position = segment_file_base_position(
        start_position, replay_position, term_buffer_length, segment_file_length);

    _session->replay_limit = replay_position + replay_length;
    _session->connect_deadline_ms = current_time_ms + connect_timeout_ms;

    /* Copy archive directory */
    if (NULL != archive_dir)
    {
        _session->archive_dir = strdup(archive_dir);
        if (NULL == _session->archive_dir)
        {
            aeron_free(_session);
            AERON_APPEND_ERR("%s", "Unable to copy archive_dir");
            return -1;
        }
    }
    else
    {
        _session->archive_dir = NULL;
    }

    /* Allocate replay buffer */
    _session->replay_buffer_length = replay_buffer_length;
    if (aeron_alloc((void **)&_session->replay_buffer, replay_buffer_length) < 0)
    {
        free(_session->archive_dir);
        aeron_free(_session);
        AERON_APPEND_ERR("%s", "Unable to allocate replay buffer");
        return -1;
    }

    *session = _session;

    return 0;
}

int aeron_archive_replay_session_do_work(
    aeron_archive_replay_session_t *session,
    int64_t current_time_ms)
{
    int work_count = 0;

    if (session->is_aborted)
    {
        session->revoke_publication = true;
        set_state(session, AERON_ARCHIVE_REPLAY_SESSION_STATE_INACTIVE);
    }

    if (AERON_ARCHIVE_REPLAY_SESSION_STATE_INIT == session->state)
    {
        work_count += replay_session_init(session, current_time_ms);
    }

    if (AERON_ARCHIVE_REPLAY_SESSION_STATE_REPLAY == session->state)
    {
        work_count += replay_session_replay(session);
    }

    if (AERON_ARCHIVE_REPLAY_SESSION_STATE_INACTIVE == session->state)
    {
        close_segment_file(session);
        set_state(session, AERON_ARCHIVE_REPLAY_SESSION_STATE_DONE);
    }

    return work_count;
}

int aeron_archive_replay_session_close(aeron_archive_replay_session_t *session)
{
    if (NULL == session)
    {
        return 0;
    }

    close_segment_file(session);

    if (NULL != session->publication)
    {
        aeron_exclusive_publication_close(session->publication, NULL, NULL);
        session->publication = NULL;
    }

    aeron_free(session->replay_buffer);
    free(session->archive_dir);
    free(session->error_message);
    aeron_free(session);

    return 0;
}

void aeron_archive_replay_session_abort(aeron_archive_replay_session_t *session, const char *reason)
{
    (void)reason;
    session->is_aborted = true;
}

bool aeron_archive_replay_session_is_done(const aeron_archive_replay_session_t *session)
{
    return AERON_ARCHIVE_REPLAY_SESSION_STATE_DONE == session->state;
}

int64_t aeron_archive_replay_session_session_id(const aeron_archive_replay_session_t *session)
{
    return session->session_id;
}

int64_t aeron_archive_replay_session_recording_id(const aeron_archive_replay_session_t *session)
{
    return session->recording_id;
}

aeron_archive_replay_session_state_t aeron_archive_replay_session_state(
    const aeron_archive_replay_session_t *session)
{
    return session->state;
}

int64_t aeron_archive_replay_session_segment_file_base_position(
    const aeron_archive_replay_session_t *session)
{
    return session->segment_file_base_position;
}

const char *aeron_archive_replay_session_error_message(const aeron_archive_replay_session_t *session)
{
    return session->error_message;
}
