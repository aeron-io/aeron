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

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "protocol/aeron_udp_protocol.h"
#include "concurrent/aeron_atomic.h"
#include "aeron_archive_recording_session.h"

#define AERON_ARCHIVE_GENERIC_ERROR (0)

static inline size_t min_size(size_t a, size_t b)
{
    return a < b ? a : b;
}

/**
 * Block poll handler that bridges aeron_image_block_poll to the recording writer.
 * The clientd pointer is the recording session itself.
 */
static void recording_session_block_handler(
    void *clientd,
    const uint8_t *buffer,
    size_t length,
    int32_t session_id,
    int32_t term_id)
{
    aeron_archive_recording_session_t *session = (aeron_archive_recording_session_t *)clientd;

    /*
     * Detect padding frames by checking the frame header type field.
     * The type field is at offset 6 within the frame header (aeron_frame_header_t).
     */
    const aeron_frame_header_t *frame_header = (const aeron_frame_header_t *)buffer;
    const bool is_padding_frame = (AERON_HDR_TYPE_PAD == frame_header->type);

    if (aeron_archive_recording_writer_write(session->recording_writer, buffer, length, is_padding_frame) < 0)
    {
        session->error_message = strdup("recording writer write failed");
        session->error_code = AERON_ARCHIVE_GENERIC_ERROR;
        session->state = AERON_ARCHIVE_RECORDING_SESSION_STATE_INACTIVE;
    }
}

static int recording_session_init(aeron_archive_recording_session_t *session)
{
    if (aeron_archive_recording_writer_init(session->recording_writer) < 0)
    {
        session->error_message = strdup("recording writer init failed");
        aeron_archive_recording_writer_close(session->recording_writer);
        session->state = AERON_ARCHIVE_RECORDING_SESSION_STATE_STOPPED;
        return 0;
    }

    if (NULL != session->recording_events_proxy)
    {
        aeron_image_constants_t image_constants;
        aeron_image_constants(session->image, &image_constants);

        aeron_subscription_constants_t sub_constants;
        aeron_subscription_constants(image_constants.subscription, &sub_constants);

        aeron_archive_recording_events_proxy_started(
            session->recording_events_proxy,
            session->recording_id,
            image_constants.join_position,
            image_constants.session_id,
            sub_constants.stream_id,
            session->original_channel,
            image_constants.source_identity);
    }

    session->state = AERON_ARCHIVE_RECORDING_SESSION_STATE_RECORDING;

    return 1;
}

static int recording_session_record(aeron_archive_recording_session_t *session)
{
    const int work_count = aeron_image_block_poll(
        session->image, recording_session_block_handler, session, session->block_length_limit);

    if (work_count > 0)
    {
        const int64_t writer_position = aeron_archive_recording_writer_position(session->recording_writer);
        int64_t *counter_addr = aeron_counter_addr(session->position);
        if (NULL != counter_addr)
        {
            /*
             * Use ordered/release store semantics matching Java's setRelease.
             */
            AERON_SET_RELEASE(*(volatile int64_t *)counter_addr, writer_position);
        }
    }
    else if (aeron_image_is_end_of_stream(session->image) || aeron_image_is_closed(session->image))
    {
        session->state = AERON_ARCHIVE_RECORDING_SESSION_STATE_INACTIVE;
    }

    /* Check for errors set by the block handler */
    if (AERON_ARCHIVE_RECORDING_SESSION_STATE_INACTIVE == session->state)
    {
        return 1;
    }

    if (NULL != session->recording_events_proxy)
    {
        const int64_t recorded_position = aeron_archive_recording_writer_position(session->recording_writer);
        if (session->progress_event_position < recorded_position)
        {
            if (aeron_archive_recording_events_proxy_progress(
                session->recording_events_proxy,
                session->recording_id,
                session->join_position,
                recorded_position))
            {
                session->progress_event_position = recorded_position;
            }
        }
    }

    return work_count;
}

int aeron_archive_recording_session_create(
    aeron_archive_recording_session_t **session,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t start_position,
    int32_t segment_length,
    const char *original_channel,
    aeron_archive_recording_events_proxy_t *recording_events_proxy,
    aeron_image_t *image,
    aeron_counter_t *position,
    const char *archive_dir,
    int32_t file_io_max_length,
    bool force_writes,
    bool force_metadata,
    bool is_auto_stop)
{
    aeron_archive_recording_session_t *_session = NULL;

    if (aeron_alloc((void **)&_session, sizeof(aeron_archive_recording_session_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_recording_session_t");
        return -1;
    }

    _session->correlation_id = correlation_id;
    _session->recording_id = recording_id;
    _session->recording_events_proxy = recording_events_proxy;
    _session->image = image;
    _session->position = position;
    _session->is_auto_stop = is_auto_stop;
    _session->is_aborted = false;
    _session->state = AERON_ARCHIVE_RECORDING_SESSION_STATE_INIT;
    _session->error_message = NULL;
    _session->error_code = AERON_ARCHIVE_GENERIC_ERROR;

    /* Copy the original channel string */
    if (NULL != original_channel)
    {
        _session->original_channel = strdup(original_channel);
        if (NULL == _session->original_channel)
        {
            aeron_free(_session);
            AERON_APPEND_ERR("%s", "Unable to copy original_channel");
            return -1;
        }
    }
    else
    {
        _session->original_channel = NULL;
    }

    /* Get image constants for term_buffer_length and join_position */
    aeron_image_constants_t image_constants;
    if (aeron_image_constants(image, &image_constants) < 0)
    {
        free(_session->original_channel);
        aeron_free(_session);
        AERON_APPEND_ERR("%s", "Unable to get image constants");
        return -1;
    }

    _session->progress_event_position = image_constants.join_position;
    _session->join_position = image_constants.join_position;
    _session->block_length_limit = min_size(
        (size_t)image_constants.term_buffer_length,
        file_io_max_length > 0 ? (size_t)file_io_max_length : (size_t)image_constants.term_buffer_length);

    /* Create the recording writer */
    if (aeron_archive_recording_writer_create(
        &_session->recording_writer,
        recording_id,
        start_position,
        image_constants.join_position,
        (int32_t)image_constants.term_buffer_length,
        segment_length,
        archive_dir,
        force_writes,
        force_metadata) < 0)
    {
        free(_session->original_channel);
        aeron_free(_session);
        AERON_APPEND_ERR("%s", "Unable to create recording writer");
        return -1;
    }

    *session = _session;

    return 0;
}

int aeron_archive_recording_session_do_work(aeron_archive_recording_session_t *session)
{
    int work_count = 0;

    if (session->is_aborted)
    {
        session->state = AERON_ARCHIVE_RECORDING_SESSION_STATE_INACTIVE;
    }

    if (AERON_ARCHIVE_RECORDING_SESSION_STATE_INIT == session->state)
    {
        work_count += recording_session_init(session);
    }

    if (AERON_ARCHIVE_RECORDING_SESSION_STATE_RECORDING == session->state)
    {
        work_count += recording_session_record(session);
    }

    if (AERON_ARCHIVE_RECORDING_SESSION_STATE_INACTIVE == session->state)
    {
        session->state = AERON_ARCHIVE_RECORDING_SESSION_STATE_STOPPED;
        aeron_archive_recording_writer_close(session->recording_writer);
        work_count++;

        if (NULL != session->recording_events_proxy)
        {
            int64_t *counter_addr = aeron_counter_addr(session->position);
            int64_t stop_position = AERON_NULL_VALUE;
            if (NULL != counter_addr)
            {
                AERON_GET_ACQUIRE(stop_position, *(volatile int64_t *)counter_addr);
            }

            aeron_archive_recording_events_proxy_stopped(
                session->recording_events_proxy,
                session->recording_id,
                session->join_position,
                stop_position);
        }
    }

    return work_count;
}

int aeron_archive_recording_session_close(aeron_archive_recording_session_t *session)
{
    if (NULL == session)
    {
        return 0;
    }

    aeron_archive_recording_writer_close(session->recording_writer);

    if (NULL != session->position)
    {
        aeron_counter_close(session->position, NULL, NULL);
        session->position = NULL;
    }

    free(session->original_channel);
    free(session->error_message);
    aeron_free(session);

    return 0;
}

void aeron_archive_recording_session_abort_close(aeron_archive_recording_session_t *session)
{
    if (NULL != session)
    {
        aeron_archive_recording_writer_close(session->recording_writer);
    }
}

bool aeron_archive_recording_session_is_done(const aeron_archive_recording_session_t *session)
{
    return AERON_ARCHIVE_RECORDING_SESSION_STATE_STOPPED == session->state;
}

int64_t aeron_archive_recording_session_recording_id(const aeron_archive_recording_session_t *session)
{
    return session->recording_id;
}

int64_t aeron_archive_recording_session_correlation_id(const aeron_archive_recording_session_t *session)
{
    return session->correlation_id;
}

int64_t aeron_archive_recording_session_session_id(const aeron_archive_recording_session_t *session)
{
    return session->recording_id;
}

void aeron_archive_recording_session_abort(aeron_archive_recording_session_t *session, const char *reason)
{
    (void)reason;
    session->is_aborted = true;
}

bool aeron_archive_recording_session_is_auto_stop(const aeron_archive_recording_session_t *session)
{
    return session->is_auto_stop;
}

int64_t aeron_archive_recording_session_recorded_position(const aeron_archive_recording_session_t *session)
{
    aeron_counter_t *counter = session->position;

    if (NULL == counter || aeron_counter_is_closed(counter))
    {
        return AERON_NULL_VALUE;
    }

    int64_t *counter_addr = aeron_counter_addr(counter);
    if (NULL == counter_addr)
    {
        return AERON_NULL_VALUE;
    }

    int64_t value;
    AERON_GET_ACQUIRE(value, *(volatile int64_t *)counter_addr);
    return value;
}

const char *aeron_archive_recording_session_error_message(const aeron_archive_recording_session_t *session)
{
    return session->error_message;
}

int aeron_archive_recording_session_error_code(const aeron_archive_recording_session_t *session)
{
    return session->error_code;
}
