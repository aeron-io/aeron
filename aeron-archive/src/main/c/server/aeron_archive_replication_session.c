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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_archive_replication_session.h"

/*
 * REPLICATE_END signal value (7) matching Java RecordingSignal.REPLICATE_END.
 * This may not be in the server-side recording_signal enum yet.
 */
#define AERON_ARCHIVE_RECORDING_SIGNAL_CODE_REPLICATE_END ((aeron_archive_recording_signal_code_t)7)

/*
 * We cannot include the archive client headers directly because they re-define types
 * already provided by the server-side conductor header (aeron_archive_recording_signal_code_t,
 * aeron_archive_catalog_recording_descriptor_t, etc.).
 *
 * Instead we declare the minimal set of client functions we need as extern.
 * The linker resolves them from the archive client library.
 */

/* Archive client replay params (mirrors the client definition). */
typedef struct aeron_archive_replay_params_stct
{
    int32_t bounding_limit_counter_id;
    int32_t file_io_max_length;
    int64_t position;
    int64_t length;
    int64_t replay_token;
    int64_t subscription_registration_id;
}
aeron_archive_replay_params_t;

extern int aeron_archive_replay_params_init(aeron_archive_replay_params_t *params);
extern int aeron_archive_async_connect(aeron_archive_async_connect_t **async, aeron_archive_context_t *ctx);
extern int aeron_archive_async_connect_poll(aeron_archive_t **aeron_archive, aeron_archive_async_connect_t *async);
extern int aeron_archive_close(aeron_archive_t *aeron_archive);
extern int aeron_archive_poll_for_error_response(aeron_archive_t *aeron_archive, char *buffer, size_t buffer_length);
extern uint64_t aeron_archive_context_get_message_timeout_ns(aeron_archive_context_t *ctx);

/* Recording descriptor consumer callback type (client-side). */
typedef void (*aeron_archive_recording_descriptor_consumer_func_t)(
    aeron_archive_catalog_recording_descriptor_t *recording_descriptor,
    void *clientd);

extern int aeron_archive_list_recording(
    int32_t *count_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    aeron_archive_recording_descriptor_consumer_func_t consumer,
    void *clientd);

extern int aeron_archive_get_recording_position(
    int64_t *recording_position_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id);

extern int aeron_archive_start_replay(
    int64_t *replay_session_id_p,
    aeron_archive_t *aeron_archive,
    int64_t recording_id,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_archive_replay_params_t *params);

extern int aeron_archive_stop_replay(
    aeron_archive_t *aeron_archive,
    int64_t replay_session_id);

/*
 * ---------------------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------------------
 */

static char *strdup_or_null(const char *s)
{
    if (NULL == s || '\0' == s[0])
    {
        return NULL;
    }
    return strdup(s);
}

static void replication_session_set_state(
    aeron_archive_replication_session_t *session,
    aeron_archive_replication_session_state_t new_state,
    int64_t now_ms)
{
    session->state = new_state;
    session->active_correlation_id = AERON_NULL_VALUE;
    session->time_of_last_action_ms = now_ms;
}

static void replication_session_signal(
    aeron_archive_replication_session_t *session,
    int64_t position,
    aeron_archive_recording_signal_code_t recording_signal)
{
    int64_t subscription_id = AERON_NULL_VALUE;
    if (NULL != session->recording_subscription)
    {
        aeron_subscription_constants_t constants;
        if (0 == aeron_subscription_constants(session->recording_subscription, &constants))
        {
            subscription_id = constants.registration_id;
        }
    }

    aeron_archive_control_session_send_signal(
        session->control_session,
        session->replication_id,
        session->dst_recording_id,
        subscription_id,
        position,
        recording_signal);
}

static void replication_session_error(
    aeron_archive_replication_session_t *session,
    const char *msg,
    int error_code)
{
    aeron_archive_control_session_send_error_response(
        session->control_session,
        session->replication_id,
        (int64_t)error_code,
        msg);
}

static int replication_session_track_action(
    aeron_archive_replication_session_t *session,
    int64_t correlation_id,
    int64_t now_ms)
{
    session->time_of_last_action_ms = now_ms;
    session->active_correlation_id = correlation_id;
    return 1;
}

static bool replication_session_should_add_live(
    aeron_archive_replication_session_t *session)
{
    if (session->is_live_added || NULL == session->image)
    {
        return false;
    }

    int64_t position = aeron_image_position(session->image);
    int64_t diff = session->src_recording_position - position;

    aeron_image_constants_t img_constants;
    int32_t term_length = 0;
    if (0 == aeron_image_constants(session->image, &img_constants))
    {
        term_length = img_constants.term_buffer_length;
    }

    int64_t window = term_length >> 2;
    if (window > AERON_ARCHIVE_REPLICATION_LIVE_ADD_MAX_WINDOW)
    {
        window = AERON_ARCHIVE_REPLICATION_LIVE_ADD_MAX_WINDOW;
    }

    return diff <= window;
}

static bool replication_session_should_stop_replay(
    aeron_archive_replication_session_t *session)
{
    if (!session->is_live_added || NULL == session->image)
    {
        return false;
    }

    int64_t position = aeron_image_position(session->image);
    int64_t diff = session->src_recording_position - position;

    return diff <= AERON_ARCHIVE_REPLICATION_REPLAY_REMOVE_THRESHOLD &&
        aeron_image_active_transport_count(session->image) >= 2;
}

static void replication_session_stop_recording(aeron_archive_replication_session_t *session)
{
    if (NULL != session->recording_subscription)
    {
        aeron_subscription_constants_t constants;
        if (0 == aeron_subscription_constants(session->recording_subscription, &constants))
        {
            /* Ask the conductor to stop the recording subscription. */
            aeron_archive_conductor_stop_recording_subscription(
                session->conductor,
                AERON_NULL_VALUE,
                constants.registration_id,
                NULL);
        }
        session->recording_subscription = NULL;
    }
}

static void replication_session_stop_replay_on_source(aeron_archive_replication_session_t *session)
{
    if (AERON_NULL_VALUE != session->src_replay_session_id && NULL != session->src_archive)
    {
        aeron_archive_stop_replay(session->src_archive, session->src_replay_session_id);
        session->src_replay_session_id = AERON_NULL_VALUE;
    }
}

/*
 * ---------------------------------------------------------------------------
 * State machine step functions
 * ---------------------------------------------------------------------------
 */

static int replication_step_connect(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    int work_count = 0;

    if (NULL == session->async_connect)
    {
        if (aeron_archive_async_connect(&session->async_connect, session->src_archive_ctx) < 0)
        {
            replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
            replication_session_error(session, "failed to initiate async connect to source archive",
                AERON_ARCHIVE_ERROR_GENERIC);
            return 0;
        }
        work_count = 1;
    }
    else
    {
        aeron_archive_t *archive = NULL;
        int rc = aeron_archive_async_connect_poll(&archive, session->async_connect);

        if (rc < 0)
        {
            replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
            replication_session_error(session, "replication connection failed",
                AERON_ARCHIVE_ERROR_GENERIC);
            return 0;
        }

        if (NULL != archive)
        {
            session->src_archive = archive;
            session->async_connect = NULL;
            replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_REPLICATE_DESCRIPTOR, now_ms);
            work_count = 1;
        }
        else if (rc > 0)
        {
            work_count = 1;
        }
    }

    return work_count;
}

/**
 * Callback for the recording descriptor consumer during replicate_descriptor.
 * Matches Java onRecordingDescriptor.
 */
static void on_src_recording_descriptor(
    aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd)
{
    aeron_archive_replication_session_t *session =
        (aeron_archive_replication_session_t *)clientd;

    session->src_stop_position = descriptor->stop_position;
    session->replay_stream_id = descriptor->stream_id;

    if (NULL == session->live_destination && AERON_NULL_VALUE != session->replication_session_id)
    {
        session->replay_session_id = session->replication_session_id;
    }
    else
    {
        session->replay_session_id = descriptor->session_id;
    }

    /* Validate file_io_max_length vs mtu_length */
    if (AERON_NULL_VALUE != session->file_io_max_length &&
        session->file_io_max_length < descriptor->mtu_length)
    {
        session->state = AERON_ARCHIVE_REPLICATION_STATE_DONE;
        char msg[256];
        snprintf(msg, sizeof(msg),
            "Replication fileIoMaxLength (%d) is less than the recording mtuLength (%d)",
            session->file_io_max_length, descriptor->mtu_length);
        session->error_message = strdup(msg);
        replication_session_error(session, msg, AERON_ARCHIVE_ERROR_GENERIC);
        return;
    }

    if (AERON_NULL_VALUE == session->dst_recording_id)
    {
        /* Create new recording in local catalog */
        session->replay_position = descriptor->start_position;
        session->dst_recording_id = aeron_archive_catalog_add_recording(
            session->catalog,
            descriptor->start_position,
            descriptor->start_position,
            descriptor->start_timestamp,
            descriptor->start_timestamp,
            descriptor->initial_term_id,
            descriptor->segment_file_length,
            descriptor->term_buffer_length,
            descriptor->mtu_length,
            descriptor->session_id,
            descriptor->stream_id,
            descriptor->stripped_channel,
            descriptor->original_channel,
            descriptor->source_identity);

        replication_session_signal(session, descriptor->start_position,
            AERON_ARCHIVE_RECORDING_SIGNAL_CODE_REPLICATE);
    }
    else if (session->is_destination_recording_empty)
    {
        /* Replace the empty destination recording with source metadata */
        session->replay_position = descriptor->start_position;
        /* For replace, we invalidate and re-add with same recording id.
         * This is a simplification; in production you would use catalog_replace_recording. */
    }

    aeron_archive_replication_session_state_t next_state = AERON_ARCHIVE_REPLICATION_STATE_EXTEND;

    if (NULL != session->live_destination)
    {
        if (AERON_ARCHIVE_NULL_POSITION != descriptor->stop_position)
        {
            session->state = AERON_ARCHIVE_REPLICATION_STATE_DONE;
            const char *err = "cannot live merge without active source recording";
            session->error_message = strdup(err);
            replication_session_error(session, err, AERON_ARCHIVE_ERROR_GENERIC);
            return;
        }

        next_state = AERON_ARCHIVE_REPLICATION_STATE_SRC_RECORDING_POSITION;
    }

    /* Check if already in sync */
    if (descriptor->start_position == descriptor->stop_position)
    {
        replication_session_signal(session, descriptor->stop_position,
            AERON_ARCHIVE_RECORDING_SIGNAL_CODE_SYNC);
        next_state = AERON_ARCHIVE_REPLICATION_STATE_DONE;
    }
    else if (AERON_NULL_VALUE != session->dst_recording_id)
    {
        /* Check if dst already has the stop position */
        aeron_archive_catalog_recording_descriptor_t dst_desc;
        if (0 == aeron_archive_catalog_find_recording(session->catalog, session->dst_recording_id, &dst_desc))
        {
            if (descriptor->stop_position == dst_desc.stop_position)
            {
                replication_session_signal(session, descriptor->stop_position,
                    AERON_ARCHIVE_RECORDING_SIGNAL_CODE_SYNC);
                next_state = AERON_ARCHIVE_REPLICATION_STATE_DONE;
            }
        }
    }

    session->state = next_state;
}

static int replication_step_replicate_descriptor(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    int work_count = 0;

    int32_t count = 0;
    int rc = aeron_archive_list_recording(
        &count,
        session->src_archive,
        session->src_recording_id,
        on_src_recording_descriptor,
        session);

    if (rc < 0)
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        char msg[256];
        snprintf(msg, sizeof(msg), "unknown src recording id %" PRId64, session->src_recording_id);
        replication_session_error(session, msg, AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING);
        return 0;
    }

    if (count == 0)
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        char msg[256];
        snprintf(msg, sizeof(msg), "unknown src recording id %" PRId64, session->src_recording_id);
        replication_session_error(session, msg, AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING);
        return 0;
    }

    /* State was set by the descriptor callback. Update action time. */
    session->time_of_last_action_ms = now_ms;
    session->active_correlation_id = AERON_NULL_VALUE;
    work_count = 1;

    return work_count;
}

static int replication_step_src_recording_position(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    int64_t position = 0;
    int rc = aeron_archive_get_recording_position(&position, session->src_archive, session->src_recording_id);

    if (rc < 0)
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        replication_session_error(session, "failed to get source recording position",
            AERON_ARCHIVE_ERROR_GENERIC);
        return 0;
    }

    session->src_recording_position = position;

    if (AERON_ARCHIVE_NULL_POSITION == position && NULL != session->live_destination)
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        replication_session_error(session, "cannot live merge without active source recording",
            AERON_ARCHIVE_ERROR_GENERIC);
        return 0;
    }

    replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_EXTEND, now_ms);
    return 1;
}

static int replication_step_extend(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    /*
     * Build the subscription channel for the local extend-recording.
     * The Java version manipulates ChannelUri; here we build a simplified channel string.
     *
     * Key properties:
     * - rejoin=false
     * - session-id=<replay_session_id> (unless response mode)
     * - If tagged (MDS): remove endpoint, add tags and control-mode=manual
     */
    const bool is_mds = session->is_tagged || NULL != session->live_destination;

    char channel[2048];
    if (is_mds)
    {
        snprintf(channel, sizeof(channel),
            "aeron:udp?rejoin=false|tags=%" PRId64 ",%" PRId64 "|control-mode=manual|session-id=%d",
            session->channel_tag_id,
            session->subscription_tag_id,
            session->replay_session_id);
    }
    else
    {
        /* Use the replication channel with rejoin=false and session-id appended */
        snprintf(channel, sizeof(channel),
            "%s|rejoin=false|session-id=%d",
            session->replication_channel,
            session->replay_session_id);
    }

    /*
     * Ask the conductor to extend the recording. In the Java code this returns
     * a Subscription or error string. In C we call the conductor's extend_recording
     * which sets up the subscription internally and returns 0/-1.
     */
    int rc = aeron_archive_conductor_extend_recording(
        session->conductor,
        session->replication_id,
        session->dst_recording_id,
        session->replay_stream_id,
        AERON_ARCHIVE_SOURCE_LOCATION_REMOTE,
        true,
        channel,
        session->control_session);

    if (rc < 0)
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        return 1;
    }

    /*
     * The conductor owns the subscription. We need to find it so we can track the image.
     * For now, store it via the conductor's recording subscription for our dst_recording_id.
     * The subscription reference will be set up by the conductor.
     */
    replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_REPLAY_TOKEN, now_ms);
    return 1;
}

static int replication_step_replay_token(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    /*
     * In the Java version, a replay token is requested for response-channel mode.
     * For the C implementation, we skip token acquisition if not using response channels
     * and proceed directly.
     */
    replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_GET_ARCHIVE_PROXY, now_ms);
    return 1;
}

static int replication_step_get_archive_proxy(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    /*
     * If no replay token was obtained, skip creating a response publication
     * and proceed directly to replay using the standard archive proxy.
     */
    if (AERON_NULL_VALUE == session->replay_token)
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_REPLAY, now_ms);
        return 1;
    }

    /*
     * Response-channel based replay proxy setup.
     * Create an exclusive publication for sending replay requests via the response path.
     * This is an advanced feature; the basic implementation proceeds to REPLAY.
     */
    replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_REPLAY, now_ms);
    return 1;
}

static int replication_step_replay(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    int work_count = 0;

    /*
     * Build the replay channel. The Java code modifies the replication channel with:
     * - resolved wildcard endpoint
     * - linger=0, eos=false if live merge
     * - session-id or response-correlation-id
     */
    char replay_channel[2048];
    if (NULL != session->live_destination)
    {
        snprintf(replay_channel, sizeof(replay_channel),
            "%s|linger=0|eos=false|session-id=%d",
            session->replication_channel,
            session->replay_session_id);
    }
    else
    {
        snprintf(replay_channel, sizeof(replay_channel),
            "%s|session-id=%d",
            session->replication_channel,
            session->replay_session_id);
    }

    /* Calculate replay length */
    int64_t replay_length = INT64_MAX; /* unbounded by default */
    if (AERON_ARCHIVE_NULL_POSITION != session->dst_stop_position)
    {
        replay_length = session->dst_stop_position - session->replay_position;
    }

    aeron_archive_replay_params_t params;
    aeron_archive_replay_params_init(&params);
    params.position = session->replay_position;
    params.length = replay_length;
    params.file_io_max_length = session->file_io_max_length;
    params.replay_token = session->replay_token;

    int64_t replay_session_id = 0;
    int rc = aeron_archive_start_replay(
        &replay_session_id,
        session->src_archive,
        session->src_recording_id,
        replay_channel,
        session->replay_stream_id,
        &params);

    if (rc < 0)
    {
        if (now_ms >= (session->time_of_last_action_ms + session->action_timeout_ms))
        {
            replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
            replication_session_error(session, "failed to start replay on source archive",
                AERON_ARCHIVE_ERROR_GENERIC);
        }
        return 0;
    }

    session->src_replay_session_id = replay_session_id;
    replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_AWAIT_IMAGE, now_ms);
    work_count = 1;

    return work_count;
}

static int replication_step_await_image(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    int work_count = 0;

    if (NULL == session->recording_subscription)
    {
        if (now_ms >= (session->time_of_last_action_ms + session->action_timeout_ms))
        {
            replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
            replication_session_error(session, "recording subscription not available",
                AERON_ARCHIVE_ERROR_GENERIC);
        }
        return 0;
    }

    aeron_image_t *image = aeron_subscription_image_by_session_id(
        session->recording_subscription,
        (int32_t)session->src_replay_session_id);

    if (NULL != image)
    {
        session->image = image;
        if (NULL == session->live_destination)
        {
            replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_REPLICATE, now_ms);
        }
        else
        {
            replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_CATCHUP, now_ms);
        }
        work_count = 1;
    }
    else if (now_ms >= (session->time_of_last_action_ms + session->action_timeout_ms))
    {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "failed to get replay image for sessionId=%d",
            (int32_t)session->src_replay_session_id);
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        replication_session_error(session, msg, AERON_ARCHIVE_ERROR_GENERIC);
    }

    return work_count;
}

static int replication_step_replicate(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    int work_count = 0;

    if (NULL == session->image)
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        return 1;
    }

    const bool is_closed = aeron_image_is_closed(session->image);
    const bool is_end_of_stream = aeron_image_is_end_of_stream(session->image);
    const int64_t position = aeron_image_position(session->image);
    const bool is_synced = AERON_ARCHIVE_NULL_POSITION != session->src_stop_position &&
        position >= session->src_stop_position;

    if (is_synced ||
        (AERON_ARCHIVE_NULL_POSITION != session->dst_stop_position && position >= session->dst_stop_position) ||
        is_end_of_stream || is_closed)
    {
        if (is_synced)
        {
            replication_session_signal(session, position, AERON_ARCHIVE_RECORDING_SIGNAL_CODE_SYNC);
        }

        session->src_replay_session_id = AERON_NULL_VALUE;
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        work_count = 1;
    }

    return work_count;
}

static int replication_step_catchup(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    int work_count = 0;

    if (NULL == session->image)
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        return 1;
    }

    int64_t position = aeron_image_position(session->image);

    if (position >= session->src_recording_position)
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_ATTEMPT_LIVE_JOIN, now_ms);
        work_count = 1;
    }
    else if (aeron_image_is_closed(session->image))
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        replication_session_error(session, "replication image closed unexpectedly",
            AERON_ARCHIVE_ERROR_GENERIC);
    }

    return work_count;
}

static int replication_step_attempt_live_join(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    int work_count = 0;

    /* Query the source archive for the current recording position */
    int64_t position = 0;
    int rc = aeron_archive_get_recording_position(&position, session->src_archive, session->src_recording_id);

    if (rc < 0)
    {
        if (aeron_image_is_closed(session->image))
        {
            replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
            replication_session_error(session, "replication image closed unexpectedly",
                AERON_ARCHIVE_ERROR_GENERIC);
            return 0;
        }

        if (now_ms >= (session->time_of_last_action_ms + session->action_timeout_ms))
        {
            session->retry_attempts--;
            if (0 == session->retry_attempts)
            {
                replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
                replication_session_error(session, "failed to get recording position",
                    AERON_ARCHIVE_ERROR_GENERIC);
                return 0;
            }
            session->time_of_last_action_ms = now_ms;
        }
        return 0;
    }

    session->retry_attempts = AERON_ARCHIVE_REPLICATION_RETRY_ATTEMPTS;
    session->src_recording_position = position;

    if (AERON_ARCHIVE_NULL_POSITION == position && NULL != session->live_destination)
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
        replication_session_error(session, "cannot live merge without active source recording",
            AERON_ARCHIVE_ERROR_GENERIC);
        return 0;
    }

    if (replication_session_should_add_live(session))
    {
        if (NULL != session->recording_subscription)
        {
            aeron_subscription_async_add_destination(
                NULL, session->aeron, session->recording_subscription, session->live_destination);
        }
        session->is_live_added = true;
    }
    else if (replication_session_should_stop_replay(session))
    {
        /* Remove replay destination and complete the merge */
        if (NULL != session->recording_subscription && NULL != session->replay_destination)
        {
            aeron_subscription_async_remove_destination(
                NULL, session->aeron, session->recording_subscription, session->replay_destination);
        }

        free(session->replay_destination);
        session->replay_destination = NULL;
        session->recording_subscription = NULL;

        int64_t image_position = aeron_image_position(session->image);
        replication_session_signal(session, image_position, AERON_ARCHIVE_RECORDING_SIGNAL_CODE_MERGE);
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
    }

    work_count = 1;
    return work_count;
}

static void replication_session_poll_source_archive_events(
    aeron_archive_replication_session_t *session,
    int64_t now_ms)
{
    if (NULL != session->src_archive &&
        AERON_ARCHIVE_REPLICATION_STATE_DONE != session->state &&
        AERON_NULL_VALUE == session->active_correlation_id)
    {
        if (now_ms > (session->time_of_last_scheduled_src_poll_ms +
            AERON_ARCHIVE_REPLICATION_SOURCE_POLL_INTERVAL_MS))
        {
            session->time_of_last_scheduled_src_poll_ms = now_ms;

            char error_buffer[1024];
            int rc = aeron_archive_poll_for_error_response(
                session->src_archive, error_buffer, sizeof(error_buffer));
            if (rc > 0)
            {
                replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, now_ms);
                session->error_message = strdup(error_buffer);
                replication_session_error(session, error_buffer, AERON_ARCHIVE_ERROR_GENERIC);
            }
        }
    }
}

/*
 * ---------------------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------------------
 */

int aeron_archive_replication_session_create(
    aeron_archive_replication_session_t **session,
    int64_t src_recording_id,
    int64_t dst_recording_id,
    int64_t channel_tag_id,
    int64_t subscription_tag_id,
    int64_t replication_id,
    int64_t stop_position,
    const char *live_destination,
    const char *replication_channel,
    int32_t file_io_max_length,
    int32_t replication_session_id,
    const aeron_archive_recording_summary_t *recording_summary,
    aeron_archive_context_t *src_archive_ctx,
    int64_t cached_epoch_clock_ms,
    aeron_archive_catalog_t *catalog,
    aeron_archive_control_session_t *control_session,
    aeron_archive_conductor_t *conductor,
    aeron_t *aeron)
{
    aeron_archive_replication_session_t *s = NULL;

    if (aeron_alloc((void **)&s, sizeof(aeron_archive_replication_session_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate replication session");
        return -1;
    }

    memset(s, 0, sizeof(aeron_archive_replication_session_t));

    s->replication_id = replication_id;
    s->src_recording_id = src_recording_id;
    s->dst_recording_id = dst_recording_id;
    s->replication_session_id = replication_session_id;
    s->file_io_max_length = file_io_max_length;

    s->live_destination = strdup_or_null(live_destination);
    s->replication_channel = strdup(replication_channel);

    s->aeron = aeron;
    s->src_archive_ctx = src_archive_ctx;
    s->catalog = catalog;
    s->control_session = control_session;
    s->conductor = conductor;

    /* Compute action timeout from the context's message timeout (ns -> ms) */
    uint64_t message_timeout_ns = aeron_archive_context_get_message_timeout_ns(src_archive_ctx);
    s->action_timeout_ms = (int64_t)(message_timeout_ns / 1000000ULL);
    if (0 == s->action_timeout_ms)
    {
        s->action_timeout_ms = 5000; /* default 5 seconds */
    }

    s->dst_stop_position = stop_position;

    /* Tag handling: mirror Java logic */
    s->is_tagged = (AERON_NULL_VALUE != channel_tag_id || AERON_NULL_VALUE != subscription_tag_id);
    s->channel_tag_id = (AERON_NULL_VALUE == channel_tag_id) ? replication_id : channel_tag_id;
    s->subscription_tag_id = (AERON_NULL_VALUE == subscription_tag_id) ? replication_id : subscription_tag_id;

    /* Initialise positions */
    s->active_correlation_id = AERON_NULL_VALUE;
    s->src_replay_session_id = AERON_NULL_VALUE;
    s->replay_position = AERON_ARCHIVE_NULL_POSITION;
    s->src_stop_position = AERON_ARCHIVE_NULL_POSITION;
    s->src_recording_position = AERON_ARCHIVE_NULL_POSITION;
    s->replay_token = AERON_NULL_VALUE;
    s->response_publication_registration_id = AERON_NULL_VALUE;

    /* Timing */
    s->time_of_last_action_ms = cached_epoch_clock_ms;
    s->time_of_last_scheduled_src_poll_ms = 0;

    /* Retry */
    s->retry_attempts = AERON_ARCHIVE_REPLICATION_RETRY_ATTEMPTS;

    /* Flags */
    s->is_live_added = false;
    s->is_destination_recording_empty = false;

    /* State */
    s->state = AERON_ARCHIVE_REPLICATION_STATE_CONNECT;

    /* If a recording summary is provided, extract positions from existing destination recording */
    if (NULL != recording_summary)
    {
        s->replay_position = recording_summary->stop_position;
        s->replay_stream_id = recording_summary->stream_id;
        s->is_destination_recording_empty =
            (recording_summary->start_position == recording_summary->stop_position);
    }

    /* Pointers init */
    s->async_connect = NULL;
    s->src_archive = NULL;
    s->recording_subscription = NULL;
    s->image = NULL;
    s->response_publication = NULL;
    s->replay_destination = NULL;
    s->error_message = NULL;

    *session = s;
    return 0;
}

int aeron_archive_replication_session_do_work(
    aeron_archive_replication_session_t *session,
    int64_t cached_epoch_clock_ms)
{
    int work_count = 0;

    /* Check if recording subscription has been closed externally */
    if (NULL != session->recording_subscription &&
        aeron_subscription_is_closed(session->recording_subscription))
    {
        replication_session_set_state(session, AERON_ARCHIVE_REPLICATION_STATE_DONE, cached_epoch_clock_ms);
        return 1;
    }

    switch (session->state)
    {
        case AERON_ARCHIVE_REPLICATION_STATE_CONNECT:
            work_count += replication_step_connect(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_REPLICATE_DESCRIPTOR:
            work_count += replication_step_replicate_descriptor(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_SRC_RECORDING_POSITION:
            work_count += replication_step_src_recording_position(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_EXTEND:
            work_count += replication_step_extend(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_REPLAY_TOKEN:
            work_count += replication_step_replay_token(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_GET_ARCHIVE_PROXY:
            work_count += replication_step_get_archive_proxy(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_REPLAY:
            work_count += replication_step_replay(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_AWAIT_IMAGE:
            work_count += replication_step_await_image(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_REPLICATE:
            work_count += replication_step_replicate(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_CATCHUP:
            work_count += replication_step_catchup(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_ATTEMPT_LIVE_JOIN:
            work_count += replication_step_attempt_live_join(session, cached_epoch_clock_ms);
            break;

        case AERON_ARCHIVE_REPLICATION_STATE_DONE:
            break;
    }

    replication_session_poll_source_archive_events(session, cached_epoch_clock_ms);

    return work_count;
}

int aeron_archive_replication_session_close(aeron_archive_replication_session_t *session)
{
    if (NULL == session)
    {
        return 0;
    }

    /* Stop local recording subscription */
    replication_session_stop_recording(session);

    /* Stop replay on the source archive */
    replication_session_stop_replay_on_source(session);

    /* Close the async connect if still pending */
    if (NULL != session->async_connect)
    {
        /* The async connect does not have a dedicated close in the C client;
         * closing the source archive context handles cleanup. */
        session->async_connect = NULL;
    }

    /* Close the source archive connection */
    if (NULL != session->src_archive)
    {
        aeron_archive_close(session->src_archive);
        session->src_archive = NULL;
    }

    /* Close response publication if created */
    if (NULL != session->response_publication)
    {
        aeron_exclusive_publication_close(session->response_publication, NULL, NULL);
        session->response_publication = NULL;
    }

    /* Signal REPLICATE_END */
    replication_session_signal(session, AERON_ARCHIVE_NULL_POSITION,
        (aeron_archive_recording_signal_code_t)7); /* REPLICATE_END = 7 in Java */

    /* Free owned strings */
    free(session->replication_channel);
    free(session->live_destination);
    free(session->replay_destination);
    free(session->error_message);

    aeron_free(session);
    return 0;
}

void aeron_archive_replication_session_abort(
    aeron_archive_replication_session_t *session,
    const char *reason)
{
    if (NULL != session)
    {
        session->state = AERON_ARCHIVE_REPLICATION_STATE_DONE;
        session->active_correlation_id = AERON_NULL_VALUE;
    }
}

bool aeron_archive_replication_session_is_done(const aeron_archive_replication_session_t *session)
{
    return NULL != session && AERON_ARCHIVE_REPLICATION_STATE_DONE == session->state;
}

int64_t aeron_archive_replication_session_id(const aeron_archive_replication_session_t *session)
{
    return NULL != session ? session->replication_id : AERON_NULL_VALUE;
}

aeron_archive_replication_session_state_t aeron_archive_replication_session_state(
    const aeron_archive_replication_session_t *session)
{
    return NULL != session ? session->state : AERON_ARCHIVE_REPLICATION_STATE_DONE;
}

int64_t aeron_archive_replication_session_dst_recording_id(
    const aeron_archive_replication_session_t *session)
{
    return NULL != session ? session->dst_recording_id : AERON_NULL_VALUE;
}

int64_t aeron_archive_replication_session_src_recording_id(
    const aeron_archive_replication_session_t *session)
{
    return NULL != session ? session->src_recording_id : AERON_NULL_VALUE;
}

const char *aeron_archive_replication_session_error_message(
    const aeron_archive_replication_session_t *session)
{
    return NULL != session ? session->error_message : NULL;
}
