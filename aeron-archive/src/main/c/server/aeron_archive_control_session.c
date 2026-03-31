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
#include <stdio.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_archive_control_session.h"
#include "aeron_archive_conductor.h"

static const char *AERON_ARCHIVE_CONTROL_SESSION_CLOSED_MSG = "session closed";
static const char *AERON_ARCHIVE_CONTROL_SESSION_RESPONSE_NOT_CONNECTED_MSG =
    "control response publication is not connected";
static const char *AERON_ARCHIVE_CONTROL_SESSION_REJECTED_MSG = "authentication rejected";

static inline void aeron_archive_control_session_set_state(
    aeron_archive_control_session_t *session,
    aeron_archive_control_session_state_t new_state)
{
    session->state = new_state;
}

static inline bool aeron_archive_control_session_has_no_activity(
    const aeron_archive_control_session_t *session, int64_t now_ms)
{
    return AERON_NULL_VALUE != session->activity_deadline_ms && now_ms > session->activity_deadline_ms;
}

static inline void aeron_archive_control_session_update_activity_deadline(
    aeron_archive_control_session_t *session, int64_t now_ms)
{
    if (AERON_NULL_VALUE == session->activity_deadline_ms)
    {
        session->activity_deadline_ms = now_ms + session->connect_timeout_ms;
    }
}

static void aeron_archive_control_session_attempt_to_activate(aeron_archive_control_session_t *session)
{
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_AUTHENTICATED == session->state &&
        NULL == session->invalid_version_message)
    {
        aeron_archive_control_session_set_state(session, AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE);
    }
}

static void aeron_archive_control_session_drain_response_queue(aeron_archive_control_session_t *session)
{
    aeron_archive_control_session_pending_response_t *entry = session->response_queue_head;
    while (NULL != entry)
    {
        aeron_archive_control_session_pending_response_t *next = entry->next;
        if (NULL != entry->error_message)
        {
            aeron_free(entry->error_message);
        }
        aeron_free(entry);
        entry = next;
    }
    session->response_queue_head = NULL;
    session->response_queue_tail = NULL;
}

static void aeron_archive_control_session_queue_response(
    aeron_archive_control_session_t *session,
    int64_t correlation_id,
    int64_t relevant_id,
    aeron_archive_control_response_code_t code,
    const char *error_message)
{
    aeron_archive_control_session_pending_response_t *entry = NULL;
    if (aeron_alloc((void **)&entry, sizeof(aeron_archive_control_session_pending_response_t)) < 0)
    {
        return;
    }

    entry->correlation_id = correlation_id;
    entry->relevant_id = relevant_id;
    entry->code = code;
    entry->error_message = NULL;
    entry->next = NULL;

    if (NULL != error_message)
    {
        const size_t len = strlen(error_message);
        if (aeron_alloc((void **)&entry->error_message, len + 1) == 0)
        {
            memcpy(entry->error_message, error_message, len + 1);
        }
    }

    if (NULL == session->response_queue_tail)
    {
        session->response_queue_head = entry;
        session->response_queue_tail = entry;
    }
    else
    {
        session->response_queue_tail->next = entry;
        session->response_queue_tail = entry;
    }
}

/* State machine step functions */

static int aeron_archive_control_session_init_step(
    aeron_archive_control_session_t *session, int64_t now_ms)
{
    int work_count = 0;

    if (NULL == session->control_publication)
    {
        aeron_exclusive_publication_t *publication = NULL;
        if (aeron_async_add_exclusive_publication_poll(&publication,
            (aeron_async_add_exclusive_publication_t *)(uintptr_t)session->control_publication_registration_id) > 0 &&
            NULL != publication)
        {
            session->control_publication = publication;
            work_count++;
        }
    }

    if (NULL != session->control_publication)
    {
        session->activity_deadline_ms = now_ms + session->connect_timeout_ms;
        aeron_archive_control_session_set_state(session, AERON_ARCHIVE_CONTROL_SESSION_STATE_CONNECTING);
    }

    return work_count;
}

static int aeron_archive_control_session_wait_for_connection(
    aeron_archive_control_session_t *session, int64_t now_ms)
{
    int work_count = 0;

    if (aeron_exclusive_publication_is_connected(session->control_publication))
    {
        aeron_archive_control_session_set_state(session, AERON_ARCHIVE_CONTROL_SESSION_STATE_CONNECTED);
        work_count++;
    }

    return work_count;
}

static int aeron_archive_control_session_send_connect_response(
    aeron_archive_control_session_t *session, int64_t now_ms)
{
    int work_count = 0;

    if (now_ms > session->resend_deadline_ms)
    {
        session->resend_deadline_ms = now_ms + AERON_ARCHIVE_CONTROL_SESSION_RESEND_INTERVAL_MS;

        if (NULL != session->invalid_version_message)
        {
            aeron_archive_control_response_proxy_send_response(
                session->control_response_proxy,
                session->control_session_id,
                session->correlation_id,
                session->control_session_id,
                AERON_ARCHIVE_CONTROL_RESPONSE_CODE_ERROR,
                session->invalid_version_message,
                session->control_publication);
        }
        else
        {
            /*
             * In the Java implementation, this calls authenticator.onConnectedSession().
             * For now, auto-authenticate by transitioning to AUTHENTICATED.
             */
            aeron_archive_control_session_authenticate(session, NULL, 0);
        }

        work_count++;
    }

    return work_count;
}

static int aeron_archive_control_session_wait_for_request(
    aeron_archive_control_session_t *session, int64_t now_ms)
{
    int work_count = 0;

    if (now_ms > session->resend_deadline_ms)
    {
        session->resend_deadline_ms = now_ms + AERON_ARCHIVE_CONTROL_SESSION_RESEND_INTERVAL_MS;

        if (aeron_archive_control_response_proxy_send_response(
            session->control_response_proxy,
            session->control_session_id,
            session->correlation_id,
            session->control_session_id,
            AERON_ARCHIVE_CONTROL_RESPONSE_CODE_OK,
            NULL,
            session->control_publication))
        {
            session->activity_deadline_ms = AERON_NULL_VALUE;
            work_count++;
        }
    }

    return work_count;
}

static int aeron_archive_control_session_perform_liveness_check(
    aeron_archive_control_session_t *session, int64_t now_ms)
{
    if (session->session_liveness_check_deadline_ms - now_ms < 0)
    {
        session->session_liveness_check_deadline_ms = now_ms + session->session_liveness_check_interval_ms;

        if (!aeron_archive_control_response_proxy_send_ping(
            session->control_response_proxy,
            session->control_session_id,
            session->control_publication))
        {
            aeron_archive_control_session_update_activity_deadline(session, now_ms);
        }
        else
        {
            session->activity_deadline_ms = AERON_NULL_VALUE;
        }
        return 1;
    }
    return 0;
}

static int aeron_archive_control_session_send_responses(
    aeron_archive_control_session_t *session, int64_t now_ms)
{
    int work_count = 0;

    if (!aeron_exclusive_publication_is_connected(session->control_publication))
    {
        aeron_archive_control_session_abort(session, AERON_ARCHIVE_CONTROL_SESSION_RESPONSE_NOT_CONNECTED_MSG);
        work_count++;
    }
    else
    {
        if (NULL != session->response_queue_head)
        {
            aeron_archive_control_session_pending_response_t *entry = session->response_queue_head;

            if (aeron_archive_control_response_proxy_send_response(
                session->control_response_proxy,
                session->control_session_id,
                entry->correlation_id,
                entry->relevant_id,
                entry->code,
                entry->error_message,
                session->control_publication))
            {
                session->response_queue_head = entry->next;
                if (NULL == session->response_queue_head)
                {
                    session->response_queue_tail = NULL;
                }
                if (NULL != entry->error_message)
                {
                    aeron_free(entry->error_message);
                }
                aeron_free(entry);

                session->activity_deadline_ms = AERON_NULL_VALUE;
                work_count++;
            }
        }
    }

    return work_count;
}

static int aeron_archive_control_session_send_reject(
    aeron_archive_control_session_t *session, int64_t now_ms)
{
    int work_count = 0;

    if (now_ms > session->resend_deadline_ms)
    {
        session->resend_deadline_ms = now_ms + AERON_ARCHIVE_CONTROL_SESSION_RESEND_INTERVAL_MS;

        aeron_archive_control_response_proxy_send_response(
            session->control_response_proxy,
            session->control_session_id,
            session->correlation_id,
            AERON_ARCHIVE_AUTHENTICATION_REJECTED,
            AERON_ARCHIVE_CONTROL_RESPONSE_CODE_ERROR,
            AERON_ARCHIVE_CONTROL_SESSION_REJECTED_MSG,
            session->control_publication);

        work_count++;
    }

    return work_count;
}

/* Public API */

int aeron_archive_control_session_create(
    aeron_archive_control_session_t **session,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t connect_timeout_ms,
    int64_t session_liveness_check_interval_ms,
    int64_t control_publication_registration_id,
    const char *control_publication_channel,
    int32_t control_publication_stream_id,
    const char *invalid_version_message,
    aeron_archive_control_session_adapter_t *control_session_adapter,
    aeron_t *aeron,
    aeron_archive_conductor_t *conductor,
    int64_t cached_epoch_clock_ms,
    aeron_archive_control_response_proxy_t *control_response_proxy)
{
    aeron_archive_control_session_t *_session = NULL;

    if (aeron_alloc((void **)&_session, sizeof(aeron_archive_control_session_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_archive_control_session_t");
        return -1;
    }

    memset(_session, 0, sizeof(aeron_archive_control_session_t));

    _session->control_session_id = control_session_id;
    _session->correlation_id = correlation_id;
    _session->connect_timeout_ms = connect_timeout_ms;
    _session->session_liveness_check_interval_ms = session_liveness_check_interval_ms;
    _session->control_publication_registration_id = control_publication_registration_id;
    _session->control_publication_stream_id = control_publication_stream_id;
    _session->control_session_adapter = control_session_adapter;
    _session->aeron = aeron;
    _session->conductor = conductor;
    _session->control_response_proxy = control_response_proxy;
    _session->state = AERON_ARCHIVE_CONTROL_SESSION_STATE_INIT;
    _session->activity_deadline_ms = cached_epoch_clock_ms + connect_timeout_ms;
    _session->session_liveness_check_deadline_ms = cached_epoch_clock_ms + session_liveness_check_interval_ms;
    _session->resend_deadline_ms = 0;
    _session->control_publication = NULL;
    _session->abort_reason = NULL;
    _session->encoded_principal = NULL;
    _session->encoded_principal_length = 0;
    _session->response_queue_head = NULL;
    _session->response_queue_tail = NULL;

    if (NULL != control_publication_channel)
    {
        const size_t len = strlen(control_publication_channel);
        if (aeron_alloc((void **)&_session->control_publication_channel, len + 1) < 0)
        {
            aeron_free(_session);
            AERON_APPEND_ERR("%s", "Unable to allocate control_publication_channel");
            return -1;
        }
        memcpy(_session->control_publication_channel, control_publication_channel, len + 1);
    }

    if (NULL != invalid_version_message)
    {
        const size_t len = strlen(invalid_version_message);
        if (aeron_alloc((void **)&_session->invalid_version_message, len + 1) < 0)
        {
            aeron_free(_session->control_publication_channel);
            aeron_free(_session);
            AERON_APPEND_ERR("%s", "Unable to allocate invalid_version_message");
            return -1;
        }
        memcpy(_session->invalid_version_message, invalid_version_message, len + 1);
    }

    *session = _session;

    return 0;
}

int aeron_archive_control_session_do_work(
    aeron_archive_control_session_t *session,
    int64_t cached_epoch_clock_ms)
{
    int work_count = 0;
    const int64_t now_ms = cached_epoch_clock_ms;

    if (aeron_archive_control_session_has_no_activity(session, now_ms))
    {
        if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
        {
            aeron_archive_control_session_abort(session,
                "failed to send response within connectTimeoutMs");
        }
        else
        {
            aeron_archive_control_session_abort(session,
                "failed to establish initial connection");
        }
        work_count++;
    }

    switch (session->state)
    {
        case AERON_ARCHIVE_CONTROL_SESSION_STATE_INIT:
            work_count += aeron_archive_control_session_init_step(session, now_ms);
            break;

        case AERON_ARCHIVE_CONTROL_SESSION_STATE_CONNECTING:
            work_count += aeron_archive_control_session_wait_for_connection(session, now_ms);
            break;

        case AERON_ARCHIVE_CONTROL_SESSION_STATE_CONNECTED:
            work_count += aeron_archive_control_session_send_connect_response(session, now_ms);
            break;

        case AERON_ARCHIVE_CONTROL_SESSION_STATE_CHALLENGED:
            /* In a full implementation, the authenticator would be polled here. */
            work_count += 1;
            break;

        case AERON_ARCHIVE_CONTROL_SESSION_STATE_AUTHENTICATED:
            work_count += aeron_archive_control_session_wait_for_request(session, now_ms);
            break;

        case AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE:
            work_count += aeron_archive_control_session_perform_liveness_check(session, now_ms);
            work_count += aeron_archive_control_session_send_responses(session, now_ms);
            break;

        case AERON_ARCHIVE_CONTROL_SESSION_STATE_REJECTED:
            work_count += aeron_archive_control_session_send_reject(session, now_ms);
            break;

        case AERON_ARCHIVE_CONTROL_SESSION_STATE_DONE:
            break;
    }

    return work_count;
}

void aeron_archive_control_session_abort(
    aeron_archive_control_session_t *session,
    const char *reason)
{
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_DONE != session->state)
    {
        if (NULL != reason)
        {
            if (NULL != session->abort_reason)
            {
                aeron_free(session->abort_reason);
            }
            const size_t len = strlen(reason);
            if (aeron_alloc((void **)&session->abort_reason, len + 1) == 0)
            {
                memcpy(session->abort_reason, reason, len + 1);
            }
        }

        aeron_archive_control_session_set_state(session, AERON_ARCHIVE_CONTROL_SESSION_STATE_DONE);
    }
}

int aeron_archive_control_session_close(aeron_archive_control_session_t *session)
{
    if (NULL == session)
    {
        return 0;
    }

    if (NULL != session->control_publication)
    {
        aeron_exclusive_publication_close(session->control_publication, NULL, NULL);
        session->control_publication = NULL;
    }

    aeron_archive_control_session_drain_response_queue(session);

    aeron_free(session->control_publication_channel);
    aeron_free(session->invalid_version_message);
    aeron_free(session->abort_reason);
    aeron_free(session->encoded_principal);
    aeron_free(session);

    return 0;
}

bool aeron_archive_control_session_is_done(const aeron_archive_control_session_t *session)
{
    return AERON_ARCHIVE_CONTROL_SESSION_STATE_DONE == session->state;
}

int64_t aeron_archive_control_session_id(const aeron_archive_control_session_t *session)
{
    return session->control_session_id;
}

aeron_archive_control_session_state_t aeron_archive_control_session_state(
    const aeron_archive_control_session_t *session)
{
    return session->state;
}

aeron_exclusive_publication_t *aeron_archive_control_session_publication(
    const aeron_archive_control_session_t *session)
{
    return session->control_publication;
}

const uint8_t *aeron_archive_control_session_encoded_principal(
    const aeron_archive_control_session_t *session,
    size_t *out_length)
{
    if (NULL != out_length)
    {
        *out_length = session->encoded_principal_length;
    }
    return session->encoded_principal;
}

void aeron_archive_control_session_authenticate(
    aeron_archive_control_session_t *session,
    const uint8_t *encoded_principal,
    size_t encoded_principal_length)
{
    aeron_free(session->encoded_principal);
    session->encoded_principal = NULL;
    session->encoded_principal_length = 0;

    if (NULL != encoded_principal && encoded_principal_length > 0)
    {
        if (aeron_alloc((void **)&session->encoded_principal, encoded_principal_length) == 0)
        {
            memcpy(session->encoded_principal, encoded_principal, encoded_principal_length);
            session->encoded_principal_length = encoded_principal_length;
        }
    }

    session->activity_deadline_ms = AERON_NULL_VALUE;
    aeron_archive_control_session_set_state(session, AERON_ARCHIVE_CONTROL_SESSION_STATE_AUTHENTICATED);
}

void aeron_archive_control_session_challenged(aeron_archive_control_session_t *session)
{
    aeron_archive_control_session_set_state(session, AERON_ARCHIVE_CONTROL_SESSION_STATE_CHALLENGED);
}

void aeron_archive_control_session_reject(aeron_archive_control_session_t *session)
{
    aeron_archive_control_session_set_state(session, AERON_ARCHIVE_CONTROL_SESSION_STATE_REJECTED);
}

/* Request handlers */

void aeron_archive_control_session_on_keep_alive(
    aeron_archive_control_session_t *session, int64_t correlation_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
}

void aeron_archive_control_session_on_challenge_response(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    const uint8_t *encoded_credentials, size_t credentials_length)
{
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_CHALLENGED == session->state)
    {
        session->correlation_id = correlation_id;
        /* In the full implementation, this would call authenticator.onChallengeResponse(). */
    }
}

void aeron_archive_control_session_on_start_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int32_t stream_id, int32_t source_location, bool auto_stop, const char *channel)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_start_recording(
            session->conductor, correlation_id, stream_id,
            (aeron_archive_source_location_t)source_location, auto_stop, channel, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_stop_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int32_t stream_id, const char *channel)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_stop_recording(
            session->conductor, correlation_id, stream_id, channel, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_stop_recording_subscription(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t subscription_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_stop_recording_subscription(
            session->conductor, correlation_id, subscription_id, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_stop_recording_by_identity(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_stop_recording_by_identity(
            session->conductor, correlation_id, recording_id, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_list_recordings(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t from_recording_id, int32_t record_count)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_list_recordings(
            session->conductor, correlation_id, from_recording_id, record_count, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_list_recordings_for_uri(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t from_recording_id, int32_t record_count, int32_t stream_id,
    const uint8_t *channel_fragment, size_t channel_fragment_length)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_list_recordings_for_uri(
            session->conductor, correlation_id, from_recording_id, record_count,
            stream_id, (const char *)channel_fragment, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_list_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_list_recording(
            session->conductor, correlation_id, recording_id, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_find_last_matching_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t min_recording_id, int32_t session_id, int32_t stream_id,
    const uint8_t *channel_fragment, size_t channel_fragment_length)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_find_last_matching_recording(
            session->conductor, correlation_id, min_recording_id, session_id,
            stream_id, (const char *)channel_fragment, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_start_replay(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int64_t position, int64_t length,
    int32_t file_io_max_length, int32_t replay_stream_id, const char *replay_channel)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_start_replay(
            session->conductor, correlation_id, recording_id, position, length,
            replay_stream_id, replay_channel, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_start_bounded_replay(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int64_t position, int64_t length,
    int32_t limit_counter_id, int32_t file_io_max_length,
    int32_t replay_stream_id, const char *replay_channel)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_start_bounded_replay(
            session->conductor, correlation_id, recording_id, position, length,
            limit_counter_id, replay_stream_id, replay_channel, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_stop_replay(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t replay_session_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_stop_replay(
            session->conductor, correlation_id, replay_session_id, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_stop_all_replays(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_stop_all_replays(
            session->conductor, correlation_id, recording_id, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_extend_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int32_t stream_id, int32_t source_location,
    bool auto_stop, const char *channel)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_extend_recording(
            session->conductor, correlation_id, recording_id, stream_id,
            (aeron_archive_source_location_t)source_location, auto_stop, channel, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_get_recording_position(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_get_recording_position(
            session->conductor, correlation_id, recording_id, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_get_stop_position(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_get_stop_position(
            session->conductor, correlation_id, recording_id, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_get_max_recorded_position(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_get_max_recorded_position(
            session->conductor, correlation_id, recording_id, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_get_start_position(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_get_start_position(
            session->conductor, correlation_id, recording_id, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_truncate_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int64_t position)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_truncate_recording(
            session->conductor, correlation_id, recording_id, position, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_purge_recording(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_purge_recording(
            session->conductor, correlation_id, recording_id, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_list_recording_subscriptions(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int32_t pseudo_index, int32_t subscription_count, bool apply_stream_id,
    int32_t stream_id, const char *channel_fragment)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        if (aeron_archive_conductor_list_recording_subscriptions(
            session->conductor, correlation_id, pseudo_index, subscription_count,
            apply_stream_id, stream_id, channel_fragment, session) < 0)
        {
            aeron_archive_control_session_send_error_response(
                session, correlation_id, AERON_ARCHIVE_ERROR_GENERIC, aeron_errmsg());
        }
    }
}

void aeron_archive_control_session_on_replicate(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t src_recording_id, int64_t dst_recording_id, int64_t stop_position,
    int64_t channel_tag_id, int64_t subscription_tag_id,
    int32_t src_control_stream_id, int32_t file_io_max_length,
    int32_t replication_session_id,
    const char *src_control_channel, const char *live_destination,
    const char *replication_channel,
    const uint8_t *encoded_credentials, size_t encoded_credentials_length,
    const char *src_response_channel)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        /* conductor->replicate(...); */
    }
}

void aeron_archive_control_session_on_stop_replication(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t replication_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        /* conductor->stopReplication(correlation_id, replication_id, session); */
    }
}

void aeron_archive_control_session_on_detach_segments(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int64_t new_start_position)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        /* conductor->detachSegments(correlation_id, recording_id, new_start_position, session); */
    }
}

void aeron_archive_control_session_on_delete_detached_segments(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        /* conductor->deleteDetachedSegments(correlation_id, recording_id, session); */
    }
}

void aeron_archive_control_session_on_purge_segments(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t recording_id, int64_t new_start_position)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        /* conductor->purgeSegments(correlation_id, recording_id, new_start_position, session); */
    }
}

void aeron_archive_control_session_on_attach_segments(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        /* conductor->attachSegments(correlation_id, recording_id, session); */
    }
}

void aeron_archive_control_session_on_migrate_segments(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t src_recording_id, int64_t dst_recording_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        /* conductor->migrateSegments(correlation_id, src_recording_id, dst_recording_id, session); */
    }
}

void aeron_archive_control_session_on_archive_id(
    aeron_archive_control_session_t *session, int64_t correlation_id)
{
    aeron_archive_control_session_attempt_to_activate(session);
    if (AERON_ARCHIVE_CONTROL_SESSION_STATE_ACTIVE == session->state)
    {
        int64_t archive_id = 0;
        if (NULL != session->conductor)
        {
            archive_id = session->conductor->ctx->archive_id;
        }
        aeron_archive_control_session_send_ok_response(session, correlation_id, archive_id);
    }
}

/* Response senders */

void aeron_archive_control_session_send_ok_response(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t relevant_id)
{
    aeron_archive_control_session_send_response(
        session, correlation_id, relevant_id, AERON_ARCHIVE_CONTROL_RESPONSE_CODE_OK, NULL);
}

void aeron_archive_control_session_send_error_response(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t relevant_id, const char *error_message)
{
    aeron_archive_control_session_send_response(
        session, correlation_id, relevant_id, AERON_ARCHIVE_CONTROL_RESPONSE_CODE_ERROR, error_message);
}

void aeron_archive_control_session_send_recording_unknown(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t recording_id)
{
    aeron_archive_control_session_send_response(
        session, correlation_id, recording_id, AERON_ARCHIVE_CONTROL_RESPONSE_CODE_RECORDING_UNKNOWN, NULL);
}

void aeron_archive_control_session_send_subscription_unknown(
    aeron_archive_control_session_t *session, int64_t correlation_id)
{
    aeron_archive_control_session_send_response(
        session, correlation_id, 0, AERON_ARCHIVE_CONTROL_RESPONSE_CODE_SUBSCRIPTION_UNKNOWN, NULL);
}

void aeron_archive_control_session_send_response(
    aeron_archive_control_session_t *session, int64_t correlation_id, int64_t relevant_id,
    aeron_archive_control_response_code_t code, const char *error_message)
{
    if (NULL != session->response_queue_head ||
        NULL == session->control_response_proxy ||
        NULL == session->control_publication ||
        !aeron_archive_control_response_proxy_send_response(
            session->control_response_proxy,
            session->control_session_id,
            correlation_id,
            relevant_id,
            code,
            error_message,
            session->control_publication))
    {
        aeron_archive_control_session_queue_response(session, correlation_id, relevant_id, code, error_message);
    }
    else
    {
        session->activity_deadline_ms = AERON_NULL_VALUE;
    }
}

bool aeron_archive_control_session_send_descriptor(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    const uint8_t *descriptor_buffer, size_t descriptor_length)
{
    const bool sent = aeron_archive_control_response_proxy_send_descriptor(
        session->control_response_proxy,
        session->control_session_id,
        correlation_id,
        descriptor_buffer,
        descriptor_length,
        session->control_publication);

    if (!sent)
    {
        /* Will be retried by the caller. */
    }
    else
    {
        session->activity_deadline_ms = AERON_NULL_VALUE;
    }

    return sent;
}

bool aeron_archive_control_session_send_subscription_descriptor(
    aeron_archive_control_session_t *session, int64_t correlation_id,
    int64_t subscription_id, int32_t stream_id, const char *channel)
{
    const bool sent = aeron_archive_control_response_proxy_send_subscription_descriptor(
        session->control_response_proxy,
        session->control_session_id,
        correlation_id,
        subscription_id,
        stream_id,
        channel,
        session->control_publication);

    if (!sent)
    {
        /* Will be retried by the caller. */
    }
    else
    {
        session->activity_deadline_ms = AERON_NULL_VALUE;
    }

    return sent;
}

void aeron_archive_control_session_send_signal(
    aeron_archive_control_session_t *session,
    int64_t correlation_id, int64_t recording_id, int64_t subscription_id,
    int64_t position, aeron_archive_recording_signal_code_t signal)
{
    if (NULL != session->response_queue_head ||
        !aeron_archive_control_response_proxy_send_signal(
            session->control_response_proxy,
            session->control_session_id,
            correlation_id,
            recording_id,
            subscription_id,
            position,
            signal,
            session->control_publication))
    {
        /* Queue not implemented for signals in initial version - will retry in send_responses. */
    }
    else
    {
        session->activity_deadline_ms = AERON_NULL_VALUE;
    }
}
