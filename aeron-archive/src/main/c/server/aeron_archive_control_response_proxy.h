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

#ifndef AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_H
#define AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_H

#include <stdint.h>
#include <stdbool.h>

#include "aeronc.h"

#define AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_SEND_ATTEMPTS (3)
#define AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_BUFFER_LENGTH (8 * 1024)

/*
 * SBE message header: blockLength(uint16) + templateId(uint16) + schemaId(uint16) + version(uint16) = 8 bytes.
 */
#ifndef AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH
#define AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH (8)
#endif

/*
 * SBE schema constants for archive control protocol messages.
 * Schema id=101, version=13.
 */
#define AERON_ARCHIVE_CONTROL_SCHEMA_ID      (101)
#define AERON_ARCHIVE_CONTROL_SCHEMA_VERSION (13)

/*
 * SBE template IDs for server-to-client response messages.
 */
#define AERON_ARCHIVE_CONTROL_RESPONSE_TEMPLATE_ID                 (1)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_TEMPLATE_ID             (22)
#define AERON_ARCHIVE_RECORDING_SUBSCRIPTION_DESCRIPTOR_TEMPLATE_ID (23)
#define AERON_ARCHIVE_RECORDING_SIGNAL_EVENT_TEMPLATE_ID           (24)
#define AERON_ARCHIVE_CHALLENGE_TEMPLATE_ID                        (59)
#define AERON_ARCHIVE_PING_TEMPLATE_ID                             (106)

/*
 * SBE block lengths for fixed-size portions of response messages.
 *
 * ControlResponse: controlSessionId(8) + correlationId(8) + relevantId(8) + code(4) + version(4) = 32
 * RecordingDescriptor: 80 bytes (see catalog.h)
 * RecordingSubscriptionDescriptor: controlSessionId(8) + correlationId(8) + subscriptionId(8) + streamId(4) = 28
 * RecordingSignalEvent: controlSessionId(8) + correlationId(8) + recordingId(8) + subscriptionId(8) + position(8) + signal(4) = 44
 * Challenge: controlSessionId(8) + correlationId(8) + version(4) = 20
 * Ping: controlSessionId(8) = 8
 */
#define AERON_ARCHIVE_CONTROL_RESPONSE_BLOCK_LENGTH                 (32)
#define AERON_ARCHIVE_RECORDING_SUBSCRIPTION_DESCRIPTOR_BLOCK_LENGTH (28)
#define AERON_ARCHIVE_RECORDING_SIGNAL_EVENT_BLOCK_LENGTH           (44)
#define AERON_ARCHIVE_CHALLENGE_BLOCK_LENGTH                        (20)
#define AERON_ARCHIVE_PING_BLOCK_LENGTH                             (8)

/*
 * ControlResponseCode enum values matching the SBE schema.
 */
typedef enum aeron_archive_control_response_code_en
{
    AERON_ARCHIVE_CONTROL_RESPONSE_CODE_OK = 0,
    AERON_ARCHIVE_CONTROL_RESPONSE_CODE_ERROR = 1,
    AERON_ARCHIVE_CONTROL_RESPONSE_CODE_RECORDING_UNKNOWN = 2,
    AERON_ARCHIVE_CONTROL_RESPONSE_CODE_SUBSCRIPTION_UNKNOWN = 3
}
aeron_archive_control_response_code_t;

/*
 * RecordingSignal enum — defined in aeron_archive_conductor.h.
 * Include that header or forward-reference the type.
 */
#include "aeron_archive_conductor.h"

/* Forward declarations */
typedef struct aeron_archive_control_session_stct aeron_archive_control_session_t;

typedef struct aeron_archive_control_response_proxy_stct
{
    uint8_t buffer[AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_BUFFER_LENGTH];
}
aeron_archive_control_response_proxy_t;

/**
 * Create a control response proxy.
 *
 * @param proxy out param for the allocated proxy.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_control_response_proxy_create(
    aeron_archive_control_response_proxy_t **proxy);

/**
 * Close and free the control response proxy.
 *
 * @param proxy the proxy to close. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_control_response_proxy_close(
    aeron_archive_control_response_proxy_t *proxy);

/**
 * Send a control response to the client.
 *
 * @param proxy              the control response proxy.
 * @param control_session_id the control session id.
 * @param correlation_id     the correlation id from the request.
 * @param relevant_id        a relevant id (e.g. recording id).
 * @param code               the response code.
 * @param error_message      the error message (may be NULL).
 * @param publication        the exclusive publication to send on.
 * @return true if the response was sent successfully.
 */
bool aeron_archive_control_response_proxy_send_response(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t relevant_id,
    aeron_archive_control_response_code_t code,
    const char *error_message,
    aeron_exclusive_publication_t *publication);

/**
 * Send a recording descriptor to the client.
 *
 * @param proxy              the control response proxy.
 * @param control_session_id the control session id.
 * @param correlation_id     the correlation id from the request.
 * @param descriptor_buffer  the raw descriptor buffer (from the catalog).
 * @param descriptor_length  the total length of the descriptor including header.
 * @param publication        the exclusive publication to send on.
 * @return true if the descriptor was sent successfully.
 */
bool aeron_archive_control_response_proxy_send_descriptor(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    const uint8_t *descriptor_buffer,
    size_t descriptor_length,
    aeron_exclusive_publication_t *publication);

/**
 * Send a recording subscription descriptor to the client.
 *
 * @param proxy              the control response proxy.
 * @param control_session_id the control session id.
 * @param correlation_id     the correlation id from the request.
 * @param subscription_id    the subscription registration id.
 * @param stream_id          the stream id.
 * @param channel            the stripped channel string.
 * @param publication        the exclusive publication to send on.
 * @return true if the descriptor was sent successfully.
 */
bool aeron_archive_control_response_proxy_send_subscription_descriptor(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t subscription_id,
    int32_t stream_id,
    const char *channel,
    aeron_exclusive_publication_t *publication);

/**
 * Send a challenge to the client.
 *
 * @param proxy              the control response proxy.
 * @param control_session_id the control session id.
 * @param correlation_id     the correlation id.
 * @param encoded_challenge  the encoded challenge bytes.
 * @param challenge_length   the length of the challenge.
 * @param publication        the exclusive publication to send on.
 * @return true if the challenge was sent successfully.
 */
bool aeron_archive_control_response_proxy_send_challenge(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    const uint8_t *encoded_challenge,
    size_t challenge_length,
    aeron_exclusive_publication_t *publication);

/**
 * Send a recording signal event to the client.
 *
 * @param proxy              the control response proxy.
 * @param control_session_id the control session id.
 * @param correlation_id     the correlation id.
 * @param recording_id       the recording id.
 * @param subscription_id    the subscription id.
 * @param position           the position.
 * @param signal             the recording signal.
 * @param publication        the exclusive publication to send on.
 * @return true if the signal was sent successfully.
 */
bool aeron_archive_control_response_proxy_send_signal(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t subscription_id,
    int64_t position,
    aeron_archive_recording_signal_code_t signal,
    aeron_exclusive_publication_t *publication);

/**
 * Send a ping heartbeat message to the client.
 *
 * @param proxy              the control response proxy.
 * @param control_session_id the control session id.
 * @param publication        the exclusive publication to send on.
 * @return true if the ping was sent successfully.
 */
bool aeron_archive_control_response_proxy_send_ping(
    aeron_archive_control_response_proxy_t *proxy,
    int64_t control_session_id,
    aeron_exclusive_publication_t *publication);

#endif /* AERON_ARCHIVE_CONTROL_RESPONSE_PROXY_H */
