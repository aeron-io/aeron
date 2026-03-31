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

#ifndef AERON_ARCHIVE_RECORDING_EVENTS_PROXY_H
#define AERON_ARCHIVE_RECORDING_EVENTS_PROXY_H

#include <stdint.h>
#include <stdbool.h>

#include "aeronc.h"

#define AERON_ARCHIVE_RECORDING_EVENTS_PROXY_SEND_ATTEMPTS (3)
#define AERON_ARCHIVE_RECORDING_EVENTS_PROXY_BUFFER_LENGTH (1024)

/*
 * SBE message IDs from the archive codecs schema.
 */
#define AERON_ARCHIVE_RECORDING_STARTED_TEMPLATE_ID  (101)
#define AERON_ARCHIVE_RECORDING_PROGRESS_TEMPLATE_ID (102)
#define AERON_ARCHIVE_RECORDING_STOPPED_TEMPLATE_ID  (103)

/*
 * SBE schema and version constants.
 */
#define AERON_ARCHIVE_RECORDING_EVENTS_SCHEMA_ID      (101)
#define AERON_ARCHIVE_RECORDING_EVENTS_SCHEMA_VERSION (11)

/*
 * SBE message header: blockLength(uint16) + templateId(uint16) + schemaId(uint16) + version(uint16) = 8 bytes.
 */
#define AERON_ARCHIVE_SBE_MESSAGE_HEADER_LENGTH (8)

/*
 * Fixed-size block lengths for messages with only fixed fields.
 * RecordingProgress: recordingId(8) + startPosition(8) + position(8) = 24 bytes.
 * RecordingStopped:  recordingId(8) + startPosition(8) + stopPosition(8) = 24 bytes.
 * RecordingStarted (fixed part): recordingId(8) + startPosition(8) + sessionId(4) + streamId(4) = 24 bytes.
 */
#define AERON_ARCHIVE_RECORDING_PROGRESS_BLOCK_LENGTH (24)
#define AERON_ARCHIVE_RECORDING_STOPPED_BLOCK_LENGTH  (24)
#define AERON_ARCHIVE_RECORDING_STARTED_BLOCK_LENGTH  (24)

typedef struct aeron_archive_recording_events_proxy_stct
{
    aeron_publication_t *publication;
    uint8_t buffer[AERON_ARCHIVE_RECORDING_EVENTS_PROXY_BUFFER_LENGTH];
}
aeron_archive_recording_events_proxy_t;

/**
 * Create a recording events proxy that publishes events via the given publication.
 *
 * @param proxy       out param for the allocated proxy.
 * @param publication the Aeron publication to send events on. May be NULL to disable events.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_recording_events_proxy_create(
    aeron_archive_recording_events_proxy_t **proxy,
    aeron_publication_t *publication);

/**
 * Send a RecordingStarted event.
 *
 * @param proxy           the recording events proxy.
 * @param recording_id    the recording id.
 * @param start_position  the start position.
 * @param session_id      the session id of the image.
 * @param stream_id       the stream id.
 * @param channel         the original channel string.
 * @param source_identity the source identity string.
 */
void aeron_archive_recording_events_proxy_started(
    aeron_archive_recording_events_proxy_t *proxy,
    int64_t recording_id,
    int64_t start_position,
    int32_t session_id,
    int32_t stream_id,
    const char *channel,
    const char *source_identity);

/**
 * Send a RecordingProgress event.
 *
 * @param proxy          the recording events proxy.
 * @param recording_id   the recording id.
 * @param start_position the start position.
 * @param position       the current recorded position.
 * @return true if the event was successfully sent.
 */
bool aeron_archive_recording_events_proxy_progress(
    aeron_archive_recording_events_proxy_t *proxy,
    int64_t recording_id,
    int64_t start_position,
    int64_t position);

/**
 * Send a RecordingStopped event.
 *
 * @param proxy          the recording events proxy.
 * @param recording_id   the recording id.
 * @param start_position the start position.
 * @param stop_position  the stop position.
 */
void aeron_archive_recording_events_proxy_stopped(
    aeron_archive_recording_events_proxy_t *proxy,
    int64_t recording_id,
    int64_t start_position,
    int64_t stop_position);

/**
 * Close and free the recording events proxy. Does NOT close the underlying publication.
 *
 * @param proxy the recording events proxy. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_recording_events_proxy_close(aeron_archive_recording_events_proxy_t *proxy);

#endif /* AERON_ARCHIVE_RECORDING_EVENTS_PROXY_H */
