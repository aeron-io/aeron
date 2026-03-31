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

#ifndef AERON_ARCHIVE_CONTROL_SESSION_ADAPTER_H
#define AERON_ARCHIVE_CONTROL_SESSION_ADAPTER_H

#include <stdint.h>
#include <stdbool.h>

#include "aeronc.h"
#include "aeron_archive_control_session.h"

/* Forward declarations */
typedef struct aeron_archive_conductor_stct aeron_archive_conductor_t;

#define AERON_ARCHIVE_CONTROL_SESSION_ADAPTER_FRAGMENT_LIMIT (10)

/*
 * SBE template IDs for client-to-server request messages, from the archive codecs schema.
 */
#define AERON_ARCHIVE_CONNECT_REQUEST_TEMPLATE_ID                    (2)
#define AERON_ARCHIVE_CLOSE_SESSION_REQUEST_TEMPLATE_ID              (3)
#define AERON_ARCHIVE_START_RECORDING_REQUEST_TEMPLATE_ID            (4)
#define AERON_ARCHIVE_STOP_RECORDING_REQUEST_TEMPLATE_ID             (5)
#define AERON_ARCHIVE_REPLAY_REQUEST_TEMPLATE_ID                     (6)
#define AERON_ARCHIVE_STOP_REPLAY_REQUEST_TEMPLATE_ID                (7)
#define AERON_ARCHIVE_LIST_RECORDINGS_REQUEST_TEMPLATE_ID            (8)
#define AERON_ARCHIVE_LIST_RECORDINGS_FOR_URI_REQUEST_TEMPLATE_ID    (9)
#define AERON_ARCHIVE_LIST_RECORDING_REQUEST_TEMPLATE_ID             (10)
#define AERON_ARCHIVE_EXTEND_RECORDING_REQUEST_TEMPLATE_ID           (11)
#define AERON_ARCHIVE_RECORDING_POSITION_REQUEST_TEMPLATE_ID         (12)
#define AERON_ARCHIVE_TRUNCATE_RECORDING_REQUEST_TEMPLATE_ID         (13)
#define AERON_ARCHIVE_STOP_RECORDING_SUBSCRIPTION_REQUEST_TEMPLATE_ID (14)
#define AERON_ARCHIVE_STOP_POSITION_REQUEST_TEMPLATE_ID              (15)
#define AERON_ARCHIVE_FIND_LAST_MATCHING_RECORDING_REQUEST_TEMPLATE_ID (16)
#define AERON_ARCHIVE_LIST_RECORDING_SUBSCRIPTIONS_REQUEST_TEMPLATE_ID (17)
#define AERON_ARCHIVE_BOUNDED_REPLAY_REQUEST_TEMPLATE_ID             (18)
#define AERON_ARCHIVE_STOP_ALL_REPLAYS_REQUEST_TEMPLATE_ID           (19)
#define AERON_ARCHIVE_REPLICATE_REQUEST_TEMPLATE_ID                  (50)
#define AERON_ARCHIVE_STOP_REPLICATION_REQUEST_TEMPLATE_ID           (51)
#define AERON_ARCHIVE_START_POSITION_REQUEST_TEMPLATE_ID             (52)
#define AERON_ARCHIVE_DETACH_SEGMENTS_REQUEST_TEMPLATE_ID            (53)
#define AERON_ARCHIVE_DELETE_DETACHED_SEGMENTS_REQUEST_TEMPLATE_ID   (54)
#define AERON_ARCHIVE_PURGE_SEGMENTS_REQUEST_TEMPLATE_ID             (55)
#define AERON_ARCHIVE_ATTACH_SEGMENTS_REQUEST_TEMPLATE_ID            (56)
#define AERON_ARCHIVE_MIGRATE_SEGMENTS_REQUEST_TEMPLATE_ID           (57)
#define AERON_ARCHIVE_AUTH_CONNECT_REQUEST_TEMPLATE_ID               (58)
#define AERON_ARCHIVE_CHALLENGE_RESPONSE_TEMPLATE_ID                 (60)
#define AERON_ARCHIVE_KEEP_ALIVE_REQUEST_TEMPLATE_ID                 (61)
#define AERON_ARCHIVE_TAGGED_REPLICATE_REQUEST_TEMPLATE_ID           (62)
#define AERON_ARCHIVE_START_RECORDING_REQUEST2_TEMPLATE_ID           (63)
#define AERON_ARCHIVE_EXTEND_RECORDING_REQUEST2_TEMPLATE_ID          (64)
#define AERON_ARCHIVE_STOP_RECORDING_BY_IDENTITY_REQUEST_TEMPLATE_ID (65)
#define AERON_ARCHIVE_REPLICATE_REQUEST2_TEMPLATE_ID                 (66)
#define AERON_ARCHIVE_MAX_RECORDED_POSITION_REQUEST_TEMPLATE_ID      (67)
#define AERON_ARCHIVE_ARCHIVE_ID_REQUEST_TEMPLATE_ID                 (68)
#define AERON_ARCHIVE_PURGE_RECORDING_REQUEST_TEMPLATE_ID            (104)
#define AERON_ARCHIVE_REPLAY_TOKEN_REQUEST_TEMPLATE_ID               (105)
#define AERON_ARCHIVE_UPDATE_CHANNEL_REQUEST_TEMPLATE_ID             (107)

/**
 * Entry in the control session map.
 */
typedef struct aeron_archive_control_session_info_stct
{
    int64_t control_session_id;
    aeron_archive_control_session_t *control_session;
    aeron_image_t *image;
    struct aeron_archive_control_session_info_stct *next;
}
aeron_archive_control_session_info_t;

/**
 * Decodes SBE control requests from the control subscription and routes them
 * to the appropriate handler on ControlSession or ArchiveConductor.
 */
typedef struct aeron_archive_control_session_adapter_stct
{
    aeron_subscription_t *control_subscription;
    aeron_subscription_t *local_control_subscription;
    aeron_archive_conductor_t *conductor;
    aeron_fragment_assembler_t *fragment_assembler;

    aeron_archive_control_session_info_t *session_map_head;
}
aeron_archive_control_session_adapter_t;

/**
 * Create a control session adapter.
 *
 * @param adapter                    out param for the allocated adapter.
 * @param control_subscription       the control subscription (may be NULL for local-only).
 * @param local_control_subscription the local control subscription.
 * @param conductor                  the archive conductor.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_control_session_adapter_create(
    aeron_archive_control_session_adapter_t **adapter,
    aeron_subscription_t *control_subscription,
    aeron_subscription_t *local_control_subscription,
    aeron_archive_conductor_t *conductor);

/**
 * Poll the control subscriptions for new fragments.
 *
 * @param adapter the control session adapter.
 * @return the number of fragments read.
 */
int aeron_archive_control_session_adapter_poll(
    aeron_archive_control_session_adapter_t *adapter);

/**
 * Add a control session to the adapter's tracking map.
 *
 * @param adapter  the control session adapter.
 * @param session  the control session.
 * @param image    the associated image.
 */
void aeron_archive_control_session_adapter_add_session(
    aeron_archive_control_session_adapter_t *adapter,
    aeron_archive_control_session_t *session,
    aeron_image_t *image);

/**
 * Remove a control session from the adapter's tracking map.
 *
 * @param adapter            the control session adapter.
 * @param control_session_id the session id to remove.
 * @param session_aborted    whether the session was aborted.
 * @param abort_reason       the abort reason (may be NULL).
 */
void aeron_archive_control_session_adapter_remove_session(
    aeron_archive_control_session_adapter_t *adapter,
    int64_t control_session_id,
    bool session_aborted,
    const char *abort_reason);

/**
 * Find and abort control sessions associated with the given image.
 *
 * @param adapter the control session adapter.
 * @param image   the unavailable image.
 */
void aeron_archive_control_session_adapter_abort_by_image(
    aeron_archive_control_session_adapter_t *adapter,
    aeron_image_t *image);

/**
 * Close and free the adapter and all resources.
 *
 * @param adapter the control session adapter. May be NULL.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_control_session_adapter_close(
    aeron_archive_control_session_adapter_t *adapter);

#endif /* AERON_ARCHIVE_CONTROL_SESSION_ADAPTER_H */
