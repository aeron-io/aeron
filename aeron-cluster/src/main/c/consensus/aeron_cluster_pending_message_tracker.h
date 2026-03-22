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

#ifndef AERON_CLUSTER_PENDING_MESSAGE_TRACKER_H
#define AERON_CLUSTER_PENDING_MESSAGE_TRACKER_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Tracks pending messages from a clustered service to ensure exactly-once
 * delivery across leader changes.  Mirrors Java PendingServiceMessageTracker.
 *
 * Each service has its own tracker identified by service_id.
 * Messages sent by the service via Cluster.offer() get a monotonically
 * increasing serviceSessionId.  On log replay (leader failover), messages
 * with serviceSessionId <= logServiceSessionId have already been committed
 * and must not be re-appended.
 */
typedef struct aeron_cluster_pending_message_tracker_stct
{
    int32_t  service_id;
    int64_t  next_service_session_id;   /* next ID to assign to outgoing messages */
    int64_t  log_service_session_id;    /* highest committed session ID in the log */
    int64_t  pending_message_capacity;
}
aeron_cluster_pending_message_tracker_t;

int  aeron_cluster_pending_message_tracker_init(
    aeron_cluster_pending_message_tracker_t *tracker,
    int32_t service_id,
    int64_t next_service_session_id,
    int64_t log_service_session_id,
    int64_t pending_message_capacity);

/** Returns true if this message should be appended (not already committed). */
bool aeron_cluster_pending_message_tracker_should_append(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id);

/** Advance next_service_session_id after a message is appended. */
void aeron_cluster_pending_message_tracker_on_appended(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id);

/** Called on log replay to update the committed high-water mark. */
void aeron_cluster_pending_message_tracker_on_committed(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_PENDING_MESSAGE_TRACKER_H */
