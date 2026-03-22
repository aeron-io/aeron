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

#include "aeron_cluster_pending_message_tracker.h"

int aeron_cluster_pending_message_tracker_init(
    aeron_cluster_pending_message_tracker_t *tracker,
    int32_t service_id,
    int64_t next_service_session_id,
    int64_t log_service_session_id,
    int64_t pending_message_capacity)
{
    tracker->service_id               = service_id;
    tracker->next_service_session_id  = next_service_session_id;
    tracker->log_service_session_id   = log_service_session_id;
    tracker->pending_message_capacity = pending_message_capacity;
    return 0;
}

bool aeron_cluster_pending_message_tracker_should_append(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id)
{
    /* During replay: skip messages already committed to the log */
    return cluster_session_id > tracker->log_service_session_id;
}

void aeron_cluster_pending_message_tracker_on_appended(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id)
{
    if (cluster_session_id >= tracker->next_service_session_id)
    {
        tracker->next_service_session_id = cluster_session_id + 1;
    }
}

void aeron_cluster_pending_message_tracker_on_committed(
    aeron_cluster_pending_message_tracker_t *tracker,
    int64_t cluster_session_id)
{
    if (cluster_session_id > tracker->log_service_session_id)
    {
        tracker->log_service_session_id = cluster_session_id;
    }
}
