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
#include "aeron_cluster_cm_snapshot_taker.h"
#include "aeron_consensus_module_configuration.h"
#include "util/aeron_error.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/snapshotMarker.h"
#include "aeron_cluster_client/clusterSession.h"
#include "aeron_cluster_client/timer.h"
#include "aeron_cluster_client/consensusModule.h"
#include "aeron_cluster_client/pendingMessageTracker.h"

#define SNAP_BUF_LEN (64 * 1024)
static uint8_t g_snap_buf[SNAP_BUF_LEN];

static int snap_offer(aeron_exclusive_publication_t *pub, size_t length)
{
    int64_t result;
    do {
        result = aeron_exclusive_publication_offer(pub, g_snap_buf, length, NULL, NULL);
    } while (AERON_PUBLICATION_BACK_PRESSURED == result || AERON_PUBLICATION_ADMIN_ACTION == result);
    if (result < 0)
    {
        AERON_SET_ERR(-1, "CM snapshot offer failed: %lld", (long long)result);
        return -1;
    }
    return 0;
}

static int write_marker(aeron_exclusive_publication_t *pub,
                        int64_t log_position, int64_t leadership_term_id,
                        int32_t app_version, int32_t mark)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_snapshotMarker msg;
    if (NULL == aeron_cluster_client_snapshotMarker_wrap_and_apply_header(
        &msg, (char *)g_snap_buf, 0, SNAP_BUF_LEN, &hdr))
    {
        AERON_SET_ERR(-1, "%s", "failed to wrap CM snapshotMarker");
        return -1;
    }

    aeron_cluster_client_snapshotMarker_set_typeId(&msg, AERON_CM_SNAPSHOT_TYPE_ID);
    aeron_cluster_client_snapshotMarker_set_logPosition(&msg, log_position);
    aeron_cluster_client_snapshotMarker_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_snapshotMarker_set_index(&msg, 0);
    aeron_cluster_client_snapshotMarker_set_mark(&msg, (enum aeron_cluster_client_snapshotMark)mark);
    aeron_cluster_client_snapshotMarker_set_appVersion(&msg, app_version);

    return snap_offer(pub,
        aeron_cluster_client_messageHeader_encoded_length() +
        aeron_cluster_client_snapshotMarker_encoded_length(&msg));
}

int aeron_cluster_cm_snapshot_taker_mark_begin(aeron_exclusive_publication_t *pub,
    int64_t log_position, int64_t leadership_term_id, int32_t app_version)
{
    return write_marker(pub, log_position, leadership_term_id, app_version,
        AERON_CM_SNAPSHOT_MARK_BEGIN);
}

int aeron_cluster_cm_snapshot_taker_mark_end(aeron_exclusive_publication_t *pub,
    int64_t log_position, int64_t leadership_term_id, int32_t app_version)
{
    return write_marker(pub, log_position, leadership_term_id, app_version,
        AERON_CM_SNAPSHOT_MARK_END);
}

int aeron_cluster_cm_snapshot_taker_snapshot_session(
    aeron_exclusive_publication_t *pub,
    aeron_cluster_cluster_session_t *session)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_clusterSession msg;
    if (NULL == aeron_cluster_client_clusterSession_wrap_and_apply_header(
        &msg, (char *)g_snap_buf, 0, SNAP_BUF_LEN, &hdr))
    {
        AERON_SET_ERR(-1, "%s", "failed to wrap clusterSession snapshot");
        return -1;
    }

    aeron_cluster_client_clusterSession_set_clusterSessionId(&msg, session->id);
    aeron_cluster_client_clusterSession_set_correlationId(&msg, session->correlation_id);
    aeron_cluster_client_clusterSession_set_openedLogPosition(&msg, session->opened_log_position);
    aeron_cluster_client_clusterSession_set_timeOfLastActivity(&msg, AERON_NULL_VALUE);
    aeron_cluster_client_clusterSession_set_closeReason(&msg,
        (enum aeron_cluster_client_closeReason)session->close_reason);
    aeron_cluster_client_clusterSession_set_responseStreamId(&msg, session->response_stream_id);

    const char *ch = session->response_channel != NULL ? session->response_channel : "";
    aeron_cluster_client_clusterSession_put_responseChannel(&msg, ch, (uint32_t)strlen(ch));

    return snap_offer(pub,
        aeron_cluster_client_messageHeader_encoded_length() +
        aeron_cluster_client_clusterSession_encoded_length(&msg));
}

int aeron_cluster_cm_snapshot_taker_snapshot_timer(
    aeron_exclusive_publication_t *pub,
    int64_t correlation_id, int64_t deadline_ns)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_timer msg;
    if (NULL == aeron_cluster_client_timer_wrap_and_apply_header(
        &msg, (char *)g_snap_buf, 0, SNAP_BUF_LEN, &hdr))
    {
        AERON_SET_ERR(-1, "%s", "failed to wrap timer snapshot");
        return -1;
    }

    aeron_cluster_client_timer_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_timer_set_deadline(&msg, deadline_ns);

    return snap_offer(pub,
        aeron_cluster_client_messageHeader_encoded_length() +
        aeron_cluster_client_timer_encoded_length(&msg));
}

int aeron_cluster_cm_snapshot_taker_snapshot_cm_state(
    aeron_exclusive_publication_t *pub,
    int64_t next_session_id,
    int64_t next_service_session_id,
    int64_t log_service_session_id,
    int32_t pending_message_capacity)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_consensusModule msg;
    if (NULL == aeron_cluster_client_consensusModule_wrap_and_apply_header(
        &msg, (char *)g_snap_buf, 0, SNAP_BUF_LEN, &hdr))
    {
        AERON_SET_ERR(-1, "%s", "failed to wrap consensusModule snapshot");
        return -1;
    }

    aeron_cluster_client_consensusModule_set_nextSessionId(&msg, next_session_id);
    aeron_cluster_client_consensusModule_set_nextServiceSessionId(&msg, next_service_session_id);
    aeron_cluster_client_consensusModule_set_logServiceSessionId(&msg, log_service_session_id);
    aeron_cluster_client_consensusModule_set_pendingMessageCapacity(&msg, pending_message_capacity);

    return snap_offer(pub,
        aeron_cluster_client_messageHeader_encoded_length() +
        aeron_cluster_client_consensusModule_encoded_length(&msg));
}

int aeron_cluster_cm_snapshot_taker_snapshot_pending_tracker(
    aeron_exclusive_publication_t *pub,
    int64_t next_service_session_id,
    int64_t log_service_session_id,
    int32_t pending_message_capacity,
    int32_t service_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_pendingMessageTracker msg;
    if (NULL == aeron_cluster_client_pendingMessageTracker_wrap_and_apply_header(
        &msg, (char *)g_snap_buf, 0, SNAP_BUF_LEN, &hdr))
    {
        AERON_SET_ERR(-1, "%s", "failed to wrap pendingMessageTracker snapshot");
        return -1;
    }

    aeron_cluster_client_pendingMessageTracker_set_nextServiceSessionId(&msg, next_service_session_id);
    aeron_cluster_client_pendingMessageTracker_set_logServiceSessionId(&msg, log_service_session_id);
    aeron_cluster_client_pendingMessageTracker_set_pendingMessageCapacity(&msg, pending_message_capacity);
    aeron_cluster_client_pendingMessageTracker_set_serviceId(&msg, service_id);

    return snap_offer(pub,
        aeron_cluster_client_messageHeader_encoded_length() +
        aeron_cluster_client_pendingMessageTracker_encoded_length(&msg));
}
