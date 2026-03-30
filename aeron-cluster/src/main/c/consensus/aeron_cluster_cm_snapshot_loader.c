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
#include <inttypes.h>
#include "aeron_cluster_cm_snapshot_loader.h"
#include "aeron_cluster_cm_snapshot_taker.h"    /* for AERON_CM_SNAPSHOT_MARK_* */
#include "aeron_consensus_module_configuration.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/snapshotMarker.h"
#include "aeron_cluster_client/clusterSession.h"
#include "aeron_cluster_client/sessionMessageHeader.h"
#include "aeron_cluster_client/timer.h"
#include "aeron_cluster_client/consensusModule.h"
#include "aeron_cluster_client/clusterMembers.h"
#include "aeron_cluster_client/pendingMessageTracker.h"

/* -----------------------------------------------------------------------
 * Fragment handler — dispatches on SBE template ID.
 * Mirrors Java ConsensusModuleSnapshotAdapter.onFragment().
 * ----------------------------------------------------------------------- */
static aeron_controlled_fragment_handler_action_t on_fragment(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_cluster_cm_snapshot_loader_t *ldr =
        (aeron_cluster_cm_snapshot_loader_t *)clientd;

    if (length < aeron_cluster_client_messageHeader_encoded_length())
    {
        return AERON_ACTION_CONTINUE;
    }

    struct aeron_cluster_client_messageHeader hdr;
    if (NULL == aeron_cluster_client_messageHeader_wrap(
        &hdr, (char *)buffer, 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), length))
    {
        return AERON_ACTION_CONTINUE;
    }

    const uint64_t hdr_len    = aeron_cluster_client_messageHeader_encoded_length();
    const int32_t  template_id = (int32_t)aeron_cluster_client_messageHeader_templateId(&hdr);

    /* ---- SessionMessageHeader (template 1) — pending service messages ---- */
    if (template_id == AERON_CLUSTER_CLIENT_SESSION_MESSAGE_HEADER_SBE_TEMPLATE_ID)
    {
        struct aeron_cluster_client_sessionMessageHeader smh;
        if (NULL == aeron_cluster_client_sessionMessageHeader_wrap_for_decode(
            &smh, (char *)buffer, hdr_len,
            aeron_cluster_client_sessionMessageHeader_sbe_block_length(),
            aeron_cluster_client_sessionMessageHeader_sbe_schema_version(),
            length))
        {
            return AERON_ACTION_CONTINUE;
        }

        int64_t cluster_session_id =
            aeron_cluster_client_sessionMessageHeader_clusterSessionId(&smh);
        int32_t service_id =
            aeron_pending_message_tracker_service_id_from_log_message(cluster_session_id);

        if (NULL != ldr->pending_trackers &&
            service_id >= 0 && service_id < ldr->pending_tracker_count)
        {
            aeron_cluster_pending_message_tracker_append_message(
                &ldr->pending_trackers[service_id],
                buffer,
                0,
                (int)length);
        }

        return AERON_ACTION_CONTINUE;
    }

    /* ---- SnapshotMarker (template 100) ---- */
    if (template_id == AERON_CLUSTER_CLIENT_SNAPSHOT_MARKER_SBE_TEMPLATE_ID)
    {
        struct aeron_cluster_client_snapshotMarker msg;
        if (NULL == aeron_cluster_client_snapshotMarker_wrap_for_decode(
            &msg, (char *)buffer, hdr_len,
            aeron_cluster_client_snapshotMarker_sbe_block_length(),
            aeron_cluster_client_snapshotMarker_sbe_schema_version(),
            length))
        {
            return AERON_ACTION_CONTINUE;
        }

        /* Validate snapshot type ID matches ConsensusModule snapshot */
        int64_t type_id = aeron_cluster_client_snapshotMarker_typeId(&msg);
        if (type_id != AERON_CM_SNAPSHOT_TYPE_ID)
        {
            AERON_SET_ERR(-1, "unexpected snapshot typeId: %" PRId64, type_id);
            ldr->has_error = true;
            return AERON_ACTION_BREAK;
        }

        enum aeron_cluster_client_snapshotMark mark;
        if (aeron_cluster_client_snapshotMarker_mark(&msg, &mark))
        {
            if ((int)mark == AERON_CM_SNAPSHOT_MARK_BEGIN)
            {
                if (ldr->in_snapshot)
                {
                    AERON_SET_ERR(-1, "%s", "already in snapshot — duplicate BEGIN marker");
                    ldr->has_error = true;
                    return AERON_ACTION_BREAK;
                }
                ldr->in_snapshot = true;
                ldr->app_version = aeron_cluster_client_snapshotMarker_appVersion(&msg);
                return AERON_ACTION_CONTINUE;
            }

            if ((int)mark == AERON_CM_SNAPSHOT_MARK_END)
            {
                if (!ldr->in_snapshot)
                {
                    AERON_SET_ERR(-1, "%s", "missing BEGIN snapshot marker");
                    ldr->has_error = true;
                    return AERON_ACTION_BREAK;
                }
                ldr->is_done = true;
                return AERON_ACTION_BREAK;
            }
        }
        return AERON_ACTION_CONTINUE;
    }

    /* ---- ClusterSession (template 103) ---- */
    if (template_id == AERON_CLUSTER_CLIENT_CLUSTER_SESSION_SBE_TEMPLATE_ID)
    {
        if (NULL == ldr->session_manager) { return AERON_ACTION_CONTINUE; }

        struct aeron_cluster_client_clusterSession msg;
        if (NULL == aeron_cluster_client_clusterSession_wrap_for_decode(
            &msg, (char *)buffer, hdr_len,
            aeron_cluster_client_clusterSession_sbe_block_length(),
            aeron_cluster_client_clusterSession_sbe_schema_version(),
            length))
        {
            return AERON_ACTION_CONTINUE;
        }

        int64_t session_id          = aeron_cluster_client_clusterSession_clusterSessionId(&msg);
        int64_t correlation_id      = aeron_cluster_client_clusterSession_correlationId(&msg);
        int64_t opened_log_pos      = aeron_cluster_client_clusterSession_openedLogPosition(&msg);
        int64_t time_of_last_act    = aeron_cluster_client_clusterSession_timeOfLastActivity(&msg);
        int32_t response_stream_id  = aeron_cluster_client_clusterSession_responseStreamId(&msg);

        enum aeron_cluster_client_closeReason close_reason_enum;
        int32_t close_reason = 0;
        if (aeron_cluster_client_clusterSession_closeReason(&msg, &close_reason_enum))
        {
            close_reason = (int32_t)close_reason_enum;
        }

        /* Read the response_channel variable-length field */
        char response_channel[512] = { '\0' };
        uint64_t ch_len = aeron_cluster_client_clusterSession_get_responseChannel(
            &msg, response_channel, sizeof(response_channel) - 1);
        if (ch_len < sizeof(response_channel))
        {
            response_channel[ch_len] = '\0';
        }
        else
        {
            response_channel[sizeof(response_channel) - 1] = '\0';
        }

        aeron_cluster_session_manager_on_load_cluster_session(
            ldr->session_manager,
            session_id,
            correlation_id,
            opened_log_pos,
            time_of_last_act,
            close_reason,
            response_stream_id,
            response_channel);

        return AERON_ACTION_CONTINUE;
    }

    /* ---- Timer (template 104) ---- */
    if (template_id == AERON_CLUSTER_CLIENT_TIMER_SBE_TEMPLATE_ID)
    {
        if (NULL == ldr->timer_service) { return AERON_ACTION_CONTINUE; }

        struct aeron_cluster_client_timer msg;
        if (NULL == aeron_cluster_client_timer_wrap_for_decode(
            &msg, (char *)buffer, hdr_len,
            aeron_cluster_client_timer_sbe_block_length(),
            aeron_cluster_client_timer_sbe_schema_version(),
            length))
        {
            return AERON_ACTION_CONTINUE;
        }

        int64_t correlation_id = aeron_cluster_client_timer_correlationId(&msg);
        int64_t deadline_ns    = aeron_cluster_client_timer_deadline(&msg);
        aeron_cluster_timer_service_schedule(ldr->timer_service, correlation_id, deadline_ns);
        return AERON_ACTION_CONTINUE;
    }

    /* ---- ConsensusModule state (template 105) ---- */
    if (template_id == AERON_CLUSTER_CLIENT_CONSENSUS_MODULE_SBE_TEMPLATE_ID)
    {
        struct aeron_cluster_client_consensusModule msg;
        if (NULL == aeron_cluster_client_consensusModule_wrap_for_decode(
            &msg, (char *)buffer, hdr_len,
            aeron_cluster_client_consensusModule_sbe_block_length(),
            aeron_cluster_client_consensusModule_sbe_schema_version(),
            length))
        {
            return AERON_ACTION_CONTINUE;
        }

        ldr->next_session_id          = aeron_cluster_client_consensusModule_nextSessionId(&msg);
        ldr->next_service_session_id  = aeron_cluster_client_consensusModule_nextServiceSessionId(&msg);
        ldr->log_service_session_id   = aeron_cluster_client_consensusModule_logServiceSessionId(&msg);
        ldr->pending_message_capacity = aeron_cluster_client_consensusModule_pendingMessageCapacity(&msg);
        ldr->has_cm_state             = true;

        /* Update session manager's next session ID if available */
        if (NULL != ldr->session_manager)
        {
            aeron_cluster_session_manager_load_next_session_id(
                ldr->session_manager, ldr->next_session_id);
        }

        /* Load service[0] tracker from the consensusModule record (legacy single-service path).
         * The pendingMessageTracker record handles multi-service snapshots. */
        if (NULL != ldr->pending_trackers && ldr->pending_tracker_count > 0)
        {
            aeron_cluster_pending_message_tracker_load_state(
                &ldr->pending_trackers[0],
                ldr->next_service_session_id,
                ldr->log_service_session_id,
                (int64_t)ldr->pending_message_capacity);
        }
        return AERON_ACTION_CONTINUE;
    }

    /* ---- ClusterMembers (template 106) — ignored, mirrors Java ---- */
    if (template_id == AERON_CLUSTER_CLIENT_CLUSTER_MEMBERS_SBE_TEMPLATE_ID)
    {
        return AERON_ACTION_CONTINUE;
    }

    /* ---- PendingMessageTracker (template 107) ---- */
    if (template_id == AERON_CLUSTER_CLIENT_PENDING_MESSAGE_TRACKER_SBE_TEMPLATE_ID)
    {
        struct aeron_cluster_client_pendingMessageTracker msg;
        if (NULL == aeron_cluster_client_pendingMessageTracker_wrap_for_decode(
            &msg, (char *)buffer, hdr_len,
            aeron_cluster_client_pendingMessageTracker_sbe_block_length(),
            aeron_cluster_client_pendingMessageTracker_sbe_schema_version(),
            length))
        {
            return AERON_ACTION_CONTINUE;
        }

        int32_t service_id = aeron_cluster_client_pendingMessageTracker_serviceId(&msg);
        if (NULL != ldr->pending_trackers &&
            service_id >= 0 && service_id < ldr->pending_tracker_count)
        {
            aeron_cluster_pending_message_tracker_load_state(
                &ldr->pending_trackers[service_id],
                aeron_cluster_client_pendingMessageTracker_nextServiceSessionId(&msg),
                aeron_cluster_client_pendingMessageTracker_logServiceSessionId(&msg),
                (int64_t)aeron_cluster_client_pendingMessageTracker_pendingMessageCapacity(&msg));
        }
        return AERON_ACTION_CONTINUE;
    }

    return AERON_ACTION_CONTINUE;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */
int aeron_cluster_cm_snapshot_loader_create(
    aeron_cluster_cm_snapshot_loader_t **loader,
    aeron_image_t                       *image,
    aeron_cluster_session_manager_t     *session_manager,
    aeron_cluster_timer_service_t       *timer_service,
    aeron_cluster_pending_message_tracker_t *pending_trackers,
    int                                  pending_tracker_count)
{
    aeron_cluster_cm_snapshot_loader_t *l = NULL;
    if (aeron_alloc((void **)&l, sizeof(aeron_cluster_cm_snapshot_loader_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate cm snapshot loader");
        return -1;
    }

    l->image                  = image;
    l->session_manager        = session_manager;
    l->timer_service          = timer_service;
    l->pending_trackers       = pending_trackers;
    l->pending_tracker_count  = pending_tracker_count;
    l->next_session_id        = 1;
    l->next_service_session_id = 0;
    l->log_service_session_id  = 0;
    l->pending_message_capacity = 0;
    l->app_version  = 0;
    l->has_cm_state = false;
    l->in_snapshot  = false;
    l->is_done      = false;
    l->has_error    = false;

    *loader = l;
    return 0;
}

int aeron_cluster_cm_snapshot_loader_poll(
    aeron_cluster_cm_snapshot_loader_t *loader, int fragment_limit)
{
    if (NULL == loader || NULL == loader->image || loader->is_done)
    {
        return 0;
    }

    int frags = aeron_image_controlled_poll(
        loader->image, on_fragment, loader, fragment_limit);

    return frags;
}

void aeron_cluster_cm_snapshot_loader_close(
    aeron_cluster_cm_snapshot_loader_t *loader)
{
    aeron_free(loader);
}
