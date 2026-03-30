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
#include <errno.h>

#include "aeron_cluster_consensus_adapter.h"
#include "aeron_consensus_module_agent_fwd.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "uri/aeron_uri.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/canvassPosition.h"
#include "aeron_cluster_client/requestVote.h"
#include "aeron_cluster_client/vote.h"
#include "aeron_cluster_client/newLeadershipTerm.h"
#include "aeron_cluster_client/appendPosition.h"
#include "aeron_cluster_client/commitPosition.h"
#include "aeron_cluster_client/catchupPosition.h"
#include "aeron_cluster_client/stopCatchup.h"
#include "aeron_cluster_client/terminationPosition.h"
#include "aeron_cluster_client/terminationAck.h"
#include "aeron_cluster_client/backupQuery.h"
#include "aeron_cluster_client/standbySnapshot.h"
#include "aeron_cluster_client/challengeResponse.h"
#include "aeron_cluster_client/heartbeatRequest.h"

static aeron_controlled_fragment_handler_action_t on_fragment(
    void *clientd, const uint8_t *buffer, size_t length, aeron_header_t *header)
{
    aeron_cluster_consensus_adapter_t *adapter = (aeron_cluster_consensus_adapter_t *)clientd;

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

    switch (template_id)
    {
        case 8: /* ChallengeResponse (forwarded by non-leader from ingress) */
        {
            struct aeron_cluster_client_challengeResponse msg;
            if (NULL == aeron_cluster_client_challengeResponse_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_challengeResponse_sbe_block_length(),
                aeron_cluster_client_challengeResponse_sbe_schema_version(), length))
            { break; }

            uint32_t cred_len = aeron_cluster_client_challengeResponse_encodedCredentials_length(&msg);
            const char *cred_ptr = aeron_cluster_client_challengeResponse_encodedCredentials(&msg);
            aeron_consensus_module_agent_on_consensus_challenge_response(adapter->agent,
                aeron_cluster_client_challengeResponse_correlationId(&msg),
                aeron_cluster_client_challengeResponse_clusterSessionId(&msg),
                (const uint8_t *)cred_ptr, (size_t)cred_len);
            break;
        }

        case 50: /* CanvassPosition */
        {
            struct aeron_cluster_client_canvassPosition msg;
            if (NULL == aeron_cluster_client_canvassPosition_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_canvassPosition_sbe_block_length(),
                aeron_cluster_client_canvassPosition_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_canvass_position(adapter->agent,
                aeron_cluster_client_canvassPosition_logLeadershipTermId(&msg),
                aeron_cluster_client_canvassPosition_logPosition(&msg),
                aeron_cluster_client_canvassPosition_leadershipTermId(&msg),
                aeron_cluster_client_canvassPosition_followerMemberId(&msg),
                aeron_cluster_client_canvassPosition_protocolVersion(&msg));
            break;
        }

        case 51: /* RequestVote */
        {
            struct aeron_cluster_client_requestVote msg;
            if (NULL == aeron_cluster_client_requestVote_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_requestVote_sbe_block_length(),
                aeron_cluster_client_requestVote_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_request_vote(adapter->agent,
                aeron_cluster_client_requestVote_logLeadershipTermId(&msg),
                aeron_cluster_client_requestVote_logPosition(&msg),
                aeron_cluster_client_requestVote_candidateTermId(&msg),
                aeron_cluster_client_requestVote_candidateMemberId(&msg));
            break;
        }

        case 52: /* Vote */
        {
            struct aeron_cluster_client_vote msg;
            if (NULL == aeron_cluster_client_vote_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_vote_sbe_block_length(),
                aeron_cluster_client_vote_sbe_schema_version(), length))
            { break; }

            enum aeron_cluster_client_booleanType vote_val = 0;
            aeron_cluster_client_vote_vote(&msg, &vote_val);

            aeron_consensus_module_agent_on_vote(adapter->agent,
                aeron_cluster_client_vote_candidateTermId(&msg),
                aeron_cluster_client_vote_logLeadershipTermId(&msg),
                aeron_cluster_client_vote_logPosition(&msg),
                aeron_cluster_client_vote_candidateMemberId(&msg),
                aeron_cluster_client_vote_followerMemberId(&msg),
                vote_val == aeron_cluster_client_booleanType_TRUE);
            break;
        }

        case 53: /* NewLeadershipTerm */
        {
            struct aeron_cluster_client_newLeadershipTerm msg;
            if (NULL == aeron_cluster_client_newLeadershipTerm_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_newLeadershipTerm_sbe_block_length(),
                aeron_cluster_client_newLeadershipTerm_sbe_schema_version(), length))
            { break; }

            enum aeron_cluster_client_booleanType is_startup = 0;
            aeron_cluster_client_newLeadershipTerm_isStartup(&msg, &is_startup);

            aeron_consensus_module_agent_on_new_leadership_term(adapter->agent,
                aeron_cluster_client_newLeadershipTerm_logLeadershipTermId(&msg),
                aeron_cluster_client_newLeadershipTerm_nextLeadershipTermId(&msg),
                aeron_cluster_client_newLeadershipTerm_nextTermBaseLogPosition(&msg),
                aeron_cluster_client_newLeadershipTerm_nextLogPosition(&msg),
                aeron_cluster_client_newLeadershipTerm_leadershipTermId(&msg),
                aeron_cluster_client_newLeadershipTerm_termBaseLogPosition(&msg),
                aeron_cluster_client_newLeadershipTerm_logPosition(&msg),
                aeron_cluster_client_newLeadershipTerm_commitPosition(&msg),
                aeron_cluster_client_newLeadershipTerm_leaderRecordingId(&msg),
                aeron_cluster_client_newLeadershipTerm_timestamp(&msg),
                aeron_cluster_client_newLeadershipTerm_leaderMemberId(&msg),
                aeron_cluster_client_newLeadershipTerm_logSessionId(&msg),
                aeron_cluster_client_newLeadershipTerm_appVersion(&msg),
                is_startup == aeron_cluster_client_booleanType_TRUE);
            break;
        }

        case 54: /* AppendPosition */
        {
            struct aeron_cluster_client_appendPosition msg;
            if (NULL == aeron_cluster_client_appendPosition_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_appendPosition_sbe_block_length(),
                aeron_cluster_client_appendPosition_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_append_position(adapter->agent,
                aeron_cluster_client_appendPosition_leadershipTermId(&msg),
                aeron_cluster_client_appendPosition_logPosition(&msg),
                aeron_cluster_client_appendPosition_followerMemberId(&msg),
                aeron_cluster_client_appendPosition_flags(&msg));
            break;
        }

        case 55: /* CommitPosition */
        {
            struct aeron_cluster_client_commitPosition msg;
            if (NULL == aeron_cluster_client_commitPosition_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_commitPosition_sbe_block_length(),
                aeron_cluster_client_commitPosition_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_commit_position(adapter->agent,
                aeron_cluster_client_commitPosition_leadershipTermId(&msg),
                aeron_cluster_client_commitPosition_logPosition(&msg),
                aeron_cluster_client_commitPosition_leaderMemberId(&msg));
            break;
        }

        case 56: /* CatchupPosition */
        {
            struct aeron_cluster_client_catchupPosition msg;
            if (NULL == aeron_cluster_client_catchupPosition_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_catchupPosition_sbe_block_length(),
                aeron_cluster_client_catchupPosition_sbe_schema_version(), length))
            { break; }

            char catchup_ep[AERON_URI_MAX_LENGTH];
            uint32_t ep_len = aeron_cluster_client_catchupPosition_catchupEndpoint_length(&msg);
            if (ep_len >= sizeof(catchup_ep)) { ep_len = sizeof(catchup_ep) - 1; }
            aeron_cluster_client_catchupPosition_get_catchupEndpoint(&msg, catchup_ep, ep_len);
            catchup_ep[ep_len] = '\0';

            aeron_consensus_module_agent_on_catchup_position(adapter->agent,
                aeron_cluster_client_catchupPosition_leadershipTermId(&msg),
                aeron_cluster_client_catchupPosition_logPosition(&msg),
                aeron_cluster_client_catchupPosition_followerMemberId(&msg),
                catchup_ep);
            break;
        }

        case 57: /* StopCatchup */
        {
            struct aeron_cluster_client_stopCatchup msg;
            if (NULL == aeron_cluster_client_stopCatchup_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_stopCatchup_sbe_block_length(),
                aeron_cluster_client_stopCatchup_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_stop_catchup(adapter->agent,
                aeron_cluster_client_stopCatchup_leadershipTermId(&msg),
                aeron_cluster_client_stopCatchup_followerMemberId(&msg));
            break;
        }

        case 75: /* TerminationPosition */
        {
            struct aeron_cluster_client_terminationPosition msg;
            if (NULL == aeron_cluster_client_terminationPosition_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_terminationPosition_sbe_block_length(),
                aeron_cluster_client_terminationPosition_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_termination_position(adapter->agent,
                aeron_cluster_client_terminationPosition_leadershipTermId(&msg),
                aeron_cluster_client_terminationPosition_logPosition(&msg));
            break;
        }

        case 76: /* TerminationAck */
        {
            struct aeron_cluster_client_terminationAck msg;
            if (NULL == aeron_cluster_client_terminationAck_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_terminationAck_sbe_block_length(),
                aeron_cluster_client_terminationAck_sbe_schema_version(), length))
            { break; }

            aeron_consensus_module_agent_on_termination_ack(adapter->agent,
                aeron_cluster_client_terminationAck_leadershipTermId(&msg),
                aeron_cluster_client_terminationAck_logPosition(&msg),
                aeron_cluster_client_terminationAck_memberId(&msg));
            break;
        }

        case 77: /* BackupQuery */
        {
            struct aeron_cluster_client_backupQuery msg;
            if (NULL == aeron_cluster_client_backupQuery_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_backupQuery_sbe_block_length(),
                aeron_cluster_client_backupQuery_sbe_schema_version(), length))
            { break; }

            char response_channel[AERON_URI_MAX_LENGTH];
            uint32_t ch_len = aeron_cluster_client_backupQuery_responseChannel_length(&msg);
            if (ch_len >= sizeof(response_channel)) { ch_len = sizeof(response_channel) - 1; }
            aeron_cluster_client_backupQuery_get_responseChannel(&msg, response_channel, ch_len);
            response_channel[ch_len] = '\0';

            const char *creds_ptr = aeron_cluster_client_backupQuery_encodedCredentials(&msg);
            uint32_t creds_len    = aeron_cluster_client_backupQuery_encodedCredentials_length(&msg);

            aeron_consensus_module_agent_on_backup_query(adapter->agent,
                aeron_cluster_client_backupQuery_correlationId(&msg),
                aeron_cluster_client_backupQuery_responseStreamId(&msg),
                aeron_cluster_client_backupQuery_version(&msg),
                aeron_cluster_client_backupQuery_logPosition(&msg),
                response_channel,
                (const uint8_t *)creds_ptr, (size_t)creds_len);
            break;
        }

        case 79: /* HeartbeatRequest */
        {
            struct aeron_cluster_client_heartbeatRequest msg;
            if (NULL == aeron_cluster_client_heartbeatRequest_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_heartbeatRequest_sbe_block_length(),
                aeron_cluster_client_heartbeatRequest_sbe_schema_version(), length))
            { break; }

            char hb_ch[AERON_URI_MAX_LENGTH];
            uint32_t hb_ch_len = aeron_cluster_client_heartbeatRequest_responseChannel_length(&msg);
            if (hb_ch_len >= sizeof(hb_ch)) { hb_ch_len = sizeof(hb_ch) - 1; }
            aeron_cluster_client_heartbeatRequest_get_responseChannel(&msg, hb_ch, hb_ch_len);
            hb_ch[hb_ch_len] = '\0';

            const char *hb_creds = aeron_cluster_client_heartbeatRequest_encodedCredentials(&msg);
            uint32_t hb_creds_len = aeron_cluster_client_heartbeatRequest_encodedCredentials_length(&msg);

            aeron_consensus_module_agent_on_heartbeat_request(adapter->agent,
                aeron_cluster_client_heartbeatRequest_correlationId(&msg),
                aeron_cluster_client_heartbeatRequest_responseStreamId(&msg),
                hb_ch,
                (const uint8_t *)hb_creds, (size_t)hb_creds_len);
            break;
        }

        case 81: /* StandbySnapshot */
        {
            struct aeron_cluster_client_standbySnapshot msg;
            if (NULL == aeron_cluster_client_standbySnapshot_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_standbySnapshot_sbe_block_length(),
                aeron_cluster_client_standbySnapshot_sbe_schema_version(), length))
            { break; }

            char resp_ch[AERON_URI_MAX_LENGTH];
            uint32_t rch_len = aeron_cluster_client_standbySnapshot_responseChannel_length(&msg);
            if (rch_len >= sizeof(resp_ch)) { rch_len = sizeof(resp_ch) - 1; }
            aeron_cluster_client_standbySnapshot_get_responseChannel(&msg, resp_ch, rch_len);
            resp_ch[rch_len] = '\0';

            /* Decode repeating group into stack arrays (max 32 per message) */
#define STANDBY_SNAP_MAX 32
            int64_t  rec_ids[STANDBY_SNAP_MAX];
            int64_t  term_ids[STANDBY_SNAP_MAX];
            int64_t  base_positions[STANDBY_SNAP_MAX];
            int64_t  log_positions[STANDBY_SNAP_MAX];
            int64_t  timestamps[STANDBY_SNAP_MAX];
            int32_t  svc_ids[STANDBY_SNAP_MAX];
            int snap_count = 0;

            struct aeron_cluster_client_standbySnapshot_snapshots grp;
            memset(&grp, 0, sizeof(grp));
            aeron_cluster_client_standbySnapshot_get_snapshots(&msg, &grp);
            while (aeron_cluster_client_standbySnapshot_snapshots_has_next(&grp) &&
                   snap_count < STANDBY_SNAP_MAX)
            {
                aeron_cluster_client_standbySnapshot_snapshots_next(&grp);
                rec_ids[snap_count]       = aeron_cluster_client_standbySnapshot_snapshots_recordingId(&grp);
                term_ids[snap_count]      = aeron_cluster_client_standbySnapshot_snapshots_leadershipTermId(&grp);
                base_positions[snap_count]= aeron_cluster_client_standbySnapshot_snapshots_termBaseLogPosition(&grp);
                log_positions[snap_count] = aeron_cluster_client_standbySnapshot_snapshots_logPosition(&grp);
                timestamps[snap_count]    = aeron_cluster_client_standbySnapshot_snapshots_timestamp(&grp);
                svc_ids[snap_count]       = aeron_cluster_client_standbySnapshot_snapshots_serviceId(&grp);
                snap_count++;
            }
#undef STANDBY_SNAP_MAX

            aeron_consensus_module_agent_on_standby_snapshot(adapter->agent,
                aeron_cluster_client_standbySnapshot_correlationId(&msg),
                aeron_cluster_client_standbySnapshot_responseStreamId(&msg),
                aeron_cluster_client_standbySnapshot_version(&msg),
                resp_ch,
                rec_ids, term_ids, base_positions, log_positions, timestamps, svc_ids,
                snap_count);
            break;
        }

        default:
            break;
    }

    return AERON_ACTION_CONTINUE;
}

int aeron_cluster_consensus_adapter_create(
    aeron_cluster_consensus_adapter_t **adapter,
    aeron_subscription_t *subscription,
    aeron_consensus_module_agent_t *agent)
{
    aeron_cluster_consensus_adapter_t *_adapter = NULL;
    if (aeron_alloc((void **)&_adapter, sizeof(aeron_cluster_consensus_adapter_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate consensus adapter");
        return -1;
    }
    _adapter->subscription = subscription;
    _adapter->agent        = agent;

    if (aeron_controlled_fragment_assembler_create(
        &_adapter->fragment_assembler, on_fragment, _adapter) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to create assembler for consensus adapter");
        aeron_free(_adapter);
        return -1;
    }

    *adapter = _adapter;
    return 0;
}

int aeron_cluster_consensus_adapter_close(aeron_cluster_consensus_adapter_t *adapter)
{
    if (NULL != adapter)
    {
        aeron_controlled_fragment_assembler_delete(adapter->fragment_assembler);
        aeron_free(adapter);
    }
    return 0;
}

int aeron_cluster_consensus_adapter_poll(aeron_cluster_consensus_adapter_t *adapter)
{
    return aeron_subscription_controlled_poll(
        adapter->subscription,
        aeron_controlled_fragment_assembler_handler,
        adapter->fragment_assembler,
        AERON_CLUSTER_CONSENSUS_ADAPTER_FRAGMENT_LIMIT);
}
