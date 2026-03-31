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
#include "aeron_cluster_consensus_publisher.h"

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
#include "aeron_cluster_client/backupResponse.h"
#include "aeron_cluster_client/heartbeatResponse.h"
#include "aeron_cluster_client/challengeResponse.h"

static uint8_t g_buf[AERON_CLUSTER_CONSENSUS_PUBLISHER_BUFFER_LENGTH];

static bool pub_offer(aeron_exclusive_publication_t *pub,
                      size_t body_length)
{
    const size_t total = aeron_cluster_client_messageHeader_encoded_length() + body_length;
    int64_t result = aeron_exclusive_publication_offer(pub, g_buf, total, NULL, NULL);
    return result > 0;
}

bool aeron_cluster_consensus_publisher_canvass_position(
    aeron_exclusive_publication_t *pub,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id,
    int32_t follower_member_id, int32_t protocol_version)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_canvassPosition msg;
    if (NULL == aeron_cluster_client_canvassPosition_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_canvassPosition_set_logLeadershipTermId(&msg, log_leadership_term_id);
    aeron_cluster_client_canvassPosition_set_logPosition(&msg, log_position);
    aeron_cluster_client_canvassPosition_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_canvassPosition_set_followerMemberId(&msg, follower_member_id);
    aeron_cluster_client_canvassPosition_set_protocolVersion(&msg, protocol_version);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_canvassPosition_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_request_vote(
    aeron_exclusive_publication_t *pub,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_requestVote msg;
    if (NULL == aeron_cluster_client_requestVote_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_requestVote_set_logLeadershipTermId(&msg, log_leadership_term_id);
    aeron_cluster_client_requestVote_set_logPosition(&msg, log_position);
    aeron_cluster_client_requestVote_set_candidateTermId(&msg, candidate_term_id);
    aeron_cluster_client_requestVote_set_candidateMemberId(&msg, candidate_member_id);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_requestVote_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_vote(
    aeron_exclusive_publication_t *pub,
    int64_t candidate_term_id, int64_t log_leadership_term_id,
    int64_t log_position, int32_t candidate_member_id,
    int32_t follower_member_id, bool vote)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_vote msg;
    if (NULL == aeron_cluster_client_vote_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_vote_set_candidateTermId(&msg, candidate_term_id);
    aeron_cluster_client_vote_set_logLeadershipTermId(&msg, log_leadership_term_id);
    aeron_cluster_client_vote_set_logPosition(&msg, log_position);
    aeron_cluster_client_vote_set_candidateMemberId(&msg, candidate_member_id);
    aeron_cluster_client_vote_set_followerMemberId(&msg, follower_member_id);
    aeron_cluster_client_vote_set_vote(&msg,
        vote ? aeron_cluster_client_booleanType_TRUE : aeron_cluster_client_booleanType_FALSE);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_vote_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_new_leadership_term(
    aeron_exclusive_publication_t *pub,
    int64_t log_leadership_term_id,
    int64_t next_leadership_term_id,
    int64_t next_term_base_log_position,
    int64_t next_log_position,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t log_position,
    int64_t leader_recording_id,
    int64_t timestamp,
    int32_t leader_member_id,
    int32_t log_session_id,
    int32_t app_version,
    bool is_startup)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_newLeadershipTerm msg;
    if (NULL == aeron_cluster_client_newLeadershipTerm_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_newLeadershipTerm_set_logLeadershipTermId(&msg, log_leadership_term_id);
    aeron_cluster_client_newLeadershipTerm_set_nextLeadershipTermId(&msg, next_leadership_term_id);
    aeron_cluster_client_newLeadershipTerm_set_nextTermBaseLogPosition(&msg, next_term_base_log_position);
    aeron_cluster_client_newLeadershipTerm_set_nextLogPosition(&msg, next_log_position);
    aeron_cluster_client_newLeadershipTerm_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_newLeadershipTerm_set_termBaseLogPosition(&msg, term_base_log_position);
    aeron_cluster_client_newLeadershipTerm_set_logPosition(&msg, log_position);
    aeron_cluster_client_newLeadershipTerm_set_leaderRecordingId(&msg, leader_recording_id);
    aeron_cluster_client_newLeadershipTerm_set_timestamp(&msg, timestamp);
    aeron_cluster_client_newLeadershipTerm_set_leaderMemberId(&msg, leader_member_id);
    aeron_cluster_client_newLeadershipTerm_set_logSessionId(&msg, log_session_id);
    aeron_cluster_client_newLeadershipTerm_set_appVersion(&msg, app_version);
    aeron_cluster_client_newLeadershipTerm_set_isStartup(&msg,
        is_startup ? aeron_cluster_client_booleanType_TRUE : aeron_cluster_client_booleanType_FALSE);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_newLeadershipTerm_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_append_position(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, int32_t flags)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_appendPosition msg;
    if (NULL == aeron_cluster_client_appendPosition_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_appendPosition_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_appendPosition_set_logPosition(&msg, log_position);
    aeron_cluster_client_appendPosition_set_followerMemberId(&msg, follower_member_id);
    aeron_cluster_client_appendPosition_set_flags(&msg, (int8_t)flags);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_appendPosition_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_commit_position(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id, int64_t log_position,
    int32_t leader_member_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_commitPosition msg;
    if (NULL == aeron_cluster_client_commitPosition_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_commitPosition_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_commitPosition_set_logPosition(&msg, log_position);
    aeron_cluster_client_commitPosition_set_leaderMemberId(&msg, leader_member_id);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_commitPosition_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_catchup_position(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, const char *catchup_endpoint)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_catchupPosition msg;
    if (NULL == aeron_cluster_client_catchupPosition_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_catchupPosition_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_catchupPosition_set_logPosition(&msg, log_position);
    aeron_cluster_client_catchupPosition_set_followerMemberId(&msg, follower_member_id);
    const char *ep = catchup_endpoint != NULL ? catchup_endpoint : "";
    aeron_cluster_client_catchupPosition_put_catchupEndpoint(&msg, ep, (uint32_t)strlen(ep));

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_catchupPosition_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_stop_catchup(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id, int32_t follower_member_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_stopCatchup msg;
    if (NULL == aeron_cluster_client_stopCatchup_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_stopCatchup_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_stopCatchup_set_followerMemberId(&msg, follower_member_id);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_stopCatchup_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_termination_position(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id, int64_t log_position)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_terminationPosition msg;
    if (NULL == aeron_cluster_client_terminationPosition_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_terminationPosition_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_terminationPosition_set_logPosition(&msg, log_position);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_terminationPosition_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_termination_ack(
    aeron_exclusive_publication_t *pub,
    int64_t leadership_term_id, int64_t log_position, int32_t member_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_terminationAck msg;
    if (NULL == aeron_cluster_client_terminationAck_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_terminationAck_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_terminationAck_set_logPosition(&msg, log_position);
    aeron_cluster_client_terminationAck_set_memberId(&msg, member_id);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_terminationAck_encoded_length(&msg));
}

/* -----------------------------------------------------------------------
 * Broadcast helpers
 * ----------------------------------------------------------------------- */
void aeron_cluster_consensus_publisher_broadcast_canvass_position(
    aeron_cluster_member_t *members, int count, int32_t self_id,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id, int32_t follower_member_id,
    int32_t protocol_version)
{
    for (int i = 0; i < count; i++)
    {
        if (members[i].id != self_id && NULL != members[i].publication)
        {
            aeron_cluster_consensus_publisher_canvass_position(
                members[i].publication,
                log_leadership_term_id, log_position,
                leadership_term_id, follower_member_id, protocol_version);
        }
    }
}

void aeron_cluster_consensus_publisher_broadcast_request_vote(
    aeron_cluster_member_t *members, int count, int32_t self_id,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id)
{
    for (int i = 0; i < count; i++)
    {
        if (members[i].id != self_id && NULL != members[i].publication)
        {
            aeron_cluster_consensus_publisher_request_vote(
                members[i].publication,
                log_leadership_term_id, log_position,
                candidate_term_id, candidate_member_id);
        }
    }
}

void aeron_cluster_consensus_publisher_broadcast_new_leadership_term(
    aeron_cluster_member_t *members, int count, int32_t self_id,
    int64_t log_leadership_term_id, int64_t next_leadership_term_id,
    int64_t next_term_base_log_position, int64_t next_log_position,
    int64_t leadership_term_id, int64_t term_base_log_position,
    int64_t log_position, int64_t leader_recording_id, int64_t timestamp,
    int32_t leader_member_id, int32_t log_session_id,
    int32_t app_version, bool is_startup)
{
    for (int i = 0; i < count; i++)
    {
        if (members[i].id != self_id && NULL != members[i].publication)
        {
            aeron_cluster_consensus_publisher_new_leadership_term(
                members[i].publication,
                log_leadership_term_id, next_leadership_term_id,
                next_term_base_log_position, next_log_position,
                leadership_term_id, term_base_log_position,
                log_position, leader_recording_id, timestamp,
                leader_member_id, log_session_id, app_version, is_startup);
        }
    }
}

void aeron_cluster_consensus_publisher_broadcast_commit_position(
    aeron_cluster_member_t *members, int count, int32_t self_id,
    int64_t leadership_term_id, int64_t log_position, int32_t leader_member_id)
{
    for (int i = 0; i < count; i++)
    {
        if (members[i].id != self_id && NULL != members[i].publication)
        {
            aeron_cluster_consensus_publisher_commit_position(
                members[i].publication,
                leadership_term_id, log_position, leader_member_id);
        }
    }
}

bool aeron_cluster_consensus_publisher_backup_query(
    aeron_exclusive_publication_t *pub,
    int64_t correlation_id,
    int32_t response_stream_id,
    int32_t version,
    int64_t log_position,
    const char *response_channel,
    const uint8_t *encoded_credentials,
    size_t encoded_credentials_length)
{
    if (NULL == pub) { return false; }

    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_backupQuery msg;
    if (NULL == aeron_cluster_client_backupQuery_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_backupQuery_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_backupQuery_set_responseStreamId(&msg, response_stream_id);
    aeron_cluster_client_backupQuery_set_version(&msg, version);
    aeron_cluster_client_backupQuery_set_logPosition(&msg, log_position);

    const size_t chan_len = response_channel ? strlen(response_channel) : 0;
    aeron_cluster_client_backupQuery_put_responseChannel(
        &msg, response_channel ? response_channel : "", (uint32_t)chan_len);
    aeron_cluster_client_backupQuery_put_encodedCredentials(
        &msg, (const char *)encoded_credentials, (uint32_t)encoded_credentials_length);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_backupQuery_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_backup_response(
    aeron_exclusive_publication_t *pub,
    int64_t correlation_id,
    int64_t log_recording_id,
    int64_t log_leadership_term_id,
    int64_t log_term_base_log_position,
    int32_t commit_position_counter_id,
    int32_t leader_member_id,
    int32_t member_id,
    const aeron_cluster_backup_response_snapshot_t *snapshots,
    int snapshot_count,
    const char *cluster_members)
{
    if (NULL == pub) { return false; }

    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_backupResponse msg;
    if (NULL == aeron_cluster_client_backupResponse_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_backupResponse_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_backupResponse_set_logRecordingId(&msg, log_recording_id);
    aeron_cluster_client_backupResponse_set_logLeadershipTermId(&msg, log_leadership_term_id);
    aeron_cluster_client_backupResponse_set_logTermBaseLogPosition(&msg, log_term_base_log_position);
    aeron_cluster_client_backupResponse_set_lastLeadershipTermId(&msg, log_leadership_term_id);
    aeron_cluster_client_backupResponse_set_lastTermBaseLogPosition(&msg, log_term_base_log_position);
    aeron_cluster_client_backupResponse_set_commitPositionCounterId(&msg, commit_position_counter_id);
    aeron_cluster_client_backupResponse_set_leaderMemberId(&msg, leader_member_id);
    aeron_cluster_client_backupResponse_set_memberId(&msg, member_id);

    struct aeron_cluster_client_backupResponse_snapshots snaps;
    aeron_cluster_client_backupResponse_snapshots_set_count(&msg, &snaps, (uint16_t)snapshot_count);
    for (int i = 0; i < snapshot_count; i++)
    {
        aeron_cluster_client_backupResponse_snapshots_next(&snaps);
        aeron_cluster_client_backupResponse_snapshots_set_recordingId(&snaps, snapshots[i].recording_id);
        aeron_cluster_client_backupResponse_snapshots_set_leadershipTermId(&snaps, snapshots[i].leadership_term_id);
        aeron_cluster_client_backupResponse_snapshots_set_termBaseLogPosition(&snaps, snapshots[i].term_base_log_position);
        aeron_cluster_client_backupResponse_snapshots_set_logPosition(&snaps, snapshots[i].log_position);
        aeron_cluster_client_backupResponse_snapshots_set_timestamp(&snaps, snapshots[i].timestamp);
        aeron_cluster_client_backupResponse_snapshots_set_serviceId(&snaps, snapshots[i].service_id);
    }

    const size_t members_len = cluster_members ? strlen(cluster_members) : 0;
    aeron_cluster_client_backupResponse_put_clusterMembers(
        &msg, cluster_members ? cluster_members : "", (uint32_t)members_len);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_backupResponse_encoded_length(&msg));
}

bool aeron_cluster_consensus_publisher_heartbeat_response(
    aeron_exclusive_publication_t *session_pub,
    int64_t correlation_id)
{
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_heartbeatResponse msg;
    if (NULL == aeron_cluster_client_heartbeatResponse_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr))
    {
        return false;
    }

    aeron_cluster_client_heartbeatResponse_set_correlationId(&msg, correlation_id);

    const size_t len = aeron_cluster_client_messageHeader_encoded_length() +
                       aeron_cluster_client_heartbeatResponse_encoded_length(&msg);

    return aeron_exclusive_publication_offer(session_pub, g_buf, len, NULL, NULL) > 0;
}

bool aeron_cluster_consensus_publisher_challenge_response(
    aeron_exclusive_publication_t *pub,
    int64_t correlation_id,
    int64_t cluster_session_id,
    const uint8_t *encoded_credentials,
    size_t encoded_credentials_length)
{
    if (NULL == pub) { return false; }

    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_challengeResponse msg;
    if (NULL == aeron_cluster_client_challengeResponse_wrap_and_apply_header(
        &msg, (char *)g_buf, 0, sizeof(g_buf), &hdr)) { return false; }

    aeron_cluster_client_challengeResponse_set_correlationId(&msg, correlation_id);
    aeron_cluster_client_challengeResponse_set_clusterSessionId(&msg, cluster_session_id);
    aeron_cluster_client_challengeResponse_put_encodedCredentials(
        &msg, (const char *)encoded_credentials, (uint32_t)encoded_credentials_length);

    return pub_offer(pub, aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_challengeResponse_encoded_length(&msg));
}
