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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>

#include "aeron_cluster_backup_agent.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "aeron_archive.h"
#include "client/aeron_cluster_configuration.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/backupResponse.h"
#include "aeron_cluster_client/sessionEvent.h"
#include "aeron_cluster_client/challenge.h"
#include "aeron_cluster_client/eventCode.h"
#include "aeron_cluster_client/adminResponseCode.h"

#include "consensus/aeron_cluster_consensus_publisher.h"

#define NULL_VALUE INT64_C(-1)

/* -----------------------------------------------------------------------
 * Helpers
 * ----------------------------------------------------------------------- */
static void agent_set_state(aeron_cluster_backup_agent_t *agent,
                            aeron_cluster_backup_state_t new_state,
                            int64_t now_ms)
{
    if (agent->ctx->on_backup_state_change)
    {
        agent->ctx->on_backup_state_change(
            agent->ctx->events_clientd, agent->state, new_state, now_ms);
    }
    agent->state = new_state;
}

static bool snapshots_to_retrieve_add(aeron_cluster_backup_agent_t *agent,
                                      int64_t recording_id,
                                      int64_t leadership_term_id,
                                      int64_t term_base_log_position,
                                      int64_t log_position,
                                      int64_t timestamp,
                                      int32_t service_id)
{
    if (agent->snapshots_to_retrieve_count == agent->snapshots_to_retrieve_capacity)
    {
        int new_cap = agent->snapshots_to_retrieve_capacity == 0
                      ? 8 : agent->snapshots_to_retrieve_capacity * 2;
        aeron_cluster_backup_snapshot_t *new_arr = NULL;
        if (aeron_alloc((void **)&new_arr,
            (size_t)new_cap * sizeof(aeron_cluster_backup_snapshot_t)) < 0)
        {
            return false;
        }
        if (agent->snapshots_to_retrieve)
        {
            memcpy(new_arr, agent->snapshots_to_retrieve,
                (size_t)agent->snapshots_to_retrieve_count *
                sizeof(aeron_cluster_backup_snapshot_t));
            aeron_free(agent->snapshots_to_retrieve);
        }
        agent->snapshots_to_retrieve          = new_arr;
        agent->snapshots_to_retrieve_capacity = new_cap;
    }

    aeron_cluster_backup_snapshot_t *s =
        &agent->snapshots_to_retrieve[agent->snapshots_to_retrieve_count++];
    s->recording_id          = recording_id;
    s->leadership_term_id    = leadership_term_id;
    s->term_base_log_position = term_base_log_position;
    s->log_position          = log_position;
    s->timestamp             = timestamp;
    s->service_id            = service_id;
    return true;
}

static void reset_agent(aeron_cluster_backup_agent_t *agent)
{
    agent->snapshots_to_retrieve_count = 0;
    agent->snapshots_retrieved_count   = 0;
    agent->live_log_recording_id              = NULL_VALUE;
    agent->live_log_recording_subscription_id = NULL_VALUE;
    agent->live_log_replay_session_id         = NULL_VALUE;
    agent->live_log_recording_counter_id      = -1;
    agent->live_log_recording_session_id      = (int32_t)NULL_VALUE;
    agent->correlation_id                     = NULL_VALUE;
    agent->cluster_log_recording_id           = NULL_VALUE;
    agent->leader_member_id                   = (int32_t)NULL_VALUE;
    agent->log_supplier_member_id             = (int32_t)NULL_VALUE;
    agent->log_supplier_member                = NULL; /* stale after next backupResponse re-parses */
    agent->leader_log_entry_pending           = false;
    agent->leader_last_term_entry_pending     = false;
    agent->current_endpoint[0] = '\0';

    if (NULL != agent->snapshot_replication)
    {
        aeron_cluster_snapshot_replication_close(agent->snapshot_replication);
        agent->snapshot_replication = NULL;
    }

    /* Stop live log replay if one is in progress */
    if (NULL_VALUE != agent->live_log_replay_session_id && NULL != agent->cluster_archive)
    {
        aeron_archive_stop_replay(agent->cluster_archive, agent->live_log_replay_session_id);
        agent->live_log_replay_session_id = NULL_VALUE;
    }

    /* Stop live log recording subscription */
    if (NULL_VALUE != agent->live_log_recording_subscription_id &&
        NULL != agent->ctx->backup_archive)
    {
        bool stopped = false;
        aeron_archive_try_stop_recording_subscription(
            &stopped, agent->ctx->backup_archive, agent->live_log_recording_subscription_id);
        agent->live_log_recording_subscription_id = NULL_VALUE;
    }

    if (NULL != agent->recording_subscription)
    {
        aeron_subscription_close(agent->recording_subscription, NULL, NULL);
        agent->recording_subscription = NULL;
    }

    if (NULL != agent->consensus_pub)
    {
        aeron_exclusive_publication_close(agent->consensus_pub, NULL, NULL);
        agent->consensus_pub = NULL;
    }

    if (NULL != agent->cluster_archive)
    {
        aeron_archive_close(agent->cluster_archive);
        agent->cluster_archive = NULL;
    }
}

/* -----------------------------------------------------------------------
 * LogSourceValidator — mirrors Java LogSourceValidator.isAcceptable()
 * ----------------------------------------------------------------------- */
static bool log_source_is_acceptable(
    aeron_cluster_backup_source_type_t source_type,
    int32_t leader_member_id,
    int32_t member_id)
{
    switch (source_type)
    {
        case AERON_CLUSTER_BACKUP_SOURCE_LEADER:
            return (int32_t)NULL_VALUE != leader_member_id && leader_member_id == member_id;
        case AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER:
            return (int32_t)NULL_VALUE == leader_member_id || leader_member_id != member_id;
        case AERON_CLUSTER_BACKUP_SOURCE_ANY:
        default:
            return true;
    }
}

/* -----------------------------------------------------------------------
 * Fragment handler
 * ----------------------------------------------------------------------- */
static void on_backup_response(
    aeron_cluster_backup_agent_t *agent,
    const uint8_t *buffer,
    size_t length,
    uint64_t hdr_len)
{
    struct aeron_cluster_client_backupResponse msg;
    if (NULL == aeron_cluster_client_backupResponse_wrap_for_decode(
        &msg, (char *)buffer, hdr_len,
        aeron_cluster_client_backupResponse_sbe_block_length(),
        aeron_cluster_client_backupResponse_sbe_schema_version(), length))
    {
        return;
    }

    int64_t corr_id = aeron_cluster_client_backupResponse_correlationId(&msg);
    if (AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY != agent->state ||
        corr_id != agent->correlation_id)
    {
        return;
    }

    agent->cluster_log_recording_id        = aeron_cluster_client_backupResponse_logRecordingId(&msg);
    agent->log_leadership_term_id          = aeron_cluster_client_backupResponse_logLeadershipTermId(&msg);
    agent->log_term_base_log_position      = aeron_cluster_client_backupResponse_logTermBaseLogPosition(&msg);
    agent->last_leadership_term_id         = aeron_cluster_client_backupResponse_lastLeadershipTermId(&msg);
    agent->last_term_base_log_position     = aeron_cluster_client_backupResponse_lastTermBaseLogPosition(&msg);
    agent->leader_commit_position_counter_id = aeron_cluster_client_backupResponse_commitPositionCounterId(&msg);
    agent->leader_member_id                = aeron_cluster_client_backupResponse_leaderMemberId(&msg);
    agent->log_supplier_member_id          = aeron_cluster_client_backupResponse_memberId_in_acting_version(&msg)
        ? aeron_cluster_client_backupResponse_memberId(&msg) : agent->leader_member_id;

    /* LogSourceValidator: reject if the responding member is not an acceptable source type */
    if ((int32_t)NULL_VALUE == agent->log_supplier_member_id)
    {
        return;  /* memberId is null — retry for a compatible node */
    }
    if (!log_source_is_acceptable(
        agent->ctx->source_type, agent->leader_member_id, agent->log_supplier_member_id))
    {
        return;  /* member does not match the configured source type */
    }
    /* If a log supplier was already established, reject a different one */
    if (NULL != agent->log_supplier_member &&
        agent->log_supplier_member->id != agent->log_supplier_member_id)
    {
        return;
    }

    /* Parse cluster members from response and find the log supplier */
    {
        /* Skip past snapshots group before reading clusterMembers VarData */
        struct aeron_cluster_client_backupResponse_snapshots snap_skip;
        if (NULL != aeron_cluster_client_backupResponse_get_snapshots(&msg, &snap_skip))
        {
            uint32_t sc = (uint32_t)aeron_cluster_client_backupResponse_snapshots_count(&snap_skip);
            for (uint32_t si = 0; si < sc; si++)
            {
                aeron_cluster_client_backupResponse_snapshots_next(&snap_skip);
            }
        }

        char members_buf[4096];
        uint64_t members_len = aeron_cluster_client_backupResponse_get_clusterMembers(
            &msg, members_buf, sizeof(members_buf) - 1);
        members_buf[members_len] = '\0';

        if (members_len > 0)
        {
            /* Free previous member list if any */
            if (NULL != agent->active_cluster_members)
            {
                aeron_cluster_members_free(agent->active_cluster_members, agent->active_cluster_member_count);
                agent->active_cluster_members = NULL;
                agent->active_cluster_member_count = 0;
                agent->log_supplier_member = NULL;
            }

            aeron_cluster_member_t *parsed = NULL;
            int parsed_count = 0;
            if (aeron_cluster_members_parse(members_buf, &parsed, &parsed_count) >= 0)
            {
                aeron_cluster_members_set_is_leader(parsed, parsed_count, agent->leader_member_id);
                agent->active_cluster_members      = parsed;
                agent->active_cluster_member_count = parsed_count;
                agent->log_supplier_member = aeron_cluster_member_find_by_id(
                    parsed, parsed_count, agent->log_supplier_member_id);
            }
        }
    }

    /* Pending term entry BEFORE snapshots: log leadership term */
    agent->leader_log_entry_pending                  = true;
    agent->leader_log_entry_recording_id             = agent->cluster_log_recording_id;
    agent->leader_log_entry_leadership_term_id       = agent->log_leadership_term_id;
    agent->leader_log_entry_term_base_log_position   = agent->log_term_base_log_position;
    agent->leader_log_entry_timestamp                = agent->current_now_ms * INT64_C(1000000);

    /* Pending term entry AFTER snapshots: last/current leadership term */
    const aeron_cluster_recording_log_entry_t *local_last =
        aeron_cluster_recording_log_find_last_term(agent->recording_log);
    if (NULL == local_last ||
        agent->last_leadership_term_id != local_last->leadership_term_id)
    {
        agent->leader_last_term_entry_pending                = true;
        agent->leader_last_term_entry_recording_id           = agent->cluster_log_recording_id;
        agent->leader_last_term_entry_leadership_term_id     = agent->last_leadership_term_id;
        agent->leader_last_term_entry_term_base_log_position = agent->last_term_base_log_position;
        agent->leader_last_term_entry_timestamp              = agent->current_now_ms * INT64_C(1000000);
    }
    else
    {
        agent->leader_last_term_entry_pending = false;
    }

    /* Iterate snapshots sub-group */
    struct aeron_cluster_client_backupResponse_snapshots snapshots;
    if (NULL == aeron_cluster_client_backupResponse_get_snapshots(&msg, &snapshots))
    {
        return;
    }
    uint32_t snap_count = (uint32_t)aeron_cluster_client_backupResponse_snapshots_count(&snapshots);

    for (uint32_t i = 0; i < snap_count; i++)
    {
        aeron_cluster_client_backupResponse_snapshots_next(&snapshots);

        int64_t rec_id    = aeron_cluster_client_backupResponse_snapshots_recordingId(&snapshots);
        int64_t term_id   = aeron_cluster_client_backupResponse_snapshots_leadershipTermId(&snapshots);
        int64_t base_pos  = aeron_cluster_client_backupResponse_snapshots_termBaseLogPosition(&snapshots);
        int64_t log_pos   = aeron_cluster_client_backupResponse_snapshots_logPosition(&snapshots);
        int64_t ts        = aeron_cluster_client_backupResponse_snapshots_timestamp(&snapshots);
        int32_t svc_id    = aeron_cluster_client_backupResponse_snapshots_serviceId(&snapshots);

        /* Skip if we already have this snapshot */
        const aeron_cluster_recording_log_entry_t *local =
            aeron_cluster_recording_log_get_latest_snapshot(agent->recording_log, svc_id);
        if (NULL != local && log_pos == local->log_position) { continue; }

        snapshots_to_retrieve_add(agent, rec_id, term_id, base_pos, log_pos, ts, svc_id);
    }

    agent->time_of_last_progress_ms = agent->current_now_ms;

    aeron_cluster_backup_state_t next =
        (agent->snapshots_to_retrieve_count == 0)
        ? AERON_CLUSTER_BACKUP_STATE_LIVE_LOG_RECORD
        : AERON_CLUSTER_BACKUP_STATE_SNAPSHOT_RETRIEVE;

    agent_set_state(agent, next, agent->current_now_ms);
}

static void on_fragment(void *clientd, const uint8_t *buffer,
                        size_t length, aeron_header_t *header)
{
    aeron_cluster_backup_agent_t *agent = (aeron_cluster_backup_agent_t *)clientd;

    if (length < aeron_cluster_client_messageHeader_encoded_length()) { return; }

    struct aeron_cluster_client_messageHeader hdr;
    if (NULL == aeron_cluster_client_messageHeader_wrap(
        &hdr, (char *)buffer, 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), length))
    {
        return;
    }

    const uint64_t hdr_len     = aeron_cluster_client_messageHeader_encoded_length();
    const int32_t  template_id = (int32_t)aeron_cluster_client_messageHeader_templateId(&hdr);

    switch (template_id)
    {
        case AERON_CLUSTER_CLIENT_BACKUP_RESPONSE_SBE_TEMPLATE_ID:
            on_backup_response(agent, buffer, length, hdr_len);
            break;

        case AERON_CLUSTER_CLIENT_CHALLENGE_SBE_TEMPLATE_ID:
        {
            struct aeron_cluster_client_challenge msg;
            if (NULL == aeron_cluster_client_challenge_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_challenge_sbe_block_length(),
                aeron_cluster_client_challenge_sbe_schema_version(), length))
            {
                break;
            }

            const int64_t cluster_session_id = aeron_cluster_client_challenge_clusterSessionId(&msg);
            const uint32_t challenge_len = aeron_cluster_client_challenge_encodedChallenge_length(&msg);
            const char *challenge_data   = aeron_cluster_client_challenge_encodedChallenge(&msg);

            /* Obtain challenge response via optional callback; fall back to static credentials */
            const uint8_t *response_data = agent->ctx->encoded_credentials;
            size_t         response_len  = agent->ctx->encoded_credentials_length;
            size_t         cb_len        = 0;
            if (NULL != agent->ctx->on_challenge)
            {
                response_data = agent->ctx->on_challenge(
                    agent->ctx->on_challenge_clientd,
                    (const uint8_t *)challenge_data, (size_t)challenge_len,
                    &cb_len);
                response_len = cb_len;
            }

            const int64_t new_corr_id = aeron_next_correlation_id(agent->ctx->aeron);
            agent->correlation_id = new_corr_id;
            aeron_cluster_consensus_publisher_challenge_response(
                agent->consensus_pub, new_corr_id, cluster_session_id,
                response_data, response_len);
            break;
        }

        case AERON_CLUSTER_CLIENT_SESSION_EVENT_SBE_TEMPLATE_ID:
        {
            struct aeron_cluster_client_sessionEvent msg;
            if (NULL == aeron_cluster_client_sessionEvent_wrap_for_decode(
                &msg, (char *)buffer, hdr_len,
                aeron_cluster_client_sessionEvent_sbe_block_length(),
                aeron_cluster_client_sessionEvent_sbe_schema_version(), length))
            {
                break;
            }

            if (aeron_cluster_client_sessionEvent_correlationId(&msg) != agent->correlation_id)
            {
                break;
            }

            enum aeron_cluster_client_eventCode code = aeron_cluster_client_eventCode_NULL_VALUE;
            aeron_cluster_client_sessionEvent_code(&msg, &code);

            if (code == aeron_cluster_client_eventCode_REDIRECT)
            {
                /* Transition back to RESET_BACKUP on redirect */
                agent_set_state(agent, AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP, 0);
            }
            break;
        }

        default:
            break;
    }
}

/* -----------------------------------------------------------------------
 * State handlers
 * ----------------------------------------------------------------------- */
static int do_backup_query(aeron_cluster_backup_agent_t *agent, int64_t now_ms)
{
    /* If no publication or response timeout, rotate to next endpoint (round-robin) */
    if (NULL == agent->consensus_pub ||
        now_ms > (agent->time_of_last_backup_query_ms +
                  agent->ctx->backup_response_timeout_ns / 1000000))
    {
        /* Close current publication before switching endpoint */
        if (NULL != agent->consensus_pub)
        {
            aeron_exclusive_publication_close(agent->consensus_pub, NULL, NULL);
            agent->consensus_pub = NULL;
        }

        if (agent->parsed_endpoints_count > 0)
        {
            /* Advance to next endpoint in round-robin order */
            const int idx = agent->next_endpoint_idx % agent->parsed_endpoints_count;
            agent->next_endpoint_idx = (agent->next_endpoint_idx + 1) % agent->parsed_endpoints_count;

            char channel[512];
            snprintf(channel, sizeof(channel), "aeron:udp?endpoint=%s",
                     agent->parsed_endpoints[idx]);
            snprintf(agent->current_endpoint, sizeof(agent->current_endpoint), "%s",
                     agent->parsed_endpoints[idx]);

            aeron_async_add_exclusive_publication_t *async = NULL;
            if (aeron_async_add_exclusive_publication(
                &async, agent->ctx->aeron, channel,
                agent->ctx->consensus_stream_id) < 0)
            {
                return 0;
            }

            int rc = 0;
            do { rc = aeron_async_add_exclusive_publication_poll(
                     &agent->consensus_pub, async); } while (0 == rc);
            if (rc < 0) { agent->consensus_pub = NULL; return 0; }
        }
        agent->correlation_id = NULL_VALUE;
        agent->time_of_last_backup_query_ms = now_ms;
        return 1;
    }

    /* Send backup query once publication is connected */
    if (NULL_VALUE == agent->correlation_id && NULL != agent->consensus_pub)
    {
        const int64_t connected = aeron_exclusive_publication_is_connected(agent->consensus_pub);
        if (!connected) { return 0; }

        int64_t corr_id = aeron_next_correlation_id(agent->ctx->aeron);

        if (aeron_cluster_consensus_publisher_backup_query(
            agent->consensus_pub,
            corr_id,
            agent->ctx->consensus_stream_id,
            aeron_cluster_semantic_version(),
            INT64_MAX,
            agent->ctx->consensus_channel,
            agent->ctx->encoded_credentials,
            agent->ctx->encoded_credentials_length))
        {
            agent->time_of_last_backup_query_ms = now_ms;
            agent->correlation_id = corr_id;
            return 1;
        }
    }

    return 0;
}
static int do_snapshot_retrieve(aeron_cluster_backup_agent_t *agent, int64_t now_ms)
{
    int work = 0;

    if (NULL == agent->cluster_archive)
    {
        /* Connect to cluster archive using log supplier's archive endpoint (if known) */
        aeron_archive_context_t *arch_ctx = NULL;
        if (aeron_archive_context_init(&arch_ctx) < 0) { return 0; }
        aeron_archive_context_set_aeron(arch_ctx, agent->ctx->aeron);

        /* Use log supplier member's archive endpoint to build control channel */
        if (NULL != agent->log_supplier_member &&
            NULL != agent->log_supplier_member->archive_endpoint &&
            agent->log_supplier_member->archive_endpoint[0] != '\0')
        {
            char control_channel[512];
            snprintf(control_channel, sizeof(control_channel),
                     "aeron:udp?endpoint=%s",
                     agent->log_supplier_member->archive_endpoint);
            aeron_archive_context_set_control_request_channel(arch_ctx, control_channel);

            /* Use archive response endpoint if available (sinceVersion 10) */
            if (NULL != agent->log_supplier_member->archive_response_endpoint &&
                agent->log_supplier_member->archive_response_endpoint[0] != '\0')
            {
                char response_channel[512];
                snprintf(response_channel, sizeof(response_channel),
                         "aeron:udp?endpoint=%s",
                         agent->log_supplier_member->archive_response_endpoint);
                aeron_archive_context_set_control_response_channel(arch_ctx, response_channel);
            }
        }

        if (aeron_archive_connect(&agent->cluster_archive, arch_ctx) < 0)
        {
            aeron_archive_context_close(arch_ctx);
            return 0;
        }
        aeron_archive_context_close(arch_ctx);
        return 1;
    }

    if (NULL == agent->snapshot_replication)
    {
        /* Use the context's cluster archive control channel from the backup context */
        const char *src_channel = agent->ctx->consensus_channel; /* fallback */
        int32_t src_stream = agent->ctx->consensus_stream_id;

        if (aeron_cluster_snapshot_replication_create(
            &agent->snapshot_replication,
            agent->ctx->backup_archive,
            agent->ctx->aeron,
            src_stream,
            src_channel,
            agent->ctx->catchup_channel,
            agent->ctx->replication_progress_timeout_ns,
            agent->ctx->replication_progress_interval_ns) < 0)
        {
            return 0;
        }

        for (int i = 0; i < agent->snapshots_to_retrieve_count; i++)
        {
            aeron_cluster_backup_snapshot_t *s = &agent->snapshots_to_retrieve[i];
            aeron_cluster_recording_log_entry_t e = {
                .recording_id          = s->recording_id,
                .leadership_term_id    = s->leadership_term_id,
                .term_base_log_position = s->term_base_log_position,
                .log_position          = s->log_position,
                .timestamp             = s->timestamp,
                .service_id            = s->service_id,
            };
            aeron_cluster_snapshot_replication_add_snapshot(agent->snapshot_replication, &e);
        }
        work++;
    }

    work += aeron_cluster_snapshot_replication_poll(
        agent->snapshot_replication, now_ms * INT64_C(1000000));
    agent->time_of_last_progress_ms = now_ms;

    if (aeron_cluster_snapshot_replication_is_complete(agent->snapshot_replication))
    {
        /* Copy retrieved snapshots for later recording log update */
        aeron_cluster_recording_log_entry_t retrieved[32];
        int count = aeron_cluster_snapshot_replication_snapshots_retrieved(
            agent->snapshot_replication, retrieved, 32);

        for (int i = 0; i < count && agent->snapshots_retrieved_count <
             agent->snapshots_to_retrieve_capacity; i++)
        {
            if (agent->snapshots_retrieved_count == agent->snapshots_retrieved_capacity)
            {
                int new_cap = agent->snapshots_retrieved_capacity == 0
                              ? 8 : agent->snapshots_retrieved_capacity * 2;
                aeron_cluster_backup_snapshot_t *new_arr = NULL;
                if (aeron_alloc((void **)&new_arr,
                    (size_t)new_cap * sizeof(aeron_cluster_backup_snapshot_t)) < 0)
                {
                    break;
                }
                if (agent->snapshots_retrieved)
                {
                    memcpy(new_arr, agent->snapshots_retrieved,
                        (size_t)agent->snapshots_retrieved_count *
                        sizeof(aeron_cluster_backup_snapshot_t));
                    aeron_free(agent->snapshots_retrieved);
                }
                agent->snapshots_retrieved          = new_arr;
                agent->snapshots_retrieved_capacity = new_cap;
            }
            aeron_cluster_backup_snapshot_t *s =
                &agent->snapshots_retrieved[agent->snapshots_retrieved_count++];
            s->recording_id          = retrieved[i].recording_id;
            s->leadership_term_id    = retrieved[i].leadership_term_id;
            s->term_base_log_position = retrieved[i].term_base_log_position;
            s->log_position          = retrieved[i].log_position;
            s->timestamp             = retrieved[i].timestamp;
            s->service_id            = retrieved[i].service_id;
        }

        aeron_cluster_snapshot_replication_close(agent->snapshot_replication);
        agent->snapshot_replication = NULL;
        agent_set_state(agent, AERON_CLUSTER_BACKUP_STATE_LIVE_LOG_RECORD, now_ms);
        work++;
    }

    return work;
}

static int do_live_log_record(aeron_cluster_backup_agent_t *agent, int64_t now_ms)
{
    if (NULL_VALUE == agent->live_log_recording_subscription_id)
    {
        if ((int32_t)NULL_VALUE == agent->live_log_recording_session_id)
        {
            agent->live_log_recording_session_id = (int32_t)(now_ms & 0x7FFFFFFF);
        }

        /* Build channel with session ID */
        char channel[1024];
        snprintf(channel, sizeof(channel), "%s|session-id=%d",
                 agent->ctx->catchup_channel,
                 agent->live_log_recording_session_id);

        snprintf(agent->recording_channel, sizeof(agent->recording_channel), "%s", channel);
        snprintf(agent->replay_channel,    sizeof(agent->replay_channel),    "%s", channel);

        /* Start recording subscription */
        if (NULL != agent->ctx->backup_archive)
        {
            int64_t sub_id = AERON_NULL_VALUE;
            if (aeron_archive_start_recording(
                &sub_id,
                agent->ctx->backup_archive,
                agent->recording_channel,
                agent->ctx->log_stream_id,
                AERON_ARCHIVE_SOURCE_LOCATION_REMOTE,
                false) < 0)
            {
                return 0;
            }
            agent->live_log_recording_subscription_id = sub_id;
        }
    }

    agent->time_of_last_progress_ms = now_ms;
    agent_set_state(agent, AERON_CLUSTER_BACKUP_STATE_LIVE_LOG_REPLAY, now_ms);
    return 1;
}

static int do_live_log_replay(aeron_cluster_backup_agent_t *agent, int64_t now_ms)
{
    int work = 0;

    if (NULL == agent->cluster_archive)
    {
        aeron_archive_context_t *ctx = NULL;
        if (aeron_archive_context_init(&ctx) < 0) { return 0; }
        aeron_archive_context_set_aeron(ctx, agent->ctx->aeron);
        if (aeron_archive_connect(&agent->cluster_archive, ctx) < 0)
        {
            aeron_archive_context_close(ctx);
            return 0;
        }
        aeron_archive_context_close(ctx);
        return 1;
    }

    if (NULL_VALUE == agent->live_log_recording_id &&
        NULL_VALUE == agent->correlation_id)
    {
        /* Find start position from recording log */
        aeron_cluster_recording_log_entry_t *last_term =
            aeron_cluster_recording_log_find_last_term(agent->recording_log);

        int64_t start_position = (NULL != last_term)
            ? last_term->log_position : agent->log_term_base_log_position;

        int64_t replay_corr_id = aeron_next_correlation_id(agent->ctx->aeron);
        aeron_archive_replay_params_t params;
        aeron_archive_replay_params_init(&params);
        params.position = start_position;
        params.length   = INT64_MAX;
        /* Bound the replay by the leader's commit position counter so we do not
         * replay beyond what the leader has committed.  This mirrors Java's
         * clusterArchive.archiveProxy().boundedReplay(). */
        if (agent->leader_commit_position_counter_id != (int32_t)NULL_VALUE)
        {
            params.bounding_limit_counter_id = agent->leader_commit_position_counter_id;
        }

        int64_t replay_session_id = AERON_NULL_VALUE;
        if (aeron_archive_start_replay(
            &replay_session_id,
            agent->cluster_archive,
            agent->cluster_log_recording_id,
            agent->replay_channel,
            agent->ctx->log_stream_id,
            &params) < 0)
        {
            return 0;
        }

        agent->live_log_replay_session_id = replay_session_id;
        agent->correlation_id             = replay_corr_id;
        work++;
    }
    else if (NULL_VALUE != agent->correlation_id &&
             NULL_VALUE == agent->live_log_recording_id)
    {
        /* Poll for recording counter via backup archive */
        int32_t sig_count = 0;
        aeron_archive_poll_for_recording_signals(&sig_count, agent->ctx->backup_archive);

        /* Find counter by session ID */
        aeron_counters_reader_t *counters_reader = aeron_counters_reader(agent->ctx->aeron);
        int32_t counter_id = aeron_archive_recording_pos_find_counter_id_by_session_id(
            counters_reader, agent->live_log_recording_session_id);

        if (counter_id != AERON_NULL_COUNTER_ID)
        {
            agent->live_log_recording_id = aeron_archive_recording_pos_get_recording_id(
                counters_reader, counter_id);
            agent->time_of_last_progress_ms = now_ms;
            agent_set_state(agent, AERON_CLUSTER_BACKUP_STATE_UPDATE_RECORDING_LOG, now_ms);
            work++;
        }
    }

    return work;
}

static int do_update_recording_log(aeron_cluster_backup_agent_t *agent, int64_t now_ms)
{
    bool was_updated = false;

    /* 1. Leader log term entry BEFORE snapshots (if unknown) */
    if (agent->leader_log_entry_pending &&
        aeron_cluster_recording_log_is_unknown(
            agent->recording_log, agent->leader_log_entry_leadership_term_id))
    {
        int64_t snap_term_id = (agent->snapshots_retrieved_count > 0)
            ? agent->snapshots_retrieved[0].leadership_term_id : INT64_MIN;

        if (agent->leader_log_entry_leadership_term_id <= snap_term_id)
        {
            aeron_cluster_recording_log_append_term(
                agent->recording_log,
                agent->leader_log_entry_recording_id,
                agent->leader_log_entry_leadership_term_id,
                agent->leader_log_entry_term_base_log_position,
                agent->leader_log_entry_timestamp);
            was_updated = true;
        }
    }
    agent->leader_log_entry_pending = false;

    /* 2. Append snapshots (newest-first order from backup response) */
    for (int i = agent->snapshots_retrieved_count - 1; i >= 0; i--)
    {
        const aeron_cluster_backup_snapshot_t *s = &agent->snapshots_retrieved[i];
        aeron_cluster_recording_log_append_snapshot(
            agent->recording_log,
            s->recording_id,
            s->leadership_term_id,
            s->term_base_log_position,
            s->log_position,
            s->timestamp,
            s->service_id);
        was_updated = true;
    }
    agent->snapshots_retrieved_count = 0;

    /* 3. Leader last term entry AFTER snapshots (if unknown) */
    if (agent->leader_last_term_entry_pending &&
        aeron_cluster_recording_log_is_unknown(
            agent->recording_log, agent->leader_last_term_entry_leadership_term_id))
    {
        aeron_cluster_recording_log_append_term(
            agent->recording_log,
            agent->leader_last_term_entry_recording_id,
            agent->leader_last_term_entry_leadership_term_id,
            agent->leader_last_term_entry_term_base_log_position,
            agent->leader_last_term_entry_timestamp);
        was_updated = true;
    }
    agent->leader_last_term_entry_pending = false;

    if (was_updated)
    {
        aeron_cluster_recording_log_force(agent->recording_log);
    }

    agent->time_of_last_progress_ms = now_ms;
    agent_set_state(agent, AERON_CLUSTER_BACKUP_STATE_BACKING_UP, now_ms);
    return 1;
}

static int do_backing_up(aeron_cluster_backup_agent_t *agent, int64_t now_ms)
{
    /* Detect stalled progress: if live log recording counter is known, check it is
     * still active; if the counter disappears, treat as stall and reset. */
    if (agent->live_log_recording_counter_id != (int32_t)AERON_NULL_COUNTER_ID &&
        NULL != agent->ctx->aeron)
    {
        aeron_counters_reader_t *counters = aeron_counters_reader(agent->ctx->aeron);
        int32_t counter_state = AERON_COUNTER_RECORD_UNUSED;
        aeron_counters_reader_counter_state(
            counters, agent->live_log_recording_counter_id, &counter_state);
        if (AERON_COUNTER_RECORD_UNUSED == counter_state)
        {
            /* Counter gone — log recording stopped, restart backup */
            agent_set_state(agent, AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP, now_ms);
            return 1;
        }
    }

    /* Stalled progress timeout: if no progress for backup_progress_timeout_ns, reset */
    if (now_ms > (agent->time_of_last_progress_ms +
                  agent->ctx->backup_progress_timeout_ns / 1000000))
    {
        agent_set_state(agent, AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP, now_ms);
        return 1;
    }

    /* Periodically send new backup queries */
    if (now_ms > (agent->time_of_last_backup_query_ms +
                  agent->ctx->backup_query_interval_ns / 1000000))
    {
        agent->time_of_last_backup_query_ms = now_ms;
        agent->time_of_last_progress_ms     = now_ms;
        agent->correlation_id = NULL_VALUE;
        agent_set_state(agent, AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY, now_ms);
        return 1;
    }
    return 0;
}

static int do_reset_backup(aeron_cluster_backup_agent_t *agent, int64_t now_ms)
{
    agent->time_of_last_progress_ms = now_ms;

    if (NULL_VALUE == agent->cool_down_deadline_ms)
    {
        agent->cool_down_deadline_ms = now_ms + agent->ctx->cool_down_interval_ns / 1000000;
        reset_agent(agent);
        return 1;
    }
    else if (now_ms > agent->cool_down_deadline_ms)
    {
        agent->cool_down_deadline_ms = NULL_VALUE;
        agent_set_state(agent, AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY, now_ms);
        return 1;
    }

    return 0;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */
int aeron_cluster_backup_context_conclude(aeron_cluster_backup_context_t *ctx)
{
    if (NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "backup context is NULL");
        return -1;
    }

    if ('\0' == ctx->cluster_consensus_endpoints[0])
    {
        AERON_SET_ERR(EINVAL, "%s", "cluster_consensus_endpoints must be set");
        return -1;
    }

    if (NULL == ctx->aeron)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron client must be set");
        return -1;
    }

    if (0 == ctx->backup_response_timeout_ns)
    {
        ctx->backup_response_timeout_ns = INT64_C(5000000000); /* 5s default */
    }

    if (0 == ctx->backup_query_interval_ns)
    {
        ctx->backup_query_interval_ns = INT64_C(60000000000); /* 60s default */
    }

    if (0 == ctx->backup_progress_timeout_ns)
    {
        ctx->backup_progress_timeout_ns = INT64_C(30000000000); /* 30s default */
    }

    if (0 == ctx->cool_down_interval_ns)
    {
        ctx->cool_down_interval_ns = INT64_C(1000000000); /* 1s default */
    }

    return 0;
}

int aeron_cluster_backup_agent_create(
    aeron_cluster_backup_agent_t **agent,
    aeron_cluster_backup_context_t *ctx,
    const char *cluster_dir)
{
    aeron_cluster_backup_agent_t *a = NULL;
    if (aeron_alloc((void **)&a, sizeof(*a)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate ClusterBackupAgent");
        return -1;
    }
    memset(a, 0, sizeof(*a));

    a->ctx   = ctx;
    a->state = AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY;

    a->cluster_log_recording_id           = NULL_VALUE;
    a->live_log_recording_subscription_id = NULL_VALUE;
    a->live_log_recording_id              = NULL_VALUE;
    a->live_log_replay_session_id         = NULL_VALUE;
    a->live_log_recording_counter_id      = -1;
    a->live_log_recording_session_id      = (int32_t)NULL_VALUE;
    a->leader_commit_position_counter_id  = (int32_t)NULL_VALUE;
    a->leader_member_id                   = (int32_t)NULL_VALUE;
    a->correlation_id                     = NULL_VALUE;
    a->cool_down_deadline_ms              = NULL_VALUE;

    /* Parse cluster_consensus_endpoints into per-endpoint array for round-robin */
    {
        const char *src = ctx->cluster_consensus_endpoints;
        a->parsed_endpoints_count = 0;
        a->next_endpoint_idx      = 0;
        while (*src != '\0' && a->parsed_endpoints_count < AERON_CLUSTER_BACKUP_MAX_ENDPOINTS)
        {
            const char *comma = strchr(src, ',');
            size_t len = (comma != NULL) ? (size_t)(comma - src) : strlen(src);
            if (len > 0 && len < 256)
            {
                memcpy(a->parsed_endpoints[a->parsed_endpoints_count], src, len);
                a->parsed_endpoints[a->parsed_endpoints_count][len] = '\0';
                a->parsed_endpoints_count++;
            }
            src = (comma != NULL) ? comma + 1 : src + len;
        }
    }

    if (aeron_cluster_recording_log_open(&a->recording_log, cluster_dir, false) < 0)
    {
        /* Try creating new */
        if (aeron_cluster_recording_log_open(&a->recording_log, cluster_dir, true) < 0)
        {
            aeron_free(a);
            AERON_APPEND_ERR("%s", "failed to open recording log");
            return -1;
        }
    }

    if (aeron_fragment_assembler_create(&a->fragment_assembler, on_fragment, a) < 0)
    {
        aeron_cluster_recording_log_close(a->recording_log);
        aeron_free(a);
        AERON_APPEND_ERR("%s", "failed to create fragment assembler");
        return -1;
    }

    if (NULL != ctx->aeron)
    {
        aeron_async_add_subscription_t *async_sub = NULL;
        int rc = aeron_async_add_subscription(
            &async_sub,
            ctx->aeron,
            ctx->consensus_channel,
            ctx->consensus_stream_id,
            NULL, NULL, NULL, NULL);
        if (rc == 0)
        {
            do { rc = aeron_async_add_subscription_poll(&a->consensus_sub, async_sub); } while (0 == rc);
        }
    }

    *agent = a;
    return 0;
}

int aeron_cluster_backup_agent_on_start(
    aeron_cluster_backup_agent_t *agent, int64_t now_ms)
{
    agent->time_of_last_progress_ms   = now_ms;
    agent->time_of_last_backup_query_ms = now_ms - 1;
    return 0;
}

int aeron_cluster_backup_agent_do_work(
    aeron_cluster_backup_agent_t *agent, int64_t now_ms)
{
    if (agent->is_closed ||
        AERON_CLUSTER_BACKUP_STATE_CLOSED == agent->state)
    {
        return 0;
    }

    /* Cache now_ms so fragment callbacks (on_backup_response) can use it */
    agent->current_now_ms = now_ms;

    int work = 0;

    /* Slow tick */
    if (now_ms >= agent->slow_tick_deadline_ms)
    {
        agent->slow_tick_deadline_ms = now_ms + AERON_CLUSTER_BACKUP_SLOW_TICK_INTERVAL_MS;
        /* Poll backup archive recording signals */
        if (NULL != agent->ctx->backup_archive)
        {
            int32_t sig_count = 0;
            aeron_archive_poll_for_recording_signals(&sig_count, agent->ctx->backup_archive);
        }
        work++;
    }

    /* Poll consensus subscription */
    if (NULL != agent->consensus_sub && NULL != agent->fragment_assembler)
    {
        work += aeron_subscription_poll(
            agent->consensus_sub,
            aeron_fragment_assembler_handler,
            agent->fragment_assembler,
            10);
    }

    switch (agent->state)
    {
        case AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY:
            work += do_backup_query(agent, now_ms);
            break;

        case AERON_CLUSTER_BACKUP_STATE_SNAPSHOT_RETRIEVE:
            work += do_snapshot_retrieve(agent, now_ms);
            break;

        case AERON_CLUSTER_BACKUP_STATE_LIVE_LOG_RECORD:
            work += do_live_log_record(agent, now_ms);
            break;

        case AERON_CLUSTER_BACKUP_STATE_LIVE_LOG_REPLAY:
            work += do_live_log_replay(agent, now_ms);
            break;

        case AERON_CLUSTER_BACKUP_STATE_UPDATE_RECORDING_LOG:
            work += do_update_recording_log(agent, now_ms);
            break;

        case AERON_CLUSTER_BACKUP_STATE_BACKING_UP:
            work += do_backing_up(agent, now_ms);
            break;

        case AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP:
            work += do_reset_backup(agent, now_ms);
            break;

        case AERON_CLUSTER_BACKUP_STATE_CLOSED:
        default:
            break;
    }

    /* Check progress timeout */
    if (now_ms > (agent->time_of_last_progress_ms +
                  agent->ctx->backup_progress_timeout_ns / 1000000) &&
        AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP != agent->state &&
        AERON_CLUSTER_BACKUP_STATE_CLOSED != agent->state)
    {
        if (agent->ctx->error_handler)
        {
            agent->ctx->error_handler(
                agent->ctx->error_handler_clientd, 0, "progress has stalled");
        }
        agent_set_state(agent, AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP, now_ms);
    }

    return work;
}

void aeron_cluster_backup_agent_close(aeron_cluster_backup_agent_t *agent)
{
    if (NULL == agent) { return; }
    agent->is_closed = true;

    if (NULL != agent->snapshot_replication)
    {
        aeron_cluster_snapshot_replication_close(agent->snapshot_replication);
        agent->snapshot_replication = NULL;
    }

    if (NULL != agent->recording_subscription)
    {
        aeron_subscription_close(agent->recording_subscription, NULL, NULL);
    }

    if (NULL != agent->consensus_sub)
    {
        aeron_subscription_close(agent->consensus_sub, NULL, NULL);
    }

    if (NULL != agent->consensus_pub)
    {
        aeron_exclusive_publication_close(agent->consensus_pub, NULL, NULL);
    }

    if (NULL != agent->fragment_assembler)
    {
        aeron_fragment_assembler_delete(agent->fragment_assembler);
    }

    if (NULL != agent->cluster_archive)
    {
        aeron_archive_close(agent->cluster_archive);
    }

    if (NULL != agent->recording_log)
    {
        aeron_cluster_recording_log_close(agent->recording_log);
    }

    if (NULL != agent->snapshots_to_retrieve)
    {
        aeron_free(agent->snapshots_to_retrieve);
    }

    if (NULL != agent->snapshots_retrieved)
    {
        aeron_free(agent->snapshots_retrieved);
    }

    if (NULL != agent->active_cluster_members)
    {
        aeron_cluster_members_free(agent->active_cluster_members, agent->active_cluster_member_count);
        agent->active_cluster_members = NULL;
    }

    aeron_free(agent);
}

aeron_cluster_backup_state_t aeron_cluster_backup_agent_state(
    const aeron_cluster_backup_agent_t *agent)
{
    return agent->state;
}

int aeron_cluster_backup_launch(
    aeron_cluster_backup_agent_t **agent,
    aeron_cluster_backup_context_t *ctx,
    const char *cluster_dir,
    int64_t now_ms)
{
    if (aeron_cluster_backup_context_conclude(ctx) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to conclude backup context");
        return -1;
    }

    if (aeron_cluster_backup_agent_create(agent, ctx, cluster_dir) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to create backup agent");
        return -1;
    }

    if (aeron_cluster_backup_agent_on_start(*agent, now_ms) < 0)
    {
        aeron_cluster_backup_agent_close(*agent);
        *agent = NULL;
        AERON_APPEND_ERR("%s", "failed to start backup agent");
        return -1;
    }

    return 0;
}
