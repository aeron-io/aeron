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
#include <string.h>
#include <errno.h>
#include <stdlib.h>

#include "aeron_cluster_election.h"
#include "aeron_consensus_module_agent.h"
#include "aeron_cm_context.h"
#include "aeron_cluster_consensus_publisher.h"
#include "aeron_cluster_log_replay.h"
#include "aeron_cluster_recording_replication.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_clock.h"

/* -----------------------------------------------------------------------
 * Real publisher ops wrappers (used in production)
 * ----------------------------------------------------------------------- */
static bool real_canvass_position(void *clientd, aeron_cluster_member_t *member,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id, int32_t follower_member_id, int32_t protocol_version)
{
    (void)clientd;
    if (NULL == member->publication) { return false; }
    return aeron_cluster_consensus_publisher_canvass_position(
        member->publication, log_leadership_term_id, log_position,
        leadership_term_id, follower_member_id, protocol_version);
}

static bool real_request_vote(void *clientd, aeron_cluster_member_t *member,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id)
{
    (void)clientd;
    if (NULL == member->publication) { return false; }
    return aeron_cluster_consensus_publisher_request_vote(
        member->publication, log_leadership_term_id, log_position,
        candidate_term_id, candidate_member_id);
}

static bool real_vote(void *clientd, aeron_cluster_member_t *member,
    int64_t candidate_term_id, int64_t log_leadership_term_id, int64_t log_position,
    int32_t candidate_member_id, int32_t follower_member_id, bool vote)
{
    (void)clientd;
    if (NULL == member->publication) { return false; }
    return aeron_cluster_consensus_publisher_vote(
        member->publication, candidate_term_id, log_leadership_term_id, log_position,
        candidate_member_id, follower_member_id, vote);
}

static bool real_new_leadership_term(void *clientd, aeron_cluster_member_t *member,
    int64_t log_leadership_term_id, int64_t next_leadership_term_id,
    int64_t next_term_base_log_position, int64_t next_log_position,
    int64_t leadership_term_id, int64_t term_base_log_position,
    int64_t log_position, int64_t leader_recording_id, int64_t timestamp,
    int32_t leader_member_id, int32_t log_session_id, int32_t app_version, bool is_startup)
{
    (void)clientd;
    if (NULL == member->publication) { return false; }
    return aeron_cluster_consensus_publisher_new_leadership_term(
        member->publication, log_leadership_term_id, next_leadership_term_id,
        next_term_base_log_position, next_log_position, leadership_term_id,
        term_base_log_position, log_position, leader_recording_id, timestamp,
        leader_member_id, log_session_id, app_version, is_startup);
}

static bool real_append_position(void *clientd, aeron_cluster_member_t *member,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, int8_t flags)
{
    (void)clientd;
    if (NULL == member->publication) { return false; }
    return aeron_cluster_consensus_publisher_append_position(
        member->publication, leadership_term_id, log_position, follower_member_id, flags);
}

static bool real_commit_position(void *clientd, aeron_cluster_member_t *member,
    int64_t leadership_term_id, int64_t log_position, int32_t leader_member_id)
{
    (void)clientd;
    if (NULL == member->publication) { return false; }
    return aeron_cluster_consensus_publisher_commit_position(
        member->publication, leadership_term_id, log_position, leader_member_id);
}

void aeron_cluster_election_publisher_ops_init_real(
    aeron_cluster_election_publisher_ops_t *ops,
    aeron_cluster_member_t *members,
    int member_count,
    int32_t self_id)
{
    ops->clientd         = NULL;
    ops->canvass_position    = real_canvass_position;
    ops->request_vote        = real_request_vote;
    ops->vote                = real_vote;
    ops->new_leadership_term = real_new_leadership_term;
    ops->append_position     = real_append_position;
    ops->commit_position     = real_commit_position;
}

/* -----------------------------------------------------------------------
 * Per-member broadcast helpers (iterate members except self)
 * ----------------------------------------------------------------------- */
static void election_broadcast_canvass(aeron_cluster_election_t *e,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id, int32_t follower_member_id, int32_t protocol_version)
{
    for (int i = 0; i < e->member_count; i++)
    {
        if (e->members[i].id != e->this_member->id)
        {
            e->pub_ops.canvass_position(e->pub_ops.clientd, &e->members[i],
                log_leadership_term_id, log_position,
                leadership_term_id, follower_member_id, protocol_version);
        }
    }
}

static void election_broadcast_request_vote(aeron_cluster_election_t *e,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id)
{
    for (int i = 0; i < e->member_count; i++)
    {
        if (e->members[i].id != e->this_member->id)
        {
            e->pub_ops.request_vote(e->pub_ops.clientd, &e->members[i],
                log_leadership_term_id, log_position,
                candidate_term_id, candidate_member_id);
        }
    }
}

static void election_broadcast_new_leadership_term(aeron_cluster_election_t *e,
    int64_t log_leadership_term_id, int64_t next_leadership_term_id,
    int64_t next_term_base_log_position, int64_t next_log_position,
    int64_t leadership_term_id, int64_t term_base_log_position,
    int64_t log_position, int64_t leader_recording_id, int64_t timestamp,
    int32_t leader_member_id, int32_t log_session_id, int32_t app_version, bool is_startup)
{
    for (int i = 0; i < e->member_count; i++)
    {
        if (e->members[i].id != e->this_member->id)
        {
            e->pub_ops.new_leadership_term(e->pub_ops.clientd, &e->members[i],
                log_leadership_term_id, next_leadership_term_id,
                next_term_base_log_position, next_log_position,
                leadership_term_id, term_base_log_position,
                log_position, leader_recording_id, timestamp,
                leader_member_id, log_session_id, app_version, is_startup);
        }
    }
}

static void election_broadcast_commit_position(aeron_cluster_election_t *e,
    int64_t leadership_term_id, int64_t log_position, int32_t leader_member_id)
{
    for (int i = 0; i < e->member_count; i++)
    {
        if (e->members[i].id != e->this_member->id)
        {
            e->pub_ops.commit_position(e->pub_ops.clientd, &e->members[i],
                leadership_term_id, log_position, leader_member_id);
        }
    }
}

/* -----------------------------------------------------------------------
 * Internal helpers
 * ----------------------------------------------------------------------- */
static bool is_quorum(int count, int member_count)
{
    return count >= (member_count / 2 + 1);
}

static bool is_better_candidate(
    aeron_cluster_election_t *e,
    int64_t log_leadership_term_id,
    int64_t log_position)
{
    if (log_leadership_term_id > e->log_leadership_term_id) { return true; }
    if (log_leadership_term_id == e->log_leadership_term_id && log_position > e->log_position) { return true; }
    return false;
}

/* -----------------------------------------------------------------------
 * Real agent ops wrappers
 * ----------------------------------------------------------------------- */
static int32_t real_get_protocol_version(void *cd)
{ return aeron_consensus_module_agent_get_protocol_version((aeron_consensus_module_agent_t *)cd); }
static int32_t real_get_app_version(void *cd)
{ return aeron_consensus_module_agent_get_app_version((aeron_consensus_module_agent_t *)cd); }
static int64_t real_get_append_position(void *cd)
{ return aeron_consensus_module_agent_get_append_position((aeron_consensus_module_agent_t *)cd); }
static int64_t real_get_log_recording_id(void *cd)
{ return aeron_consensus_module_agent_get_log_recording_id((aeron_consensus_module_agent_t *)cd); }
static void real_on_state_change(void *cd, aeron_cluster_election_state_t s, int64_t ns)
{ aeron_consensus_module_agent_on_election_state_change((aeron_consensus_module_agent_t *)cd, s, ns); }
static void real_on_election_complete(void *cd, aeron_cluster_member_t *leader, int64_t ns)
{ aeron_consensus_module_agent_on_election_complete((aeron_consensus_module_agent_t *)cd, leader, ns); }
static void real_begin_new_leadership_term(void *cd,
    int64_t log_term_id, int64_t new_term_id, int64_t log_pos, int64_t ts, bool startup)
{ aeron_consensus_module_agent_begin_new_leadership_term(
    (aeron_consensus_module_agent_t *)cd, log_term_id, new_term_id, log_pos, ts, startup); }
static void real_on_follower_new_leadership_term(void *cd,
    int64_t log_term_id, int64_t next_term_id,
    int64_t next_base, int64_t next_log_pos,
    int64_t term_id, int64_t base, int64_t log_pos,
    int64_t rec_id, int64_t ts, int32_t leader_id,
    int32_t session_id, int32_t app_ver, bool startup)
{ aeron_consensus_module_agent_on_follower_new_leadership_term(
    (aeron_consensus_module_agent_t *)cd,
    log_term_id, next_term_id, next_base, next_log_pos,
    term_id, base, log_pos, rec_id, ts, leader_id, session_id, app_ver, startup); }
static void real_on_replay_new_leadership_term(void *cd,
    int64_t term_id, int64_t log_pos, int64_t ts, int64_t base,
    int32_t leader_id, int32_t session_id, int32_t app_ver)
{ aeron_consensus_module_agent_on_replay_new_leadership_term_event(
    (aeron_consensus_module_agent_t *)cd,
    term_id, log_pos, ts, base, 0 /* time_unit unknown here */, app_ver); }
static void real_notify_commit_position(void *cd, int64_t pos)
{ aeron_consensus_module_agent_notify_commit_position((aeron_consensus_module_agent_t *)cd, pos); }

/* --- Phase 1.5 real wrappers --- */
static int64_t real_quorum_position(void *cd, int64_t ap, int64_t now_ns)
{ return aeron_consensus_module_agent_quorum_position((aeron_consensus_module_agent_t *)cd, ap, now_ns); }

static int real_publish_new_leadership_term_on_interval(void *cd, int64_t qp, int64_t now_ns)
{ return aeron_consensus_module_agent_publish_new_leadership_term_on_interval(
    (aeron_consensus_module_agent_t *)cd, qp, now_ns); }

static int real_publish_commit_position_on_interval(void *cd, int64_t qp, int64_t now_ns)
{ return aeron_consensus_module_agent_publish_commit_position_on_interval(
    (aeron_consensus_module_agent_t *)cd, qp, now_ns); }

static void *real_new_log_replay(void *cd, int64_t from, int64_t to)
{ return aeron_consensus_module_agent_new_log_replay(
    (aeron_consensus_module_agent_t *)cd, from, to); }

static int real_log_replay_do_work(void *cd, void *replay)
{ (void)cd; return aeron_cluster_log_replay_do_work((aeron_cluster_log_replay_t *)replay); }

static bool real_log_replay_is_done(void *cd, void *replay)
{ (void)cd; return aeron_cluster_log_replay_is_done((aeron_cluster_log_replay_t *)replay); }

static int64_t real_log_replay_position(void *cd, void *replay)
{ (void)cd; return aeron_cluster_log_replay_position((aeron_cluster_log_replay_t *)replay); }

static void real_close_log_replay(void *cd, void *replay)
{ (void)cd; aeron_cluster_log_replay_close((aeron_cluster_log_replay_t *)replay); }

static void real_join_log_as_leader(void *cd, int64_t term_id, int64_t log_pos,
    int32_t session_id, bool is_startup)
{ aeron_consensus_module_agent_join_log_as_leader(
    (aeron_consensus_module_agent_t *)cd, term_id, log_pos, session_id, is_startup); }

static void real_update_recording_log(void *cd, int64_t now_ns)
{ aeron_consensus_module_agent_update_recording_log(
    (aeron_consensus_module_agent_t *)cd, now_ns); }

static int real_update_leader_position(void *cd, int64_t now_ns, int64_t ap, int64_t qp)
{ return aeron_consensus_module_agent_update_leader_position(
    (aeron_consensus_module_agent_t *)cd, now_ns, ap, qp); }

static bool real_append_new_leadership_term_event(void *cd, int64_t now_ns)
{ return aeron_consensus_module_agent_append_new_leadership_term_event(
    (aeron_consensus_module_agent_t *)cd, now_ns); }

static void *real_new_log_replication(void *cd,
    const char *archive_ep, const char *resp_ep,
    int64_t rec_id, int64_t stop_pos, int64_t now_ns)
{ return aeron_consensus_module_agent_new_log_replication(
    (aeron_consensus_module_agent_t *)cd, archive_ep, resp_ep, rec_id, stop_pos, now_ns); }

static void real_close_log_replication(void *cd, void *rep)
{ (void)cd; aeron_cluster_recording_replication_close(
    (aeron_cluster_recording_replication_t *)rep); }

static int real_poll_archive_events(void *cd)
{ return aeron_consensus_module_agent_poll_archive_events((aeron_consensus_module_agent_t *)cd); }

static int real_publish_follower_replication_position(void *cd, int64_t now_ns)
{ return aeron_consensus_module_agent_publish_follower_replication_position(
    (aeron_consensus_module_agent_t *)cd, now_ns); }

static void real_update_recording_log_for_replication(void *cd,
    int64_t term_id, int64_t base_pos, int64_t stop_pos, int64_t now_ns)
{ aeron_consensus_module_agent_update_recording_log_for_replication(
    (aeron_consensus_module_agent_t *)cd, term_id, base_pos, stop_pos, now_ns); }

static aeron_subscription_t *real_add_follower_subscription(void *cd, int32_t session_id)
{ return aeron_consensus_module_agent_add_follower_subscription(
    (aeron_consensus_module_agent_t *)cd, session_id); }

static aeron_image_t *real_get_log_image(void *cd, aeron_subscription_t *sub, int32_t session_id)
{ return aeron_subscription_image_by_session_id(sub, session_id); }

static void real_add_catchup_log_destination(void *cd, aeron_subscription_t *sub, const char *ep)
{ aeron_consensus_module_agent_add_catchup_log_destination(
    (aeron_consensus_module_agent_t *)cd, sub, ep); }

static void real_add_live_log_destination(void *cd)
{ aeron_consensus_module_agent_add_live_log_destination((aeron_consensus_module_agent_t *)cd); }

static const char *real_this_catchup_endpoint(void *cd)
{ return aeron_consensus_module_agent_this_catchup_endpoint((aeron_consensus_module_agent_t *)cd); }

static bool real_send_catchup_position(void *cd, const char *ep)
{ return aeron_consensus_module_agent_send_catchup_position(
    (aeron_consensus_module_agent_t *)cd, ep); }

static void real_catchup_initiated(void *cd, int64_t now_ns)
{ aeron_consensus_module_agent_catchup_initiated((aeron_consensus_module_agent_t *)cd, now_ns); }

static bool real_try_join_log_as_follower(void *cd, aeron_image_t *image,
    bool is_startup, int64_t now_ns)
{ return aeron_consensus_module_agent_try_join_log_as_follower(
    (aeron_consensus_module_agent_t *)cd, image, is_startup, now_ns); }

static int real_catchup_poll(void *cd, int64_t commit_pos, int64_t now_ns)
{ return aeron_consensus_module_agent_catchup_poll(
    (aeron_consensus_module_agent_t *)cd, commit_pos, now_ns); }

static bool real_is_catchup_near_live(void *cd, int64_t pos)
{ return aeron_consensus_module_agent_is_catchup_near_live(
    (aeron_consensus_module_agent_t *)cd, pos); }

static const char *real_live_log_destination(void *cd)
{ return aeron_consensus_module_agent_live_log_destination((aeron_consensus_module_agent_t *)cd); }

static const char *real_catchup_log_destination(void *cd)
{ return aeron_consensus_module_agent_catchup_log_destination((aeron_consensus_module_agent_t *)cd); }

static int64_t real_get_commit_position(void *cd)
{ return aeron_consensus_module_agent_get_commit_position((aeron_consensus_module_agent_t *)cd); }

static int real_agent_state(void *cd)
{ return (int)aeron_consensus_module_agent_state((aeron_consensus_module_agent_t *)cd); }

void aeron_cluster_election_agent_ops_init_real(
    aeron_cluster_election_agent_ops_t *ops,
    aeron_consensus_module_agent_t *agent)
{
    ops->clientd                    = agent;
    ops->get_protocol_version       = real_get_protocol_version;
    ops->get_app_version            = real_get_app_version;
    ops->get_append_position        = real_get_append_position;
    ops->get_log_recording_id       = real_get_log_recording_id;
    ops->on_state_change            = real_on_state_change;
    ops->on_election_complete       = real_on_election_complete;
    ops->begin_new_leadership_term  = real_begin_new_leadership_term;
    ops->on_follower_new_leadership_term = real_on_follower_new_leadership_term;
    ops->on_replay_new_leadership_term   = real_on_replay_new_leadership_term;
    ops->notify_commit_position          = real_notify_commit_position;

    /* Phase 1.5 callbacks */
    ops->quorum_position                          = real_quorum_position;
    ops->publish_new_leadership_term_on_interval  = real_publish_new_leadership_term_on_interval;
    ops->publish_commit_position_on_interval      = real_publish_commit_position_on_interval;
    ops->new_log_replay                           = real_new_log_replay;
    ops->log_replay_do_work                       = real_log_replay_do_work;
    ops->log_replay_is_done                       = real_log_replay_is_done;
    ops->log_replay_position                      = real_log_replay_position;
    ops->close_log_replay                         = real_close_log_replay;
    ops->join_log_as_leader                       = real_join_log_as_leader;
    ops->update_recording_log                     = real_update_recording_log;
    ops->update_leader_position                   = real_update_leader_position;
    ops->append_new_leadership_term_event         = real_append_new_leadership_term_event;
    ops->new_log_replication                      = real_new_log_replication;
    ops->close_log_replication                    = real_close_log_replication;
    ops->poll_archive_events                      = real_poll_archive_events;
    ops->publish_follower_replication_position    = real_publish_follower_replication_position;
    ops->update_recording_log_for_replication     = real_update_recording_log_for_replication;
    ops->add_follower_subscription                = real_add_follower_subscription;
    ops->get_log_image                            = real_get_log_image;
    ops->add_catchup_log_destination              = real_add_catchup_log_destination;
    ops->add_live_log_destination                 = real_add_live_log_destination;
    ops->this_catchup_endpoint                    = real_this_catchup_endpoint;
    ops->send_catchup_position                    = real_send_catchup_position;
    ops->catchup_initiated                        = real_catchup_initiated;
    ops->try_join_log_as_follower                 = real_try_join_log_as_follower;
    ops->catchup_poll                             = real_catchup_poll;
    ops->is_catchup_near_live                     = real_is_catchup_near_live;
    ops->live_log_destination                     = real_live_log_destination;
    ops->catchup_log_destination                  = real_catchup_log_destination;
    ops->get_commit_position                      = real_get_commit_position;
    ops->agent_state                              = real_agent_state;
}

static void transition_to(aeron_cluster_election_t *e,
                           aeron_cluster_election_state_t new_state,
                           int64_t now_ns)
{
    e->state                  = new_state;
    e->time_of_state_change_ns = now_ns;
    e->agent_ops.on_state_change(e->agent_ops.clientd, new_state, now_ns);
}

/* -----------------------------------------------------------------------
 * State handlers
 * ----------------------------------------------------------------------- */
static int do_init(aeron_cluster_election_t *e, int64_t now_ns)
{
    e->is_first_init = false;

    /* Reset per-election tracking */
    for (int i = 0; i < e->member_count; i++)
    {
        e->members[i].candidate_term_id = -1;
        e->members[i].log_position      = -1;
        e->members[i].leadership_term_id = -1;
    }
    e->this_member->log_position      = e->log_position;
    e->this_member->leadership_term_id = e->log_leadership_term_id;
    e->this_member->candidate_term_id  = e->candidate_term_id;

    /* Single-node cluster: immediately become leader */
    if (1 == e->member_count)
    {
        e->leader_member     = e->this_member;
        e->candidate_term_id = e->log_leadership_term_id + 1;
        e->leadership_term_id = e->candidate_term_id;
        e->is_leader_startup = e->is_node_startup;
        transition_to(e, AERON_ELECTION_LEADER_LOG_REPLICATION, now_ns);
        return 1;
    }

    transition_to(e, AERON_ELECTION_CANVASS, now_ns);
    return 1;
}

static int do_canvass(aeron_cluster_election_t *e, int64_t now_ns)
{
    /* Broadcast our position to all peers */
    if ((now_ns - e->time_of_last_update_ns) >= e->election_status_interval_ns)
    {
        election_broadcast_canvass(e,
            e->log_leadership_term_id, e->log_position,
            e->leadership_term_id, e->this_member->id,
            e->agent_ops.get_protocol_version(e->agent_ops.clientd));
        e->time_of_last_update_ns = now_ns;
    }

    /* Count how many have reported canvass positions */
    int canvassed = 0;
    for (int i = 0; i < e->member_count; i++)
    {
        if (e->members[i].log_position >= 0) { canvassed++; }
    }

    bool has_quorum = is_quorum(canvassed, e->member_count);
    bool timed_out  = (now_ns - e->time_of_state_change_ns) >= e->startup_canvass_timeout_ns;

    if (has_quorum || timed_out)
    {
        /* If an appointed leader is set and we are not it, stay in canvass.
         * Mirrors Java: if (ctx.appointedLeaderId() != NULL_VALUE && ctx.appointedLeaderId() != thisMember.id()) return; */
        if (e->appointed_leader_id >= 0 && e->appointed_leader_id != e->this_member->id)
        {
            return 0;
        }

        /* Check if we are the best candidate */
        bool is_best = true;
        for (int i = 0; i < e->member_count; i++)
        {
            if (e->members[i].id == e->this_member->id) { continue; }
            if (is_better_candidate(e,
                e->members[i].leadership_term_id,
                e->members[i].log_position))
            {
                is_best = false;
                break;
            }
        }

        if (is_best)
        {
            /* Set nomination deadline with randomized jitter [0, electionTimeoutNs/2) */
            int64_t half_timeout = e->election_timeout_ns >> 1;
            int64_t delay_ns = half_timeout > 0 ? (int64_t)(rand() % (int)half_timeout) : 0;
            e->nomination_deadline_ns = now_ns + delay_ns;
            transition_to(e, AERON_ELECTION_NOMINATE, now_ns);
        }
        else
        {
            transition_to(e, AERON_ELECTION_FOLLOWER_BALLOT, now_ns);
        }
        return 1;
    }

    return 0;
}

static int do_nominate(aeron_cluster_election_t *e, int64_t now_ns)
{
    if (now_ns >= e->nomination_deadline_ns)
    {
        e->candidate_term_id++;

        /* Vote for self */
        e->this_member->candidate_term_id = e->candidate_term_id;

        /* Request votes from all peers */
        election_broadcast_request_vote(e,
            e->log_leadership_term_id, e->log_position,
            e->candidate_term_id, e->this_member->id);

        transition_to(e, AERON_ELECTION_CANDIDATE_BALLOT, now_ns);
        return 1;
    }

    /* While waiting, keep broadcasting canvass */
    if ((now_ns - e->time_of_last_update_ns) >= e->election_status_interval_ns)
    {
        election_broadcast_canvass(e,
            e->log_leadership_term_id, e->log_position,
            e->leadership_term_id, e->this_member->id,
            e->agent_ops.get_protocol_version(e->agent_ops.clientd));
        e->time_of_last_update_ns = now_ns;
    }

    return 0;
}

static int do_candidate_ballot(aeron_cluster_election_t *e, int64_t now_ns)
{
    int votes = aeron_cluster_member_count_votes(
        e->members, e->member_count, e->candidate_term_id);

    if (is_quorum(votes, e->member_count))
    {
        /* Won! */
        e->leader_member     = e->this_member;
        e->leadership_term_id = e->candidate_term_id;
        e->is_leader_startup  = e->is_node_startup;
        transition_to(e, AERON_ELECTION_LEADER_LOG_REPLICATION, now_ns);
        return 1;
    }

    if ((now_ns - e->time_of_state_change_ns) >= e->election_timeout_ns)
    {
        /* Timed out — reset per-election member tracking before restarting */
        for (int i = 0; i < e->member_count; i++)
        {
            if (&e->members[i] != e->this_member)
            {
                e->members[i].candidate_term_id  = -1;
                e->members[i].log_position       = -1;
                e->members[i].leadership_term_id = -1;
            }
        }
        transition_to(e, AERON_ELECTION_CANVASS, now_ns);
        return 1;
    }

    return 0;
}

static int do_follower_ballot(aeron_cluster_election_t *e, int64_t now_ns)
{
    if ((now_ns - e->time_of_state_change_ns) >= e->election_timeout_ns)
    {
        /* No leader announced — try nominating */
        e->nomination_deadline_ns = now_ns;
        transition_to(e, AERON_ELECTION_NOMINATE, now_ns);
        return 1;
    }
    return 0;
}

static int do_leader_log_replication(aeron_cluster_election_t *e, int64_t now_ns)
{
    int64_t ap = e->agent_ops.get_append_position(e->agent_ops.clientd);
    int64_t qp = e->agent_ops.quorum_position(e->agent_ops.clientd, ap, now_ns);

    e->this_member->log_position = ap;
    e->this_member->time_of_last_append_position_ns = now_ns;

    int work_count = 0;
    work_count += e->agent_ops.publish_new_leadership_term_on_interval(e->agent_ops.clientd, qp, now_ns);
    work_count += e->agent_ops.publish_commit_position_on_interval(e->agent_ops.clientd, qp, now_ns);

    if (qp >= ap)
    {
        transition_to(e, AERON_ELECTION_LEADER_REPLAY, now_ns);
        work_count++;
    }

    return work_count;
}

static int do_leader_replay(aeron_cluster_election_t *e, int64_t now_ns)
{
    int64_t ap = e->agent_ops.get_append_position(e->agent_ops.clientd);
    int work_count = 0;

    if (NULL == e->log_replay)
    {
        if (e->log_position < ap)
        {
            e->log_replay = e->agent_ops.new_log_replay(e->agent_ops.clientd, e->log_position, ap);
            if (NULL == e->log_replay)
            {
                /* Archive unavailable — skip replay, proceed to leader init */
                e->log_position = ap;
                e->is_leader_startup = e->is_node_startup;
                e->this_member->log_position = ap;
                transition_to(e, AERON_ELECTION_LEADER_INIT, now_ns);
                return 1;
            }
            work_count++;
        }
        else
        {
            transition_to(e, AERON_ELECTION_LEADER_INIT, now_ns);
            work_count++;
        }

        e->is_leader_startup = e->is_node_startup;
        e->this_member->leadership_term_id = e->leadership_term_id;
        e->this_member->log_position = ap;
        e->this_member->time_of_last_append_position_ns = now_ns;
    }
    else
    {
        int rc = e->agent_ops.log_replay_do_work(e->agent_ops.clientd, e->log_replay);
        if (rc < 0)
        {
            return -1;
        }
        work_count += rc;

        if (e->agent_ops.log_replay_is_done(e->agent_ops.clientd, e->log_replay))
        {
            e->log_position = e->agent_ops.log_replay_position(e->agent_ops.clientd, e->log_replay);
            e->agent_ops.close_log_replay(e->agent_ops.clientd, e->log_replay);
            e->log_replay = NULL;
            transition_to(e, AERON_ELECTION_LEADER_INIT, now_ns);
            work_count++;
        }
    }

    int64_t qp = e->agent_ops.quorum_position(e->agent_ops.clientd, ap, now_ns);
    work_count += e->agent_ops.publish_new_leadership_term_on_interval(e->agent_ops.clientd, qp, now_ns);
    work_count += e->agent_ops.publish_commit_position_on_interval(e->agent_ops.clientd, qp, now_ns);

    return work_count;
}

static int do_leader_init(aeron_cluster_election_t *e, int64_t now_ns)
{
    int64_t log_pos = e->agent_ops.get_append_position(e->agent_ops.clientd);

    e->agent_ops.join_log_as_leader(e->agent_ops.clientd,
        e->candidate_term_id, log_pos, e->log_session_id, e->is_leader_startup);

    /* Update election's log_session_id from agent's cached value (set during join_log_as_leader) */
    if (NULL != e->agent && e->agent->log_session_id_cache >= 0)
    {
        e->log_session_id = e->agent->log_session_id_cache;
    }

    e->agent_ops.begin_new_leadership_term(e->agent_ops.clientd,
        e->log_leadership_term_id, e->candidate_term_id,
        log_pos, aeron_nano_clock(), e->is_leader_startup);

    election_broadcast_new_leadership_term(e,
        e->log_leadership_term_id,
        e->candidate_term_id, log_pos, log_pos,
        e->candidate_term_id, log_pos, log_pos,
        e->agent_ops.get_log_recording_id(e->agent_ops.clientd),
        aeron_nano_clock(),
        e->this_member->id,
        e->log_session_id,
        e->agent_ops.get_app_version(e->agent_ops.clientd),
        e->is_leader_startup);

    e->agent_ops.update_recording_log(e->agent_ops.clientd, now_ns);
    e->leadership_term_id = e->candidate_term_id;
    transition_to(e, AERON_ELECTION_LEADER_READY, now_ns);
    return 1;
}

static int do_leader_ready(aeron_cluster_election_t *e, int64_t now_ns)
{
    int64_t ap = e->agent_ops.get_append_position(e->agent_ops.clientd);
    int64_t qp = e->agent_ops.quorum_position(e->agent_ops.clientd, ap, now_ns);

    int work_count = e->agent_ops.update_leader_position(e->agent_ops.clientd, now_ns, ap, qp);
    int pub_wc = e->agent_ops.publish_new_leadership_term_on_interval(e->agent_ops.clientd, qp, now_ns);
    work_count += pub_wc;

    /* Check quorum has acknowledged the new term at our log position.
     * Mirrors Java: ClusterMember.hasQuorumAtPosition → hasReachedPosition (isActive + term + position) */
    int64_t leader_log_pos = e->agent_ops.get_append_position(e->agent_ops.clientd);
    int ready = 0;
    for (int i = 0; i < e->member_count; i++)
    {
        aeron_cluster_member_t *m = &e->members[i];
        bool is_active = (m->time_of_last_append_position_ns + e->leader_heartbeat_timeout_ns) > now_ns;
        if (is_active &&
            m->leadership_term_id == e->leadership_term_id &&
            m->log_position >= leader_log_pos)
        {
            ready++;
        }
    }

    if (is_quorum(ready, e->member_count))
    {
        if (e->agent_ops.append_new_leadership_term_event(e->agent_ops.clientd, now_ns))
        {
            e->agent_ops.on_election_complete(e->agent_ops.clientd, e->leader_member, now_ns);
            transition_to(e, AERON_ELECTION_CLOSED, now_ns);
            work_count++;
        }
    }

    /* Periodically re-broadcast in case some followers missed it */
    if ((now_ns - e->time_of_last_update_ns) >= e->election_status_interval_ns)
    {
        int64_t log_pos = e->agent_ops.get_append_position(e->agent_ops.clientd);
        election_broadcast_commit_position(e,
            e->leadership_term_id, log_pos, e->this_member->id);
        e->time_of_last_update_ns = now_ns;
    }

    return work_count;
}

static int do_follower_ballot_handler(aeron_cluster_election_t *e, int64_t now_ns)
{
    /* Already covered by do_follower_ballot above */
    return do_follower_ballot(e, now_ns);
}

static int do_follower_log_replication(aeron_cluster_election_t *e, int64_t now_ns)
{
    int work_count = 0;

    if (NULL == e->log_replication)
    {
        if (e->append_position < e->replication_stop_position)
        {
            /* leader_member carries the archive endpoint in its metadata */
            const char *archive_ep  = (e->leader_member) ? e->leader_member->archive_endpoint : NULL;
            const char *resp_ep     = (e->leader_member) ? e->leader_member->archive_response_endpoint : NULL;
            e->log_replication = e->agent_ops.new_log_replication(e->agent_ops.clientd,
                archive_ep, resp_ep,
                e->leader_recording_id, e->replication_stop_position, now_ns);
            e->replication_deadline_ns = now_ns + e->leader_heartbeat_timeout_ns;
            work_count++;
        }
        else
        {
            e->agent_ops.update_recording_log_for_replication(e->agent_ops.clientd,
                e->replication_leader_term_id,
                e->replication_term_base_log_position,
                e->replication_stop_position, now_ns);
            transition_to(e, AERON_ELECTION_CANVASS, now_ns);
            work_count++;
        }
    }
    else
    {
        work_count += e->agent_ops.poll_archive_events(e->agent_ops.clientd);

        aeron_cluster_recording_replication_t *rep =
            (aeron_cluster_recording_replication_t *)e->log_replication;
        int rc = aeron_cluster_recording_replication_poll(rep, now_ns);
        if (rc < 0) { return -1; }

        bool replication_done =
            aeron_cluster_recording_replication_has_replication_ended(rep) &&
            aeron_cluster_recording_replication_has_stopped(rep);

        work_count += e->agent_ops.publish_follower_replication_position(e->agent_ops.clientd, now_ns);

        if (replication_done)
        {
            if (e->notified_commit_position >= e->append_position)
            {
                e->append_position = aeron_cluster_recording_replication_position(rep);
                e->agent_ops.close_log_replication(e->agent_ops.clientd, e->log_replication);
                e->log_replication = NULL;
                e->agent_ops.update_recording_log_for_replication(e->agent_ops.clientd,
                    e->replication_leader_term_id,
                    e->replication_term_base_log_position,
                    e->replication_stop_position, now_ns);
                transition_to(e, AERON_ELECTION_CANVASS, now_ns);
                work_count++;
            }
            else if (now_ns >= e->replication_deadline_ns)
            {
                AERON_SET_ERR(ETIMEDOUT, "%s", "timeout awaiting commit position after replication");
                return -1;
            }
        }
    }

    return work_count;
}

static int do_follower_replay(aeron_cluster_election_t *e, int64_t now_ns)
{
    int work_count = 0;

    if (NULL == e->log_replay)
    {
        if (e->log_position < e->append_position)
        {
            if (0 == e->notified_commit_position)
            {
                /* Send our position so leader knows we need catchup */
                if (e->leader_member && (now_ns - e->time_of_last_update_ns) >= e->election_status_interval_ns)
                {
                    e->pub_ops.append_position(e->pub_ops.clientd, e->leader_member,
                        e->leadership_term_id, e->log_position, e->this_member->id, 0);
                    e->time_of_last_update_ns = now_ns;
                }
                return 0;
            }
            else if (e->log_position >= e->notified_commit_position)
            {
                transition_to(e, AERON_ELECTION_CANVASS, now_ns);
                work_count++;
            }
            else
            {
                int64_t stop = (e->append_position < e->notified_commit_position)
                    ? e->append_position : e->notified_commit_position;
                e->log_replay = e->agent_ops.new_log_replay(e->agent_ops.clientd, e->log_position, stop);
                work_count++;
            }
        }
        else
        {
            aeron_cluster_election_state_t next =
                (AERON_NULL_VALUE != e->catchup_join_position)
                    ? AERON_ELECTION_FOLLOWER_CATCHUP_INIT
                    : AERON_ELECTION_FOLLOWER_LOG_INIT;
            transition_to(e, next, now_ns);
            work_count++;
        }
    }
    else
    {
        int rc = e->agent_ops.log_replay_do_work(e->agent_ops.clientd, e->log_replay);
        if (rc < 0) { return -1; }
        work_count += rc;

        if (e->agent_ops.log_replay_is_done(e->agent_ops.clientd, e->log_replay))
        {
            e->log_position = e->agent_ops.log_replay_position(e->agent_ops.clientd, e->log_replay);
            e->agent_ops.close_log_replay(e->agent_ops.clientd, e->log_replay);
            e->log_replay = NULL;

            if (e->log_position == e->append_position)
            {
                aeron_cluster_election_state_t next =
                    (AERON_NULL_VALUE != e->catchup_join_position)
                        ? AERON_ELECTION_FOLLOWER_CATCHUP_INIT
                        : AERON_ELECTION_FOLLOWER_LOG_INIT;
                transition_to(e, next, now_ns);
            }
            else
            {
                transition_to(e, AERON_ELECTION_CANVASS, now_ns);
            }
            work_count++;
        }
    }

    return work_count;
}

static int do_follower_catchup_init(aeron_cluster_election_t *e, int64_t now_ns)
{
    if (NULL == e->log_subscription)
    {
        e->log_subscription = e->agent_ops.add_follower_subscription(
            e->agent_ops.clientd, e->log_session_id);

        if (NULL != e->log_subscription)
        {
            const char *ep = e->agent_ops.this_catchup_endpoint(e->agent_ops.clientd);
            if (NULL != ep)
            {
                e->agent_ops.add_catchup_log_destination(
                    e->agent_ops.clientd, e->log_subscription, ep);
            }
        }
    }

    /* Resolve dynamic port: if endpoint ends with ":0", use the OS-assigned port */
    const char *static_ep = e->agent_ops.this_catchup_endpoint(e->agent_ops.clientd);
    char resolved_ep_buf[512];
    const char *catchup_ep = static_ep;

    if (NULL != static_ep && NULL != e->log_subscription)
    {
        size_t ep_len = strlen(static_ep);
        if (ep_len >= 2 && static_ep[ep_len - 2] == ':' && static_ep[ep_len - 1] == '0')
        {
            char resolved[512] = { 0 };
            if (aeron_subscription_resolved_endpoint(
                e->log_subscription, resolved, sizeof(resolved)) >= 0 && resolved[0] != '\0')
            {
                /* Replace trailing ":0" with the resolved ":port" */
                const char *colon = strrchr(resolved, ':');
                if (NULL != colon)
                {
                    snprintf(resolved_ep_buf, sizeof(resolved_ep_buf), "%.*s%s",
                        (int)(ep_len - 2), static_ep, colon);
                    catchup_ep = resolved_ep_buf;
                }
            }
            else
            {
                catchup_ep = NULL; /* not yet resolved */
            }
        }
    }

    if (NULL != catchup_ep &&
        e->agent_ops.send_catchup_position(e->agent_ops.clientd, catchup_ep))
    {
        e->time_of_last_update_ns = now_ns;
        e->agent_ops.catchup_initiated(e->agent_ops.clientd, now_ns);
        transition_to(e, AERON_ELECTION_FOLLOWER_CATCHUP_AWAIT, now_ns);
    }
    else if (now_ns >= (e->time_of_state_change_ns + e->leader_heartbeat_timeout_ns))
    {
        AERON_SET_ERR(ETIMEDOUT, "%s", "failed to send catchup position");
        return -1;
    }

    return 1;
}

static int do_follower_catchup_await(aeron_cluster_election_t *e, int64_t now_ns)
{
    int work_count = 0;

    if (NULL != e->log_subscription)
    {
        aeron_image_t *image = e->agent_ops.get_log_image(
            e->agent_ops.clientd, e->log_subscription, e->log_session_id);

        if (NULL != image)
        {
            if (e->agent_ops.try_join_log_as_follower(
                e->agent_ops.clientd, image, e->is_leader_startup, now_ns))
            {
                transition_to(e, AERON_ELECTION_FOLLOWER_CATCHUP, now_ns);
                work_count++;
            }
            else if (-1 == aeron_subscription_channel_status(e->log_subscription))
            {
                AERON_SET_ERR(EIO, "%s", "failed to add catchup log as follower - channel errored");
                return -1;
            }
            else if (now_ns >= (e->time_of_state_change_ns + e->leader_heartbeat_timeout_ns))
            {
                AERON_SET_ERR(ETIMEDOUT, "%s", "failed to join catchup log as follower");
                return -1;
            }
        }
        else if (now_ns >= (e->time_of_state_change_ns + e->leader_heartbeat_timeout_ns))
        {
            AERON_SET_ERR(ETIMEDOUT, "%s", "failed to join catchup log");
            return -1;
        }
    }

    return work_count;
}

static int do_follower_catchup(aeron_cluster_election_t *e, int64_t now_ns)
{
    int work_count = e->agent_ops.catchup_poll(
        e->agent_ops.clientd, e->notified_commit_position, now_ns);

    if (NULL == e->agent_ops.live_log_destination(e->agent_ops.clientd) &&
        e->agent_ops.is_catchup_near_live(e->agent_ops.clientd,
            (e->catchup_join_position > e->notified_commit_position)
                ? e->catchup_join_position : e->notified_commit_position))
    {
        e->agent_ops.add_live_log_destination(e->agent_ops.clientd);
        work_count++;
    }

    int64_t pos = e->agent_ops.get_commit_position(e->agent_ops.clientd);
    if (pos >= e->catchup_join_position &&
        pos >= e->notified_commit_position &&
        NULL == e->agent_ops.catchup_log_destination(e->agent_ops.clientd) &&
        AERON_CM_STATE_SNAPSHOT != (aeron_cm_state_t)e->agent_ops.agent_state(e->agent_ops.clientd))
    {
        e->append_position = pos;
        e->log_position    = pos;
        transition_to(e, AERON_ELECTION_FOLLOWER_LOG_INIT, now_ns);
        work_count++;
    }

    return work_count;
}

static int do_follower_log_init(aeron_cluster_election_t *e, int64_t now_ns)
{
    if (NULL == e->log_subscription)
    {
        /* No subscription yet (came from FOLLOWER_REPLAY, not catchup path).
         * Add the follower subscription and live log destination, then wait for the image. */
        if (-1 != e->log_session_id)
        {
            e->log_subscription = e->agent_ops.add_follower_subscription(
                e->agent_ops.clientd, e->log_session_id);
            if (NULL != e->log_subscription)
            {
                e->agent_ops.add_live_log_destination(e->agent_ops.clientd);
                transition_to(e, AERON_ELECTION_FOLLOWER_LOG_AWAIT, now_ns);
                return 1;
            }
            /* Subscription not yet available — retry next tick */
            return 0;
        }
        /* logSessionId unknown — retry next tick */
        return 0;
    }
    else
    {
        /* Subscription already exists (came from FOLLOWER_CATCHUP path). */
        transition_to(e, AERON_ELECTION_FOLLOWER_READY, now_ns);
        return 1;
    }
}

static int do_follower_log_await(aeron_cluster_election_t *e, int64_t now_ns)
{
    int work_count = 0;

    if (NULL != e->log_subscription)
    {
        aeron_image_t *image = e->agent_ops.get_log_image(
            e->agent_ops.clientd, e->log_subscription, e->log_session_id);

        if (NULL != image)
        {
            if (e->agent_ops.try_join_log_as_follower(
                e->agent_ops.clientd, image, e->is_leader_startup, now_ns))
            {
                e->agent_ops.update_recording_log(e->agent_ops.clientd, now_ns);
                transition_to(e, AERON_ELECTION_FOLLOWER_READY, now_ns);
                work_count++;
            }
            else if (now_ns >= (e->time_of_state_change_ns + e->leader_heartbeat_timeout_ns))
            {
                AERON_SET_ERR(ETIMEDOUT, "%s", "failed to join live log as follower");
                return -1;
            }
        }
        else if (-1 == aeron_subscription_channel_status(e->log_subscription))
        {
            AERON_SET_ERR(EIO, "%s", "failed to add live log as follower - channel errored");
            return -1;
        }
        else if (now_ns >= (e->time_of_state_change_ns + e->leader_heartbeat_timeout_ns))
        {
            AERON_SET_ERR(ETIMEDOUT, "%s", "failed to join live log");
            return -1;
        }
    }

    return work_count;
}

static int do_follower_ready(aeron_cluster_election_t *e, int64_t now_ns)
{
    /* Notify the leader; only complete the election once the send succeeds. */
    if (NULL != e->leader_member &&
        e->pub_ops.append_position(e->pub_ops.clientd, e->leader_member,
            e->leadership_term_id, e->log_position, e->this_member->id, 0))
    {
        e->agent_ops.on_election_complete(e->agent_ops.clientd, e->leader_member, now_ns);
        transition_to(e, AERON_ELECTION_CLOSED, now_ns);
    }
    else if (now_ns >= (e->time_of_state_change_ns + e->leader_heartbeat_timeout_ns))
    {
        AERON_SET_ERR(ETIMEDOUT, "%s", "ready follower failed to notify leader");
        return -1;
    }

    return 1;
}

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */
int aeron_cluster_election_create(
    aeron_cluster_election_t **election,
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_member_t *this_member,
    aeron_cluster_member_t *members, int member_count,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id, int64_t leader_recording_id,
    int64_t startup_canvass_timeout_ns,
    int64_t election_timeout_ns,
    int64_t election_status_interval_ns,
    int64_t leader_heartbeat_timeout_ns,
    bool is_node_startup)
{
    aeron_cluster_election_t *e = NULL;
    if (aeron_alloc((void **)&e, sizeof(aeron_cluster_election_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate election");
        return -1;
    }

    e->state                      = AERON_ELECTION_INIT;
    e->agent                      = agent;
    e->this_member                = this_member;
    e->members                    = members;
    e->member_count               = member_count;
    e->leader_member              = NULL;
    e->log_leadership_term_id     = log_leadership_term_id;
    e->log_position               = log_position;
    e->leadership_term_id         = leadership_term_id;
    e->candidate_term_id          = leadership_term_id;
    e->leader_recording_id        = leader_recording_id;
    e->append_position            = log_position;
    e->notified_commit_position   = 0;
    e->log_session_id             = -1;
    e->now_ns                     = aeron_nano_clock();
    e->time_of_state_change_ns    = e->now_ns;
    e->time_of_last_update_ns     = 0;
    e->nomination_deadline_ns     = INT64_MAX;
    e->startup_canvass_timeout_ns = startup_canvass_timeout_ns;
    e->election_timeout_ns        = election_timeout_ns;
    e->election_status_interval_ns = election_status_interval_ns;
    e->leader_heartbeat_timeout_ns = leader_heartbeat_timeout_ns;
    e->is_node_startup            = is_node_startup;
    e->is_leader_startup          = false;
    e->is_extended_canvass        = false;
    e->is_first_init              = true;
    e->log_replay                 = NULL;
    e->log_replication            = NULL;
    e->log_subscription           = NULL;
    e->catchup_join_position      = AERON_NULL_VALUE;
    e->replication_stop_position  = AERON_NULL_VALUE;
    e->replication_leader_term_id = AERON_NULL_VALUE;
    e->replication_term_base_log_position = 0;
    e->replication_deadline_ns    = INT64_MAX;
    e->appointed_leader_id        = (NULL != agent && NULL != agent->ctx)
                                    ? agent->ctx->appointed_leader_id : -1;

    /* Default: use real publisher ops */
    aeron_cluster_election_publisher_ops_init_real(&e->pub_ops, members, member_count,
        this_member->id);

    /* Default: use real agent ops */
    aeron_cluster_election_agent_ops_init_real(&e->agent_ops, agent);

    *election = e;
    return 0;
}

int aeron_cluster_election_close(aeron_cluster_election_t *election)
{
    aeron_free(election);
    return 0;
}

int aeron_cluster_election_do_work(aeron_cluster_election_t *election, int64_t now_ns)
{
    int work_count = 0;
    int result;

    election->now_ns = now_ns;

    do
    {
        switch (election->state)
        {
            case AERON_ELECTION_INIT:                     result = do_init(election, now_ns); break;
            case AERON_ELECTION_CANVASS:                  result = do_canvass(election, now_ns); break;
            case AERON_ELECTION_NOMINATE:                 result = do_nominate(election, now_ns); break;
            case AERON_ELECTION_CANDIDATE_BALLOT:         result = do_candidate_ballot(election, now_ns); break;
            case AERON_ELECTION_FOLLOWER_BALLOT:          result = do_follower_ballot_handler(election, now_ns); break;
            case AERON_ELECTION_LEADER_LOG_REPLICATION:   result = do_leader_log_replication(election, now_ns); break;
            case AERON_ELECTION_LEADER_REPLAY:            result = do_leader_replay(election, now_ns); break;
            case AERON_ELECTION_LEADER_INIT:              result = do_leader_init(election, now_ns); break;
            case AERON_ELECTION_LEADER_READY:             result = do_leader_ready(election, now_ns); break;
            case AERON_ELECTION_FOLLOWER_LOG_REPLICATION: result = do_follower_log_replication(election, now_ns); break;
            case AERON_ELECTION_FOLLOWER_REPLAY:          result = do_follower_replay(election, now_ns); break;
            case AERON_ELECTION_FOLLOWER_CATCHUP_INIT:    result = do_follower_catchup_init(election, now_ns); break;
            case AERON_ELECTION_FOLLOWER_CATCHUP_AWAIT:   result = do_follower_catchup_await(election, now_ns); break;
            case AERON_ELECTION_FOLLOWER_CATCHUP:         result = do_follower_catchup(election, now_ns); break;
            case AERON_ELECTION_FOLLOWER_LOG_INIT:        result = do_follower_log_init(election, now_ns); break;
            case AERON_ELECTION_FOLLOWER_LOG_AWAIT:       result = do_follower_log_await(election, now_ns); break;
            case AERON_ELECTION_FOLLOWER_READY:           result = do_follower_ready(election, now_ns); break;
            case AERON_ELECTION_CLOSED:                   result = 0; break;
            default:                                      result = 0; break;
        }
        work_count += result;
    }
    while (result > 0);

    return work_count;
}

/* -----------------------------------------------------------------------
 * Incoming message handlers (called by ConsensusAdapter)
 * ----------------------------------------------------------------------- */
void aeron_cluster_election_on_canvass_position(aeron_cluster_election_t *e,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id, int32_t follower_member_id,
    int32_t protocol_version)
{
    aeron_cluster_member_t *m = aeron_cluster_member_find_by_id(
        e->members, e->member_count, follower_member_id);
    if (NULL == m) { return; }

    m->log_position      = log_position;
    m->leadership_term_id = log_leadership_term_id;
    m->time_of_last_append_position_ns = e->now_ns;
}

void aeron_cluster_election_on_request_vote(aeron_cluster_election_t *e,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id)
{
    aeron_cluster_member_t *candidate = aeron_cluster_member_find_by_id(
        e->members, e->member_count, candidate_member_id);
    if (NULL == candidate) { return; }

    /* Vote YES if candidate has higher term and their log is at least as up-to-date as ours */
    bool log_ok = (log_leadership_term_id > e->log_leadership_term_id) ||
                  (log_leadership_term_id == e->log_leadership_term_id &&
                   log_position >= e->log_position);
    bool vote = (candidate_term_id > e->candidate_term_id) && log_ok;

    if (vote)
    {
        e->candidate_term_id = candidate_term_id;
    }

    e->pub_ops.vote(e->pub_ops.clientd, candidate,
        candidate_term_id, log_leadership_term_id, log_position,
        candidate_member_id, e->this_member->id, vote);

    /* Transition to FOLLOWER_BALLOT if voted YES from any pre-leader state.
     * Mirrors Java: CANVASS || NOMINATE || CANDIDATE_BALLOT || FOLLOWER_BALLOT */
    if (vote && (e->state == AERON_ELECTION_CANVASS ||
                 e->state == AERON_ELECTION_NOMINATE ||
                 e->state == AERON_ELECTION_CANDIDATE_BALLOT ||
                 e->state == AERON_ELECTION_FOLLOWER_BALLOT))
    {
        transition_to(e, AERON_ELECTION_FOLLOWER_BALLOT, e->now_ns);
    }
}

void aeron_cluster_election_on_vote(aeron_cluster_election_t *e,
    int64_t candidate_term_id, int64_t log_leadership_term_id,
    int64_t log_position, int32_t candidate_member_id,
    int32_t follower_member_id, bool vote)
{
    if (e->state != AERON_ELECTION_CANDIDATE_BALLOT) { return; }
    if (candidate_member_id != e->this_member->id) { return; }
    if (candidate_term_id != e->candidate_term_id) { return; }

    aeron_cluster_member_t *m = aeron_cluster_member_find_by_id(
        e->members, e->member_count, follower_member_id);
    if (NULL != m && vote)
    {
        m->candidate_term_id = candidate_term_id;
    }
}

void aeron_cluster_election_on_new_leadership_term(aeron_cluster_election_t *e,
    int64_t log_leadership_term_id,
    int64_t next_leadership_term_id,
    int64_t next_term_base_log_position,
    int64_t next_log_position,
    int64_t leadership_term_id,
    int64_t term_base_log_position,
    int64_t log_position,
    int64_t commit_position,
    int64_t leader_recording_id,
    int64_t timestamp,
    int32_t leader_member_id,
    int32_t log_session_id,
    int32_t app_version,
    bool is_startup)
{
    aeron_cluster_member_t *leader = aeron_cluster_member_find_by_id(
        e->members, e->member_count, leader_member_id);
    if (NULL == leader) { return; }

    e->leader_member      = leader;
    e->leadership_term_id = next_leadership_term_id;
    e->log_session_id     = log_session_id;
    e->is_leader_startup  = is_startup;

    /* Mirror Java: notifiedCommitPosition = max(notifiedCommitPosition, commitPosition) */
    if (commit_position > e->notified_commit_position)
    {
        e->notified_commit_position = commit_position;
    }

    /* Notify agent to set up the log subscription as a follower */
    e->agent_ops.on_follower_new_leadership_term(e->agent_ops.clientd,
        log_leadership_term_id, next_leadership_term_id,
        next_term_base_log_position, next_log_position,
        leadership_term_id, term_base_log_position, log_position,
        leader_recording_id, timestamp, leader_member_id,
        log_session_id, app_version, is_startup);

    int64_t now_ns = e->now_ns;

    if (e->append_position < term_base_log_position)
    {
        /* Follower is behind: attempt log replication to catch up.
         * Mirrors Java Election.onNewLeadershipTerm when appendPosition < termBaseLogPosition. */
        if (next_leadership_term_id != AERON_NULL_VALUE)
        {
            if (e->append_position < next_term_base_log_position)
            {
                e->replication_leader_term_id         = log_leadership_term_id;
                e->replication_stop_position          = next_term_base_log_position;
                e->replication_term_base_log_position = AERON_NULL_VALUE;
                transition_to(e, AERON_ELECTION_FOLLOWER_LOG_REPLICATION, now_ns);
            }
            else if (e->append_position == next_term_base_log_position &&
                     next_log_position != AERON_NULL_VALUE)
            {
                e->replication_leader_term_id         = next_leadership_term_id;
                e->replication_stop_position          = next_log_position;
                e->replication_term_base_log_position = next_term_base_log_position;
                transition_to(e, AERON_ELECTION_FOLLOWER_LOG_REPLICATION, now_ns);
            }
            else
            {
                /* Unclear position — fall back to canvass */
                transition_to(e, AERON_ELECTION_CANVASS, now_ns);
            }
        }
        else
        {
            /* nextLeadershipTermId not provided — invalid, re-canvass for safety */
            transition_to(e, AERON_ELECTION_CANVASS, now_ns);
        }
    }
    else
    {
        /* appendPosition >= termBaseLogPosition: may need log replay then log join.
         * Mirrors Java: state(FOLLOWER_REPLAY). */
        e->catchup_join_position =
            (e->append_position < log_position) ? log_position : AERON_NULL_VALUE;
        transition_to(e, AERON_ELECTION_FOLLOWER_REPLAY, now_ns);
    }
}

void aeron_cluster_election_on_append_position(aeron_cluster_election_t *e,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, int8_t flags)
{
    aeron_cluster_member_t *m = aeron_cluster_member_find_by_id(
        e->members, e->member_count, follower_member_id);
    if (NULL == m) { return; }

    m->log_position      = log_position;
    m->leadership_term_id = leadership_term_id;
    m->time_of_last_append_position_ns = e->now_ns;
}

void aeron_cluster_election_on_commit_position(aeron_cluster_election_t *e,
    int64_t leadership_term_id, int64_t log_position, int32_t leader_member_id)
{
    if (e->notified_commit_position < log_position)
    {
        e->notified_commit_position = log_position;
        e->agent_ops.notify_commit_position(e->agent_ops.clientd, log_position);
    }
}

/* -----------------------------------------------------------------------
 * Accessors
 * ----------------------------------------------------------------------- */
aeron_cluster_election_state_t aeron_cluster_election_state(aeron_cluster_election_t *e)
{ return e->state; }

aeron_cluster_member_t *aeron_cluster_election_leader(aeron_cluster_election_t *e)
{ return e->leader_member; }

int64_t aeron_cluster_election_leadership_term_id(aeron_cluster_election_t *e)
{ return e->leadership_term_id; }

int64_t aeron_cluster_election_log_position(aeron_cluster_election_t *e)
{ return e->log_position; }

int32_t aeron_cluster_election_log_session_id(aeron_cluster_election_t *e)
{ return e->log_session_id; }

bool aeron_cluster_election_is_leader_startup(aeron_cluster_election_t *e)
{ return e->is_leader_startup; }

bool aeron_cluster_election_is_closed(aeron_cluster_election_t *e)
{ return e->state == AERON_ELECTION_CLOSED; }
