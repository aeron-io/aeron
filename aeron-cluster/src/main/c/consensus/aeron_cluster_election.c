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
#include <stdlib.h>

#include "aeron_cluster_election.h"
#include "aeron_consensus_module_agent.h"
#include "aeron_cluster_consensus_publisher.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_clock.h"

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

static void transition_to(aeron_cluster_election_t *e,
                           aeron_cluster_election_state_t new_state,
                           int64_t now_ns)
{
    e->state                  = new_state;
    e->time_of_state_change_ns = now_ns;
    aeron_consensus_module_agent_on_election_state_change(e->agent, new_state, now_ns);
}

/* -----------------------------------------------------------------------
 * State handlers
 * ----------------------------------------------------------------------- */
static int do_init(aeron_cluster_election_t *e, int64_t now_ns)
{
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
        aeron_cluster_consensus_publisher_broadcast_canvass_position(
            e->members, e->member_count, e->this_member->id,
            e->log_leadership_term_id, e->log_position,
            e->leadership_term_id,
            e->this_member->id,
            aeron_consensus_module_agent_get_protocol_version(e->agent));
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
            /* Set nomination deadline with randomized jitter */
            e->nomination_deadline_ns = now_ns + e->election_timeout_ns +
                (int64_t)(rand() % (int)(e->election_timeout_ns));
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
        e->candidate_term_id = e->log_leadership_term_id + 1;

        /* Vote for self */
        e->this_member->candidate_term_id = e->candidate_term_id;

        /* Request votes from all peers */
        aeron_cluster_consensus_publisher_broadcast_request_vote(
            e->members, e->member_count, e->this_member->id,
            e->log_leadership_term_id, e->log_position,
            e->candidate_term_id, e->this_member->id);

        transition_to(e, AERON_ELECTION_CANDIDATE_BALLOT, now_ns);
        return 1;
    }

    /* While waiting, keep broadcasting canvass */
    if ((now_ns - e->time_of_last_update_ns) >= e->election_status_interval_ns)
    {
        aeron_cluster_consensus_publisher_broadcast_canvass_position(
            e->members, e->member_count, e->this_member->id,
            e->log_leadership_term_id, e->log_position,
            e->leadership_term_id, e->this_member->id,
            aeron_consensus_module_agent_get_protocol_version(e->agent));
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
        /* Timed out — restart from canvass */
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
    /* Ensure all members have replicated the log up to our position.
     * For simplicity: if all members within heartbeat timeout have sent positions
     * within range, proceed.  Full implementation requires tracking AppendPosition
     * from each follower here. */
    transition_to(e, AERON_ELECTION_LEADER_REPLAY, now_ns);
    return 1;
}

static int do_leader_replay(aeron_cluster_election_t *e, int64_t now_ns)
{
    /* Replay the local log entries.  In the full implementation this drives
     * LogAdapter to replay up to log_position.  Simplified: proceed immediately. */
    aeron_consensus_module_agent_on_replay_new_leadership_term_event(e->agent,
        e->log_leadership_term_id,
        e->log_position,
        aeron_nano_clock(),
        e->log_position,
        e->this_member->id,
        e->log_session_id,
        aeron_consensus_module_agent_get_app_version(e->agent));

    transition_to(e, AERON_ELECTION_LEADER_INIT, now_ns);
    return 1;
}

static int do_leader_init(aeron_cluster_election_t *e, int64_t now_ns)
{
    /* Append NewLeadershipTermEvent to the log and publish to all followers. */
    int64_t new_term_id = e->candidate_term_id;
    int64_t log_pos     = aeron_consensus_module_agent_get_append_position(e->agent);

    aeron_consensus_module_agent_begin_new_leadership_term(e->agent,
        e->log_leadership_term_id,
        new_term_id,
        log_pos,
        aeron_nano_clock(),
        e->is_leader_startup);

    aeron_cluster_consensus_publisher_broadcast_new_leadership_term(
        e->members, e->member_count, e->this_member->id,
        e->log_leadership_term_id,
        new_term_id,
        log_pos,
        log_pos,
        new_term_id,
        log_pos,
        log_pos,
        aeron_consensus_module_agent_get_log_recording_id(e->agent),
        aeron_nano_clock(),
        e->this_member->id,
        e->log_session_id,
        aeron_consensus_module_agent_get_app_version(e->agent),
        e->is_leader_startup);

    e->leadership_term_id = new_term_id;
    transition_to(e, AERON_ELECTION_LEADER_READY, now_ns);
    return 1;
}

static int do_leader_ready(aeron_cluster_election_t *e, int64_t now_ns)
{
    /* Wait for quorum of followers to send AppendPosition acknowledging new term. */
    int ready = 1;  /* count self */
    for (int i = 0; i < e->member_count; i++)
    {
        if (e->members[i].id == e->this_member->id) { continue; }
        if (e->members[i].leadership_term_id == e->leadership_term_id) { ready++; }
    }

    if (is_quorum(ready, e->member_count))
    {
        aeron_consensus_module_agent_on_election_complete(e->agent, e->leader_member, now_ns);
        transition_to(e, AERON_ELECTION_CLOSED, now_ns);
        return 1;
    }

    /* Periodically re-broadcast in case some followers missed it */
    if ((now_ns - e->time_of_last_update_ns) >= e->election_status_interval_ns)
    {
        int64_t log_pos = aeron_consensus_module_agent_get_append_position(e->agent);
        aeron_cluster_consensus_publisher_broadcast_commit_position(
            e->members, e->member_count, e->this_member->id,
            e->leadership_term_id, log_pos, e->this_member->id);
        e->time_of_last_update_ns = now_ns;
    }

    return 0;
}

static int do_follower_ballot_handler(aeron_cluster_election_t *e, int64_t now_ns)
{
    /* Already covered by do_follower_ballot above */
    return do_follower_ballot(e, now_ns);
}

static int do_follower_log_replication(aeron_cluster_election_t *e, int64_t now_ns)
{
    transition_to(e, AERON_ELECTION_FOLLOWER_REPLAY, now_ns);
    return 1;
}

static int do_follower_replay(aeron_cluster_election_t *e, int64_t now_ns)
{
    transition_to(e, AERON_ELECTION_FOLLOWER_CATCHUP_INIT, now_ns);
    return 1;
}

static int do_follower_catchup_init(aeron_cluster_election_t *e, int64_t now_ns)
{
    transition_to(e, AERON_ELECTION_FOLLOWER_LOG_INIT, now_ns);
    return 1;
}

static int do_follower_log_init(aeron_cluster_election_t *e, int64_t now_ns)
{
    transition_to(e, AERON_ELECTION_FOLLOWER_LOG_AWAIT, now_ns);
    return 1;
}

static int do_follower_log_await(aeron_cluster_election_t *e, int64_t now_ns)
{
    /* Wait until we receive NewLeadershipTerm from the leader. */
    if ((now_ns - e->time_of_state_change_ns) >= e->election_timeout_ns)
    {
        /* Timeout — restart */
        transition_to(e, AERON_ELECTION_CANVASS, now_ns);
        return 1;
    }
    return 0;
}

static int do_follower_ready(aeron_cluster_election_t *e, int64_t now_ns)
{
    /* Send AppendPosition to confirm we are ready. */
    if (NULL != e->leader_member && NULL != e->leader_member->publication)
    {
        aeron_cluster_consensus_publisher_append_position(
            e->leader_member->publication,
            e->leadership_term_id,
            e->log_position,
            e->this_member->id,
            0);
    }

    aeron_consensus_module_agent_on_election_complete(e->agent, e->leader_member, now_ns);
    transition_to(e, AERON_ELECTION_CLOSED, now_ns);
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
    e->time_of_state_change_ns    = aeron_nano_clock();
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
    switch (election->state)
    {
        case AERON_ELECTION_INIT:                    return do_init(election, now_ns);
        case AERON_ELECTION_CANVASS:                 return do_canvass(election, now_ns);
        case AERON_ELECTION_NOMINATE:                return do_nominate(election, now_ns);
        case AERON_ELECTION_CANDIDATE_BALLOT:        return do_candidate_ballot(election, now_ns);
        case AERON_ELECTION_FOLLOWER_BALLOT:         return do_follower_ballot_handler(election, now_ns);
        case AERON_ELECTION_LEADER_LOG_REPLICATION:  return do_leader_log_replication(election, now_ns);
        case AERON_ELECTION_LEADER_REPLAY:           return do_leader_replay(election, now_ns);
        case AERON_ELECTION_LEADER_INIT:             return do_leader_init(election, now_ns);
        case AERON_ELECTION_LEADER_READY:            return do_leader_ready(election, now_ns);
        case AERON_ELECTION_FOLLOWER_LOG_REPLICATION: return do_follower_log_replication(election, now_ns);
        case AERON_ELECTION_FOLLOWER_REPLAY:         return do_follower_replay(election, now_ns);
        case AERON_ELECTION_FOLLOWER_CATCHUP_INIT:   return do_follower_catchup_init(election, now_ns);
        case AERON_ELECTION_FOLLOWER_CATCHUP_AWAIT:  return 0;  /* handled via on_new_leadership_term */
        case AERON_ELECTION_FOLLOWER_CATCHUP:        return 0;
        case AERON_ELECTION_FOLLOWER_LOG_INIT:       return do_follower_log_init(election, now_ns);
        case AERON_ELECTION_FOLLOWER_LOG_AWAIT:      return do_follower_log_await(election, now_ns);
        case AERON_ELECTION_FOLLOWER_READY:          return do_follower_ready(election, now_ns);
        case AERON_ELECTION_CLOSED:                  return 0;
        default:                                     return 0;
    }
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
}

void aeron_cluster_election_on_request_vote(aeron_cluster_election_t *e,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id)
{
    aeron_cluster_member_t *candidate = aeron_cluster_member_find_by_id(
        e->members, e->member_count, candidate_member_id);
    if (NULL == candidate || NULL == candidate->publication) { return; }

    /* Vote YES only if candidate is at least as up-to-date as us */
    bool vote = (candidate_term_id > e->candidate_term_id) &&
                !is_better_candidate(e, log_leadership_term_id, log_position);

    if (vote)
    {
        e->candidate_term_id = candidate_term_id;
    }

    aeron_cluster_consensus_publisher_vote(candidate->publication,
        candidate_term_id, log_leadership_term_id, log_position,
        candidate_member_id, e->this_member->id, vote);

    if (vote && e->state == AERON_ELECTION_CANVASS)
    {
        transition_to(e, AERON_ELECTION_FOLLOWER_BALLOT, aeron_nano_clock());
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

    /* Notify agent to set up the log subscription as a follower */
    aeron_consensus_module_agent_on_follower_new_leadership_term(e->agent,
        log_leadership_term_id, next_leadership_term_id,
        next_term_base_log_position, next_log_position,
        leadership_term_id, term_base_log_position, log_position,
        leader_recording_id, timestamp, leader_member_id,
        log_session_id, app_version, is_startup);

    transition_to(e, AERON_ELECTION_FOLLOWER_READY, aeron_nano_clock());
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
    m->time_of_last_append_position_ns = aeron_nano_clock();
}

void aeron_cluster_election_on_commit_position(aeron_cluster_election_t *e,
    int64_t leadership_term_id, int64_t log_position, int32_t leader_member_id)
{
    if (e->notified_commit_position < log_position)
    {
        e->notified_commit_position = log_position;
        aeron_consensus_module_agent_notify_commit_position(e->agent, log_position);
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
