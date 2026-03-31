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

#ifndef AERON_CLUSTER_ELECTION_H
#define AERON_CLUSTER_ELECTION_H

#include <stdint.h>
#include <stdbool.h>

#include "aeron_consensus_module_configuration.h"
#include "aeron_cluster_member.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct aeron_consensus_module_agent_stct aeron_consensus_module_agent_t;

/**
 * Injectable publisher dispatch table.
 * Production code fills this with real broadcast functions.
 * Tests fill it with recording mock functions.
 * Mirrors Java's ConsensusPublisher dependency injection into Election.
 */
typedef struct aeron_cluster_election_publisher_ops_stct
{
    void *clientd;

    bool (*canvass_position)(
        void *clientd, aeron_cluster_member_t *member,
        int64_t log_leadership_term_id, int64_t log_position,
        int64_t leadership_term_id, int32_t follower_member_id,
        int32_t protocol_version);

    bool (*request_vote)(
        void *clientd, aeron_cluster_member_t *member,
        int64_t log_leadership_term_id, int64_t log_position,
        int64_t candidate_term_id, int32_t candidate_member_id);

    bool (*vote)(
        void *clientd, aeron_cluster_member_t *member,
        int64_t candidate_term_id, int64_t log_leadership_term_id,
        int64_t log_position, int32_t candidate_member_id,
        int32_t follower_member_id, bool vote);

    bool (*new_leadership_term)(
        void *clientd, aeron_cluster_member_t *member,
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
        bool is_startup);

    bool (*append_position)(
        void *clientd, aeron_cluster_member_t *member,
        int64_t leadership_term_id, int64_t log_position,
        int32_t follower_member_id, int8_t flags);

    bool (*commit_position)(
        void *clientd, aeron_cluster_member_t *member,
        int64_t leadership_term_id, int64_t log_position,
        int32_t leader_member_id);
}
aeron_cluster_election_publisher_ops_t;

/**
 * Fill ops with the real broadcast implementations (used in production).
 */
void aeron_cluster_election_publisher_ops_init_real(
    aeron_cluster_election_publisher_ops_t *ops,
    aeron_cluster_member_t *members,
    int member_count,
    int32_t self_id);

/**
 * Injectable agent operations table — mirrors Java's ConsensusModuleAgent dependency.
 */
typedef struct aeron_cluster_election_agent_ops_stct
{
    void *clientd;
    int32_t (*get_protocol_version)(void *clientd);
    int32_t (*get_app_version)(void *clientd);
    int64_t (*get_append_position)(void *clientd);
    int64_t (*get_log_recording_id)(void *clientd);
    void (*on_state_change)(void *clientd,
        aeron_cluster_election_state_t new_state, int64_t now_ns);
    void (*on_election_complete)(void *clientd,
        aeron_cluster_member_t *leader, int64_t now_ns, bool is_startup);
    void (*begin_new_leadership_term)(void *clientd,
        int64_t log_leadership_term_id, int64_t new_term_id,
        int64_t log_position, int64_t timestamp, bool is_startup);
    void (*on_follower_new_leadership_term)(void *clientd,
        int64_t log_leadership_term_id, int64_t next_term_id,
        int64_t next_term_base, int64_t next_log_pos,
        int64_t leadership_term_id, int64_t term_base, int64_t log_position,
        int64_t leader_recording_id, int64_t timestamp,
        int32_t leader_member_id, int32_t log_session_id,
        int32_t app_version, bool is_startup);
    void (*on_replay_new_leadership_term)(void *clientd,
        int64_t leadership_term_id, int64_t log_position, int64_t timestamp,
        int64_t term_base, int32_t leader_member_id,
        int32_t log_session_id, int32_t app_version);
    void (*notify_commit_position)(void *clientd, int64_t commit_position);

    /* ---- Phase 1.5: complex state callbacks ---- */

    /** Compute quorum position across members, bounded by append_position. */
    int64_t (*quorum_position)(void *clientd, int64_t append_position, int64_t now_ns);

    /** Publish NewLeadershipTerm to followers on interval; returns work count. */
    int (*publish_new_leadership_term_on_interval)(void *clientd,
        int64_t quorum_position, int64_t now_ns);

    /** Publish CommitPosition to followers on interval; returns work count. */
    int (*publish_commit_position_on_interval)(void *clientd,
        int64_t quorum_position, int64_t now_ns);

    /** Create a LogReplay from [from_position, to_position).  Returns opaque handle or NULL. */
    void *(*new_log_replay)(void *clientd, int64_t from_position, int64_t to_position);

    /** Drive LogReplay.doWork(); returns fragments/events processed or -1 on error. */
    int (*log_replay_do_work)(void *clientd, void *log_replay);

    /** Returns true when the log_replay has consumed up to stop_position. */
    bool (*log_replay_is_done)(void *clientd, void *log_replay);

    /** Returns the current position of the log_replay. */
    int64_t (*log_replay_position)(void *clientd, void *log_replay);

    /** Stop and free a LogReplay. */
    void (*close_log_replay)(void *clientd, void *log_replay);

    /** Join the log as leader; sets up log publication and records session ID. */
    void (*join_log_as_leader)(void *clientd,
        int64_t term_id, int64_t log_position, int32_t session_id, bool is_startup);

    /** Update the RecordingLog with the new term. */
    void (*update_recording_log)(void *clientd, int64_t now_ns);

    /** Update the leader position counters; returns work count. */
    int (*update_leader_position)(void *clientd,
        int64_t now_ns, int64_t append_position, int64_t quorum_position);

    /** Append a NewLeadershipTermEvent to the log; returns true on success. */
    bool (*append_new_leadership_term_event)(void *clientd, int64_t now_ns);

    /* --- Follower log replication --- */

    /** Create a RecordingReplication toward the leader archive.  Opaque handle or NULL. */
    void *(*new_log_replication)(void *clientd,
        const char *archive_endpoint, const char *resp_endpoint,
        int64_t leader_recording_id, int64_t stop_position, int64_t now_ns);

    /** Stop and free a RecordingReplication. */
    void (*close_log_replication)(void *clientd, void *replication);

    /** Poll the archive for RecordingSignals; returns work count. */
    int (*poll_archive_events)(void *clientd);

    /** Publish follower replication position; returns work count. */
    int (*publish_follower_replication_position)(void *clientd, int64_t now_ns);

    /** Update the recording log after replication completes. */
    void (*update_recording_log_for_replication)(void *clientd,
        int64_t term_id, int64_t base_position, int64_t stop_position, int64_t now_ns);

    /* --- Follower catchup --- */

    /** Create the follower log subscription.  Returns subscription or NULL. */
    aeron_subscription_t *(*add_follower_subscription)(void *clientd, int32_t session_id);

    /**
     * Get the log image for the given session from the subscription.
     * Wraps aeron_subscription_image_by_session_id so tests can intercept it.
     * Returns NULL if the image has not yet joined.
     */
    aeron_image_t *(*get_log_image)(void *clientd,
        aeron_subscription_t *subscription, int32_t session_id);

    /** Add a catchup destination to the follower subscription. */
    void (*add_catchup_log_destination)(void *clientd,
        aeron_subscription_t *subscription, const char *endpoint);

    /** Add the live log destination to the follower subscription. */
    void (*add_live_log_destination)(void *clientd);

    /** Return this member's catchup endpoint (static string, NULL if not configured). */
    const char *(*this_catchup_endpoint)(void *clientd);

    /** Send CatchupPosition to the leader; returns true if successfully sent. */
    bool (*send_catchup_position)(void *clientd, const char *catchup_endpoint);

    /** Notify agent that catchup has been initiated. */
    void (*catchup_initiated)(void *clientd, int64_t now_ns);

    /** Try to join the log stream as a follower; returns true when done. */
    bool (*try_join_log_as_follower)(void *clientd,
        aeron_image_t *image, bool is_startup, int64_t now_ns);

    /** Drive catchup polling; returns work count. */
    int (*catchup_poll)(void *clientd, int64_t commit_position, int64_t now_ns);

    /** Returns true when the follower is close enough to live to merge. */
    bool (*is_catchup_near_live)(void *clientd, int64_t position);

    /** Returns non-NULL when a live log destination has been added. */
    const char *(*live_log_destination)(void *clientd);

    /** Returns non-NULL while catchup destination is still active. */
    const char *(*catchup_log_destination)(void *clientd);

    /** Returns the current commit position counter value. */
    int64_t (*get_commit_position)(void *clientd);

    /** Returns the agent's current state enum value. */
    int (*agent_state)(void *clientd);
}
aeron_cluster_election_agent_ops_t;

void aeron_cluster_election_agent_ops_init_real(
    aeron_cluster_election_agent_ops_t *ops,
    aeron_consensus_module_agent_t *agent);

typedef struct aeron_cluster_election_stct
{
    aeron_cluster_election_state_t  state;
    aeron_consensus_module_agent_t *agent;
    aeron_cluster_election_publisher_ops_t pub_ops;    /* injectable publisher */
    aeron_cluster_election_agent_ops_t     agent_ops;  /* injectable agent */

    aeron_cluster_member_t         *this_member;
    aeron_cluster_member_t         *members;
    int                             member_count;
    aeron_cluster_member_t         *leader_member;

    /* Term tracking */
    int64_t  log_leadership_term_id;
    int64_t  log_position;
    int64_t  leadership_term_id;
    int64_t  candidate_term_id;
    int64_t  leader_recording_id;
    int64_t  append_position;
    int64_t  notified_commit_position;
    int32_t  log_session_id;

    /* Timing */
    int64_t  time_of_state_change_ns;
    int64_t  time_of_last_update_ns;
    int64_t  nomination_deadline_ns;
    int64_t  startup_canvass_timeout_ns;
    int64_t  election_timeout_ns;
    int64_t  election_status_interval_ns;
    int64_t  leader_heartbeat_timeout_ns;
    int64_t  now_ns;  /* last do_work time; used by message handlers for state transitions */

    bool     is_node_startup;
    bool     is_leader_startup;
    bool     is_extended_canvass;
    bool     is_first_init;

    /* --- Phase 1.5: complex state resources --- */

    /* LogReplay handle (opaque void*) used in LEADER_REPLAY and FOLLOWER_REPLAY */
    void    *log_replay;

    /* RecordingReplication handle (opaque void*) used in FOLLOWER_LOG_REPLICATION */
    void    *log_replication;

    /* Follower log subscription (FOLLOWER_CATCHUP_INIT onwards) */
    aeron_subscription_t *log_subscription;

    /* Position at which the follower log image joins — must match leader's nextLogPosition */
    int64_t  catchup_join_position;

    /* Follower replication target: leader's log position to replicate up to */
    int64_t  replication_stop_position;

    /* Term metadata saved when entering FOLLOWER_LOG_REPLICATION */
    int64_t  replication_leader_term_id;
    int64_t  replication_term_base_log_position;

    /* Deadline for replication to reach committed position after ending */
    int64_t  replication_deadline_ns;

    /* Appointed leader ID (-1 = none) */
    int32_t  appointed_leader_id;
}
aeron_cluster_election_t;

int aeron_cluster_election_create(
    aeron_cluster_election_t **election,
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_member_t *this_member,
    aeron_cluster_member_t *members,
    int member_count,
    int64_t log_leadership_term_id,
    int64_t log_position,
    int64_t leadership_term_id,
    int64_t leader_recording_id,
    int64_t startup_canvass_timeout_ns,
    int64_t election_timeout_ns,
    int64_t election_status_interval_ns,
    int64_t leader_heartbeat_timeout_ns,
    bool is_node_startup);

int aeron_cluster_election_close(aeron_cluster_election_t *election);

/** Drive the election state machine.  Returns work count (> 0 if state advanced). */
int aeron_cluster_election_do_work(aeron_cluster_election_t *election, int64_t now_ns);

/** Called by ConsensuAdapter to deliver incoming messages. */
void aeron_cluster_election_on_canvass_position(aeron_cluster_election_t *election,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t leadership_term_id, int32_t follower_member_id,
    int32_t protocol_version);

void aeron_cluster_election_on_request_vote(aeron_cluster_election_t *election,
    int64_t log_leadership_term_id, int64_t log_position,
    int64_t candidate_term_id, int32_t candidate_member_id);

void aeron_cluster_election_on_vote(aeron_cluster_election_t *election,
    int64_t candidate_term_id, int64_t log_leadership_term_id,
    int64_t log_position, int32_t candidate_member_id,
    int32_t follower_member_id, bool vote);

void aeron_cluster_election_on_new_leadership_term(aeron_cluster_election_t *election,
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
    bool is_startup);

void aeron_cluster_election_on_append_position(aeron_cluster_election_t *election,
    int64_t leadership_term_id, int64_t log_position,
    int32_t follower_member_id, int8_t flags);

void aeron_cluster_election_on_commit_position(aeron_cluster_election_t *election,
    int64_t leadership_term_id, int64_t log_position,
    int32_t leader_member_id);

/* Accessors */
aeron_cluster_election_state_t aeron_cluster_election_state(aeron_cluster_election_t *election);
aeron_cluster_member_t *aeron_cluster_election_leader(aeron_cluster_election_t *election);
int64_t aeron_cluster_election_leadership_term_id(aeron_cluster_election_t *election);
int64_t aeron_cluster_election_log_position(aeron_cluster_election_t *election);
int32_t aeron_cluster_election_log_session_id(aeron_cluster_election_t *election);
bool    aeron_cluster_election_is_leader_startup(aeron_cluster_election_t *election);
bool    aeron_cluster_election_is_closed(aeron_cluster_election_t *election);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_ELECTION_H */
