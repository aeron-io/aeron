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

#ifndef AERON_CONSENSUS_MODULE_AGENT_H
#define AERON_CONSENSUS_MODULE_AGENT_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#include "aeronc.h"
#include "aeron_consensus_module_configuration.h"
#include "aeron_cluster_member.h"
#include "aeron_cluster_recording_log.h"
#include "aeron_cluster_timer_service.h"
#include "aeron_cluster_session_manager.h"
#include "aeron_cluster_service_proxy_cm.h"
#include "aeron_cluster_consensus_publisher.h"
#include "aeron_cluster_egress_publisher.h"
#include "aeron_cluster_log_publisher.h"
#include "aeron_cluster_consensus_adapter.h"
#include "aeron_cluster_ingress_adapter_cm.h"
#include "aeron_cluster_cm_snapshot_taker.h"
#include "aeron_cluster_election.h"
#include "aeron_cluster_pending_message_tracker.h"
#include "aeron_cluster_log_adapter.h"
#include "aeron_consensus_module_agent_fwd.h"
#include "aeron_archive.h"
#include "aeron_cluster_consensus_module_adapter.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct aeron_cm_context_stct aeron_cm_context_t;
typedef struct aeron_cluster_log_replay_stct aeron_cluster_log_replay_t;
typedef struct aeron_cluster_recording_replication_stct aeron_cluster_recording_replication_t;

struct aeron_consensus_module_agent_stct
{
    aeron_cm_context_t              *ctx;
    aeron_t                         *aeron;

    /* Identity */
    int32_t  member_id;
    aeron_cm_state_t  state;
    aeron_cluster_role_t role;

    /* Members */
    aeron_cluster_member_t          *active_members;
    int                              active_member_count;
    aeron_cluster_member_t          *this_member;
    aeron_cluster_member_t          *leader_member;
    int64_t                         *ranked_positions;

    /* Log / term tracking */
    int64_t  leadership_term_id;
    int64_t  expected_ack_position;
    int64_t  service_ack_id;
    int64_t  last_append_position;
    int64_t  notified_commit_position;
    int64_t  termination_position;
    int64_t  log_subscription_id;
    int64_t  log_recording_id;
    int64_t  initial_log_leadership_term_id;  /* leadership term id at election start */
    int64_t  initial_term_base_log_position;  /* term base log position at election start */
    int32_t  log_session_id_cache;   /* session_id of the log publication */
    int32_t  app_version;
    int32_t  protocol_version;

    /* Snapshot state — used while state == AERON_CM_STATE_SNAPSHOT */
    int64_t  snapshot_log_position;  /* log position at which snapshot was triggered */
    int64_t  snapshot_timestamp;     /* timestamp when snapshot was triggered (ns) */
    int      service_ack_count;      /* count of services that have acked at expected_ack_position */

    /* Termination state — used while state == AERON_CM_STATE_LEAVING */
    int64_t  termination_leadership_term_id; /* term id when termination was requested (-1 = none) */
    int64_t  termination_deadline_ns;        /* deadline for forced termination (ns) */
    bool     has_cluster_termination;        /* true when a ClusterTermination is in progress */
    bool     is_awaiting_services;           /* true until all services have acked termination */

    /* Timing */
    int64_t  leader_heartbeat_interval_ns;
    int64_t  leader_heartbeat_timeout_ns;
    int64_t  session_timeout_ns;
    int64_t  time_of_last_log_update_ns;
    int64_t  last_do_work_ns;  /* last now_ns passed to do_work — for consistent time in handlers */
    int64_t  time_of_last_append_position_send_ns;
    int64_t  slow_tick_deadline_ns;
    /* Diagnostic: last quorum position that was less than notified_commit_position.
     * Used to emit a single warning when quorum goes backwards (not every tick).
     * Mirrors Java ConsensusModuleAgent.lastQuorumBacktrackCommitPosition. */
    int64_t  last_quorum_backtrack_commit_position;

    /* Mark file activity timestamp deadline — update every ~1s */
    int64_t  mark_file_update_deadline_ns;

    /* Subscriptions and publications */
    aeron_subscription_t            *ingress_subscription;
    aeron_subscription_t            *consensus_subscription;
    aeron_exclusive_publication_t   *log_publication;      /* leader only */
    aeron_exclusive_publication_t   *service_pub;          /* CM→service */
    aeron_subscription_t            *service_sub;          /* service→CM */

    /* Components */
    aeron_cluster_ingress_adapter_cm_t  *ingress_adapter;
    aeron_cluster_consensus_adapter_t   *consensus_adapter;
    aeron_cluster_log_publisher_t        log_publisher;
    aeron_cluster_service_proxy_cm_t     service_proxy;
    aeron_cluster_session_manager_t     *session_manager;
    aeron_cluster_timer_service_t       *timer_service;
    aeron_cluster_recording_log_t       *recording_log;
    aeron_cluster_election_t            *election;

    /* Counters */
    aeron_counter_t                 *commit_position_counter;
    aeron_counter_t                 *cluster_role_counter;
    aeron_counter_t                 *module_state_counter;
    aeron_counter_t                 *control_toggle_counter;  /* cluster control toggle (optional) */

    /* Service ACK tracking */
    int64_t                         *service_ack_positions;          /* [service_count] — last ACK position per service */
    int64_t                         *service_snapshot_recording_ids; /* [service_count] — relevant_id from snapshot ACK */
    int                              service_count;

    /* Archive (IPC) — log recording, snapshot, follower catchup */
    aeron_archive_t                 *archive;

    /* Pending message trackers (one per service) */
    aeron_cluster_pending_message_tracker_t *pending_trackers; /* [service_count] */

    /* Uncommitted timer entries — pairs of (appendPosition, correlationId).
     * Appended on timer event; swept when quorum position advances. */
    int64_t *uncommitted_timers;          /* flat array: [pos0, id0, pos1, id1, ...] */
    int      uncommitted_timers_count;    /* number of (pos,id) pairs */
    int      uncommitted_timers_capacity; /* allocated pair slots */

    /* Uncommitted previous-state entries — pairs of (appendPosition, previousState).
     * Pushed when state changes to SUSPEND/SNAPSHOT; swept on commit; restored on step-down.
     * Mirrors Java ConsensusModuleAgent.uncommittedPreviousState. */
    int64_t *uncommitted_previous_states;       /* flat array: [pos0, state0, pos1, state1, ...] */
    int      uncommitted_previous_states_count;
    int      uncommitted_previous_states_capacity;

    /* Log adapter for replay / catchup */
    aeron_cluster_log_adapter_t             *log_adapter;

    /* ConsensusModuleAdapter — polls service → CM subscription */
    aeron_cluster_consensus_module_adapter_t *cm_adapter;

    /* Follower catchup destination strings */
    char                                    *live_log_destination;
    char                                    *catchup_log_destination;

    /* Tracks whether the current leadership term started during node startup.
     * Set by on_election_complete; forwarded to service JoinLog messages. */
    bool                                     is_leader_startup;
};

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */
int aeron_consensus_module_agent_create(
    aeron_consensus_module_agent_t **agent,
    aeron_cm_context_t *ctx);

int aeron_consensus_module_agent_close(aeron_consensus_module_agent_t *agent);
int aeron_consensus_module_agent_on_start(aeron_consensus_module_agent_t *agent);
int aeron_consensus_module_agent_do_work(aeron_consensus_module_agent_t *agent, int64_t now_ns);

/* -----------------------------------------------------------------------
 * Accessor helpers used by election and adapters
 * ----------------------------------------------------------------------- */
int32_t aeron_consensus_module_agent_get_protocol_version(aeron_consensus_module_agent_t *agent);
int32_t aeron_consensus_module_agent_get_app_version(aeron_consensus_module_agent_t *agent);
int64_t aeron_consensus_module_agent_get_append_position(aeron_consensus_module_agent_t *agent);
int64_t aeron_consensus_module_agent_get_log_recording_id(aeron_consensus_module_agent_t *agent);

/**
 * Stop all follower catchup replays and reset catchup state on each active member.
 * Mirrors Java ConsensusModuleAgent.stopAllCatchups().
 */
void aeron_consensus_module_agent_stop_all_catchups(aeron_consensus_module_agent_t *agent);

/**
 * Stop all replays of the log recording, truncate it at log_position, commit the
 * log position in the recording log, and disconnect the log adapter.
 * Mirrors Java ConsensusModuleAgent.truncateLogEntry().
 * Returns 0 on success, -1 on archive error.
 */
int aeron_consensus_module_agent_truncate_log_entry(
    aeron_consensus_module_agent_t *agent,
    int64_t leadership_term_id,
    int64_t log_position);

/* -----------------------------------------------------------------------
 * Election callbacks (called by aeron_cluster_election_t)
 * ----------------------------------------------------------------------- */
void aeron_consensus_module_agent_on_election_state_change(
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_election_state_t new_state,
    int64_t now_ns);

void aeron_consensus_module_agent_on_election_complete(
    aeron_consensus_module_agent_t *agent,
    aeron_cluster_member_t *leader,
    int64_t now_ns,
    bool is_startup);

void aeron_consensus_module_agent_begin_new_leadership_term(
    aeron_consensus_module_agent_t *agent,
    int64_t log_leadership_term_id,
    int64_t new_term_id,
    int64_t log_position,
    int64_t timestamp,
    bool is_startup);

void aeron_consensus_module_agent_on_follower_new_leadership_term(
    aeron_consensus_module_agent_t *agent,
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

void aeron_consensus_module_agent_notify_commit_position(
    aeron_consensus_module_agent_t *agent, int64_t commit_position);

/* -----------------------------------------------------------------------
 * Phase 1.5: complex election state callbacks
 * ----------------------------------------------------------------------- */

/** Compute quorum position across members (bounded by append_position). */
int64_t aeron_consensus_module_agent_quorum_position(
    aeron_consensus_module_agent_t *agent, int64_t append_position, int64_t now_ns);

/**
 * Handle unavailable counter event — detects service Aeron client shutdown.
 * Register via aeron_context_set_on_unavailable_counter() before aeron_start().
 * Mirrors Java ConsensusModuleAgent.onUnavailableCounter().
 */
void aeron_consensus_module_agent_on_unavailable_counter(
    void *clientd, int64_t registration_id, int32_t counter_id);

/**
 * Handle unavailable ingress image — times out sessions from the lost image.
 * Can be registered as on_unavailable_image on the ingress subscription.
 * Mirrors Java ConsensusModuleAgent.onUnavailableIngressImage().
 */
void aeron_consensus_module_agent_on_unavailable_ingress_image(
    void *clientd, aeron_subscription_t *subscription, aeron_image_t *image);

/** Publish NewLeadershipTerm on broadcast interval; returns work count. */
int aeron_consensus_module_agent_publish_new_leadership_term_on_interval(
    aeron_consensus_module_agent_t *agent, int64_t quorum_position, int64_t now_ns);

/** Publish CommitPosition on broadcast interval; returns work count. */
int aeron_consensus_module_agent_publish_commit_position_on_interval(
    aeron_consensus_module_agent_t *agent, int64_t quorum_position, int64_t now_ns);

/** Create a LogReplay from [from_position, to_position). Returns handle or NULL. */
aeron_cluster_log_replay_t *aeron_consensus_module_agent_new_log_replay(
    aeron_consensus_module_agent_t *agent, int64_t from_position, int64_t to_position);

/** Set up the leader log publication and join it. */
void aeron_consensus_module_agent_join_log_as_leader(
    aeron_consensus_module_agent_t *agent,
    int64_t term_id, int64_t log_position, int32_t session_id, bool is_startup);

/** Update the RecordingLog with the current term. */
void aeron_consensus_module_agent_update_recording_log(
    aeron_consensus_module_agent_t *agent, int64_t now_ns);

/** Publish heartbeat/commit position updates; returns work count. */
int aeron_consensus_module_agent_update_leader_position(
    aeron_consensus_module_agent_t *agent,
    int64_t now_ns, int64_t append_position, int64_t quorum_position);

/** Append a NewLeadershipTermEvent to the log; returns true on success. */
bool aeron_consensus_module_agent_append_new_leadership_term_event(
    aeron_consensus_module_agent_t *agent, int64_t now_ns);

/** Create a RecordingReplication for follower log catchup. Returns handle or NULL. */
aeron_cluster_recording_replication_t *aeron_consensus_module_agent_new_log_replication(
    aeron_consensus_module_agent_t *agent,
    const char *archive_endpoint, const char *resp_endpoint,
    int64_t leader_recording_id, int64_t stop_position, int64_t now_ns);

/** Poll the archive for recording signals; returns work count. */
int aeron_consensus_module_agent_poll_archive_events(
    aeron_consensus_module_agent_t *agent);

/** Publish the follower's replication position; returns work count. */
int aeron_consensus_module_agent_publish_follower_replication_position(
    aeron_consensus_module_agent_t *agent, int64_t now_ns);

/** Update the RecordingLog after follower replication completes. */
void aeron_consensus_module_agent_update_recording_log_for_replication(
    aeron_consensus_module_agent_t *agent,
    int64_t term_id, int64_t base_position, int64_t stop_position, int64_t now_ns);

/** Create the follower log subscription for a given session. */
aeron_subscription_t *aeron_consensus_module_agent_add_follower_subscription(
    aeron_consensus_module_agent_t *agent, int32_t session_id);

/** Add a catchup destination to the follower subscription. */
void aeron_consensus_module_agent_add_catchup_log_destination(
    aeron_consensus_module_agent_t *agent,
    aeron_subscription_t *subscription, const char *endpoint);

/** Add the live log destination to the follower subscription. */
void aeron_consensus_module_agent_add_live_log_destination(
    aeron_consensus_module_agent_t *agent);

/** Return this member's catchup endpoint string (NULL if not configured). */
const char *aeron_consensus_module_agent_this_catchup_endpoint(
    aeron_consensus_module_agent_t *agent);

/** Send CatchupPosition message to the leader; returns true if sent. */
bool aeron_consensus_module_agent_send_catchup_position(
    aeron_consensus_module_agent_t *agent, const char *catchup_endpoint);

/** Notify the agent that catchup has been initiated. */
void aeron_consensus_module_agent_catchup_initiated(
    aeron_consensus_module_agent_t *agent, int64_t now_ns);

/** Try to join the log as a follower via the given image; returns true on success. */
bool aeron_consensus_module_agent_try_join_log_as_follower(
    aeron_consensus_module_agent_t *agent,
    aeron_image_t *image, bool is_startup, int64_t now_ns);

/** Poll catchup log up to commit_position; returns work count. */
int aeron_consensus_module_agent_catchup_poll(
    aeron_consensus_module_agent_t *agent, int64_t commit_position, int64_t now_ns);

/** Returns true when the follower is close enough to live to merge. */
bool aeron_consensus_module_agent_is_catchup_near_live(
    aeron_consensus_module_agent_t *agent, int64_t position);

/** Returns the live log destination string (NULL if not set). */
const char *aeron_consensus_module_agent_live_log_destination(
    aeron_consensus_module_agent_t *agent);

/** Returns the catchup log destination string (NULL if done/not set). */
const char *aeron_consensus_module_agent_catchup_log_destination(
    aeron_consensus_module_agent_t *agent);

/** Returns the current commit position counter value. */
int64_t aeron_consensus_module_agent_get_commit_position(
    aeron_consensus_module_agent_t *agent);

/** Returns the agent's current CM state. */
aeron_cm_state_t aeron_consensus_module_agent_state(
    aeron_consensus_module_agent_t *agent);

/**
 * Initiate a graceful cluster termination (SHUTDOWN / ABORT).
 * For SHUTDOWN, snapshot_first must be true — the termination proceeds after snapshot acks.
 * For ABORT, snapshot_first must be false — goes directly to LEAVING state.
 * Mirrors the leader-side logic in Java ConsensusModuleAgent.
 */
void aeron_consensus_module_agent_begin_termination(
    aeron_consensus_module_agent_t *agent, int64_t now_ns, bool snapshot_first);

/* -----------------------------------------------------------------------
 * Consensus message callbacks (from ConsensusAdapter)
 * already declared in aeron_consensus_module_agent_fwd.h — bodies here
 * ----------------------------------------------------------------------- */

/* -----------------------------------------------------------------------
 * Ingress callbacks (from IngressAdapter)
 * already declared in aeron_consensus_module_agent_fwd.h
 * ----------------------------------------------------------------------- */

#ifdef __cplusplus
}
#endif

#endif /* AERON_CONSENSUS_MODULE_AGENT_H */
