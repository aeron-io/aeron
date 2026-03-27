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

#ifndef AERON_CLUSTER_SESSION_MANAGER_H
#define AERON_CLUSTER_SESSION_MANAGER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_cluster_cluster_session.h"
#include "aeron_cluster_log_publisher.h"
#include "aeron_cluster_recording_log.h"

/* -----------------------------------------------------------------------
 * Standby-snapshot pending queue types
 * One batch = one StandbySnapshot message (up to 32 per-service entries).
 * ----------------------------------------------------------------------- */
#define AERON_STANDBY_SNAPSHOT_MAX_PER_BATCH 32

typedef struct aeron_standby_snapshot_entry_stct
{
    int64_t recording_id;
    int64_t leadership_term_id;
    int64_t term_base_log_position;
    int64_t log_position;
    int64_t timestamp;
    int32_t service_id;
}
aeron_standby_snapshot_entry_t;

typedef struct aeron_standby_snapshot_batch_stct
{
    aeron_standby_snapshot_entry_t entries[AERON_STANDBY_SNAPSHOT_MAX_PER_BATCH];
    int                            count;
    /* deadline_ns == 0 means "process immediately once logPosition committed" */
    int64_t                        deadline_ns;
}
aeron_standby_snapshot_batch_t;

#ifdef __cplusplus
extern "C"
{
#endif

/* -----------------------------------------------------------------------
 * Authenticator interface — pluggable auth callbacks.
 * A no-op implementation that immediately authenticates is provided.
 * ----------------------------------------------------------------------- */
typedef struct aeron_cluster_authenticator_stct
{
    /* Called when a new connect request arrives. Implementation may call
       aeron_cluster_cluster_session_authenticate() or _reject(). */
    void (*on_connect_request)(void *clientd, aeron_cluster_cluster_session_t *session,
                               int64_t now_ms);

    /* Called each tick while a session is in CONNECTED state (pub connected but not yet auth).
       Implementation may call authenticate(), reject() or challenge(). */
    void (*on_connected_session)(void *clientd, aeron_cluster_cluster_session_t *session,
                                 int64_t now_ms);

    /* Called each tick while a session is in CHALLENGED state. */
    void (*on_challenged_session)(void *clientd, aeron_cluster_cluster_session_t *session,
                                  int64_t now_ms);

    /* Called when a challenge response arrives. */
    void (*on_challenge_response)(void *clientd, int64_t session_id,
                                  const uint8_t *encoded_credentials, size_t len,
                                  int64_t now_ms);

    void *clientd;
}
aeron_cluster_authenticator_t;

/* Built-in no-op authenticator: authenticates every session immediately. */
extern const aeron_cluster_authenticator_t AERON_CLUSTER_AUTHENTICATOR_NULL;

/* -----------------------------------------------------------------------
 * Snapshot taker callback — called per session during snapshotSessions.
 * ----------------------------------------------------------------------- */
typedef void (*aeron_cluster_session_snapshot_fn_t)(
    void *clientd, const aeron_cluster_cluster_session_t *session);

/* -----------------------------------------------------------------------
 * Session manager struct
 * ----------------------------------------------------------------------- */
typedef struct aeron_cluster_session_manager_stct
{
    /* ---- Active sessions ---- */
    aeron_cluster_cluster_session_t **sessions;
    int                               session_count;
    int                               session_capacity;

    /* ---- Pending user (client) sessions ---- */
    aeron_cluster_cluster_session_t **pending_user;
    int                               pending_user_count;
    int                               pending_user_capacity;

    /* ---- Rejected user sessions (awaiting event send) ---- */
    aeron_cluster_cluster_session_t **rejected_user;
    int                               rejected_user_count;
    int                               rejected_user_capacity;

    /* ---- Redirect user sessions (non-leader, awaiting send) ---- */
    aeron_cluster_cluster_session_t **redirect_user;
    int                               redirect_user_count;
    int                               redirect_user_capacity;

    /* ---- Pending backup (ClusterBackup query) sessions ---- */
    aeron_cluster_cluster_session_t **pending_backup;
    int                               pending_backup_count;
    int                               pending_backup_capacity;

    /* ---- Rejected backup sessions ---- */
    aeron_cluster_cluster_session_t **rejected_backup;
    int                               rejected_backup_count;
    int                               rejected_backup_capacity;

    /* ---- Uncommitted-close queue (ring buffer of closed session ptrs) ---- */
    aeron_cluster_cluster_session_t **uncommitted_closed;
    int                               uncommitted_closed_head;
    int                               uncommitted_closed_tail;
    int                               uncommitted_closed_capacity;

    /* ---- Session ID tracking ---- */
    int64_t  next_session_id;
    int64_t  next_committed_session_id;

    /* ---- Aeron ---- */
    aeron_t *aeron;

    /* ---- Context references ---- */
    aeron_cluster_log_publisher_t    *log_publisher;      /* may be NULL for followers */
    aeron_exclusive_publication_t    *egress_pub;          /* log egress pub (multicast) */
    aeron_cluster_recording_log_t    *recording_log;       /* for BACKUP responses */
    const char                       *cluster_members_str; /* encoded members string; not owned */
    aeron_cluster_authenticator_t     authenticator;

    /* Extension hooks — called when sessions are opened/closed */
    void (*on_session_opened_hook)(void *clientd, int64_t cluster_session_id);
    void (*on_session_closed_hook)(void *clientd, int64_t cluster_session_id, int32_t close_reason);
    void  *extension_hook_clientd;

    /* ---- Pending standby snapshot notification queue ---- */
    aeron_standby_snapshot_batch_t *pending_standby_snapshots;
    int                             pending_standby_count;
    int                             pending_standby_capacity;

    /* ---- Configuration ---- */
    int64_t  session_timeout_ns;
    int32_t  max_concurrent_sessions;
    int32_t  service_count;
    int32_t  cluster_id;
    int32_t  member_id;
    int32_t  commit_position_counter_id;
    int64_t  standby_snapshot_notification_delay_ns;  /* 0 = immediate */
}
aeron_cluster_session_manager_t;

/* -----------------------------------------------------------------------
 * Lifecycle
 * ----------------------------------------------------------------------- */
int aeron_cluster_session_manager_create(
    aeron_cluster_session_manager_t **manager,
    int64_t initial_session_id,
    aeron_t *aeron);

int aeron_cluster_session_manager_close(aeron_cluster_session_manager_t *manager);

/* -----------------------------------------------------------------------
 * Basic session operations
 * ----------------------------------------------------------------------- */

/** Allocate and register a new session, returning the session or NULL. */
aeron_cluster_cluster_session_t *aeron_cluster_session_manager_new_session(
    aeron_cluster_session_manager_t *manager,
    int64_t correlation_id,
    int32_t response_stream_id,
    const char *response_channel,
    const uint8_t *encoded_principal,
    size_t principal_length);

aeron_cluster_cluster_session_t *aeron_cluster_session_manager_find(
    aeron_cluster_session_manager_t *manager, int64_t session_id);

int aeron_cluster_session_manager_remove(
    aeron_cluster_session_manager_t *manager, int64_t session_id);

/** Remove session from the active list and free it. */
void aeron_cluster_session_manager_close_session(
    aeron_cluster_session_manager_t *manager,
    aeron_cluster_cluster_session_t *session);

int aeron_cluster_session_manager_session_count(aeron_cluster_session_manager_t *manager);

/* -----------------------------------------------------------------------
 * Leader-side connect / close handling
 * ----------------------------------------------------------------------- */

/**
 * Handle an incoming client connect request.
 * - is_leader: if false, adds to redirect list
 * - On version mismatch or session limit, adds to rejected list
 * - Otherwise adds to pending_user list
 * Mirrors Java SessionManager.onSessionConnect().
 */
void aeron_cluster_session_manager_on_session_connect(
    aeron_cluster_session_manager_t *manager,
    int64_t correlation_id,
    int32_t response_stream_id,
    int32_t version,
    const char *response_channel,
    const uint8_t *encoded_credentials, size_t credentials_len,
    int64_t now_ns,
    bool is_leader,
    const char *ingress_endpoints);

/**
 * Handle a client close request (logged and removed).
 * Mirrors Java SessionManager.onSessionClose().
 */
void aeron_cluster_session_manager_on_session_close(
    aeron_cluster_session_manager_t *manager,
    int64_t cluster_session_id,
    int64_t leadership_term_id,
    int64_t timestamp_ms);

/* -----------------------------------------------------------------------
 * Log replay handlers (called when replaying the log, not from network)
 * ----------------------------------------------------------------------- */

void aeron_cluster_session_manager_on_replay_session_open(
    aeron_cluster_session_manager_t *manager,
    int64_t log_position,
    int64_t correlation_id,
    int64_t cluster_session_id,
    int64_t timestamp,
    int32_t response_stream_id,
    const char *response_channel);

void aeron_cluster_session_manager_on_replay_session_close(
    aeron_cluster_session_manager_t *manager,
    int64_t cluster_session_id,
    int32_t close_reason);

/* -----------------------------------------------------------------------
 * Snapshot load / save
 * ----------------------------------------------------------------------- */

void aeron_cluster_session_manager_on_load_cluster_session(
    aeron_cluster_session_manager_t *manager,
    int64_t cluster_session_id,
    int64_t correlation_id,
    int64_t opened_position,
    int64_t time_of_last_activity_ns,
    int32_t close_reason,
    int32_t response_stream_id,
    const char *response_channel);

void aeron_cluster_session_manager_load_next_session_id(
    aeron_cluster_session_manager_t *manager, int64_t next_session_id);

/**
 * Call snapshot_fn for each OPEN/CLOSING session (for snapshot writing).
 */
void aeron_cluster_session_manager_snapshot_sessions(
    aeron_cluster_session_manager_t *manager,
    aeron_cluster_session_snapshot_fn_t snapshot_fn,
    void *clientd);

/* -----------------------------------------------------------------------
 * Periodic tick operations
 * ----------------------------------------------------------------------- */

/**
 * Process pending sessions through the authentication state machine.
 * Returns work count.
 */
int aeron_cluster_session_manager_process_pending_sessions(
    aeron_cluster_session_manager_t *manager,
    int64_t now_ns,
    int64_t now_ms,
    int32_t leader_member_id,
    int64_t leadership_term_id);

/**
 * Check active sessions for timeouts and new-leader events.
 * Returns work count.
 */
int aeron_cluster_session_manager_check_sessions(
    aeron_cluster_session_manager_t *manager,
    int64_t now_ns,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    const char *ingress_endpoints);

/**
 * Send redirect events to non-leader clients. Returns work count.
 */
int aeron_cluster_session_manager_send_redirects(
    aeron_cluster_session_manager_t *manager,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    int64_t now_ns);

/**
 * Send rejection events. Returns work count.
 */
int aeron_cluster_session_manager_send_rejections(
    aeron_cluster_session_manager_t *manager,
    int64_t leadership_term_id,
    int32_t leader_member_id,
    int64_t now_ns);

/**
 * Poll for sessions that have timed out (legacy helper for follower path).
 */
typedef void (*aeron_cluster_session_timeout_fn_t)(void *clientd,
    aeron_cluster_cluster_session_t *session);

int aeron_cluster_session_manager_check_timeouts(
    aeron_cluster_session_manager_t *manager,
    int64_t now_ns,
    int64_t session_timeout_ns,
    aeron_cluster_session_timeout_fn_t on_timeout,
    void *clientd);

/* -----------------------------------------------------------------------
 * State transitions
 * ----------------------------------------------------------------------- */

/** Remove sessions opened after logPosition (e.g. on election loss). */
void aeron_cluster_session_manager_clear_sessions_after(
    aeron_cluster_session_manager_t *manager,
    int64_t log_position,
    int64_t leadership_term_id);

/**
 * Sweep sessions whose close was committed at or before commitPosition.
 * Mirrors Java SessionManager.sweepUncommittedSessions().
 */
void aeron_cluster_session_manager_sweep_uncommitted_sessions(
    aeron_cluster_session_manager_t *manager,
    int64_t commit_position);

/**
 * Restore sessions from the uncommitted-close ring whose close was NOT committed
 * (closed_log_position > commit_position). Those sessions are re-added to the
 * active list with state reset to OPEN. Committed sessions are freed.
 * Mirrors Java SessionManager.restoreUncommittedSessions(commitPosition).
 * Called when stepping down from leader during an election.
 */
void aeron_cluster_session_manager_restore_uncommitted_sessions(
    aeron_cluster_session_manager_t *manager,
    int64_t commit_position);

/**
 * Close the response publication for every active session (without freeing sessions).
 * Mirrors Java ConsensusModuleAgent.disconnectSessions() which calls
 * session.closePublication() on step-down.
 */
void aeron_cluster_session_manager_disconnect_sessions(
    aeron_cluster_session_manager_t *manager);

/** Prepare sessions for new leadership term. */
void aeron_cluster_session_manager_prepare_for_new_term(
    aeron_cluster_session_manager_t *manager,
    bool is_startup,
    int64_t now_ns);

/** Reset all session last-activity timestamps to now. */
void aeron_cluster_session_manager_update_activity(
    aeron_cluster_session_manager_t *manager, int64_t now_ns);

/** Handle challenge response for a user session. */
void aeron_cluster_session_manager_on_challenge_response(
    aeron_cluster_session_manager_t *manager,
    int64_t correlation_id,
    int64_t cluster_session_id,
    const uint8_t *encoded_credentials, size_t len,
    int64_t now_ns);

/** Handle challenge response for a backup (ClusterBackup) session. */
void aeron_cluster_session_manager_on_backup_challenge_response(
    aeron_cluster_session_manager_t *manager,
    int64_t correlation_id,
    int64_t cluster_session_id,
    const uint8_t *encoded_credentials, size_t len,
    int64_t now_ns);

/* -----------------------------------------------------------------------
 * Standby snapshot pending notification queue
 * ----------------------------------------------------------------------- */

/**
 * Enqueue a batch of standby snapshot entries for deferred recording.
 * Called by the agent when a StandbySnapshot message is received.
 */
void aeron_cluster_session_manager_enqueue_standby_snapshot(
    aeron_cluster_session_manager_t *manager,
    int64_t *recording_ids,
    int64_t *leadership_term_ids,
    int64_t *term_base_log_positions,
    int64_t *log_positions,
    int64_t *timestamps,
    int32_t *service_ids,
    int      count);

/**
 * Drain batches whose logPosition <= commitPosition into the recording log.
 * Also processes any delayed batches whose deadline has passed.
 * Returns work count.
 * Mirrors Java SessionManager.processPendingStandbySnapshotNotifications().
 */
int aeron_cluster_session_manager_process_pending_standby_snapshot_notifications(
    aeron_cluster_session_manager_t *manager,
    int64_t commit_position,
    int64_t now_ns);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SESSION_MANAGER_H */
