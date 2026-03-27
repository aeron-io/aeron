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

#ifndef AERON_CLUSTER_BACKUP_AGENT_H
#define AERON_CLUSTER_BACKUP_AGENT_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_archive.h"
#include "consensus/aeron_cluster_recording_log.h"
#include "consensus/aeron_cluster_member.h"
#include "aeron_cluster_snapshot_replication.h"

#ifdef __cplusplus
extern "C"
{
#endif

/* -----------------------------------------------------------------------
 * ClusterBackup state enum — mirrors Java ClusterBackup.State
 * ----------------------------------------------------------------------- */
typedef enum aeron_cluster_backup_state_en
{
    AERON_CLUSTER_BACKUP_STATE_BACKUP_QUERY        = 0,
    AERON_CLUSTER_BACKUP_STATE_SNAPSHOT_RETRIEVE   = 1,
    AERON_CLUSTER_BACKUP_STATE_LIVE_LOG_RECORD     = 2,
    AERON_CLUSTER_BACKUP_STATE_LIVE_LOG_REPLAY     = 3,
    AERON_CLUSTER_BACKUP_STATE_UPDATE_RECORDING_LOG = 4,
    AERON_CLUSTER_BACKUP_STATE_BACKING_UP          = 5,
    AERON_CLUSTER_BACKUP_STATE_RESET_BACKUP        = 6,
    AERON_CLUSTER_BACKUP_STATE_CLOSED              = 7,
}
aeron_cluster_backup_state_t;

#define AERON_CLUSTER_BACKUP_MARK_FILE_UPDATE_INTERVAL_MS INT64_C(1000)
#define AERON_CLUSTER_BACKUP_SLOW_TICK_INTERVAL_MS        INT64_C(10)
#define AERON_CLUSTER_BACKUP_MAX_ENDPOINTS                20

/* -----------------------------------------------------------------------
 * Context — configuration passed into the backup agent at startup.
 * Caller fills all fields before calling aeron_cluster_backup_agent_create.
 * ----------------------------------------------------------------------- */
/* -----------------------------------------------------------------------
 * LogSourceValidator: which cluster member may serve as the log source.
 * Mirrors Java ClusterBackup.SourceType.
 * ----------------------------------------------------------------------- */
typedef enum aeron_cluster_backup_source_type_en
{
    AERON_CLUSTER_BACKUP_SOURCE_ANY      = 0, /* accept any member (default — zero-init safe) */
    AERON_CLUSTER_BACKUP_SOURCE_LEADER   = 1, /* only accept if member is the current leader  */
    AERON_CLUSTER_BACKUP_SOURCE_FOLLOWER = 2  /* only accept non-leader members               */
}
aeron_cluster_backup_source_type_t;

typedef struct aeron_cluster_backup_context_stct
{
    aeron_t       *aeron;

    /* Archive to use for local backup recordings */
    aeron_archive_t *backup_archive;

    /* Consensus channel / stream — used to poll for backup responses */
    char     consensus_channel[512];
    int32_t  consensus_stream_id;

    /* Catchup channel / endpoint — used for live log recording */
    char     catchup_channel[512];
    char     catchup_endpoint[128];

    /* Log stream ID */
    int32_t  log_stream_id;

    /* Comma-separated cluster consensus endpoints to round-robin for backup queries */
    char     cluster_consensus_endpoints[1024];

    /* Timeouts in nanoseconds */
    int64_t  backup_response_timeout_ns;   /* default 2 s */
    int64_t  backup_query_interval_ns;     /* default 1 s */
    int64_t  backup_progress_timeout_ns;   /* default 10 s */
    int64_t  cool_down_interval_ns;        /* default 1 s */

    /* Replication progress thresholds */
    int64_t  replication_progress_timeout_ns;
    int64_t  replication_progress_interval_ns;

    /* Error handler — called on non-fatal errors */
    void (*error_handler)(void *clientd, int errcode, const char *msg);
    void *error_handler_clientd;

    /* Events listener — may be NULL */
    void (*on_backup_state_change)(void *clientd, aeron_cluster_backup_state_t old_state,
                                   aeron_cluster_backup_state_t new_state, int64_t now_ms);
    void *events_clientd;

    /* Credentials supplier — may be NULL (zero-length credentials used) */
    const uint8_t *encoded_credentials;
    size_t         encoded_credentials_length;

    /**
     * Optional challenge handler — called when the cluster issues a challenge.
     * Should return a pointer to the response bytes and set *response_length.
     * If NULL, encoded_credentials/encoded_credentials_length are sent as-is.
     * Mirrors Java CredentialsSupplier.onChallenge().
     */
    const uint8_t *(*on_challenge)(void *clientd,
                                   const uint8_t *challenge,
                                   size_t challenge_length,
                                   size_t *response_length);
    void *on_challenge_clientd;

    /**
     * Which type of cluster member may serve as the log source.
     * Defaults to AERON_CLUSTER_BACKUP_SOURCE_ANY.
     * Mirrors Java ClusterBackup.Context.sourceType() / LogSourceValidator.
     */
    aeron_cluster_backup_source_type_t source_type;
}
aeron_cluster_backup_context_t;

/* -----------------------------------------------------------------------
 * Pending-snapshot entry (parallel to Java RecordingLog.Snapshot)
 * ----------------------------------------------------------------------- */
typedef struct aeron_cluster_backup_snapshot_stct
{
    int64_t recording_id;
    int64_t leadership_term_id;
    int64_t term_base_log_position;
    int64_t log_position;
    int64_t timestamp;
    int32_t service_id;
}
aeron_cluster_backup_snapshot_t;

/* -----------------------------------------------------------------------
 * Agent struct
 * ----------------------------------------------------------------------- */
typedef struct aeron_cluster_backup_agent_stct
{
    aeron_cluster_backup_context_t *ctx;
    aeron_cluster_recording_log_t  *recording_log;

    aeron_subscription_t           *consensus_sub;
    aeron_fragment_assembler_t     *fragment_assembler;

    /* Parsed consensus endpoints for round-robin */
    char                            parsed_endpoints[AERON_CLUSTER_BACKUP_MAX_ENDPOINTS][256];
    int                             parsed_endpoints_count;
    int                             next_endpoint_idx;

    /* Current consensus publication — rotated across parsed_endpoints */
    aeron_exclusive_publication_t  *consensus_pub;
    char                            current_endpoint[256];

    /* Cluster archive (remote) — connected lazily */
    aeron_archive_t                *cluster_archive;

    /* Snapshot replication */
    aeron_cluster_snapshot_replication_t *snapshot_replication;

    /* Snapshots from backup response */
    aeron_cluster_backup_snapshot_t *snapshots_to_retrieve;
    int                              snapshots_to_retrieve_count;
    int                              snapshots_to_retrieve_capacity;
    aeron_cluster_backup_snapshot_t *snapshots_retrieved;
    int                              snapshots_retrieved_count;
    int                              snapshots_retrieved_capacity;

    /* Cluster members parsed from backup response */
    aeron_cluster_member_t  *active_cluster_members;
    int                      active_cluster_member_count;
    aeron_cluster_member_t  *log_supplier_member; /* pointer into active_cluster_members */
    int32_t                  log_supplier_member_id;

    /* Live log tracking */
    int64_t  cluster_log_recording_id;
    int64_t  live_log_recording_subscription_id;
    int64_t  live_log_recording_id;
    int64_t  live_log_replay_session_id;
    int32_t  live_log_recording_counter_id;
    int32_t  live_log_recording_session_id;
    int32_t  leader_commit_position_counter_id;
    int32_t  leader_member_id;

    char    recording_channel[512];
    char    replay_channel[512];

    aeron_subscription_t *recording_subscription;

    /* Backup response state */
    int64_t  correlation_id;
    int64_t  log_leadership_term_id;
    int64_t  log_term_base_log_position;
    int64_t  last_leadership_term_id;
    int64_t  last_term_base_log_position;

    /* Pending term entries to write during UPDATE_RECORDING_LOG.
     * Mirrors Java's leaderLogEntry and leaderLastTermEntry:
     *   leader_log_entry       — term for log_leadership_term_id (written BEFORE snapshots)
     *   leader_last_term_entry — term for last_leadership_term_id (written AFTER snapshots)
     * Only written if the leadership_term_id is unknown in the recording log. */
    bool     leader_log_entry_pending;
    int64_t  leader_log_entry_recording_id;
    int64_t  leader_log_entry_leadership_term_id;
    int64_t  leader_log_entry_term_base_log_position;
    int64_t  leader_log_entry_timestamp;

    bool     leader_last_term_entry_pending;
    int64_t  leader_last_term_entry_recording_id;
    int64_t  leader_last_term_entry_leadership_term_id;
    int64_t  leader_last_term_entry_term_base_log_position;
    int64_t  leader_last_term_entry_timestamp;

    /* now_ms cached at the start of do_work so fragment callbacks can use it */
    int64_t  current_now_ms;

    /* Timing */
    int64_t  slow_tick_deadline_ms;
    int64_t  mark_file_update_deadline_ms;
    int64_t  time_of_last_backup_query_ms;
    int64_t  time_of_last_progress_ms;
    int64_t  cool_down_deadline_ms;

    /* Current state */
    aeron_cluster_backup_state_t state;
    bool is_closed;
}
aeron_cluster_backup_agent_t;

/* -----------------------------------------------------------------------
 * Public API
 * ----------------------------------------------------------------------- */

/**
 * Validate and conclude the backup context — ensures required fields are set.
 * Mirrors Java ClusterBackup.Context.conclude().
 * Call before aeron_cluster_backup_agent_create().
 */
int aeron_cluster_backup_context_conclude(aeron_cluster_backup_context_t *ctx);

/**
 * Create the backup agent and open the recording log.
 * ctx must remain valid for the lifetime of the agent.
 */
int aeron_cluster_backup_agent_create(
    aeron_cluster_backup_agent_t **agent,
    aeron_cluster_backup_context_t *ctx,
    const char *cluster_dir);

/**
 * Start — opens cluster archive, sets initial deadline.
 */
int aeron_cluster_backup_agent_on_start(aeron_cluster_backup_agent_t *agent, int64_t now_ms);

/**
 * Do one unit of work.  Call repeatedly in a poll loop.
 * Returns work count (0 = nothing done).
 */
int aeron_cluster_backup_agent_do_work(aeron_cluster_backup_agent_t *agent, int64_t now_ms);

/** Close and free all resources. */
void aeron_cluster_backup_agent_close(aeron_cluster_backup_agent_t *agent);

/** Return current state. */
aeron_cluster_backup_state_t aeron_cluster_backup_agent_state(
    const aeron_cluster_backup_agent_t *agent);

/* -----------------------------------------------------------------------
 * ClusterBackup lifecycle (mirrors Java ClusterBackup.launch/close)
 * ----------------------------------------------------------------------- */

/**
 * Launch a ClusterBackup agent — conclude context, create agent, call on_start.
 * Equivalent to Java ClusterBackup.launch().
 *
 * @param agent       out: created agent handle.
 * @param ctx         backup context (must be fully configured).
 * @param cluster_dir directory for recording log.
 * @param now_ms      current time in milliseconds.
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_backup_launch(
    aeron_cluster_backup_agent_t **agent,
    aeron_cluster_backup_context_t *ctx,
    const char *cluster_dir,
    int64_t now_ms);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_BACKUP_AGENT_H */
