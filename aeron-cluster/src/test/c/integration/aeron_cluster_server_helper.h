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

#ifndef AERON_CLUSTER_SERVER_HELPER_H
#define AERON_CLUSTER_SERVER_HELPER_H

#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * Opaque server handle -- bundles CM agent + echo service agent.
 * Each component creates its own Aeron client from the given aeron_dir,
 * matching the Java ClusteredMediaDriver pattern.
 */
typedef struct cluster_server_handle_stct cluster_server_handle_t;

/**
 * Create and start a single-node echo cluster server.
 * Creates its own Aeron clients internally from the given aeron_dir.
 *
 * @param aeron_dir      path to the shared MediaDriver directory
 * @param cluster_dir    empty directory for recording log
 * @param cluster_members cluster members string
 * @param ingress_port   ingress port
 * @param consensus_port consensus port
 * @return handle, or NULL on failure
 */
cluster_server_handle_t *cluster_server_start(
    const char *aeron_dir,
    const char *cluster_dir,
    const char *cluster_members,
    int ingress_port,
    int consensus_port);

/** Returns true once the CM has elected itself as leader. */
bool cluster_server_is_leader(const cluster_server_handle_t *srv);

/** Returns the CM state (aeron_cm_state_t cast to int). */
int  cluster_server_cm_state(const cluster_server_handle_t *srv);

/** Returns session_count in the session manager (-1 if unavailable). */
int  cluster_server_session_count(const cluster_server_handle_t *srv);

/** Returns pending_user_count in the session manager (-1 if unavailable). */
int  cluster_server_pending_session_count(const cluster_server_handle_t *srv);

/** Returns rejected_user_count + redirect_user_count (-1 if unavailable). */
int  cluster_server_rejected_session_count(const cluster_server_handle_t *srv);

/** Returns 1 if first session has has_open_event_pending, 0 otherwise. */
int  cluster_server_session_open_pending(const cluster_server_handle_t *srv);

/** Returns 1 if log_publication exists, 0 otherwise. */
int  cluster_server_has_log_pub(const cluster_server_handle_t *srv);

/** Returns 1 if session_manager->log_publisher is set. */
int  cluster_server_has_session_log_pub(const cluster_server_handle_t *srv);

/** Returns 1 if service has a log_adapter (on_join_log was called). */
int  cluster_server_svc_has_log_adapter(const cluster_server_handle_t *srv);

void cluster_server_do_work(cluster_server_handle_t *srv, int64_t now_ns);

/**
 * Create and start a service-only echo container (no CM).
 * Use when the CM is already running (e.g. inside ClusteredMediaDriver).
 * Creates its own Aeron client internally from the given aeron_dir.
 *
 * @param aeron_dir      path to the shared MediaDriver directory
 * @param cluster_dir    cluster dir for recording log
 * @return handle, or NULL on failure
 */
cluster_server_handle_t *cluster_service_start(
    const char *aeron_dir,
    const char *cluster_dir);

/**
 * Start a background thread that drives the CM and/or service agents.
 * The server must already be started via cluster_server_start() or cluster_service_start().
 */
void cluster_server_start_background(cluster_server_handle_t *srv);

/**
 * Stop the background thread (if running) and free the server.
 * Closes its own Aeron clients.
 */
void cluster_server_stop(cluster_server_handle_t *srv);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SERVER_HELPER_H */
