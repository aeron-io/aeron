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

typedef struct aeron_stct aeron_t;

/**
 * Opaque server handle — bundles CM agent + echo service agent running in a thread.
 * Deliberately hides the service-side aeron_cluster_t typedef so client-only TUs
 * can include this header without typedef conflicts.
 */
typedef struct cluster_server_handle_stct cluster_server_handle_t;

/**
 * Create and start a single-node echo cluster server.
 *
 * @param aeron_dir    path to the shared MediaDriver (already running)
 * @param cluster_dir  empty directory for recording log
 * @param archive_port port the ArchivingMediaDriver control listener is on
 * @return handle, or NULL on failure
 */
cluster_server_handle_t *cluster_server_start(
    aeron_t *aeron,                 /* caller-owned Aeron instance to reuse */
    const char *cluster_dir,
    int archive_port,
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

/** Returns the server's Aeron client (for cross-client diagnostics). */
aeron_t *cluster_server_get_aeron(const cluster_server_handle_t *srv);
void cluster_server_do_work(cluster_server_handle_t *srv, int64_t now_ns);

/** Stop and free the server. */
void cluster_server_stop(cluster_server_handle_t *srv);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SERVER_HELPER_H */
