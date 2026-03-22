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
#include "aeronc.h"
#include "aeron_cluster_cluster_session.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct aeron_cluster_session_manager_stct
{
    aeron_cluster_cluster_session_t **sessions;
    int                               session_count;
    int                               session_capacity;
    int64_t                           next_session_id;
    aeron_t                          *aeron;
}
aeron_cluster_session_manager_t;

int aeron_cluster_session_manager_create(
    aeron_cluster_session_manager_t **manager,
    int64_t initial_session_id,
    aeron_t *aeron);

int aeron_cluster_session_manager_close(aeron_cluster_session_manager_t *manager);

/** Allocate and register a new session, returning its id. */
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

/** Poll for sessions that have timed out; calls close_session_fn for each. */
typedef void (*aeron_cluster_session_timeout_fn_t)(void *clientd,
    aeron_cluster_cluster_session_t *session);

int aeron_cluster_session_manager_check_timeouts(
    aeron_cluster_session_manager_t *manager,
    int64_t now_ns,
    int64_t session_timeout_ns,
    aeron_cluster_session_timeout_fn_t on_timeout,
    void *clientd);

int aeron_cluster_session_manager_session_count(aeron_cluster_session_manager_t *manager);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SESSION_MANAGER_H */
