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

#ifndef AERON_CLUSTER_SERVICE_SNAPSHOT_TAKER_H
#define AERON_CLUSTER_SERVICE_SNAPSHOT_TAKER_H

#include <stdint.h>
#include <stddef.h>

#include "aeronc.h"
#include "aeron_cluster_client_session.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_CLUSTER_SNAPSHOT_MARK_BEGIN  0
#define AERON_CLUSTER_SNAPSHOT_MARK_SECTION 1
#define AERON_CLUSTER_SNAPSHOT_MARK_END    2

/**
 * Write a SnapshotMarker(BEGIN) to the snapshot publication.
 */
int aeron_cluster_service_snapshot_taker_mark_begin(
    aeron_exclusive_publication_t *publication,
    int64_t type_id,
    int64_t log_position,
    int64_t leadership_term_id,
    int32_t index,
    int32_t app_version);

/**
 * Write a SnapshotMarker(END) to the snapshot publication.
 */
int aeron_cluster_service_snapshot_taker_mark_end(
    aeron_exclusive_publication_t *publication,
    int64_t type_id,
    int64_t log_position,
    int64_t leadership_term_id,
    int32_t index,
    int32_t app_version);

/**
 * Encode one ClientSession into the snapshot publication.
 */
int aeron_cluster_service_snapshot_taker_snapshot_session(
    aeron_exclusive_publication_t *publication,
    aeron_cluster_client_session_t *session);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_SERVICE_SNAPSHOT_TAKER_H */
