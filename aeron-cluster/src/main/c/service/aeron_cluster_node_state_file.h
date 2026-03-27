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

#ifndef AERON_CLUSTER_NODE_STATE_FILE_H
#define AERON_CLUSTER_NODE_STATE_FILE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Persistent node state file (node-state.dat). Stores candidateTermId and
 * associated log position / timestamp across restarts. Backed by an mmap'd file.
 *
 * Binary layout (all little-endian):
 *   Offset  0  : int32_t  version (= AERON_CLUSTER_NODE_STATE_FILE_VERSION when valid)
 *   Offset  4  : int32_t  reserved / padding
 *   Offset  8  : int64_t  candidateTermId  (written last — acts as commit sentinel)
 *   Offset 16  : int64_t  logPosition
 *   Offset 24  : int64_t  timestamp (epoch ms)
 */

#define AERON_CLUSTER_NODE_STATE_FILE_FILENAME  "node-state.dat"
#define AERON_CLUSTER_NODE_STATE_FILE_LENGTH    4096
#define AERON_CLUSTER_NODE_STATE_FILE_VERSION   (1)

/* Field offsets */
#define AERON_CLUSTER_NSF_VERSION_OFFSET         (0)
#define AERON_CLUSTER_NSF_CANDIDATE_TERM_OFFSET  (8)
#define AERON_CLUSTER_NSF_LOG_POSITION_OFFSET    (16)
#define AERON_CLUSTER_NSF_TIMESTAMP_OFFSET       (24)

typedef struct aeron_cluster_node_state_file_stct
{
    int      fd;
    uint8_t *mapped;
    size_t   mapped_length;
    int      file_sync_level;   /* 0 = no fsync, >0 = msync on write */
    char     path[4096];
}
aeron_cluster_node_state_file_t;

/**
 * Open or create the node-state file in the given directory.
 *
 * @param nsf         out: allocated/opened node state file handle.
 * @param cluster_dir directory containing the file.
 * @param create_new  if true, create the file when it does not exist.
 * @param file_sync_level  0 = no sync, >0 = msync on write.
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_node_state_file_open(
    aeron_cluster_node_state_file_t **nsf,
    const char *cluster_dir,
    bool create_new,
    int file_sync_level);

/** Close (munmap + free) the node state file handle. */
int aeron_cluster_node_state_file_close(aeron_cluster_node_state_file_t *nsf);

/**
 * Write the candidateTermId, logPosition, and timestamp.
 * logPosition and timestamp are written before candidateTermId to
 * prevent partial-update confusion on recovery.
 */
void aeron_cluster_node_state_file_update_candidate_term_id(
    aeron_cluster_node_state_file_t *nsf,
    int64_t candidate_term_id,
    int64_t log_position,
    int64_t timestamp_ms);

/**
 * Atomically propose a new max candidateTermId.  If candidate_term_id is
 * greater than the current value it is written and returned; otherwise the
 * current value is returned unchanged.
 */
int64_t aeron_cluster_node_state_file_propose_max_candidate_term_id(
    aeron_cluster_node_state_file_t *nsf,
    int64_t candidate_term_id,
    int64_t log_position,
    int64_t timestamp_ms);

/** Read the current candidateTermId. Returns AERON_NULL_VALUE (-1) if file invalid. */
int64_t aeron_cluster_node_state_file_candidate_term_id(const aeron_cluster_node_state_file_t *nsf);

/** Read the log position associated with the current candidateTermId. */
int64_t aeron_cluster_node_state_file_log_position(const aeron_cluster_node_state_file_t *nsf);

/** Read the timestamp associated with the current candidateTermId (epoch ms). */
int64_t aeron_cluster_node_state_file_timestamp(const aeron_cluster_node_state_file_t *nsf);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_NODE_STATE_FILE_H */
