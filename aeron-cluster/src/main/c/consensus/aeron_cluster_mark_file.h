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

#ifndef AERON_CLUSTER_MARK_FILE_H
#define AERON_CLUSTER_MARK_FILE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* -----------------------------------------------------------------------
 * File layout (matches Java ClusterMarkFile + Agrona MarkFile binary format)
 *
 * Offset  Size  Field
 *      0     8  SBE MessageHeader (templateId=200, version, blockLength=128, schemaId)
 *      8     4  version  (0=not ready, >0=ready, -1=failed)
 *     12     4  componentType  (1=CONSENSUS_MODULE, 2=CONTAINER, 3=BACKUP)
 *     16     8  activityTimestamp (ms, updated periodically)
 *     24     8  startTimestamp (ms)
 *     32     8  pid
 *     40     8  candidateTermId
 *     48     4  archiveStreamId
 *     52     4  serviceStreamId
 *     56     4  consensusModuleStreamId
 *     60     4  ingressStreamId
 *     64     4  memberId
 *     68     4  serviceId
 *     72     4  headerLength (= HEADER_LENGTH = 8192)
 *     76     4  errorBufferLength
 *     80     4  clusterId
 *     84    52  reserved (pad to 136 bytes block end)
 *   8192+      error buffer
 *
 * File is HEADER_LENGTH (8192) + errorBufferLength bytes.
 * ----------------------------------------------------------------------- */

#define AERON_CLUSTER_MARK_FILE_HEADER_LENGTH        8192
#define AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_MIN     (1024 * 1024)
#define AERON_CLUSTER_MARK_FILE_SEMANTIC_VERSION     0x00010000  /* 1.0.0 */
#define AERON_CLUSTER_MARK_FILE_VERSION_FAILED       (-1)

/* Byte offsets within the file */
#define AERON_CLUSTER_MARK_FILE_SBE_HEADER_OFFSET    0
#define AERON_CLUSTER_MARK_FILE_VERSION_OFFSET       8
#define AERON_CLUSTER_MARK_FILE_COMPONENT_TYPE_OFFSET 12
#define AERON_CLUSTER_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET 16
#define AERON_CLUSTER_MARK_FILE_START_TIMESTAMP_OFFSET 24
#define AERON_CLUSTER_MARK_FILE_PID_OFFSET           32
#define AERON_CLUSTER_MARK_FILE_CANDIDATE_TERM_ID_OFFSET 40
#define AERON_CLUSTER_MARK_FILE_HEADER_LENGTH_OFFSET 72
#define AERON_CLUSTER_MARK_FILE_ERROR_BUFFER_LENGTH_OFFSET 76

typedef enum aeron_cluster_component_type_en
{
    AERON_CLUSTER_COMPONENT_UNKNOWN          = 0,
    AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE = 1,
    AERON_CLUSTER_COMPONENT_CONTAINER        = 2,
    AERON_CLUSTER_COMPONENT_BACKUP           = 3,
}
aeron_cluster_component_type_t;

#define AERON_CLUSTER_MARK_FILE_FILENAME            "cluster-mark.dat"
#define AERON_CLUSTER_MARK_FILE_SERVICE_PREFIX      "cluster-mark-service-"
#define AERON_CLUSTER_MARK_FILE_LINK_FILENAME       "cluster-mark.lnk"
#define AERON_CLUSTER_MARK_FILE_SERVICE_LINK_PREFIX "cluster-mark-service-"
#define AERON_CLUSTER_MARK_FILE_EXT                 ".dat"
#define AERON_CLUSTER_MARK_FILE_LINK_EXT            ".lnk"

typedef struct aeron_cluster_mark_file_stct
{
    int      fd;
    uint8_t *mapped;
    size_t   mapped_length;
    char     path[4096];
}
aeron_cluster_mark_file_t;

/**
 * Open or create a ClusterMarkFile.
 * If the file exists and is active (version>0 and activityTimestamp within timeout_ms),
 * returns -1 and sets error "active mark file detected: <path>".
 *
 * @param mark_file     out parameter
 * @param path          full path to the .dat file
 * @param component_type  e.g. AERON_CLUSTER_COMPONENT_CONSENSUS_MODULE
 * @param error_buffer_length  must be >= ERROR_BUFFER_MIN
 * @param now_ms        current epoch time in milliseconds
 * @param timeout_ms    how long to wait for an existing active file to go stale
 */
int aeron_cluster_mark_file_open(
    aeron_cluster_mark_file_t **mark_file,
    const char *path,
    aeron_cluster_component_type_t component_type,
    int error_buffer_length,
    int64_t now_ms,
    int64_t timeout_ms);

int  aeron_cluster_mark_file_close(aeron_cluster_mark_file_t *mark_file);

/** Mark the file as ready (version = SEMANTIC_VERSION, activityTimestamp = now_ms). */
void aeron_cluster_mark_file_signal_ready(aeron_cluster_mark_file_t *mark_file, int64_t now_ms);

/** Update the activity timestamp to prevent timeout. */
void aeron_cluster_mark_file_update_activity_timestamp(aeron_cluster_mark_file_t *mark_file, int64_t now_ms);

int64_t aeron_cluster_mark_file_candidate_term_id(aeron_cluster_mark_file_t *mark_file);
void    aeron_cluster_mark_file_set_candidate_term_id(aeron_cluster_mark_file_t *mark_file, int64_t term_id);

/** Returns true if file is active (version > 0 and timestamp within timeout_ms of now_ms). */
bool aeron_cluster_mark_file_is_active(const char *path, int64_t now_ms, int64_t timeout_ms);

/** Build filename for a CM mark file: cluster-mark.dat */
void aeron_cluster_mark_file_filename(char *buf, size_t buf_len);

/** Build filename for a service mark file: cluster-mark-service-<id>.dat */
void aeron_cluster_mark_file_service_filename(char *buf, size_t buf_len, int service_id);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_MARK_FILE_H */
