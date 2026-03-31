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

#ifndef AERON_ARCHIVE_MARK_FILE_H
#define AERON_ARCHIVE_MARK_FILE_H

#include <stdint.h>
#include <stdbool.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C"
{
#endif

/* -----------------------------------------------------------------------
 * Archive Mark File binary layout (matches Java ArchiveMarkFile SBE encoding)
 *
 * Offset  Size  Field
 *      0     8  SBE MessageHeader (templateId, version, blockLength, schemaId)
 *      8     4  version  (0 = not ready, >0 = ready / semantic version, -1 = failed)
 *     12     4  (padding)
 *     16     8  activityTimestamp (ms epoch, updated periodically as proof of life)
 *     24     8  startTimestamp (ms epoch, set at creation)
 *     32     8  pid
 *     40     4  controlStreamId
 *     44     4  localControlStreamId
 *     48     4  eventsStreamId
 *     52     4  headerLength  (= HEADER_LENGTH = 8192)
 *     56     4  errorBufferLength
 *     60     8  archiveId
 *     68+       variable-length strings: controlChannel, localControlChannel,
 *               eventsChannel, aeronDirectory
 *   8192+       error buffer
 *
 * Total file size = HEADER_LENGTH + errorBufferLength, page-aligned.
 * ----------------------------------------------------------------------- */

/** Major version for archive files on disk. */
#define AERON_ARCHIVE_MARK_FILE_MAJOR_VERSION     3
/** Minor version for archive files on disk. */
#define AERON_ARCHIVE_MARK_FILE_MINOR_VERSION     1
/** Patch version for archive files on disk. */
#define AERON_ARCHIVE_MARK_FILE_PATCH_VERSION     0

/** Combined semantic version (major << 16 | minor << 8 | patch). */
#define AERON_ARCHIVE_MARK_FILE_SEMANTIC_VERSION  \
    ((AERON_ARCHIVE_MARK_FILE_MAJOR_VERSION << 16) | \
     (AERON_ARCHIVE_MARK_FILE_MINOR_VERSION << 8)  | \
      AERON_ARCHIVE_MARK_FILE_PATCH_VERSION)

/** Version value written on failure / termination. */
#define AERON_ARCHIVE_MARK_FILE_VERSION_FAILED    (-1)

/** Fixed header area before the error buffer. */
#define AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH     8192

/** Default error buffer length (1 MiB). */
#define AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_DEFAULT  (1024 * 1024)

/** Name of the archive mark file. */
#define AERON_ARCHIVE_MARK_FILE_FILENAME          "archive-mark.dat"

/** Name of the link file pointing to the mark file. */
#define AERON_ARCHIVE_MARK_FILE_LINK_FILENAME     "archive-mark.lnk"

/** Mark file update interval in milliseconds. */
#define AERON_ARCHIVE_MARK_FILE_UPDATE_INTERVAL_MS  1000

/** Liveness timeout: 10x the update interval. */
#define AERON_ARCHIVE_MARK_FILE_LIVENESS_TIMEOUT_MS \
    (10 * AERON_ARCHIVE_MARK_FILE_UPDATE_INTERVAL_MS)

/* Byte offsets within the memory-mapped header */
#define AERON_ARCHIVE_MARK_FILE_SBE_HEADER_OFFSET             0
#define AERON_ARCHIVE_MARK_FILE_VERSION_OFFSET                 8
#define AERON_ARCHIVE_MARK_FILE_ACTIVITY_TIMESTAMP_OFFSET     16
#define AERON_ARCHIVE_MARK_FILE_START_TIMESTAMP_OFFSET        24
#define AERON_ARCHIVE_MARK_FILE_PID_OFFSET                    32
#define AERON_ARCHIVE_MARK_FILE_CONTROL_STREAM_ID_OFFSET      40
#define AERON_ARCHIVE_MARK_FILE_LOCAL_CONTROL_STREAM_ID_OFFSET 44
#define AERON_ARCHIVE_MARK_FILE_EVENTS_STREAM_ID_OFFSET       48
#define AERON_ARCHIVE_MARK_FILE_HEADER_LENGTH_OFFSET          52
#define AERON_ARCHIVE_MARK_FILE_ERROR_BUFFER_LENGTH_OFFSET    56
#define AERON_ARCHIVE_MARK_FILE_ARCHIVE_ID_OFFSET             60

typedef struct aeron_archive_mark_file_stct
{
    int      fd;
    uint8_t *mapped;
    size_t   mapped_length;
    int32_t  error_buffer_length;
    char     path[4096];
}
aeron_archive_mark_file_t;

/**
 * Create or re-open the archive mark file.
 *
 * If the file already exists and is active (version > 0 and activity timestamp within
 * timeout_ms of now_ms), returns -1 with error set.
 *
 * @param mark_file            out parameter for the allocated mark file struct.
 * @param directory            directory to create the mark file in.
 * @param error_buffer_length  size of the error buffer region (>= ERROR_BUFFER_LENGTH_DEFAULT).
 * @param archive_id           archive identity to store.
 * @param control_stream_id    control stream id.
 * @param local_control_stream_id local control stream id.
 * @param events_stream_id     recording events stream id.
 * @param control_channel      control channel string (may be NULL).
 * @param local_control_channel local control channel string.
 * @param events_channel       recording events channel string (may be NULL).
 * @param aeron_directory      aeron directory name.
 * @param now_ms               current epoch time in milliseconds.
 * @param timeout_ms           liveness timeout in milliseconds.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_mark_file_create(
    aeron_archive_mark_file_t **mark_file,
    const char *directory,
    int32_t error_buffer_length,
    int64_t archive_id,
    int32_t control_stream_id,
    int32_t local_control_stream_id,
    int32_t events_stream_id,
    const char *control_channel,
    const char *local_control_channel,
    const char *events_channel,
    const char *aeron_directory,
    int64_t now_ms,
    int64_t timeout_ms);

/**
 * Close and unmap the mark file, freeing its memory.
 *
 * @param mark_file  the mark file to close (may be NULL).
 * @return 0 on success.
 */
int aeron_archive_mark_file_close(aeron_archive_mark_file_t *mark_file);

/**
 * Signal that the archive has concluded successfully and is ready.
 * Sets the version field to SEMANTIC_VERSION and updates the activity timestamp.
 *
 * @param mark_file  the mark file.
 * @param now_ms     current epoch time in milliseconds.
 */
void aeron_archive_mark_file_signal_ready(aeron_archive_mark_file_t *mark_file, int64_t now_ms);

/**
 * Signal that the archive has terminated.
 * Sets activity timestamp to -1 (NULL_VALUE).
 *
 * @param mark_file  the mark file.
 */
void aeron_archive_mark_file_signal_terminated(aeron_archive_mark_file_t *mark_file);

/**
 * Update the activity timestamp as proof of life.
 *
 * @param mark_file  the mark file.
 * @param now_ms     current epoch time in milliseconds.
 */
void aeron_archive_mark_file_update_activity_timestamp(
    aeron_archive_mark_file_t *mark_file, int64_t now_ms);

/**
 * Read the activity timestamp with volatile semantics.
 *
 * @param mark_file  the mark file.
 * @return the activity timestamp, or -1 if closed/NULL.
 */
int64_t aeron_archive_mark_file_activity_timestamp_volatile(
    const aeron_archive_mark_file_t *mark_file);

/**
 * Read the archive id from the mark file.
 *
 * @param mark_file  the mark file.
 * @return the archive id, or -1 if NULL.
 */
int64_t aeron_archive_mark_file_archive_id(const aeron_archive_mark_file_t *mark_file);

/**
 * Check whether an existing archive mark file in the given directory is active.
 *
 * @param directory   directory containing the mark file.
 * @param now_ms      current epoch time in milliseconds.
 * @param timeout_ms  liveness timeout in milliseconds.
 * @return true if an active archive is detected.
 */
bool aeron_archive_mark_file_is_active(
    const char *directory, int64_t now_ms, int64_t timeout_ms);

/**
 * Get a pointer to the error buffer region.
 *
 * @param mark_file  the mark file.
 * @return pointer to the error buffer, or NULL.
 */
uint8_t *aeron_archive_mark_file_error_buffer(aeron_archive_mark_file_t *mark_file);

/**
 * Get the length of the error buffer.
 *
 * @param mark_file  the mark file.
 * @return the error buffer length.
 */
int32_t aeron_archive_mark_file_error_buffer_length(const aeron_archive_mark_file_t *mark_file);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_MARK_FILE_H */
