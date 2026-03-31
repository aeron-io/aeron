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

#ifndef AERON_ARCHIVE_CATALOG_H
#define AERON_ARCHIVE_CATALOG_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#include "util/aeron_fileutil.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_ARCHIVE_CATALOG_FILE_NAME "archive.catalog"
#define AERON_ARCHIVE_CATALOG_DEFAULT_ALIGNMENT (1024)
#define AERON_ARCHIVE_CATALOG_DEFAULT_CAPACITY (1024 * 1024)

/**
 * RecordingState enum values matching the SBE schema.
 */
typedef enum aeron_archive_recording_state_en
{
    AERON_ARCHIVE_RECORDING_INVALID = 0,
    AERON_ARCHIVE_RECORDING_VALID = 1,
    AERON_ARCHIVE_RECORDING_DELETED = 2
}
aeron_archive_recording_state_t;

/*
 * SBE layout constants - CatalogHeader (message id=20, block length=32).
 * Encoded directly in the catalog file at offset 0 with no SBE message header.
 *
 * Layout:
 *   +0:  version           (int32)
 *   +4:  length            (int32)
 *   +8:  nextRecordingId   (int64)
 *   +16: alignment         (int32)
 *   +31: reserved          (int8)
 *   Total block length: 32 bytes
 */
#define AERON_ARCHIVE_CATALOG_HEADER_LENGTH (32)
#define AERON_ARCHIVE_CATALOG_HEADER_VERSION_OFFSET (0)
#define AERON_ARCHIVE_CATALOG_HEADER_HEADER_LENGTH_OFFSET (4)
#define AERON_ARCHIVE_CATALOG_HEADER_NEXT_RECORDING_ID_OFFSET (8)
#define AERON_ARCHIVE_CATALOG_HEADER_ALIGNMENT_OFFSET (16)

/*
 * SBE layout constants - RecordingDescriptorHeader (message id=21, block length=32).
 *
 * Layout:
 *   +0:  length    (int32) - length of the RecordingDescriptor body
 *   +4:  state     (int32) - RecordingState enum
 *   +8:  checksum  (int32)
 *   +31: reserved  (int8)
 *   Total block length: 32 bytes
 */
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH (32)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH_OFFSET (0)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_STATE_OFFSET (4)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_CHECKSUM_OFFSET (8)

/*
 * SBE layout constants - RecordingDescriptor (message id=22).
 * Follows immediately after RecordingDescriptorHeader.
 *
 * Fixed-size fields (block length = 80 bytes):
 *   +0:  controlSessionId   (int64)
 *   +8:  correlationId      (int64)
 *   +16: recordingId        (int64)
 *   +24: startTimestamp     (int64)
 *   +32: stopTimestamp      (int64)
 *   +40: startPosition      (int64)
 *   +48: stopPosition       (int64)
 *   +56: initialTermId      (int32)
 *   +60: segmentFileLength  (int32)
 *   +64: termBufferLength   (int32)
 *   +68: mtuLength          (int32)
 *   +72: sessionId          (int32)
 *   +76: streamId           (int32)
 *
 * Variable-length fields (after fixed block):
 *   strippedChannel:  uint32 length + ASCII data
 *   originalChannel:  uint32 length + ASCII data
 *   sourceIdentity:   uint32 length + ASCII data
 */
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_CONTROL_SESSION_ID_OFFSET (0)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_CORRELATION_ID_OFFSET (8)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_RECORDING_ID_OFFSET (16)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_START_TIMESTAMP_OFFSET (24)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_STOP_TIMESTAMP_OFFSET (32)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_START_POSITION_OFFSET (40)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_STOP_POSITION_OFFSET (48)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_INITIAL_TERM_ID_OFFSET (56)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_SEGMENT_FILE_LENGTH_OFFSET (60)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_TERM_BUFFER_LENGTH_OFFSET (64)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_MTU_LENGTH_OFFSET (68)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_SESSION_ID_OFFSET (72)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_STREAM_ID_OFFSET (76)
#define AERON_ARCHIVE_RECORDING_DESCRIPTOR_BLOCK_LENGTH (80)

/**
 * A read-only view of a recording descriptor in the catalog.
 */
typedef struct aeron_archive_catalog_recording_descriptor_stct
{
    int64_t control_session_id;
    int64_t correlation_id;
    int64_t recording_id;
    int64_t start_timestamp;
    int64_t stop_timestamp;
    int64_t start_position;
    int64_t stop_position;
    int32_t initial_term_id;
    int32_t segment_file_length;
    int32_t term_buffer_length;
    int32_t mtu_length;
    int32_t session_id;
    int32_t stream_id;
    const char *stripped_channel;
    size_t stripped_channel_length;
    const char *original_channel;
    size_t original_channel_length;
    const char *source_identity;
    size_t source_identity_length;
    aeron_archive_recording_state_t state;
}
aeron_archive_catalog_recording_descriptor_t;

/**
 * Callback invoked for each valid recording descriptor during iteration.
 *
 * @param descriptor the recording descriptor.
 * @param clientd    user-supplied context.
 */
typedef void (*aeron_archive_catalog_entry_func_t)(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd);

/**
 * In-memory index mapping recording_id to file offset.
 * Entries are stored as sorted pairs: [recording_id, offset, recording_id, offset, ...].
 */
typedef struct aeron_archive_catalog_index_stct
{
    int64_t *entries;
    int32_t count;
    int32_t capacity;
}
aeron_archive_catalog_index_t;

/**
 * The Catalog manages the memory-mapped archive.catalog file containing recording descriptors.
 */
typedef struct aeron_archive_catalog_stct
{
    aeron_mapped_file_t mapped_file;
    uint8_t *buffer;
    size_t capacity;
    char *archive_dir;
    char *catalog_file_path;
    int32_t alignment;
    int32_t first_recording_descriptor_offset;
    int64_t next_recording_id;
    int32_t next_recording_descriptor_offset;
    bool force_writes;
    bool force_metadata;
    bool is_closed;
    aeron_archive_catalog_index_t index;
}
aeron_archive_catalog_t;

/**
 * Create a new catalog file in the given archive directory.
 *
 * @param catalog       out param for the allocated catalog.
 * @param archive_dir   the archive directory path.
 * @param capacity      the initial capacity of the catalog file in bytes.
 * @param force_writes  whether to msync after writes.
 * @param force_metadata whether to msync metadata as well.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_catalog_create(
    aeron_archive_catalog_t **catalog,
    const char *archive_dir,
    size_t capacity,
    bool force_writes,
    bool force_metadata);

/**
 * Open an existing catalog file from the given archive directory.
 *
 * @param catalog       out param for the allocated catalog.
 * @param archive_dir   the archive directory path.
 * @param writable      whether to open for read-write or read-only.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_catalog_open(
    aeron_archive_catalog_t **catalog,
    const char *archive_dir,
    bool writable);

/**
 * Close the catalog and release all resources.
 *
 * @param catalog the catalog to close.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_catalog_close(aeron_archive_catalog_t *catalog);

/**
 * Add a new recording to the catalog.
 *
 * @param catalog             the catalog.
 * @param start_position      the start position of the recording.
 * @param stop_position       the stop position (AERON_NULL_VALUE if still recording).
 * @param start_timestamp     the start timestamp in ms.
 * @param stop_timestamp      the stop timestamp in ms (AERON_NULL_VALUE if still recording).
 * @param initial_term_id     the initial term id of the image.
 * @param segment_file_length the segment file length.
 * @param term_buffer_length  the term buffer length.
 * @param mtu_length          the MTU length.
 * @param session_id          the session id.
 * @param stream_id           the stream id.
 * @param stripped_channel    the stripped channel URI.
 * @param original_channel    the original channel URI.
 * @param source_identity     the source identity.
 * @return the new recording_id on success, or AERON_NULL_VALUE on failure.
 */
int64_t aeron_archive_catalog_add_recording(
    aeron_archive_catalog_t *catalog,
    int64_t start_position,
    int64_t stop_position,
    int64_t start_timestamp,
    int64_t stop_timestamp,
    int32_t initial_term_id,
    int32_t segment_file_length,
    int32_t term_buffer_length,
    int32_t mtu_length,
    int32_t session_id,
    int32_t stream_id,
    const char *stripped_channel,
    const char *original_channel,
    const char *source_identity);

/**
 * Find a recording descriptor by recording_id.
 *
 * @param catalog      the catalog.
 * @param recording_id the recording id to find.
 * @param descriptor   out param filled with the recording descriptor fields.
 * @return 0 on success, -1 if not found or on error.
 */
int aeron_archive_catalog_find_recording(
    aeron_archive_catalog_t *catalog,
    int64_t recording_id,
    aeron_archive_catalog_recording_descriptor_t *descriptor);

/**
 * Update the stop position and stop timestamp for a recording.
 *
 * @param catalog      the catalog.
 * @param recording_id the recording id.
 * @param position     the new stop position.
 * @param timestamp_ms the new stop timestamp in ms.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_catalog_update_recording_position(
    aeron_archive_catalog_t *catalog,
    int64_t recording_id,
    int64_t position,
    int64_t timestamp_ms);

/**
 * Mark a recording as invalid and remove it from the index.
 *
 * @param catalog      the catalog.
 * @param recording_id the recording id to invalidate.
 * @return 0 on success, -1 on failure.
 */
int aeron_archive_catalog_invalidate_recording(
    aeron_archive_catalog_t *catalog,
    int64_t recording_id);

/**
 * Iterate over all valid recording descriptors in the catalog.
 *
 * @param catalog  the catalog.
 * @param func     the callback function.
 * @param clientd  user-supplied context passed to the callback.
 * @return the number of valid entries iterated, or -1 on error.
 */
int32_t aeron_archive_catalog_for_each(
    aeron_archive_catalog_t *catalog,
    aeron_archive_catalog_entry_func_t func,
    void *clientd);

/**
 * Get the next recording id that will be assigned.
 *
 * @param catalog the catalog.
 * @return the next recording id.
 */
int64_t aeron_archive_catalog_next_recording_id(const aeron_archive_catalog_t *catalog);

/**
 * Get the count of valid recordings in the catalog.
 *
 * @param catalog the catalog.
 * @return the number of valid recording entries.
 */
int32_t aeron_archive_catalog_recording_count(const aeron_archive_catalog_t *catalog);

#ifdef __cplusplus
}
#endif

#endif /* AERON_ARCHIVE_CATALOG_H */
