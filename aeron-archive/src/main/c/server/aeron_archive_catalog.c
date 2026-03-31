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

#include "aeron_archive_catalog.h"

#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <inttypes.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
#include "util/aeron_bitutil.h"
#include "aeronc.h"

/*
 * Semantic version for the catalog format.
 * Major=2, Minor=2, Patch=0 packed as: (major << 16) | (minor << 8) | patch
 */
#define AERON_ARCHIVE_SEMANTIC_VERSION ((2 << 16) | (2 << 8) | 0)

#define AERON_ARCHIVE_CATALOG_NULL_POSITION AERON_NULL_VALUE
#define AERON_ARCHIVE_CATALOG_NULL_TIMESTAMP AERON_NULL_VALUE

#define AERON_ARCHIVE_CATALOG_INDEX_DEFAULT_CAPACITY (10)

/* --------------------------------------------------------------------------
 * Catalog index (sorted array of [recording_id, offset] pairs)
 * -------------------------------------------------------------------------- */

static int aeron_archive_catalog_index_init(aeron_archive_catalog_index_t *index)
{
    index->count = 0;
    index->capacity = AERON_ARCHIVE_CATALOG_INDEX_DEFAULT_CAPACITY;

    if (aeron_alloc((void **)&index->entries, (size_t)(index->capacity * 2) * sizeof(int64_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate catalog index");
        return -1;
    }

    memset(index->entries, 0, (size_t)(index->capacity * 2) * sizeof(int64_t));
    return 0;
}

static void aeron_archive_catalog_index_close(aeron_archive_catalog_index_t *index)
{
    aeron_free(index->entries);
    index->entries = NULL;
    index->count = 0;
    index->capacity = 0;
}

static int aeron_archive_catalog_index_ensure_capacity(aeron_archive_catalog_index_t *index)
{
    if (index->count >= index->capacity)
    {
        int32_t new_capacity = index->capacity + (index->capacity >> 1);
        int64_t *new_entries = NULL;

        if (aeron_alloc((void **)&new_entries, (size_t)(new_capacity * 2) * sizeof(int64_t)) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to expand catalog index");
            return -1;
        }

        memcpy(new_entries, index->entries, (size_t)(index->count * 2) * sizeof(int64_t));
        memset(
            new_entries + (index->count * 2),
            0,
            (size_t)((new_capacity - index->count) * 2) * sizeof(int64_t));
        aeron_free(index->entries);
        index->entries = new_entries;
        index->capacity = new_capacity;
    }

    return 0;
}

/**
 * Add a (recording_id, offset) pair to the index. Recording IDs must be added in increasing order.
 */
static int aeron_archive_catalog_index_add(
    aeron_archive_catalog_index_t *index, int64_t recording_id, int64_t offset)
{
    if (aeron_archive_catalog_index_ensure_capacity(index) < 0)
    {
        return -1;
    }

    int32_t pos = index->count * 2;
    index->entries[pos] = recording_id;
    index->entries[pos + 1] = offset;
    index->count++;

    return 0;
}

/**
 * Binary interpolation search for recording_id in the sorted index.
 * Returns the position (pair index * 2) or -1 if not found.
 */
static int32_t aeron_archive_catalog_index_find(
    const aeron_archive_catalog_index_t *index, int64_t recording_id)
{
    if (index->count == 0)
    {
        return -1;
    }

    const int64_t *entries = index->entries;
    const int32_t last_position = (index->count - 1) * 2;

    if (last_position > 0 && recording_id >= entries[0] && recording_id <= entries[last_position])
    {
        int32_t position = (int32_t)(
            (recording_id - entries[0]) * last_position / (entries[last_position] - entries[0]));
        position = (0 == (position & 1)) ? position : position + 1;

        if (recording_id == entries[position])
        {
            return position;
        }
        else if (recording_id > entries[position])
        {
            for (int32_t i = position + 2; i <= last_position; i += 2)
            {
                int64_t id = entries[i];
                if (recording_id == id)
                {
                    return i;
                }
                else if (id > recording_id)
                {
                    break;
                }
            }
        }
        else
        {
            for (int32_t i = position - 2; i >= 0; i -= 2)
            {
                int64_t id = entries[i];
                if (recording_id == id)
                {
                    return i;
                }
                else if (id < recording_id)
                {
                    break;
                }
            }
        }
    }
    else if (0 == last_position && recording_id == entries[0])
    {
        return 0;
    }

    return -1;
}

/**
 * Remove a recording_id from the index.
 * Returns the file offset of the removed entry, or -1 if not found.
 */
static int64_t aeron_archive_catalog_index_remove(
    aeron_archive_catalog_index_t *index, int64_t recording_id)
{
    int32_t position = aeron_archive_catalog_index_find(index, recording_id);
    if (position < 0)
    {
        return -1;
    }

    int64_t offset = index->entries[position + 1];
    int32_t last_position = (index->count - 1) * 2;

    /* Shift remaining entries left */
    for (int32_t i = position; i < last_position; i += 2)
    {
        index->entries[i] = index->entries[i + 2];
        index->entries[i + 1] = index->entries[i + 3];
    }
    /* Clear the vacated last slot */
    index->entries[last_position] = 0;
    index->entries[last_position + 1] = 0;

    index->count--;

    return offset;
}

/**
 * Get the file offset for a recording_id, or -1 if not found.
 */
static int64_t aeron_archive_catalog_index_recording_offset(
    const aeron_archive_catalog_index_t *index, int64_t recording_id)
{
    int32_t position = aeron_archive_catalog_index_find(index, recording_id);
    if (position < 0)
    {
        return -1;
    }

    return index->entries[position + 1];
}

/* --------------------------------------------------------------------------
 * Internal helpers for reading/writing the memory-mapped catalog
 * -------------------------------------------------------------------------- */

static inline int32_t aeron_archive_catalog_get_int32(const uint8_t *base, int32_t offset)
{
    int32_t value;
    memcpy(&value, base + offset, sizeof(value));
    return value;
}

static inline void aeron_archive_catalog_put_int32(uint8_t *base, int32_t offset, int32_t value)
{
    memcpy(base + offset, &value, sizeof(value));
}

static inline int64_t aeron_archive_catalog_get_int64(const uint8_t *base, int32_t offset)
{
    int64_t value;
    memcpy(&value, base + offset, sizeof(value));
    return value;
}

static inline void aeron_archive_catalog_put_int64(uint8_t *base, int32_t offset, int64_t value)
{
    memcpy(base + offset, &value, sizeof(value));
}

/**
 * Compute the total frame length for a recording descriptor entry (header + body), aligned.
 */
static int32_t aeron_archive_catalog_recording_descriptor_frame_length(
    int32_t alignment,
    size_t stripped_channel_length,
    size_t original_channel_length,
    size_t source_identity_length)
{
    int32_t body_length =
        (int32_t)(7 * sizeof(int64_t) +
        6 * sizeof(int32_t) +
        sizeof(int32_t) + stripped_channel_length +
        sizeof(int32_t) + original_channel_length +
        sizeof(int32_t) + source_identity_length);

    return (int32_t)AERON_ALIGN(
        (uint32_t)(AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH + body_length),
        (uint32_t)alignment);
}

/**
 * Read one descriptor header at the given offset. Returns the aligned frame length, or -1 if invalid.
 */
static int32_t aeron_archive_catalog_wrap_descriptor_at_offset(
    const uint8_t *buffer, int32_t offset, int32_t alignment)
{
    int32_t recording_length = aeron_archive_catalog_get_int32(
        buffer,
        offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH_OFFSET);

    if (recording_length > 0)
    {
        return (int32_t)AERON_ALIGN(
            (uint32_t)(recording_length + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH),
            (uint32_t)alignment);
    }

    return -1;
}

/**
 * Decode the variable-length strings from a recording descriptor body.
 * The body starts at (entry_offset + DESCRIPTOR_HEADER_LENGTH).
 */
static void aeron_archive_catalog_decode_descriptor(
    const uint8_t *buffer,
    int32_t entry_offset,
    aeron_archive_catalog_recording_descriptor_t *descriptor)
{
    const uint8_t *header_base = buffer + entry_offset;
    const uint8_t *body_base = header_base + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH;

    /* State from header */
    descriptor->state = (aeron_archive_recording_state_t)aeron_archive_catalog_get_int32(
        header_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_STATE_OFFSET);

    /* Fixed fields from body */
    descriptor->control_session_id = aeron_archive_catalog_get_int64(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_CONTROL_SESSION_ID_OFFSET);
    descriptor->correlation_id = aeron_archive_catalog_get_int64(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_CORRELATION_ID_OFFSET);
    descriptor->recording_id = aeron_archive_catalog_get_int64(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_RECORDING_ID_OFFSET);
    descriptor->start_timestamp = aeron_archive_catalog_get_int64(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_START_TIMESTAMP_OFFSET);
    descriptor->stop_timestamp = aeron_archive_catalog_get_int64(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_STOP_TIMESTAMP_OFFSET);
    descriptor->start_position = aeron_archive_catalog_get_int64(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_START_POSITION_OFFSET);
    descriptor->stop_position = aeron_archive_catalog_get_int64(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_STOP_POSITION_OFFSET);
    descriptor->initial_term_id = aeron_archive_catalog_get_int32(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_INITIAL_TERM_ID_OFFSET);
    descriptor->segment_file_length = aeron_archive_catalog_get_int32(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_SEGMENT_FILE_LENGTH_OFFSET);
    descriptor->term_buffer_length = aeron_archive_catalog_get_int32(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_TERM_BUFFER_LENGTH_OFFSET);
    descriptor->mtu_length = aeron_archive_catalog_get_int32(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_MTU_LENGTH_OFFSET);
    descriptor->session_id = aeron_archive_catalog_get_int32(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_SESSION_ID_OFFSET);
    descriptor->stream_id = aeron_archive_catalog_get_int32(
        body_base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_STREAM_ID_OFFSET);

    /* Variable-length strings: each has a uint32 length prefix then ASCII data */
    int32_t var_offset = AERON_ARCHIVE_RECORDING_DESCRIPTOR_BLOCK_LENGTH;

    /* strippedChannel */
    uint32_t str_len;
    memcpy(&str_len, body_base + var_offset, sizeof(str_len));
    descriptor->stripped_channel_length = (size_t)str_len;
    descriptor->stripped_channel = (const char *)(body_base + var_offset + sizeof(uint32_t));
    var_offset += (int32_t)(sizeof(uint32_t) + str_len);

    /* originalChannel */
    memcpy(&str_len, body_base + var_offset, sizeof(str_len));
    descriptor->original_channel_length = (size_t)str_len;
    descriptor->original_channel = (const char *)(body_base + var_offset + sizeof(uint32_t));
    var_offset += (int32_t)(sizeof(uint32_t) + str_len);

    /* sourceIdentity */
    memcpy(&str_len, body_base + var_offset, sizeof(str_len));
    descriptor->source_identity_length = (size_t)str_len;
    descriptor->source_identity = (const char *)(body_base + var_offset + sizeof(uint32_t));
}

/**
 * Encode a recording descriptor into the catalog buffer at the given offset.
 */
static void aeron_archive_catalog_encode_descriptor(
    uint8_t *buffer,
    int32_t entry_offset,
    int32_t frame_length,
    int64_t recording_id,
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
    size_t stripped_channel_length,
    const char *original_channel,
    size_t original_channel_length,
    const char *source_identity,
    size_t source_identity_length)
{
    uint8_t *base = buffer + entry_offset;
    uint8_t *body = base + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH;

    /* Zero the entire frame first */
    memset(base, 0, (size_t)frame_length);

    /* Encode body fixed fields */
    aeron_archive_catalog_put_int64(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_CONTROL_SESSION_ID_OFFSET, 0);
    aeron_archive_catalog_put_int64(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_CORRELATION_ID_OFFSET, 0);
    aeron_archive_catalog_put_int64(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_RECORDING_ID_OFFSET, recording_id);
    aeron_archive_catalog_put_int64(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_START_TIMESTAMP_OFFSET, start_timestamp);
    aeron_archive_catalog_put_int64(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_STOP_TIMESTAMP_OFFSET, stop_timestamp);
    aeron_archive_catalog_put_int64(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_START_POSITION_OFFSET, start_position);
    aeron_archive_catalog_put_int64(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_STOP_POSITION_OFFSET, stop_position);
    aeron_archive_catalog_put_int32(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_INITIAL_TERM_ID_OFFSET, initial_term_id);
    aeron_archive_catalog_put_int32(
        body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_SEGMENT_FILE_LENGTH_OFFSET, segment_file_length);
    aeron_archive_catalog_put_int32(
        body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_TERM_BUFFER_LENGTH_OFFSET, term_buffer_length);
    aeron_archive_catalog_put_int32(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_MTU_LENGTH_OFFSET, mtu_length);
    aeron_archive_catalog_put_int32(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_SESSION_ID_OFFSET, session_id);
    aeron_archive_catalog_put_int32(body, AERON_ARCHIVE_RECORDING_DESCRIPTOR_STREAM_ID_OFFSET, stream_id);

    /* Encode variable-length strings */
    int32_t var_offset = AERON_ARCHIVE_RECORDING_DESCRIPTOR_BLOCK_LENGTH;

    uint32_t len32 = (uint32_t)stripped_channel_length;
    memcpy(body + var_offset, &len32, sizeof(len32));
    var_offset += (int32_t)sizeof(uint32_t);
    if (stripped_channel_length > 0)
    {
        memcpy(body + var_offset, stripped_channel, stripped_channel_length);
        var_offset += (int32_t)stripped_channel_length;
    }

    len32 = (uint32_t)original_channel_length;
    memcpy(body + var_offset, &len32, sizeof(len32));
    var_offset += (int32_t)sizeof(uint32_t);
    if (original_channel_length > 0)
    {
        memcpy(body + var_offset, original_channel, original_channel_length);
        var_offset += (int32_t)original_channel_length;
    }

    len32 = (uint32_t)source_identity_length;
    memcpy(body + var_offset, &len32, sizeof(len32));
    var_offset += (int32_t)sizeof(uint32_t);
    if (source_identity_length > 0)
    {
        memcpy(body + var_offset, source_identity, source_identity_length);
    }

    /* Encode the descriptor header */
    int32_t recording_length = frame_length - AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH;
    aeron_archive_catalog_put_int32(
        base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH_OFFSET, recording_length);
    aeron_archive_catalog_put_int32(
        base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_STATE_OFFSET, (int32_t)AERON_ARCHIVE_RECORDING_VALID);
    aeron_archive_catalog_put_int32(
        base, AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_CHECKSUM_OFFSET, 0);
}

/**
 * Build the in-memory index from the on-disk catalog entries.
 */
static int aeron_archive_catalog_build_index(aeron_archive_catalog_t *catalog)
{
    int32_t offset = catalog->first_recording_descriptor_offset;
    int64_t last_recording_id = -1;

    while ((size_t)offset < catalog->capacity)
    {
        int32_t frame_length = aeron_archive_catalog_wrap_descriptor_at_offset(
            catalog->buffer, offset, catalog->alignment);

        if (frame_length < 0)
        {
            break;
        }

        /* Read the recording_id from the body */
        int64_t recording_id = aeron_archive_catalog_get_int64(
            catalog->buffer,
            offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH +
            AERON_ARCHIVE_RECORDING_DESCRIPTOR_RECORDING_ID_OFFSET);

        /* Only index VALID entries */
        int32_t state = aeron_archive_catalog_get_int32(
            catalog->buffer,
            offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_STATE_OFFSET);

        if (state == (int32_t)AERON_ARCHIVE_RECORDING_VALID)
        {
            if (aeron_archive_catalog_index_add(&catalog->index, recording_id, offset) < 0)
            {
                return -1;
            }
        }

        last_recording_id = recording_id;
        offset += frame_length;
    }

    catalog->next_recording_descriptor_offset = offset;

    if (0 == catalog->next_recording_id && last_recording_id >= 0)
    {
        catalog->next_recording_id = last_recording_id + 1;
    }

    return 0;
}

static void aeron_archive_catalog_force_writes(aeron_archive_catalog_t *catalog)
{
    if (catalog->force_writes && catalog->mapped_file.addr != NULL)
    {
        aeron_msync(catalog->mapped_file.addr, catalog->mapped_file.length);
    }
}

/* --------------------------------------------------------------------------
 * Public API
 * -------------------------------------------------------------------------- */

int aeron_archive_catalog_create(
    aeron_archive_catalog_t **catalog_p,
    const char *archive_dir,
    size_t capacity,
    bool force_writes,
    bool force_metadata)
{
    if (NULL == catalog_p || NULL == archive_dir)
    {
        AERON_SET_ERR(EINVAL, "%s", "catalog_p and archive_dir must not be NULL");
        return -1;
    }

    if (capacity < AERON_ARCHIVE_CATALOG_HEADER_LENGTH)
    {
        capacity = AERON_ARCHIVE_CATALOG_DEFAULT_CAPACITY;
    }

    aeron_archive_catalog_t *catalog = NULL;
    if (aeron_alloc((void **)&catalog, sizeof(aeron_archive_catalog_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate catalog");
        return -1;
    }
    memset(catalog, 0, sizeof(aeron_archive_catalog_t));

    /* Store archive directory */
    size_t dir_len = strlen(archive_dir);
    if (aeron_alloc((void **)&catalog->archive_dir, dir_len + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate archive_dir");
        aeron_free(catalog);
        return -1;
    }
    memcpy(catalog->archive_dir, archive_dir, dir_len + 1);

    /* Build catalog file path */
    size_t path_len = dir_len + 1 + strlen(AERON_ARCHIVE_CATALOG_FILE_NAME) + 1;
    if (aeron_alloc((void **)&catalog->catalog_file_path, path_len) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate catalog_file_path");
        aeron_free(catalog->archive_dir);
        aeron_free(catalog);
        return -1;
    }
    snprintf(catalog->catalog_file_path, path_len, "%s/%s", archive_dir, AERON_ARCHIVE_CATALOG_FILE_NAME);

    /* Initialize the index */
    if (aeron_archive_catalog_index_init(&catalog->index) < 0)
    {
        aeron_free(catalog->catalog_file_path);
        aeron_free(catalog->archive_dir);
        aeron_free(catalog);
        return -1;
    }

    /* Memory-map a new file */
    catalog->mapped_file.length = capacity;
    if (aeron_map_new_file(&catalog->mapped_file, catalog->catalog_file_path, true) < 0)
    {
        AERON_APPEND_ERR("failed to create catalog file: %s", catalog->catalog_file_path);
        aeron_archive_catalog_index_close(&catalog->index);
        aeron_free(catalog->catalog_file_path);
        aeron_free(catalog->archive_dir);
        aeron_free(catalog);
        return -1;
    }

    catalog->buffer = (uint8_t *)catalog->mapped_file.addr;
    catalog->capacity = catalog->mapped_file.length;
    catalog->alignment = AERON_CACHE_LINE_LENGTH;
    catalog->force_writes = force_writes;
    catalog->force_metadata = force_metadata;
    catalog->is_closed = false;
    catalog->next_recording_id = 0;
    catalog->first_recording_descriptor_offset = AERON_ARCHIVE_CATALOG_HEADER_LENGTH;
    catalog->next_recording_descriptor_offset = AERON_ARCHIVE_CATALOG_HEADER_LENGTH;

    /* Write catalog header */
    aeron_archive_catalog_put_int32(
        catalog->buffer, AERON_ARCHIVE_CATALOG_HEADER_VERSION_OFFSET, AERON_ARCHIVE_SEMANTIC_VERSION);
    aeron_archive_catalog_put_int32(
        catalog->buffer, AERON_ARCHIVE_CATALOG_HEADER_HEADER_LENGTH_OFFSET, AERON_ARCHIVE_CATALOG_HEADER_LENGTH);
    aeron_archive_catalog_put_int64(
        catalog->buffer, AERON_ARCHIVE_CATALOG_HEADER_NEXT_RECORDING_ID_OFFSET, 0);
    aeron_archive_catalog_put_int32(
        catalog->buffer, AERON_ARCHIVE_CATALOG_HEADER_ALIGNMENT_OFFSET, catalog->alignment);

    aeron_archive_catalog_force_writes(catalog);

    *catalog_p = catalog;
    return 0;
}

int aeron_archive_catalog_open(
    aeron_archive_catalog_t **catalog_p,
    const char *archive_dir,
    bool writable)
{
    if (NULL == catalog_p || NULL == archive_dir)
    {
        AERON_SET_ERR(EINVAL, "%s", "catalog_p and archive_dir must not be NULL");
        return -1;
    }

    aeron_archive_catalog_t *catalog = NULL;
    if (aeron_alloc((void **)&catalog, sizeof(aeron_archive_catalog_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate catalog");
        return -1;
    }
    memset(catalog, 0, sizeof(aeron_archive_catalog_t));

    /* Store archive directory */
    size_t dir_len = strlen(archive_dir);
    if (aeron_alloc((void **)&catalog->archive_dir, dir_len + 1) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate archive_dir");
        aeron_free(catalog);
        return -1;
    }
    memcpy(catalog->archive_dir, archive_dir, dir_len + 1);

    /* Build catalog file path */
    size_t path_len = dir_len + 1 + strlen(AERON_ARCHIVE_CATALOG_FILE_NAME) + 1;
    if (aeron_alloc((void **)&catalog->catalog_file_path, path_len) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to allocate catalog_file_path");
        aeron_free(catalog->archive_dir);
        aeron_free(catalog);
        return -1;
    }
    snprintf(catalog->catalog_file_path, path_len, "%s/%s", archive_dir, AERON_ARCHIVE_CATALOG_FILE_NAME);

    /* Initialize the index */
    if (aeron_archive_catalog_index_init(&catalog->index) < 0)
    {
        aeron_free(catalog->catalog_file_path);
        aeron_free(catalog->archive_dir);
        aeron_free(catalog);
        return -1;
    }

    /* Memory-map the existing file */
    if (aeron_map_existing_file(&catalog->mapped_file, catalog->catalog_file_path) < 0)
    {
        AERON_APPEND_ERR("failed to open catalog file: %s", catalog->catalog_file_path);
        aeron_archive_catalog_index_close(&catalog->index);
        aeron_free(catalog->catalog_file_path);
        aeron_free(catalog->archive_dir);
        aeron_free(catalog);
        return -1;
    }

    catalog->buffer = (uint8_t *)catalog->mapped_file.addr;
    catalog->capacity = catalog->mapped_file.length;
    catalog->force_writes = writable;
    catalog->force_metadata = false;
    catalog->is_closed = false;

    /* Read catalog header */
    if (catalog->capacity < AERON_ARCHIVE_CATALOG_HEADER_LENGTH)
    {
        AERON_SET_ERR(EINVAL, "catalog file too small: %" PRIu64, (uint64_t)catalog->capacity);
        aeron_unmap(&catalog->mapped_file);
        aeron_archive_catalog_index_close(&catalog->index);
        aeron_free(catalog->catalog_file_path);
        aeron_free(catalog->archive_dir);
        aeron_free(catalog);
        return -1;
    }

    int32_t alignment = aeron_archive_catalog_get_int32(
        catalog->buffer, AERON_ARCHIVE_CATALOG_HEADER_ALIGNMENT_OFFSET);
    if (0 != alignment)
    {
        catalog->alignment = alignment;
        catalog->first_recording_descriptor_offset = AERON_ARCHIVE_CATALOG_HEADER_LENGTH;
    }
    else
    {
        catalog->alignment = AERON_ARCHIVE_CATALOG_DEFAULT_ALIGNMENT;
        catalog->first_recording_descriptor_offset = AERON_ARCHIVE_CATALOG_DEFAULT_ALIGNMENT;
    }

    catalog->next_recording_id = aeron_archive_catalog_get_int64(
        catalog->buffer, AERON_ARCHIVE_CATALOG_HEADER_NEXT_RECORDING_ID_OFFSET);

    /* Build the index from existing entries */
    if (aeron_archive_catalog_build_index(catalog) < 0)
    {
        aeron_unmap(&catalog->mapped_file);
        aeron_archive_catalog_index_close(&catalog->index);
        aeron_free(catalog->catalog_file_path);
        aeron_free(catalog->archive_dir);
        aeron_free(catalog);
        return -1;
    }

    *catalog_p = catalog;
    return 0;
}

int aeron_archive_catalog_close(aeron_archive_catalog_t *catalog)
{
    if (NULL == catalog)
    {
        return 0;
    }

    if (!catalog->is_closed)
    {
        catalog->is_closed = true;

        if (catalog->mapped_file.addr != NULL)
        {
            aeron_unmap(&catalog->mapped_file);
            catalog->mapped_file.addr = NULL;
            catalog->mapped_file.length = 0;
        }

        catalog->buffer = NULL;

        aeron_archive_catalog_index_close(&catalog->index);
        aeron_free(catalog->catalog_file_path);
        aeron_free(catalog->archive_dir);
        aeron_free(catalog);
    }

    return 0;
}

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
    const char *source_identity)
{
    if (NULL == catalog || catalog->is_closed)
    {
        AERON_SET_ERR(EINVAL, "%s", "catalog is NULL or closed");
        return AERON_NULL_VALUE;
    }

    size_t stripped_len = (NULL != stripped_channel) ? strlen(stripped_channel) : 0;
    size_t original_len = (NULL != original_channel) ? strlen(original_channel) : 0;
    size_t source_len = (NULL != source_identity) ? strlen(source_identity) : 0;

    int32_t frame_length = aeron_archive_catalog_recording_descriptor_frame_length(
        catalog->alignment, stripped_len, original_len, source_len);

    int32_t recording_descriptor_offset = catalog->next_recording_descriptor_offset;

    /* Check if we have enough space */
    if ((size_t)(recording_descriptor_offset + frame_length) > catalog->capacity)
    {
        AERON_SET_ERR(
            ENOMEM,
            "catalog is full: offset=%d, frame_length=%d, capacity=%" PRIu64,
            recording_descriptor_offset, frame_length, (uint64_t)catalog->capacity);
        return AERON_NULL_VALUE;
    }

    int64_t recording_id = catalog->next_recording_id;

    /* Encode the descriptor */
    aeron_archive_catalog_encode_descriptor(
        catalog->buffer,
        recording_descriptor_offset,
        frame_length,
        recording_id,
        start_position,
        stop_position,
        start_timestamp,
        stop_timestamp,
        initial_term_id,
        segment_file_length,
        term_buffer_length,
        mtu_length,
        session_id,
        stream_id,
        stripped_channel,
        stripped_len,
        original_channel,
        original_len,
        source_identity,
        source_len);

    /* Update the catalog header's nextRecordingId */
    aeron_archive_catalog_put_int64(
        catalog->buffer,
        AERON_ARCHIVE_CATALOG_HEADER_NEXT_RECORDING_ID_OFFSET,
        recording_id + 1);

    aeron_archive_catalog_force_writes(catalog);

    /* Update in-memory state */
    catalog->next_recording_id = recording_id + 1;
    catalog->next_recording_descriptor_offset = recording_descriptor_offset + frame_length;

    if (aeron_archive_catalog_index_add(&catalog->index, recording_id, recording_descriptor_offset) < 0)
    {
        return AERON_NULL_VALUE;
    }

    return recording_id;
}

int aeron_archive_catalog_find_recording(
    aeron_archive_catalog_t *catalog,
    int64_t recording_id,
    aeron_archive_catalog_recording_descriptor_t *descriptor)
{
    if (NULL == catalog || catalog->is_closed || NULL == descriptor)
    {
        AERON_SET_ERR(EINVAL, "%s", "catalog, or descriptor is NULL, or catalog is closed");
        return -1;
    }

    if (recording_id < 0)
    {
        return -1;
    }

    int64_t offset = aeron_archive_catalog_index_recording_offset(&catalog->index, recording_id);
    if (offset < 0)
    {
        return -1;
    }

    int32_t frame_length = aeron_archive_catalog_wrap_descriptor_at_offset(
        catalog->buffer, (int32_t)offset, catalog->alignment);
    if (frame_length < 0)
    {
        return -1;
    }

    aeron_archive_catalog_decode_descriptor(catalog->buffer, (int32_t)offset, descriptor);

    return 0;
}

int aeron_archive_catalog_update_recording_position(
    aeron_archive_catalog_t *catalog,
    int64_t recording_id,
    int64_t position,
    int64_t timestamp_ms)
{
    if (NULL == catalog || catalog->is_closed)
    {
        AERON_SET_ERR(EINVAL, "%s", "catalog is NULL or closed");
        return -1;
    }

    if (recording_id < 0)
    {
        AERON_SET_ERR(EINVAL, "invalid recording_id: %" PRId64, recording_id);
        return -1;
    }

    int64_t offset64 = aeron_archive_catalog_index_recording_offset(&catalog->index, recording_id);
    if (offset64 < 0)
    {
        AERON_SET_ERR(EINVAL, "recording not found: %" PRId64, recording_id);
        return -1;
    }

    int32_t descriptor_offset = (int32_t)offset64;
    int32_t body_offset = descriptor_offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_LENGTH;

    /* Update stop timestamp */
    aeron_archive_catalog_put_int64(
        catalog->buffer,
        body_offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_STOP_TIMESTAMP_OFFSET,
        timestamp_ms);

    /* Update stop position (use volatile semantics in Java; here we just write and ensure ordering) */
    aeron_archive_catalog_put_int64(
        catalog->buffer,
        body_offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_STOP_POSITION_OFFSET,
        position);

    aeron_archive_catalog_force_writes(catalog);

    return 0;
}

int aeron_archive_catalog_invalidate_recording(
    aeron_archive_catalog_t *catalog,
    int64_t recording_id)
{
    if (NULL == catalog || catalog->is_closed)
    {
        AERON_SET_ERR(EINVAL, "%s", "catalog is NULL or closed");
        return -1;
    }

    if (recording_id < 0)
    {
        return -1;
    }

    int64_t offset = aeron_archive_catalog_index_remove(&catalog->index, recording_id);
    if (offset < 0)
    {
        return -1;
    }

    /* Set state to INVALID */
    aeron_archive_catalog_put_int32(
        catalog->buffer,
        (int32_t)offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_STATE_OFFSET,
        (int32_t)AERON_ARCHIVE_RECORDING_INVALID);

    aeron_archive_catalog_force_writes(catalog);

    return 0;
}

int32_t aeron_archive_catalog_for_each(
    aeron_archive_catalog_t *catalog,
    aeron_archive_catalog_entry_func_t func,
    void *clientd)
{
    if (NULL == catalog || catalog->is_closed || NULL == func)
    {
        AERON_SET_ERR(EINVAL, "%s", "catalog is NULL/closed or func is NULL");
        return -1;
    }

    int32_t count = 0;
    int32_t offset = catalog->first_recording_descriptor_offset;

    while (offset < catalog->next_recording_descriptor_offset)
    {
        int32_t frame_length = aeron_archive_catalog_wrap_descriptor_at_offset(
            catalog->buffer, offset, catalog->alignment);

        if (frame_length < 0)
        {
            break;
        }

        int32_t state = aeron_archive_catalog_get_int32(
            catalog->buffer,
            offset + AERON_ARCHIVE_RECORDING_DESCRIPTOR_HEADER_STATE_OFFSET);

        if (state == (int32_t)AERON_ARCHIVE_RECORDING_VALID)
        {
            aeron_archive_catalog_recording_descriptor_t descriptor;
            aeron_archive_catalog_decode_descriptor(catalog->buffer, offset, &descriptor);
            func(&descriptor, clientd);
            count++;
        }

        offset += frame_length;
    }

    return count;
}

int64_t aeron_archive_catalog_next_recording_id(const aeron_archive_catalog_t *catalog)
{
    if (NULL == catalog)
    {
        return AERON_NULL_VALUE;
    }

    return catalog->next_recording_id;
}

int32_t aeron_archive_catalog_recording_count(const aeron_archive_catalog_t *catalog)
{
    if (NULL == catalog)
    {
        return 0;
    }

    return catalog->index.count;
}
