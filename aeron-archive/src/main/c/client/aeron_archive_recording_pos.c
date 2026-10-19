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

#include <errno.h>
#include <inttypes.h>
#include <string.h>

#include "util/aeron_error.h"
#include "concurrent/aeron_counters_manager.h"

#define AERON_ARCHIVE_RECORDING_POSITION_TYPE_ID 100

#pragma pack(push)
#pragma pack(4)
struct aeron_archive_recording_pos_key_defn
{
    int64_t recording_id;
    int32_t session_id;
    int32_t source_identity_length;
};
#pragma pack(pop)

typedef bool (*key_matcher_func_t)(struct aeron_archive_recording_pos_key_defn *key, void *clientd);

static int32_t find_counter_id(aeron_counters_reader_t *counters_reader, key_matcher_func_t matcher, void *clientd)
{
    for (int32_t i = 0, max_id = aeron_counters_reader_max_counter_id(counters_reader); i <= max_id; i++)
    {
        int32_t counter_state;

        if (aeron_counters_reader_counter_state(counters_reader, i, &counter_state) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return AERON_NULL_COUNTER_ID;
        }

        if (AERON_COUNTER_RECORD_ALLOCATED == counter_state)
        {
            int32_t type_id;

            if (aeron_counters_reader_counter_type_id(counters_reader, i, &type_id) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return AERON_NULL_COUNTER_ID;
            }

            if (AERON_ARCHIVE_RECORDING_POSITION_TYPE_ID == type_id)
            {
                struct aeron_archive_recording_pos_key_defn *key;

                if (aeron_counters_reader_metadata_key(counters_reader, i, (uint8_t **)&key) < 0)
                {
                    AERON_SET_ERR(-1, "unable to locate metadata key for counter %i", i);
                    return AERON_NULL_COUNTER_ID;
                }

                if (matcher(key, clientd))
                {
                    return i;
                }
            }
        }
        else if (AERON_COUNTER_RECORD_UNUSED == counter_state)
        {
            break;
        }
    }

    return AERON_NULL_COUNTER_ID;
}

static bool recording_id_matcher(struct aeron_archive_recording_pos_key_defn *key, void *clientd)
{
    return key->recording_id == *(int64_t *)clientd;
}

int32_t aeron_archive_recording_pos_find_counter_id_by_recording_id(aeron_counters_reader_t *counters_reader, int64_t recording_id)
{
    return find_counter_id(counters_reader, recording_id_matcher, &recording_id);
}

static bool session_id_matcher(struct aeron_archive_recording_pos_key_defn *key, void *clientd)
{
    return key->session_id == *(int32_t *)clientd;
}

int32_t aeron_archive_recording_pos_find_counter_id_by_session_id(aeron_counters_reader_t *counters_reader, int32_t session_id)
{
    return find_counter_id(counters_reader, session_id_matcher, &session_id);
}

int64_t aeron_archive_recording_pos_get_recording_id(aeron_counters_reader_t *counters_reader, int32_t counter_id)
{
    int32_t state, type_id;

    if (aeron_counters_reader_counter_state(counters_reader, counter_id, &state) < 0 ||
        aeron_counters_reader_counter_type_id(counters_reader, counter_id, &type_id) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return AERON_NULL_COUNTER_ID;
    }

    if (AERON_COUNTER_RECORD_ALLOCATED != state ||
        AERON_ARCHIVE_RECORDING_POSITION_TYPE_ID != type_id)
    {
        return AERON_NULL_COUNTER_ID;
    }

    struct aeron_archive_recording_pos_key_defn *key;

    if (aeron_counters_reader_metadata_key(counters_reader, counter_id, (uint8_t **)&key) < 0)
    {
        return AERON_NULL_COUNTER_ID;
    }

    return key->recording_id;
}

int aeron_archive_recording_pos_get_source_identity(aeron_counters_reader_t *counters_reader, int32_t counter_id, const char *dst, size_t *len_p)
{
    int32_t state, type_id;
    struct aeron_archive_recording_pos_key_defn *key;

    if (aeron_counters_reader_counter_state(counters_reader, counter_id, &state) < 0 ||
        aeron_counters_reader_counter_type_id(counters_reader, counter_id, &type_id) < 0 ||
        aeron_counters_reader_metadata_key(counters_reader, counter_id, (uint8_t **)&key) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    size_t len = 0;

    if (AERON_COUNTER_RECORD_ALLOCATED == state &&
        AERON_ARCHIVE_RECORDING_POSITION_TYPE_ID == type_id)
    {
        const int32_t source_identity_length = key->source_identity_length;
        if (source_identity_length < 0)
        {
            AERON_SET_ERR(
                EINVAL,
                "invalid source identity length %" PRId32 " for counter id %" PRId32,
                source_identity_length,
                counter_id);
            return -1;
        }

        // the source identity is followed by the archive id in the key, as written by RecordingPos.allocate
        const size_t max_source_identity_length =
            AERON_COUNTER_MAX_KEY_LENGTH - sizeof(struct aeron_archive_recording_pos_key_defn) - sizeof(int64_t);

        // use the shortest of the stored length, the space in the key and the caller's buffer
        len = (size_t)source_identity_length;
        len = len < max_source_identity_length ? len : max_source_identity_length;
        len = len < *len_p ? len : *len_p;

        // the source_identity string comes right after the key definition
        memcpy((void *)dst, ((uint8_t *)key + sizeof(struct aeron_archive_recording_pos_key_defn)), len);
    }

    *len_p = len;

    return 0;
}

int aeron_archive_recording_pos_is_active(bool *is_active, aeron_counters_reader_t *counters_reader, int32_t counter_id, int64_t recording_id)
{
    int32_t state, type_id;
    struct aeron_archive_recording_pos_key_defn *key;

    if (aeron_counters_reader_counter_state(counters_reader, counter_id, &state) < 0 ||
        aeron_counters_reader_counter_type_id(counters_reader, counter_id, &type_id) < 0 ||
        aeron_counters_reader_metadata_key(counters_reader, counter_id, (uint8_t **)&key) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    *is_active = AERON_COUNTER_RECORD_ALLOCATED == state &&
        AERON_ARCHIVE_RECORDING_POSITION_TYPE_ID == type_id &&
        key->recording_id == recording_id;

    return 0;
}
