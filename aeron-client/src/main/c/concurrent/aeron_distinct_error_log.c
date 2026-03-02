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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#endif

#include <string.h>
#include <stdio.h>
#include <errno.h>
#include "util/aeron_error.h"
#include "util/aeron_strutil.h"
#include "aeron_alloc.h"
#include "aeron_distinct_error_log.h"

int aeron_distinct_error_log_init(
    aeron_distinct_error_log_t *log, uint8_t *buffer, size_t buffer_size, aeron_clock_func_t clock)
{
    if (NULL == log || NULL == clock)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters can not be null, log: %s, clock: %s",
            NULL == log ? "NULL" : "OK",
            NULL == clock ? "NULL" : "OK");
        return -1;
    }

    if (aeron_alloc((void **)&log->observation_list, sizeof(aeron_distinct_error_log_observation_list_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate distinct error log");
        return -1;
    }

    log->buffer = buffer;
    log->buffer_capacity = buffer_size;
    log->clock = clock;
    log->next_offset = 0;
    log->observation_list->num_observations = 0;
    log->observation_list->observations = NULL;
    aeron_mutex_init(&log->mutex);

    return 0;
}

void aeron_distinct_error_log_close(aeron_distinct_error_log_t *log)
{
    aeron_mutex_lock(&log->mutex);

    aeron_distinct_error_log_observation_list_t *list = log->observation_list;
    aeron_distinct_observation_t *observations = list->observations;
    size_t num_observations = (size_t)list->num_observations;

    for (size_t i = 0; i < num_observations; i++)
    {
        aeron_free((void *)observations[i].description);
    }

    aeron_free((void *)log->observation_list);

    aeron_mutex_unlock(&log->mutex);
    aeron_mutex_destroy(&log->mutex);
}

static aeron_distinct_observation_t *aeron_distinct_error_log_find_observation(
    aeron_distinct_observation_t *observations, size_t num_observations, int error_code, const char *description)
{
    for (size_t i = 0; i < num_observations; i++)
    {
        if (observations[i].error_code == error_code &&
            strncmp(observations[i].description, description, observations[i].description_length) == 0)
        {
            return &observations[i];
        }
    }

    return NULL;
}

static int aeron_distinct_error_log_observation_list_alloc(
    aeron_distinct_error_log_observation_list_t **list, uint64_t num_observations)
{
    *list = NULL;
    size_t alloc_length =
        sizeof(aeron_distinct_error_log_observation_list_t) +
        ((size_t)num_observations * sizeof(aeron_distinct_observation_t));

    int result = aeron_alloc((void **)list, alloc_length);
    if (result >= 0)
    {
        (*list)->observations =
            (aeron_distinct_observation_t *)((uint8_t *)*list + sizeof(aeron_distinct_error_log_observation_list_t));
        (*list)->num_observations = num_observations;
    }

    return result;
}

static aeron_distinct_observation_t *aeron_distinct_error_log_new_observation(
    aeron_distinct_error_log_t *log, int64_t timestamp, int error_code, const char *description)
{
    aeron_distinct_error_log_observation_list_t *list = log->observation_list;
    size_t num_observations = (size_t)list->num_observations;
    aeron_distinct_observation_t *observations = list->observations;

    size_t description_length = strlen(description);
    size_t length = AERON_ERROR_LOG_HEADER_LENGTH + description_length;
    aeron_distinct_error_log_observation_list_t *new_list = NULL;
    char *new_description = NULL;
    size_t offset = log->next_offset;
    aeron_error_log_entry_t *entry = (aeron_error_log_entry_t *)(log->buffer + offset);

    if ((offset + length) > log->buffer_capacity ||
        aeron_distinct_error_log_observation_list_alloc(&new_list, num_observations + 1) ||
        aeron_alloc((void **)&new_description, description_length + 1) < 0)
    {
        return NULL;
    }

    memcpy(log->buffer + offset + AERON_ERROR_LOG_HEADER_LENGTH, description, description_length);
    entry->first_observation_timestamp = timestamp;
    entry->observation_count = 0;

    log->next_offset = AERON_ALIGN(offset + length, AERON_ERROR_LOG_RECORD_ALIGNMENT);

    aeron_distinct_observation_t *new_array = new_list->observations;

    new_array[0].error_code = error_code;
    new_array[0].description = new_description;
    strncpy(new_description, description, description_length + 1);
    new_array[0].description_length = description_length;
    new_array[0].offset = offset;

    if (num_observations != 0)
    {
        memcpy(&new_array[1], observations, sizeof(aeron_distinct_observation_t) * num_observations);
    }

    log->observation_list = new_list;
    aeron_free(list);

    AERON_SET_RELEASE(entry->length, (int32_t)length);

    return &new_array[0];
}

int aeron_distinct_error_log_record(aeron_distinct_error_log_t *log, int error_code, const char *description)
{
    if (NULL == log)
    {
        AERON_SET_ERR(EINVAL, "%s", "log is null");
        return -1;
    }

    aeron_mutex_lock(&log->mutex);

    int64_t timestamp = log->clock();
    aeron_distinct_error_log_observation_list_t *list = log->observation_list;
    size_t num_observations = (size_t)list->num_observations;
    aeron_distinct_observation_t *observation = aeron_distinct_error_log_find_observation(
        list->observations, num_observations, error_code, description);

    if (NULL == observation)
    {
        observation = aeron_distinct_error_log_new_observation(log, timestamp, error_code, description);

        if (NULL == observation)
        {
            aeron_mutex_unlock(&log->mutex);

            char buffer[AERON_ERROR_MAX_TOTAL_LENGTH];

            aeron_format_date(buffer, sizeof(buffer), timestamp);
            fprintf(stderr, "%s - unrecordable error %s\n", buffer, description);
            aeron_set_errno(ENOMEM);
            return -1;
        }
    }

    size_t entry_offset = observation->offset;
    aeron_mutex_unlock(&log->mutex);

    aeron_error_log_entry_t *entry = (aeron_error_log_entry_t *)(log->buffer + entry_offset);
#if defined(__clang__) && defined(AERON_CPU_ARM)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-but-set-variable"
#endif
    int32_t dest;
    AERON_GET_AND_ADD_INT32(dest, entry->observation_count, 1);
    AERON_SET_RELEASE(entry->last_observation_timestamp, timestamp);

    return 0;
}
#if defined(__clang__) && defined(AERON_CPU_ARM)
#pragma clang diagnostic pop
#endif

bool aeron_error_log_exists(const uint8_t *buffer, size_t buffer_size)
{
    aeron_error_log_entry_t *entry = (aeron_error_log_entry_t *)buffer;
    int32_t length;

    AERON_GET_ACQUIRE(length, entry->length);

    return 0 != length;
}

size_t aeron_error_log_read(
    const uint8_t *buffer,
    size_t buffer_size,
    aeron_error_log_reader_func_t reader,
    void *clientd,
    int64_t since_timestamp)
{
    size_t entries = 0;
    size_t offset = 0;

    while (offset < buffer_size)
    {
        aeron_error_log_entry_t *entry = (aeron_error_log_entry_t *)(buffer + offset);
        int32_t length;

        AERON_GET_ACQUIRE(length, entry->length);
        if (0 == length)
        {
            break;
        }

        int64_t last_observation_timestamp;
        AERON_GET_ACQUIRE(last_observation_timestamp, entry->last_observation_timestamp);

        if (last_observation_timestamp >= since_timestamp)
        {
            ++entries;

            reader(
                entry->observation_count,
                entry->first_observation_timestamp,
                last_observation_timestamp,
                (const char *)(buffer + offset + AERON_ERROR_LOG_HEADER_LENGTH),
                length - AERON_ERROR_LOG_HEADER_LENGTH,
                clientd);
        }

        offset += AERON_ALIGN(length, AERON_ERROR_LOG_RECORD_ALIGNMENT);
    }

    return entries;
}

size_t aeron_distinct_error_log_num_observations(aeron_distinct_error_log_t *log)
{
    aeron_mutex_lock(&log->mutex);

    aeron_distinct_error_log_observation_list_t *list = log->observation_list;
    size_t result = (size_t)list->num_observations;

    aeron_mutex_unlock(&log->mutex);

    return result;
}
