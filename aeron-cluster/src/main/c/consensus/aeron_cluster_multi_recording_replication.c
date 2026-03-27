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

#include <string.h>
#include <errno.h>
#include <inttypes.h>

#include "aeron_cluster_multi_recording_replication.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

#define INITIAL_PENDING_CAPACITY  8
#define INITIAL_COMPLETED_CAPACITY 8

static int copy_channel(char *dst, size_t dst_len, const char *src)
{
    if (NULL == src || '\0' == src[0])
    {
        dst[0] = '\0';
        return 0;
    }
    size_t src_len = strlen(src);
    if (src_len >= dst_len)
    {
        AERON_SET_ERR(EINVAL, "channel too long: %zu", src_len);
        return -1;
    }
    memcpy(dst, src, src_len + 1);
    return 0;
}

int aeron_cluster_multi_recording_replication_create(
    aeron_cluster_multi_recording_replication_t **multi,
    aeron_archive_t *archive,
    aeron_t *aeron,
    int32_t src_control_stream_id,
    const char *src_control_channel,
    const char *replication_channel,
    const char *src_response_channel,
    int64_t progress_timeout_ns,
    int64_t progress_interval_ns)
{
    aeron_cluster_multi_recording_replication_t *m = NULL;
    if (aeron_alloc((void **)&m, sizeof(aeron_cluster_multi_recording_replication_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate multi recording replication");
        return -1;
    }

    m->archive                 = archive;
    m->aeron                   = aeron;
    m->src_control_stream_id   = src_control_stream_id;
    m->progress_timeout_ns     = progress_timeout_ns;
    m->progress_interval_ns    = progress_interval_ns;
    m->recording_cursor        = 0;
    m->recording_replication   = NULL;
    m->on_replication_ended    = NULL;
    m->event_listener_clientd  = NULL;

    if (copy_channel(m->src_control_channel, sizeof(m->src_control_channel), src_control_channel) < 0 ||
        copy_channel(m->replication_channel,  sizeof(m->replication_channel),  replication_channel)  < 0 ||
        copy_channel(m->src_response_channel, sizeof(m->src_response_channel), src_response_channel) < 0)
    {
        aeron_free(m);
        return -1;
    }

    if (aeron_alloc(
        (void **)&m->recordings_pending,
        sizeof(aeron_cluster_recording_info_t) * INITIAL_PENDING_CAPACITY) < 0)
    {
        aeron_free(m);
        AERON_APPEND_ERR("%s", "unable to allocate pending recordings array");
        return -1;
    }
    m->recordings_pending_capacity = INITIAL_PENDING_CAPACITY;
    m->recordings_pending_count    = 0;

    if (aeron_alloc(
        (void **)&m->recordings_completed,
        sizeof(aeron_cluster_recording_completed_t) * INITIAL_COMPLETED_CAPACITY) < 0)
    {
        aeron_free(m->recordings_pending);
        aeron_free(m);
        AERON_APPEND_ERR("%s", "unable to allocate completed recordings array");
        return -1;
    }
    m->recordings_completed_capacity = INITIAL_COMPLETED_CAPACITY;
    m->recordings_completed_count    = 0;

    *multi = m;
    return 0;
}

int aeron_cluster_multi_recording_replication_close(
    aeron_cluster_multi_recording_replication_t *multi)
{
    if (NULL == multi)
    {
        return 0;
    }

    aeron_cluster_recording_replication_close(multi->recording_replication);
    multi->recording_replication = NULL;

    aeron_free(multi->recordings_pending);
    aeron_free(multi->recordings_completed);
    aeron_free(multi);
    return 0;
}

int aeron_cluster_multi_recording_replication_add_recording(
    aeron_cluster_multi_recording_replication_t *multi,
    int64_t src_recording_id,
    int64_t dst_recording_id,
    int64_t stop_position)
{
    if (multi->recordings_pending_count >= multi->recordings_pending_capacity)
    {
        int new_cap = multi->recordings_pending_capacity * 2;
        if (aeron_reallocf(
            (void **)&multi->recordings_pending,
            sizeof(aeron_cluster_recording_info_t) * (size_t)new_cap) < 0)
        {
            AERON_APPEND_ERR("%s", "unable to grow pending recordings array");
            return -1;
        }
        multi->recordings_pending_capacity = new_cap;
    }

    aeron_cluster_recording_info_t *info =
        &multi->recordings_pending[multi->recordings_pending_count++];
    info->src_recording_id = src_recording_id;
    info->dst_recording_id = dst_recording_id;
    info->stop_position    = stop_position;
    return 0;
}

static int replicate_current(
    aeron_cluster_multi_recording_replication_t *m, int64_t now_ns)
{
    const aeron_cluster_recording_info_t *info =
        &m->recordings_pending[m->recording_cursor];

    aeron_archive_replication_params_t params;
    aeron_archive_replication_params_init(&params);
    params.dst_recording_id      = info->dst_recording_id;
    params.stop_position         = info->stop_position;
    params.replication_channel   = m->replication_channel[0]   ? m->replication_channel   : NULL;
    params.src_response_channel  = m->src_response_channel[0]  ? m->src_response_channel  : NULL;
    params.replication_session_id = (int32_t)aeron_next_correlation_id(m->aeron);

    return aeron_cluster_recording_replication_create(
        &m->recording_replication,
        m->archive,
        m->aeron,
        info->src_recording_id,
        m->src_control_channel,
        m->src_control_stream_id,
        &params,
        m->progress_timeout_ns,
        m->progress_interval_ns,
        now_ns);
}

static int push_completed(
    aeron_cluster_multi_recording_replication_t *m,
    int64_t src_recording_id,
    int64_t dst_recording_id)
{
    if (m->recordings_completed_count >= m->recordings_completed_capacity)
    {
        int new_cap = m->recordings_completed_capacity * 2;
        if (aeron_reallocf(
            (void **)&m->recordings_completed,
            sizeof(aeron_cluster_recording_completed_t) * (size_t)new_cap) < 0)
        {
            AERON_APPEND_ERR("%s", "unable to grow completed recordings array");
            return -1;
        }
        m->recordings_completed_capacity = new_cap;
    }
    aeron_cluster_recording_completed_t *entry =
        &m->recordings_completed[m->recordings_completed_count++];
    entry->src_recording_id = src_recording_id;
    entry->dst_recording_id = dst_recording_id;
    return 0;
}

int aeron_cluster_multi_recording_replication_poll(
    aeron_cluster_multi_recording_replication_t *multi, int64_t now_ns)
{
    if (aeron_cluster_multi_recording_replication_is_complete(multi))
    {
        return 0;
    }

    int work_count = 0;

    if (NULL == multi->recording_replication)
    {
        if (replicate_current(multi, now_ns) < 0)
        {
            return -1;
        }
        work_count++;
    }
    else
    {
        int rc = aeron_cluster_recording_replication_poll(multi->recording_replication, now_ns);
        if (rc < 0)
        {
            /* Fatal error already set in recording_replication_poll */
            return -1;
        }

        if (aeron_cluster_recording_replication_has_replication_ended(multi->recording_replication))
        {
            const aeron_cluster_recording_info_t *pending =
                &multi->recordings_pending[multi->recording_cursor];

            int64_t dst_rec_id  = aeron_cluster_recording_replication_recording_id(multi->recording_replication);
            int64_t pos         = aeron_cluster_recording_replication_position(multi->recording_replication);
            bool    has_synced  = aeron_cluster_recording_replication_has_synced(multi->recording_replication);

            /* Notify listener */
            if (NULL != multi->on_replication_ended)
            {
                multi->on_replication_ended(
                    multi->src_control_channel,
                    pending->src_recording_id,
                    dst_rec_id,
                    pos,
                    has_synced,
                    multi->event_listener_clientd);
            }

            aeron_cluster_recording_replication_close(multi->recording_replication);
            multi->recording_replication = NULL;

            if (has_synced)
            {
                if (push_completed(multi, pending->src_recording_id, dst_rec_id) < 0)
                {
                    return -1;
                }
                multi->recording_cursor++;
            }
            else
            {
                /* Retry */
                if (replicate_current(multi, now_ns) < 0)
                {
                    return -1;
                }
            }

            work_count++;
        }
    }

    return work_count;
}

void aeron_cluster_multi_recording_replication_on_signal(
    aeron_cluster_multi_recording_replication_t *multi,
    const aeron_archive_recording_signal_t *signal)
{
    if (NULL != multi->recording_replication)
    {
        aeron_cluster_recording_replication_on_signal(multi->recording_replication, signal);
    }
}

bool aeron_cluster_multi_recording_replication_is_complete(
    const aeron_cluster_multi_recording_replication_t *multi)
{
    return multi->recording_cursor >= multi->recordings_pending_count;
}

int64_t aeron_cluster_multi_recording_replication_completed_dst(
    const aeron_cluster_multi_recording_replication_t *multi,
    int64_t src_recording_id)
{
    for (int i = 0; i < multi->recordings_completed_count; i++)
    {
        if (multi->recordings_completed[i].src_recording_id == src_recording_id)
        {
            return multi->recordings_completed[i].dst_recording_id;
        }
    }
    return AERON_NULL_VALUE;
}

int64_t aeron_cluster_multi_recording_replication_current_replication_id(
    const aeron_cluster_multi_recording_replication_t *multi)
{
    if (NULL != multi->recording_replication)
    {
        return aeron_cluster_recording_replication_replication_id(multi->recording_replication);
    }
    return AERON_NULL_VALUE;
}

int64_t aeron_cluster_multi_recording_replication_current_src_recording_id(
    const aeron_cluster_multi_recording_replication_t *multi)
{
    if (multi->recording_cursor >= 0 && multi->recording_cursor < multi->recordings_pending_count)
    {
        return multi->recordings_pending[multi->recording_cursor].src_recording_id;
    }
    return AERON_NULL_VALUE;
}

void aeron_cluster_multi_recording_replication_set_event_listener(
    aeron_cluster_multi_recording_replication_t *multi,
    aeron_cluster_multi_recording_replication_event_func_t on_replication_ended,
    void *clientd)
{
    multi->on_replication_ended   = on_replication_ended;
    multi->event_listener_clientd = clientd;
}
