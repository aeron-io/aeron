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

#include "aeron_cluster_recording_replication.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

int aeron_cluster_recording_replication_create(
    aeron_cluster_recording_replication_t **replication,
    aeron_archive_t *archive,
    aeron_t *aeron,
    int64_t src_recording_id,
    const char *src_control_channel,
    int32_t src_control_stream_id,
    aeron_archive_replication_params_t *params,
    int64_t progress_check_timeout_ns,
    int64_t progress_check_interval_ns,
    int64_t now_ns)
{
    aeron_cluster_recording_replication_t *r = NULL;
    if (aeron_alloc((void **)&r, sizeof(aeron_cluster_recording_replication_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate recording replication");
        return -1;
    }

    r->archive                      = archive;
    r->aeron                        = aeron;
    r->stop_position                = params->stop_position;
    r->progress_check_timeout_ns    = progress_check_timeout_ns;
    r->progress_check_interval_ns   = progress_check_interval_ns;
    r->progress_deadline_ns         = now_ns + progress_check_timeout_ns;
    r->progress_check_deadline_ns   = now_ns + progress_check_interval_ns;
    r->recording_position_counter_id = AERON_NULL_COUNTER_ID;
    r->recording_id                 = AERON_NULL_VALUE;
    r->position                     = AERON_NULL_VALUE;
    r->last_recording_signal        = AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_NULL_VALUE;
    r->has_replication_ended        = false;
    r->has_synced                   = false;
    r->has_stopped                  = false;

    size_t chan_len = strlen(src_control_channel);
    if (chan_len >= sizeof(r->src_archive_channel))
    {
        AERON_SET_ERR(EINVAL, "src_control_channel too long: %zu", chan_len);
        aeron_free(r);
        return -1;
    }
    memcpy(r->src_archive_channel, src_control_channel, chan_len + 1);

    if (aeron_archive_replicate(
        &r->replication_id,
        archive,
        src_recording_id,
        src_control_channel,
        src_control_stream_id,
        params) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to start recording replication");
        aeron_free(r);
        return -1;
    }

    *replication = r;
    return 0;
}

int aeron_cluster_recording_replication_close(aeron_cluster_recording_replication_t *replication)
{
    if (NULL == replication)
    {
        return 0;
    }

    if (!replication->has_replication_ended)
    {
        replication->has_replication_ended = true;
        bool stopped = false;
        if (aeron_archive_try_stop_replication(&stopped, replication->archive, replication->replication_id) < 0)
        {
            /* best-effort — log but don't fail */
            AERON_APPEND_ERR("%" PRId64, replication->replication_id);
        }
    }

    aeron_free(replication);
    return 0;
}

static bool poll_dst_recording_position(aeron_cluster_recording_replication_t *r)
{
    if (AERON_NULL_COUNTER_ID == r->recording_position_counter_id)
    {
        return false;
    }

    aeron_counters_reader_t *counters = aeron_counters_reader(r->aeron);
    if (NULL == counters)
    {
        return false;
    }

    int64_t *addr = aeron_counters_reader_addr(counters, r->recording_position_counter_id);
    if (NULL == addr)
    {
        return false;
    }

    int64_t recording_position = *addr;

    bool is_active = false;
    if (aeron_archive_recording_pos_is_active(
        &is_active, counters, r->recording_position_counter_id, r->recording_id) < 0)
    {
        return false;
    }

    if (is_active && recording_position > r->position)
    {
        r->position = recording_position;
        return true;
    }

    return false;
}

int aeron_cluster_recording_replication_poll(
    aeron_cluster_recording_replication_t *replication, int64_t now_ns)
{
    if (replication->has_replication_ended)
    {
        return 0;
    }

    int work_count = 0;

    if (now_ns >= replication->progress_check_deadline_ns)
    {
        replication->progress_check_deadline_ns = now_ns + replication->progress_check_interval_ns;
        if (poll_dst_recording_position(replication))
        {
            replication->progress_deadline_ns = now_ns + replication->progress_check_timeout_ns;
        }
        work_count++;
    }

    if (now_ns >= replication->progress_deadline_ns)
    {
        if (!replication->has_replication_ended)
        {
            replication->has_replication_ended = true;
            bool stopped = false;
            aeron_archive_try_stop_replication(&stopped, replication->archive, replication->replication_id);
        }

        if (AERON_NULL_VALUE == replication->stop_position ||
            replication->position < replication->stop_position)
        {
            AERON_SET_ERR(EINVAL,
                "log replication has not progressed: replicationId=%" PRId64 " position=%" PRId64,
                replication->replication_id, replication->position);
        }
        else
        {
            AERON_SET_ERR(EINVAL,
                "log replication failed to stop: replicationId=%" PRId64,
                replication->replication_id);
        }
        return -1;
    }

    return work_count;
}

void aeron_cluster_recording_replication_on_signal(
    aeron_cluster_recording_replication_t *replication,
    const aeron_archive_recording_signal_t *signal)
{
    if (signal->control_session_id != replication->replication_id)
    {
        return;
    }

    const int32_t code = signal->recording_signal_code;

    if (AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_EXTEND == (aeron_archive_client_recording_signal_t)code)
    {
        aeron_counters_reader_t *counters = aeron_counters_reader(replication->aeron);
        if (NULL != counters)
        {
            replication->recording_position_counter_id =
                aeron_archive_recording_pos_find_counter_id_by_recording_id(counters, signal->recording_id);
        }
    }
    else if (AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_SYNC == (aeron_archive_client_recording_signal_t)code)
    {
        replication->has_synced = true;
    }
    else if (AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_REPLICATE_END == (aeron_archive_client_recording_signal_t)code)
    {
        replication->has_replication_ended = true;
    }
    else if (AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_STOP == (aeron_archive_client_recording_signal_t)code)
    {
        if (AERON_NULL_VALUE != signal->position)
        {
            replication->position = signal->position;
        }
        replication->has_stopped = true;
    }
    else if (AERON_ARCHIVE_CLIENT_RECORDING_SIGNAL_DELETE == (aeron_archive_client_recording_signal_t)code)
    {
        AERON_SET_ERR(EINVAL,
            "recording was deleted during replication: replicationId=%" PRId64 " recordingId=%" PRId64,
            replication->replication_id, signal->recording_id);
        replication->has_replication_ended = true;
    }

    replication->last_recording_signal = code;

    if (AERON_NULL_VALUE != signal->recording_id)
    {
        replication->recording_id = signal->recording_id;
    }

    if (AERON_NULL_VALUE != signal->position)
    {
        replication->position = signal->position;
    }
}

/* --- Accessors --- */

int64_t aeron_cluster_recording_replication_replication_id(
    const aeron_cluster_recording_replication_t *replication)
{
    return replication->replication_id;
}

int64_t aeron_cluster_recording_replication_position(
    const aeron_cluster_recording_replication_t *replication)
{
    return replication->position;
}

int64_t aeron_cluster_recording_replication_recording_id(
    const aeron_cluster_recording_replication_t *replication)
{
    return replication->recording_id;
}

bool aeron_cluster_recording_replication_has_replication_ended(
    const aeron_cluster_recording_replication_t *replication)
{
    return replication->has_replication_ended;
}

bool aeron_cluster_recording_replication_has_synced(
    const aeron_cluster_recording_replication_t *replication)
{
    return replication->has_synced;
}

bool aeron_cluster_recording_replication_has_stopped(
    const aeron_cluster_recording_replication_t *replication)
{
    return replication->has_stopped;
}

const char *aeron_cluster_recording_replication_src_archive_channel(
    const aeron_cluster_recording_replication_t *replication)
{
    return replication->src_archive_channel;
}
