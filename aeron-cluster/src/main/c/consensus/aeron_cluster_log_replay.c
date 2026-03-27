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

#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>

#include "aeron_cluster_log_replay.h"

#include "aeron_alloc.h"
#include "util/aeron_error.h"

/* Build "channel?sessionId=N" or "channel|sessionId=N" depending on whether
 * the base channel already contains query parameters. */
static int build_session_channel(
    char *out, size_t out_len, const char *base_channel, int32_t session_id)
{
    /* If the channel has no '?' yet, use '?' separator, else '|' */
    const char sep = (strchr(base_channel, '?') == NULL) ? '?' : '|';
    int n = snprintf(out, out_len, "%s%csessionId=%d", base_channel, sep, session_id);
    if (n < 0 || (size_t)n >= out_len)
    {
        AERON_SET_ERR(EINVAL, "replay channel too long: %s", base_channel);
        return -1;
    }
    return 0;
}

int aeron_cluster_log_replay_create(
    aeron_cluster_log_replay_t **replay,
    aeron_archive_t *archive,
    aeron_t *aeron,
    int64_t recording_id,
    int64_t start_position,
    int64_t stop_position,
    const char *replay_channel,
    int32_t replay_stream_id,
    aeron_cluster_log_adapter_t *log_adapter)
{
    aeron_cluster_log_replay_t *r = NULL;
    if (aeron_alloc((void **)&r, sizeof(aeron_cluster_log_replay_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to allocate log replay");
        return -1;
    }

    r->archive           = archive;
    r->aeron             = aeron;
    r->log_adapter       = log_adapter;
    r->log_subscription  = NULL;
    r->async_subscription = NULL;
    r->start_position    = start_position;
    r->stop_position     = stop_position;
    r->replay_session_id = -1;
    r->log_session_id    = -1;

    /* Start the archive replay */
    aeron_archive_replay_params_t params;
    aeron_archive_replay_params_init(&params);
    params.position = start_position;
    params.length   = stop_position - start_position;

    if (aeron_archive_start_replay(
        &r->replay_session_id, archive, recording_id,
        replay_channel, replay_stream_id, &params) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to start archive replay");
        aeron_free(r);
        return -1;
    }

    r->log_session_id = (int32_t)r->replay_session_id;

    /* Build a session-filtered channel so we only receive the replay stream */
    char session_channel[512];
    if (build_session_channel(
        session_channel, sizeof(session_channel), replay_channel, r->log_session_id) < 0)
    {
        aeron_archive_stop_replay(archive, r->replay_session_id);
        aeron_free(r);
        return -1;
    }

    /* Start async subscription creation */
    if (aeron_async_add_subscription(
        &r->async_subscription, aeron,
        session_channel, replay_stream_id,
        NULL, NULL, NULL, NULL) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to start subscription for log replay");
        aeron_archive_stop_replay(archive, r->replay_session_id);
        aeron_free(r);
        return -1;
    }

    *replay = r;
    return 0;
}

int aeron_cluster_log_replay_close(aeron_cluster_log_replay_t *replay)
{
    if (NULL == replay)
    {
        return 0;
    }

    /* Stop the archive replay (best-effort) */
    if (replay->replay_session_id >= 0)
    {
        aeron_archive_stop_replay(replay->archive, replay->replay_session_id);
    }

    /* Disconnect the log adapter (saves position, closes subscription) */
    aeron_cluster_log_adapter_disconnect(replay->log_adapter);

    /* Close the subscription if it was created but adapter didn't own it */
    if (NULL != replay->log_subscription)
    {
        aeron_subscription_close(replay->log_subscription, NULL, NULL);
        replay->log_subscription = NULL;
    }

    aeron_free(replay);
    return 0;
}

int aeron_cluster_log_replay_do_work(aeron_cluster_log_replay_t *replay)
{
    int work_count = 0;

    /* Phase 1: poll subscription creation */
    if (NULL == replay->log_subscription)
    {
        if (NULL != replay->async_subscription)
        {
            int rc = aeron_async_add_subscription_poll(
                &replay->log_subscription, replay->async_subscription);
            if (rc < 0)
            {
                AERON_APPEND_ERR("%s", "log replay subscription creation failed");
                return -1;
            }
            if (NULL != replay->log_subscription)
            {
                /* Subscription ready — update log_adapter's subscription reference */
                replay->async_subscription = NULL;
                work_count++;
            }
        }
        return work_count;
    }

    /* Phase 2: wait for image to join */
    if (aeron_cluster_log_adapter_is_image_closed(replay->log_adapter) &&
        aeron_cluster_log_adapter_position(replay->log_adapter) < replay->start_position)
    {
        aeron_image_t *image = aeron_subscription_image_by_session_id(
            replay->log_subscription, replay->log_session_id);

        if (NULL != image)
        {
            /* Validate join position matches start */
            aeron_image_constants_t consts;
            if (aeron_image_constants(image, &consts) == 0 &&
                consts.join_position != replay->start_position)
            {
                AERON_SET_ERR(EINVAL,
                    "log replay joinPosition=%" PRId64 " expected startPosition=%" PRId64,
                    consts.join_position, replay->start_position);
                return -1;
            }

            aeron_cluster_log_adapter_set_image(
                replay->log_adapter, image, replay->log_subscription);
            work_count++;
        }
        return work_count;
    }

    /* Phase 3: drive log adapter polling up to stop_position */
    int polled = aeron_cluster_log_adapter_poll(replay->log_adapter, replay->stop_position);
    if (polled < 0)
    {
        AERON_APPEND_ERR("%s", "log replay poll failed");
        return -1;
    }
    work_count += polled;

    return work_count;
}

bool aeron_cluster_log_replay_is_done(aeron_cluster_log_replay_t *replay)
{
    return aeron_cluster_log_adapter_position(replay->log_adapter) >= replay->stop_position;
}

int64_t aeron_cluster_log_replay_position(aeron_cluster_log_replay_t *replay)
{
    return aeron_cluster_log_adapter_position(replay->log_adapter);
}
