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

#include "aeron_cluster_service_snapshot_loader.h"
#include "aeron_cluster_service_snapshot_taker.h"
#include "util/aeron_error.h"
#include "uri/aeron_uri.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/snapshotMarker.h"
#include "aeron_cluster_client/clientSession.h"

typedef struct
{
    aeron_cluster_snapshot_image_t             *snapshot;
    aeron_cluster_snapshot_session_loader_func_t on_session;
    void                                        *clientd;
    int                                          error;
}
loader_ctx_t;

static aeron_controlled_fragment_handler_action_t loader_on_fragment(
    void *clientd,
    const uint8_t *buffer,
    size_t length,
    aeron_header_t *header)
{
    loader_ctx_t *ctx = (loader_ctx_t *)clientd;

    if (length < aeron_cluster_client_messageHeader_encoded_length())
    {
        return AERON_ACTION_CONTINUE;
    }

    struct aeron_cluster_client_messageHeader hdr;
    if (NULL == aeron_cluster_client_messageHeader_wrap(
        &hdr, (char *)buffer, 0,
        aeron_cluster_client_messageHeader_sbe_schema_version(), length))
    {
        return AERON_ACTION_CONTINUE;
    }

    const uint64_t hdr_len    = aeron_cluster_client_messageHeader_encoded_length();
    const int32_t  template_id = (int32_t)aeron_cluster_client_messageHeader_templateId(&hdr);

    if (100 == template_id) /* SnapshotMarker */
    {
        struct aeron_cluster_client_snapshotMarker msg;
        if (NULL == aeron_cluster_client_snapshotMarker_wrap_for_decode(
            &msg, (char *)buffer, hdr_len,
            aeron_cluster_client_snapshotMarker_sbe_block_length(),
            aeron_cluster_client_snapshotMarker_sbe_schema_version(),
            length))
        {
            return AERON_ACTION_CONTINUE;
        }

        enum aeron_cluster_client_snapshotMark mark = 0;
        aeron_cluster_client_snapshotMarker_mark(&msg, &mark);
        if ((int32_t)mark == AERON_CLUSTER_SNAPSHOT_MARK_END)
        {
            ctx->snapshot->is_done = true;
            return AERON_ACTION_BREAK;
        }
        return AERON_ACTION_CONTINUE;
    }

    if (102 == template_id) /* ClientSession */
    {
        struct aeron_cluster_client_clientSession msg;
        if (NULL == aeron_cluster_client_clientSession_wrap_for_decode(
            &msg, (char *)buffer, hdr_len,
            aeron_cluster_client_clientSession_sbe_block_length(),
            aeron_cluster_client_clientSession_sbe_schema_version(),
            length))
        {
            return AERON_ACTION_CONTINUE;
        }

        int64_t session_id      = aeron_cluster_client_clientSession_clusterSessionId(&msg);
        int32_t response_stream = aeron_cluster_client_clientSession_responseStreamId(&msg);

        char response_channel[AERON_URI_MAX_LENGTH];
        uint32_t ch_len = aeron_cluster_client_clientSession_responseChannel_length(&msg);
        if (ch_len >= sizeof(response_channel)) { ch_len = sizeof(response_channel) - 1; }
        aeron_cluster_client_clientSession_get_responseChannel(&msg, response_channel, ch_len);
        response_channel[ch_len] = '\0';

        uint32_t principal_len = aeron_cluster_client_clientSession_encodedPrincipal_length(&msg);
        const char *principal_ptr = aeron_cluster_client_clientSession_encodedPrincipal(&msg);

        if (NULL != ctx->on_session)
        {
            ctx->on_session(ctx->clientd, session_id, response_stream,
                response_channel,
                (const uint8_t *)principal_ptr, principal_len);
        }

        return AERON_ACTION_COMMIT;
    }

    return AERON_ACTION_CONTINUE;
}

int aeron_cluster_service_snapshot_loader_load_sessions(
    aeron_cluster_snapshot_image_t *snapshot,
    aeron_cluster_snapshot_session_loader_func_t on_session,
    void *clientd)
{
    loader_ctx_t ctx = { snapshot, on_session, clientd, 0 };
    aeron_controlled_fragment_assembler_t *assembler = NULL;

    if (aeron_controlled_fragment_assembler_create(&assembler, loader_on_fragment, &ctx) < 0)
    {
        AERON_APPEND_ERR("%s", "unable to create assembler for snapshot loader");
        return -1;
    }

    while (!snapshot->is_done && !aeron_image_is_closed(snapshot->image))
    {
        aeron_image_controlled_poll(snapshot->image,
            aeron_controlled_fragment_assembler_handler, assembler, 10);
    }

    aeron_controlled_fragment_assembler_delete(assembler);
    return ctx.error;
}

bool aeron_cluster_snapshot_image_is_done(aeron_cluster_snapshot_image_t *snapshot)
{
    return snapshot->is_done;
}
