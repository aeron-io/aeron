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

#include "aeron_cluster_service_snapshot_taker.h"
#include "util/aeron_error.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/snapshotMarker.h"
#include "aeron_cluster_client/clientSession.h"

#define SNAPSHOT_BUFFER_LENGTH (64 * 1024)

static int snapshot_offer(aeron_exclusive_publication_t *pub,
                           uint8_t *buffer, size_t length)
{
    int64_t result;
    /* Block-offer: retry until the publication accepts or closes */
    do
    {
        result = aeron_exclusive_publication_offer(pub, buffer, length, NULL, NULL);
    }
    while (AERON_PUBLICATION_BACK_PRESSURED == result ||
           AERON_PUBLICATION_ADMIN_ACTION  == result);

    if (result < 0)
    {
        AERON_SET_ERR(-1, "snapshot offer failed: %lld", (long long)result);
        return -1;
    }
    return 0;
}

static int write_snapshot_marker(
    aeron_exclusive_publication_t *publication,
    int64_t type_id, int64_t log_position, int64_t leadership_term_id,
    int32_t index, int32_t app_version, int32_t mark)
{
    uint8_t buffer[SNAPSHOT_BUFFER_LENGTH];
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_snapshotMarker msg;

    if (NULL == aeron_cluster_client_snapshotMarker_wrap_and_apply_header(
        &msg, (char *)buffer, 0, sizeof(buffer), &hdr))
    {
        AERON_SET_ERR(-1, "%s", "failed to wrap snapshotMarker");
        return -1;
    }

    aeron_cluster_client_snapshotMarker_set_typeId(&msg, type_id);
    aeron_cluster_client_snapshotMarker_set_logPosition(&msg, log_position);
    aeron_cluster_client_snapshotMarker_set_leadershipTermId(&msg, leadership_term_id);
    aeron_cluster_client_snapshotMarker_set_index(&msg, index);
    aeron_cluster_client_snapshotMarker_set_mark(&msg, (enum aeron_cluster_client_snapshotMark)mark);
    aeron_cluster_client_snapshotMarker_set_appVersion(&msg, app_version);

    return snapshot_offer(publication, buffer,
        aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_snapshotMarker_encoded_length(&msg));
}

int aeron_cluster_service_snapshot_taker_mark_begin(
    aeron_exclusive_publication_t *publication,
    int64_t type_id, int64_t log_position, int64_t leadership_term_id,
    int32_t index, int32_t app_version)
{
    return write_snapshot_marker(publication, type_id, log_position,
        leadership_term_id, index, app_version, AERON_CLUSTER_SNAPSHOT_MARK_BEGIN);
}

int aeron_cluster_service_snapshot_taker_mark_end(
    aeron_exclusive_publication_t *publication,
    int64_t type_id, int64_t log_position, int64_t leadership_term_id,
    int32_t index, int32_t app_version)
{
    return write_snapshot_marker(publication, type_id, log_position,
        leadership_term_id, index, app_version, AERON_CLUSTER_SNAPSHOT_MARK_END);
}

int aeron_cluster_service_snapshot_taker_snapshot_session(
    aeron_exclusive_publication_t *publication,
    aeron_cluster_client_session_t *session)
{
    uint8_t buffer[SNAPSHOT_BUFFER_LENGTH];
    struct aeron_cluster_client_messageHeader hdr;
    struct aeron_cluster_client_clientSession msg;

    if (NULL == aeron_cluster_client_clientSession_wrap_and_apply_header(
        &msg, (char *)buffer, 0, sizeof(buffer), &hdr))
    {
        AERON_SET_ERR(-1, "%s", "failed to wrap clientSession snapshot");
        return -1;
    }

    aeron_cluster_client_clientSession_set_clusterSessionId(&msg, session->cluster_session_id);
    aeron_cluster_client_clientSession_set_responseStreamId(&msg, session->response_stream_id);

    const char *ch = session->response_channel != NULL ? session->response_channel : "";
    aeron_cluster_client_clientSession_put_responseChannel(&msg, ch, (uint32_t)strlen(ch));

    const uint8_t *principal = session->encoded_principal;
    const uint32_t plen = (uint32_t)session->encoded_principal_length;
    aeron_cluster_client_clientSession_put_encodedPrincipal(
        &msg, (const char *)(principal != NULL ? principal : (const uint8_t *)""), plen);

    return snapshot_offer(publication, buffer,
        aeron_cluster_client_messageHeader_encoded_length() + aeron_cluster_client_clientSession_encoded_length(&msg));
}
