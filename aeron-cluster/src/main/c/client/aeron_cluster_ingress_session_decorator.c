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

#include "aeron_cluster_ingress_session_decorator.h"
#include "aeron_cluster_client.h"
#include "aeron_cluster_configuration.h"

#include "aeron_cluster_client/messageHeader.h"
#include "aeron_cluster_client/sessionMessageHeader.h"

#define DECORATOR_MAX_VECTORS 8

void aeron_cluster_ingress_session_decorator_init(
    aeron_cluster_ingress_session_decorator_t *decorator,
    aeron_publication_t *publication,
    int64_t cluster_session_id,
    int64_t leadership_term_id)
{
    decorator->publication        = publication;
    decorator->cluster_session_id = cluster_session_id;
    decorator->leadership_term_id = leadership_term_id;
}

void aeron_cluster_ingress_session_decorator_update_leadership_term_id(
    aeron_cluster_ingress_session_decorator_t *decorator,
    int64_t leadership_term_id)
{
    decorator->leadership_term_id = leadership_term_id;
}

static void decorator_fill_header(
    uint8_t *hdr_buf,
    int64_t leadership_term_id,
    int64_t cluster_session_id)
{
    struct aeron_cluster_client_messageHeader msg_hdr;
    struct aeron_cluster_client_sessionMessageHeader hdr;
    aeron_cluster_client_sessionMessageHeader_wrap_and_apply_header(
        &hdr, (char *)hdr_buf, 0, AERON_CLUSTER_SESSION_HEADER_LENGTH, &msg_hdr);
    aeron_cluster_client_sessionMessageHeader_set_leadershipTermId(&hdr, leadership_term_id);
    aeron_cluster_client_sessionMessageHeader_set_clusterSessionId(&hdr, cluster_session_id);
    aeron_cluster_client_sessionMessageHeader_set_timestamp(&hdr, 0);
}

int64_t aeron_cluster_ingress_session_decorator_offer(
    aeron_cluster_ingress_session_decorator_t *decorator,
    const uint8_t *buffer, size_t length)
{
    uint8_t hdr_buf[AERON_CLUSTER_SESSION_HEADER_LENGTH];
    decorator_fill_header(hdr_buf,
        decorator->leadership_term_id, decorator->cluster_session_id);

    aeron_iovec_t vectors[2];
    vectors[0].iov_base = hdr_buf;
    vectors[0].iov_len  = AERON_CLUSTER_SESSION_HEADER_LENGTH;
    vectors[1].iov_base = (uint8_t *)buffer;
    vectors[1].iov_len  = length;

    return aeron_publication_offerv(decorator->publication, vectors, 2, NULL, NULL);
}

int64_t aeron_cluster_ingress_session_decorator_offer_vectors(
    aeron_cluster_ingress_session_decorator_t *decorator,
    const aeron_iovec_t *user_vectors, size_t vector_count)
{
    if (vector_count > DECORATOR_MAX_VECTORS) { return AERON_PUBLICATION_ERROR; }

    uint8_t hdr_buf[AERON_CLUSTER_SESSION_HEADER_LENGTH];
    decorator_fill_header(hdr_buf,
        decorator->leadership_term_id, decorator->cluster_session_id);

    aeron_iovec_t vectors[1 + DECORATOR_MAX_VECTORS];
    vectors[0].iov_base = hdr_buf;
    vectors[0].iov_len  = AERON_CLUSTER_SESSION_HEADER_LENGTH;
    for (size_t i = 0; i < vector_count; i++) { vectors[1 + i] = user_vectors[i]; }

    return aeron_publication_offerv(
        decorator->publication, vectors, 1 + vector_count, NULL, NULL);
}
