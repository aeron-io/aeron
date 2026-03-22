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

#ifndef AERON_CLUSTER_EGRESS_POLLER_H
#define AERON_CLUSTER_EGRESS_POLLER_H

#include "aeron_cluster.h"
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif


#define AERON_CLUSTER_EGRESS_POLLER_FRAGMENT_LIMIT_DEFAULT 10

typedef struct aeron_cluster_egress_poller_stct
{
    aeron_subscription_t              *subscription;
    int                                fragment_limit;
    aeron_controlled_fragment_assembler_t *fragment_assembler;

    /* last decoded state — read by async_connect to drive its state machine */
    int32_t  template_id;
    int64_t  cluster_session_id;
    int64_t  leadership_term_id;
    int64_t  correlation_id;
    int32_t  leader_member_id;
    int32_t  event_code;       /* aeron_cluster_client_eventCode enum as int */

    char    *detail;           /* heap buffer, grown as needed */
    uint32_t detail_malloced_len;
    uint32_t detail_length;

    char    *encoded_challenge_buffer;
    uint32_t encoded_challenge_malloced_len;
    aeron_cluster_encoded_credentials_t encoded_challenge;

    bool is_poll_complete;
    bool was_challenged;

    /* context callbacks — set by aeron_cluster_egress_poller_set_context */
    aeron_cluster_context_t *ctx;
}
aeron_cluster_egress_poller_t;

int aeron_cluster_egress_poller_create(
    aeron_cluster_egress_poller_t **poller,
    aeron_subscription_t *subscription,
    int fragment_limit);

int aeron_cluster_egress_poller_close(aeron_cluster_egress_poller_t *poller);

/**
 * Associate a context so the poller fires application callbacks (on_message etc.)
 * in addition to populating its state fields.  May be NULL for async_connect phase.
 */
void aeron_cluster_egress_poller_set_context(
    aeron_cluster_egress_poller_t *poller,
    aeron_cluster_context_t *ctx);

int aeron_cluster_egress_poller_poll(aeron_cluster_egress_poller_t *poller);


#ifdef __cplusplus
}
#endif
#endif /* AERON_CLUSTER_EGRESS_POLLER_H */
