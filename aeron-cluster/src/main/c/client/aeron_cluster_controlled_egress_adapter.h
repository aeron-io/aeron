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

#ifndef AERON_CLUSTER_CONTROLLED_EGRESS_ADAPTER_H
#define AERON_CLUSTER_CONTROLLED_EGRESS_ADAPTER_H

#include <stdint.h>
#include <stddef.h>
#include "aeronc.h"
#include "aeron_cluster_egress_adapter.h"

#ifdef __cplusplus
extern "C"
{
#endif

/**
 * Controlled variant of EgressAdapter.  Uses a ControlledFragmentAssembler so
 * that returning AERON_ACTION_BREAK from any callback stops polling immediately.
 * Mirrors Java ControlledEgressAdapter.
 *
 * The listener interface is shared with EgressAdapter
 * (aeron_cluster_egress_listener_t).
 */
#define AERON_CLUSTER_CONTROLLED_EGRESS_ADAPTER_FRAGMENT_LIMIT 10

typedef struct aeron_cluster_controlled_egress_adapter_stct
{
    aeron_subscription_t                   *subscription;
    aeron_cluster_egress_listener_t        *listener;
    aeron_controlled_fragment_assembler_t  *assembler;
    int64_t                                 cluster_session_id;
}
aeron_cluster_controlled_egress_adapter_t;

/**
 * Create a ControlledEgressAdapter.
 *
 * @param adapter             out: adapter handle.
 * @param subscription        egress subscription to poll.
 * @param listener            callback table.
 * @param cluster_session_id  session ID for filtering messages.
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_controlled_egress_adapter_create(
    aeron_cluster_controlled_egress_adapter_t **adapter,
    aeron_subscription_t *subscription,
    aeron_cluster_egress_listener_t *listener,
    int64_t cluster_session_id);

/** Poll; returns fragment count. */
int aeron_cluster_controlled_egress_adapter_poll(aeron_cluster_controlled_egress_adapter_t *adapter);

/** Close (does NOT close subscription). */
void aeron_cluster_controlled_egress_adapter_close(aeron_cluster_controlled_egress_adapter_t *adapter);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_CONTROLLED_EGRESS_ADAPTER_H */
