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

#ifndef AERON_CLUSTER_CONSENSUS_MODULE_ADAPTER_H
#define AERON_CLUSTER_CONSENSUS_MODULE_ADAPTER_H

#include <stdint.h>
#include <stdbool.h>
#include "aeronc.h"
#include "aeron_consensus_module_agent_fwd.h"

#ifdef __cplusplus
extern "C"
{
#endif

#define AERON_CLUSTER_CONSENSUS_MODULE_ADAPTER_FRAGMENT_LIMIT  25

typedef struct aeron_cluster_consensus_module_adapter_stct
{
    aeron_subscription_t               *subscription;  /* service → CM subscription */
    aeron_consensus_module_agent_t     *agent;
    aeron_controlled_fragment_assembler_t *assembler;
}
aeron_cluster_consensus_module_adapter_t;

/**
 * Create the adapter.  The subscription is owned by the agent and must outlive this adapter.
 *
 * @param adapter   out: newly created adapter.
 * @param sub       subscription on the service → CM channel.
 * @param agent     the ConsensusModuleAgent to dispatch callbacks to.
 * @return 0 on success, -1 on error.
 */
int aeron_cluster_consensus_module_adapter_create(
    aeron_cluster_consensus_module_adapter_t **adapter,
    aeron_subscription_t *sub,
    aeron_consensus_module_agent_t *agent);

/** Poll the subscription; returns number of fragments processed. */
int aeron_cluster_consensus_module_adapter_poll(
    aeron_cluster_consensus_module_adapter_t *adapter);

/** Close and free (does NOT close the subscription — that is owned by the agent). */
void aeron_cluster_consensus_module_adapter_close(
    aeron_cluster_consensus_module_adapter_t *adapter);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_CONSENSUS_MODULE_ADAPTER_H */
