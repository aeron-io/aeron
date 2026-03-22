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

#ifndef AERON_CLUSTER_INGRESS_ADAPTER_CM_H
#define AERON_CLUSTER_INGRESS_ADAPTER_CM_H

#include <stdint.h>
#include "aeronc.h"

#ifdef __cplusplus
extern "C"
{
#endif

typedef struct aeron_consensus_module_agent_stct aeron_consensus_module_agent_t;

typedef struct aeron_cluster_ingress_adapter_cm_stct
{
    aeron_subscription_t               *subscription;
    aeron_controlled_fragment_assembler_t *fragment_assembler;
    aeron_consensus_module_agent_t     *agent;
    int                                 fragment_limit;
}
aeron_cluster_ingress_adapter_cm_t;

int aeron_cluster_ingress_adapter_cm_create(
    aeron_cluster_ingress_adapter_cm_t **adapter,
    aeron_subscription_t *subscription,
    aeron_consensus_module_agent_t *agent,
    int fragment_limit);

int aeron_cluster_ingress_adapter_cm_close(aeron_cluster_ingress_adapter_cm_t *adapter);
int aeron_cluster_ingress_adapter_cm_poll(aeron_cluster_ingress_adapter_cm_t *adapter);

#ifdef __cplusplus
}
#endif

#endif /* AERON_CLUSTER_INGRESS_ADAPTER_CM_H */
