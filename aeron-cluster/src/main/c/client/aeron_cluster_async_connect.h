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

#ifndef AERON_CLUSTER_ASYNC_CONNECT_H
#define AERON_CLUSTER_ASYNC_CONNECT_H

#include "aeron_cluster.h"

#ifdef __cplusplus
extern "C"
{
#endif


/**
 * Returns the current state integer of the async connect state machine.
 * Exposed primarily for testing; do not rely on specific values in production.
 */
uint8_t aeron_cluster_async_connect_step(aeron_cluster_async_connect_t *async);

/**
 * Poll the async connect state machine.
 *   0  = not done yet, call again (idle between calls)
 *   1  = connected, *cluster is valid
 *  -1  = error, aeron_errmsg() has details
 */
int aeron_cluster_async_connect_poll(aeron_cluster_t **cluster, aeron_cluster_async_connect_t *async);


#ifdef __cplusplus
}
#endif
#endif /* AERON_CLUSTER_ASYNC_CONNECT_H */
