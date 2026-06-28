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

#ifndef AERON_DRIVER_NATIVE_RESOURCE_AGENT_H
#define AERON_DRIVER_NATIVE_RESOURCE_AGENT_H

#include "aeron_driver_native_resource_agent_proxy.h"
#include "aeron_name_resolver.h"
#include "util/aeron_deque.h"

typedef struct aeron_driver_native_resource_agent_stct
{
    aeron_driver_native_resource_agent_proxy_t native_resource_agent_proxy;
    aeron_driver_context_t *context;
    aeron_name_resolver_t name_resolver;
    aeron_deque_t end_of_life_queue;
}
aeron_driver_native_resource_agent_t;

int aeron_driver_native_resource_agent_init(
    aeron_driver_native_resource_agent_t *native_resource_agent, aeron_driver_context_t *context);

int aeron_driver_native_resource_agent_do_work(void *clientd);

void aeron_driver_native_resource_agent_on_start(void *state, const char *role_name);

void aeron_driver_native_resource_agent_on_close(void *clientd);

void aeron_driver_native_resource_agent_on_resolve_address(
    aeron_driver_native_resource_agent_t *native_resource_agent, aeron_driver_native_resource_agent_proxy_cmd_t *cmd);

void aeron_driver_native_resource_agent_on_parse_udp_channel(
    aeron_driver_native_resource_agent_t *native_resource_agent, aeron_driver_native_resource_agent_proxy_cmd_t *cmd);

void aeron_driver_native_resource_agent_on_free_resource(
    aeron_driver_native_resource_agent_t *native_resource_agent, aeron_driver_native_resource_agent_proxy_cmd_t *cmd);

#endif //AERON_DRIVER_NATIVE_RESOURCE_AGENT_H
