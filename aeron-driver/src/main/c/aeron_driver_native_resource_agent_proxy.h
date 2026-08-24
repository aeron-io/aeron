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

#ifndef AERON_DRIVER_NATIVE_RESOURCE_AGENT_PROXY_H
#define AERON_DRIVER_NATIVE_RESOURCE_AGENT_PROXY_H

#include "aeron_driver_context.h"
#include "aeron_name_resolver.h"
#include "concurrent/aeron_spsc_rb.h"
#include "media/aeron_udp_channel.h"

typedef struct aeron_driver_native_resource_agent_stct aeron_driver_native_resource_agent_t;

typedef enum aeron_driver_native_resource_agent_command_state_enum
{
    AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_PENDING,
    AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_SUCCEEDED,
    AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_STATE_FAILED
}
aeron_driver_native_resource_agent_command_state_t;

typedef enum aeron_driver_native_resource_agent_command_type_enum
{
    AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_TYPE_RESOLVE_ADDRESS = 1,
    AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_TYPE_PARSE_CHANNEL = 2,
    AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_TYPE_FREE_LOG_BUFFER = 3,
    AERON_DRIVER_NATIVE_RESOURCE_AGENT_COMMAND_TYPE_MAP_LOG_BUFFER = 4
}
aeron_driver_native_resource_agent_command_type_t;

typedef struct aeron_driver_native_resource_agent_command_result_stct
{
    volatile aeron_driver_native_resource_agent_command_state_t state;
    union
    {
        void *success;
        struct aeron_driver_native_resource_agent_command_error_stct
        {
            int code;
            char* message;
        }
        error;
    }
    payload;
}
aeron_driver_native_resource_agent_command_result_t;

typedef struct aeron_driver_native_resource_agent_proxy_stct
{
    aeron_driver_native_resource_agent_t *native_resource_agent;
    int64_t *fail_counter;
    aeron_spsc_rb_t *command_queue;
}
aeron_driver_native_resource_agent_proxy_t;

typedef struct aeron_driver_native_resource_agent_proxy_cmd_resolve_address_stct
{
    aeron_name_resolver_async_resolve_t *address_resolution_params;
    aeron_driver_native_resource_agent_command_result_t *result;
}
aeron_driver_native_resource_agent_proxy_cmd_resolve_address_t;

typedef struct aeron_driver_native_resource_agent_proxy_cmd_parse_channel_stct
{
    aeron_udp_channel_async_parse_t *async_parse;
    aeron_driver_native_resource_agent_command_result_t *result;
}
aeron_driver_native_resource_agent_proxy_cmd_parse_channel_t;

typedef struct aeron_driver_native_resource_agent_proxy_cmd_free_log_buffer_stct
{
    aeron_mapped_raw_log_t *mapped_raw_log;
    const char *log_file_name;
}
aeron_driver_native_resource_agent_proxy_cmd_free_log_buffer_t;

typedef struct aeron_driver_native_resource_agent_proxy_cmd_map_log_buffer_stct
{
    const char *log_file_name;
    size_t term_length;
    bool is_sparse;
    aeron_driver_native_resource_agent_command_result_t *result;
}
aeron_driver_native_resource_agent_proxy_cmd_map_log_buffer_t;

void aeron_driver_native_resource_agent_proxy_resolve_address(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_name_resolver_async_resolve_t *address_resolution_params,
    aeron_driver_native_resource_agent_command_result_t *result);

void aeron_driver_native_resource_agent_proxy_parse_udp_channel(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_udp_channel_async_parse_t *async_parse,
    aeron_driver_native_resource_agent_command_result_t *result);

void aeron_driver_native_resource_agent_proxy_free_log_buffer(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    aeron_mapped_raw_log_t *mapped_raw_log,
    const char *log_file_name);

void aeron_driver_native_resource_agent_proxy_map_log_buffer(
    aeron_driver_native_resource_agent_proxy_t *native_resource_agent_proxy,
    const char *log_file_name,
    size_t term_length,
    bool is_sparse,
    aeron_driver_native_resource_agent_command_result_t *result);

#endif //AERON_DRIVER_NATIVE_RESOURCE_AGENT_PROXY_H
