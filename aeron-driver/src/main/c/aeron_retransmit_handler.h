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

#ifndef AERON_RETRANSMIT_HANDLER_H
#define AERON_RETRANSMIT_HANDLER_H

#include "collections/aeron_int64_to_ptr_hash_map.h"
#include "aeron_driver_common.h"
#include "aeronmd.h"

typedef enum aeron_retransmit_action_state_enum
{
    AERON_RETRANSMIT_ACTION_STATE_DELAYED,
    AERON_RETRANSMIT_ACTION_STATE_LINGERING,
    AERON_RETRANSMIT_ACTION_STATE_INACTIVE,
}
aeron_retransmit_action_state_t;

typedef struct aeron_retransmit_action_stct
{
    int64_t expiry_ns;
    int32_t term_id;
    int32_t term_offset;
    size_t length;
    aeron_retransmit_action_state_t state;
}
aeron_retransmit_action_t;

#define AERON_RETRANSMIT_HANDLER_MAX_RESEND (16)
#define AERON_RETRANSMIT_HANDLER_MAX_RESEND_MAX (256)

typedef int (*aeron_retransmit_handler_resend_func_t)(
    void *clientd, int32_t term_id, int32_t term_offset, size_t length);

typedef struct aeron_retransmit_handler_stct
{
    aeron_retransmit_action_t *retransmit_action_pool;
    uint64_t delay_timeout_ns;
    uint64_t linger_timeout_ns;

    int64_t *invalid_packets_counter;

    int active_retransmit_count;

    bool has_group_semantics;
    size_t max_retransmits;
    int64_t *retransmit_overflow_counter;
}
aeron_retransmit_handler_t;

int aeron_retransmit_handler_init(
    aeron_retransmit_handler_t *handler,
    int64_t *invalid_packets_counter,
    uint64_t delay_timeout_ns,
    uint64_t linger_timeout_ns,
    bool has_group_semantics,
    uint32_t max_retransmits,
    int64_t *retransmit_overflow_counter);

void aeron_retransmit_handler_close(aeron_retransmit_handler_t *handler);

int aeron_retransmit_handler_on_nak(
    aeron_retransmit_handler_t *handler,
    int32_t term_id,
    int32_t term_offset,
    int32_t length,
    size_t term_length,
    size_t mtu_length,
    aeron_flow_control_strategy_t *flow_control,
    int64_t now_ns,
    aeron_retransmit_handler_resend_func_t resend,
    void *resend_clientd);

int aeron_retransmit_handler_process_timeouts(
    aeron_retransmit_handler_t *handler,
    int64_t now_ns,
    aeron_retransmit_handler_resend_func_t resend,
    void *resend_clientd);

#endif //AERON_RETRANSMIT_HANDLER_H
