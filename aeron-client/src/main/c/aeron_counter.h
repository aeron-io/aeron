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

#ifndef AERON_C_COUNTER_H
#define AERON_C_COUNTER_H

#include "aeronc.h"
#include "aeron_agent.h"
#include "aeron_context.h"
#include "aeron_client_conductor.h"

typedef struct aeron_counter_stct
{
    aeron_client_command_base_t command_base;
    aeron_client_conductor_t *conductor;

    int64_t *counter_addr;
    int64_t correlation_id;
    int64_t registration_id;
    int32_t counter_id;

    aeron_notification_t on_close_complete;
    void *on_close_complete_clientd;
    volatile bool is_closed;
    volatile bool pending_close_action;
}
aeron_counter_t;

int aeron_counter_create(
    aeron_counter_t **counter,
    aeron_client_conductor_t *conductor,
    int64_t correlation_id,
    int64_t registration_id,
    int32_t counter_id,
    int64_t *counter_addr);

int aeron_counter_delete(aeron_counter_t *counter);
void aeron_counter_force_close(aeron_counter_t *counter);

#endif //AERON_C_COUNTER_H
