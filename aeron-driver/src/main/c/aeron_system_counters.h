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

#ifndef AERON_SYSTEM_COUNTERS_H
#define AERON_SYSTEM_COUNTERS_H

#include "concurrent/aeron_counters_manager.h"

typedef enum aeron_system_counter_enum_stct
{
    AERON_SYSTEM_COUNTER_BYTES_SENT = 0,
    AERON_SYSTEM_COUNTER_BYTES_RECEIVED = 1,
    AERON_SYSTEM_COUNTER_RECEIVER_PROXY_FAILS = 2,
    AERON_SYSTEM_COUNTER_SENDER_PROXY_FAILS = 3,
    AERON_SYSTEM_COUNTER_CONDUCTOR_PROXY_FAILS = 4,
    AERON_SYSTEM_COUNTER_NAK_MESSAGES_SENT = 5,
    AERON_SYSTEM_COUNTER_NAK_MESSAGES_RECEIVED = 6,
    AERON_SYSTEM_COUNTER_STATUS_MESSAGES_SENT = 7,
    AERON_SYSTEM_COUNTER_STATUS_MESSAGES_RECEIVED = 8,
    AERON_SYSTEM_COUNTER_HEARTBEATS_SENT = 9,
    AERON_SYSTEM_COUNTER_HEARTBEATS_RECEIVED = 10,
    AERON_SYSTEM_COUNTER_RETRANSMITS_SENT = 11,
    AERON_SYSTEM_COUNTER_FLOW_CONTROL_UNDER_RUNS = 12,
    AERON_SYSTEM_COUNTER_FLOW_CONTROL_OVER_RUNS = 13,
    AERON_SYSTEM_COUNTER_INVALID_PACKETS = 14,
    AERON_SYSTEM_COUNTER_ERRORS = 15,
    AERON_SYSTEM_COUNTER_SHORT_SENDS = 16,
    AERON_SYSTEM_COUNTER_FREE_FAILS = 17,
    AERON_SYSTEM_COUNTER_SENDER_FLOW_CONTROL_LIMITS = 18,
    AERON_SYSTEM_COUNTER_UNBLOCKED_PUBLICATIONS = 19,
    AERON_SYSTEM_COUNTER_UNBLOCKED_COMMANDS = 20,
    AERON_SYSTEM_COUNTER_POSSIBLE_TTL_ASYMMETRY = 21,
    AERON_SYSTEM_COUNTER_CONTROLLABLE_IDLE_STRATEGY = 22,
    AERON_SYSTEM_COUNTER_LOSS_GAP_FILLS = 23,
    AERON_SYSTEM_COUNTER_CLIENT_TIMEOUTS = 24,
    AERON_SYSTEM_COUNTER_RESOLUTION_CHANGES = 25,
    AERON_SYSTEM_COUNTER_CONDUCTOR_MAX_CYCLE_TIME = 26,
    AERON_SYSTEM_COUNTER_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED = 27,
    AERON_SYSTEM_COUNTER_SENDER_MAX_CYCLE_TIME = 28,
    AERON_SYSTEM_COUNTER_SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED = 29,
    AERON_SYSTEM_COUNTER_RECEIVER_MAX_CYCLE_TIME = 30,
    AERON_SYSTEM_COUNTER_RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED = 31,
    AERON_SYSTEM_COUNTER_NAME_RESOLVER_MAX_TIME = 32,
    AERON_SYSTEM_COUNTER_NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED = 33,
    AERON_SYSTEM_COUNTER_AERON_VERSION = 34,
    AERON_SYSTEM_COUNTER_BYTES_CURRENTLY_MAPPED = 35,
    AERON_SYSTEM_COUNTER_RETRANSMITTED_BYTES = 36,
    AERON_SYSTEM_COUNTER_RETRANSMIT_OVERFLOW = 37,
    AERON_SYSTEM_COUNTER_ERROR_FRAMES_RECEIVED = 38,
    AERON_SYSTEM_COUNTER_ERROR_FRAMES_SENT = 39,
    AERON_SYSTEM_COUNTER_IMAGES_REJECTED = 40,

    // Add all new counters before this one (used for a static assertion).
    AERON_SYSTEM_COUNTER_DUMMY_LAST,
}
aeron_system_counter_enum_t;

typedef struct aeron_system_counter_stct
{
    const char *label;
    int32_t id;
}
aeron_system_counter_t;

typedef struct aeron_system_counters_stct
{
    int32_t *counter_ids;
    aeron_counters_manager_t *manager;
}
aeron_system_counters_t;

int aeron_system_counters_init(aeron_system_counters_t *counters, aeron_counters_manager_t *manager);

void aeron_system_counters_close(aeron_system_counters_t *counters);

inline int64_t *aeron_system_counter_addr(aeron_system_counters_t *counters, aeron_system_counter_enum_t type)
{
    return aeron_counters_manager_addr(counters->manager, counters->counter_ids[type]);
}

#endif //AERON_SYSTEM_COUNTERS_H
