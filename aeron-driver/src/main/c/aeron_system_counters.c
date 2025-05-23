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

#include <string.h>
#include <errno.h>
#include "util/aeron_error.h"
#include "aeron_system_counters.h"
#include "aeron_alloc.h"
#include "aeron_counters.h"

static aeron_system_counter_t system_counters[] =
    {
        { "Bytes sent", AERON_SYSTEM_COUNTER_BYTES_SENT },
        { "Bytes received", AERON_SYSTEM_COUNTER_BYTES_RECEIVED },
        { "Failed offers to ReceiverProxy", AERON_SYSTEM_COUNTER_RECEIVER_PROXY_FAILS },
        { "Failed offers to SenderProxy", AERON_SYSTEM_COUNTER_SENDER_PROXY_FAILS },
        { "Failed offers to DriverConductorProxy", AERON_SYSTEM_COUNTER_CONDUCTOR_PROXY_FAILS },
        { "NAKs sent", AERON_SYSTEM_COUNTER_NAK_MESSAGES_SENT },
        { "NAKs received", AERON_SYSTEM_COUNTER_NAK_MESSAGES_RECEIVED },
        { "Status Messages sent", AERON_SYSTEM_COUNTER_STATUS_MESSAGES_SENT },
        { "Status Messages received", AERON_SYSTEM_COUNTER_STATUS_MESSAGES_RECEIVED },
        { "Heartbeats sent", AERON_SYSTEM_COUNTER_HEARTBEATS_SENT },
        { "Heartbeats received", AERON_SYSTEM_COUNTER_HEARTBEATS_RECEIVED },
        { "Retransmits sent", AERON_SYSTEM_COUNTER_RETRANSMITS_SENT },
        { "Flow control under runs", AERON_SYSTEM_COUNTER_FLOW_CONTROL_UNDER_RUNS },
        { "Flow control over runs", AERON_SYSTEM_COUNTER_FLOW_CONTROL_OVER_RUNS },
        { "Invalid packets", AERON_SYSTEM_COUNTER_INVALID_PACKETS },
        { "Errors: version=" AERON_VERSION_TXT " commit=" AERON_VERSION_GITSHA, AERON_SYSTEM_COUNTER_ERRORS },
        { "Short sends", AERON_SYSTEM_COUNTER_SHORT_SENDS },
        { "Failed attempts to free log buffers", AERON_SYSTEM_COUNTER_FREE_FAILS },
        { "Sender flow control limits, i.e. back-pressure events", AERON_SYSTEM_COUNTER_SENDER_FLOW_CONTROL_LIMITS },
        { "Unblocked Publications", AERON_SYSTEM_COUNTER_UNBLOCKED_PUBLICATIONS },
        { "Unblocked Control Commands", AERON_SYSTEM_COUNTER_UNBLOCKED_COMMANDS },
        { "Possible TTL Asymmetry", AERON_SYSTEM_COUNTER_POSSIBLE_TTL_ASYMMETRY },
        { "ControllableIdleStrategy status", AERON_SYSTEM_COUNTER_CONTROLLABLE_IDLE_STRATEGY },
        { "Loss gap fills", AERON_SYSTEM_COUNTER_LOSS_GAP_FILLS },
        { "Client liveness timeouts", AERON_SYSTEM_COUNTER_CLIENT_TIMEOUTS },
        { "Resolution changes", AERON_SYSTEM_COUNTER_RESOLUTION_CHANGES },
        { "Conductor max cycle time doing its work in ns", AERON_SYSTEM_COUNTER_CONDUCTOR_MAX_CYCLE_TIME },
        { "Conductor work cycle exceeded threshold count", AERON_SYSTEM_COUNTER_CONDUCTOR_CYCLE_TIME_THRESHOLD_EXCEEDED },
        { "Sender max cycle time doing its work in ns", AERON_SYSTEM_COUNTER_SENDER_MAX_CYCLE_TIME },
        { "Sender work cycle exceeded threshold count", AERON_SYSTEM_COUNTER_SENDER_CYCLE_TIME_THRESHOLD_EXCEEDED },
        { "Receiver max cycle time doing its work in ns", AERON_SYSTEM_COUNTER_RECEIVER_MAX_CYCLE_TIME },
        { "Receiver work cycle exceeded threshold count", AERON_SYSTEM_COUNTER_RECEIVER_CYCLE_TIME_THRESHOLD_EXCEEDED },
        { "NameResolver max time in ns", AERON_SYSTEM_COUNTER_NAME_RESOLVER_MAX_TIME },
        { "NameResolver exceeded threshold count", AERON_SYSTEM_COUNTER_NAME_RESOLVER_TIME_THRESHOLD_EXCEEDED },
        { "Aeron software: version=" AERON_VERSION_TXT " commit=" AERON_VERSION_GITSHA, AERON_SYSTEM_COUNTER_AERON_VERSION },
        { "Bytes currently mapped", AERON_SYSTEM_COUNTER_BYTES_CURRENTLY_MAPPED },
        { "Retransmitted bytes", AERON_SYSTEM_COUNTER_RETRANSMITTED_BYTES },
        { "Retransmit Pool Overflow count", AERON_SYSTEM_COUNTER_RETRANSMIT_OVERFLOW },
        { "Error Frames received", AERON_SYSTEM_COUNTER_ERROR_FRAMES_RECEIVED },
        { "Error Frames sent", AERON_SYSTEM_COUNTER_ERROR_FRAMES_SENT },
        { "Images rejected", AERON_SYSTEM_COUNTER_IMAGES_REJECTED }
    };

static size_t num_system_counters = sizeof(system_counters) / sizeof(aeron_system_counter_t);

#ifdef AERON_COMPILER_GCC
_Static_assert(
    AERON_SYSTEM_COUNTER_DUMMY_LAST == sizeof(system_counters) / sizeof(aeron_system_counter_t),
    "Please add counters in aeron_system_counter_t enum to the system_counters table");
#endif

int aeron_system_counters_init(aeron_system_counters_t *counters, aeron_counters_manager_t *manager)
{
    if (NULL == counters || NULL == manager)
    {
        AERON_SET_ERR(
            EINVAL, "counters=%s, manager=%s", NULL == counters ? "NULL" : "OK", NULL == manager ? "NULL" : "OK");
        return -1;
    }

    counters->manager = manager;
    if (aeron_alloc((void **)&counters->counter_ids, sizeof(int32_t) * num_system_counters) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate counter ids");
        return -1;
    }

    for (size_t i = 0; i < num_system_counters; i++)
    {
        if ((counters->counter_ids[i] = aeron_counters_manager_allocate(
             manager,
             AERON_COUNTER_SYSTEM_COUNTER_TYPE_ID,
             (const uint8_t *)&(system_counters[i].id),
             sizeof(system_counters[i].id),
             system_counters[i].label,
             strlen(system_counters[i].label))) < 0)
        {
            return -1;
        }
    }

    return 0;
}

void aeron_system_counters_close(aeron_system_counters_t *counters)
{
    for (int32_t i = 0; i < (int32_t)num_system_counters; i++)
    {
        aeron_counters_manager_free(counters->manager, counters->counter_ids[i]);
    }

    aeron_free(counters->counter_ids);
}

extern int64_t *aeron_system_counter_addr(aeron_system_counters_t *counters, aeron_system_counter_enum_t type);
