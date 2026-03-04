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

#include "aeron_counter.h"

int aeron_counter_create(
    aeron_counter_t **counter,
    aeron_client_conductor_t *conductor,
    int64_t registration_id,
    int32_t counter_id,
    int64_t *counter_addr)
{
    aeron_counter_t *_counter;

    *counter = NULL;
    if (aeron_alloc((void **)&_counter, sizeof(aeron_counter_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate counter");
        return -1;
    }

    _counter->command_base.type = AERON_CLIENT_MANAGED_RESOURCE_TYPE_COUNTER;

    _counter->counter_addr = counter_addr;

    _counter->conductor = conductor;
    _counter->registration_id = registration_id;
    _counter->counter_id = counter_id;
    _counter->is_closed = false;

    *counter = _counter;
    return 0;
}

int aeron_counter_delete(aeron_counter_t *counter)
{
    aeron_free(counter);
    return 0;
}

void aeron_counter_force_close(aeron_counter_t *counter)
{
    AERON_SET_RELEASE(counter->is_closed, true);
}

int64_t *aeron_counter_addr(aeron_counter_t *counter)
{
    return counter->counter_addr;
}

int aeron_counter_constants(aeron_counter_t *counter, aeron_counter_constants_t *constants)
{
    if (NULL == counter || NULL == constants)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, counter: %s, constants: %s",
            AERON_NULL_STR(counter),
            AERON_NULL_STR(constants));
        return -1;
    }

    constants->registration_id = counter->registration_id;
    constants->counter_id = counter->counter_id;

    return 0;
}

int aeron_counter_close(
    aeron_counter_t *counter, aeron_notification_t on_close_complete, void *on_close_complete_clientd)
{
    if (NULL != counter)
    {
        bool is_closed;

        AERON_GET_ACQUIRE(is_closed, counter->is_closed);
        if (!is_closed)
        {
            AERON_SET_RELEASE(counter->is_closed, true);
            return aeron_client_conductor_async_close_counter(
                counter->conductor, counter, on_close_complete, on_close_complete_clientd);
        }
    }

    return 0;
}

bool aeron_counter_is_closed(aeron_counter_t *counter)
{
    bool is_closed = false;
    if (NULL != counter)
    {
        AERON_GET_ACQUIRE(is_closed, counter->is_closed);
    }

    return is_closed;
}
