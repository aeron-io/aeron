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

#ifndef AERON_DRIVER_COMMON_H
#define AERON_DRIVER_COMMON_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#define AERON_MAX_HOSTNAME_LEN (256)
#define AERON_CHANNEL_STATUS_INDICATOR_NOT_ALLOCATED (-1)
#define AERON_URI_INVALID_TAG (-1)

typedef void (*aeron_idle_strategy_func_t)(void *state, int work_count);
typedef int (*aeron_idle_strategy_init_func_t)(void **state, const char *env_var, const char *init_args);

typedef enum aeron_driver_managed_resource_event_enum
{
    AERON_DRIVER_MANAGED_RESOURCE_EVENT_INCREF,
    AERON_DRIVER_MANAGED_RESOURCE_EVENT_DECREF,
    AERON_DRIVER_MANAGED_RESOURCE_EVENT_REVOKE
}
aeron_driver_managed_resource_event_t;

typedef struct aeron_driver_managed_resource_stct
{
    int64_t registration_id;
    int64_t time_of_last_state_change_ns;
    void *clientd;
    void (*handle_event)(aeron_driver_managed_resource_event_t, void *);
}
aeron_driver_managed_resource_t;

#define AERON_DRIVER_MANAGED_RESOURCE_HANDLE_EVENT(_resource, _event) \
do                                                                    \
{                                                                     \
    if (NULL != (_resource) && NULL != (_resource)->handle_event)     \
    {                                                                 \
        (_resource)->handle_event((_event), (_resource)->clientd);    \
    }                                                                 \
}                                                                     \
while (0)

#define AERON_DRIVER_MANAGED_RESOURCE_INCREF(_resource) AERON_DRIVER_MANAGED_RESOURCE_HANDLE_EVENT(_resource, AERON_DRIVER_MANAGED_RESOURCE_EVENT_INCREF)
#define AERON_DRIVER_MANAGED_RESOURCE_DECREF(_resource) AERON_DRIVER_MANAGED_RESOURCE_HANDLE_EVENT(_resource, AERON_DRIVER_MANAGED_RESOURCE_EVENT_DECREF)
#define AERON_DRIVER_MANAGED_RESOURCE_REVOKE(_resource) AERON_DRIVER_MANAGED_RESOURCE_HANDLE_EVENT(_resource, AERON_DRIVER_MANAGED_RESOURCE_EVENT_REVOKE)

typedef struct aeron_position_stct
{
    int32_t counter_id;
    volatile int64_t *value_addr;
}
aeron_position_t;

typedef struct aeron_position_stct aeron_atomic_counter_t;

typedef enum aeron_subscription_tether_state_enum
{
    AERON_SUBSCRIPTION_TETHER_ACTIVE,
    AERON_SUBSCRIPTION_TETHER_LINGER,
    AERON_SUBSCRIPTION_TETHER_RESTING
}
aeron_subscription_tether_state_t;

typedef struct aeron_tetherable_position_stct
{
    bool is_tether;
    aeron_subscription_tether_state_t state;
    int32_t counter_id;
    volatile int64_t *value_addr;
    int64_t subscription_registration_id;
    int64_t time_of_last_update_ns;
}
aeron_tetherable_position_t;

typedef void (*aeron_untethered_subscription_state_change_func_t)(
    aeron_tetherable_position_t *tetherable_position,
    int64_t now_ns,
    aeron_subscription_tether_state_t old_state,
    aeron_subscription_tether_state_t new_state,
    int32_t stream_id,
    int32_t session_id);

void aeron_untethered_subscription_state_change(
    aeron_tetherable_position_t *tetherable_position,
    int64_t now_ns,
    aeron_subscription_tether_state_t new_state,
    int32_t stream_id,
    int32_t session_id);

typedef struct aeron_subscribable_stct
{
    int64_t correlation_id;
    size_t length;
    size_t capacity;
    aeron_tetherable_position_t *array;
    size_t resting_count;
    void (*add_position_hook_func)(void *clientd, volatile int64_t *value_addr);
    void (*remove_position_hook_func)(void *clientd, volatile int64_t *value_addr);
    void *clientd;
}
aeron_subscribable_t;

void aeron_driver_subscribable_state(
    aeron_subscribable_t *subscribable,
    aeron_tetherable_position_t *tetherable_position,
    aeron_subscription_tether_state_t state,
    int64_t now_ns);

size_t aeron_driver_subscribable_working_position_count(aeron_subscribable_t *subscribable);

bool aeron_driver_subscribable_has_working_positions(aeron_subscribable_t *subscribable);

typedef struct aeron_command_base_stct
{
    void (*func)(void *clientd, void *command);
    void *item;
}
aeron_command_base_t;

typedef struct aeron_feedback_delay_generator_state_stct aeron_feedback_delay_generator_state_t;

typedef int64_t (*aeron_feedback_delay_generator_func_t)(aeron_feedback_delay_generator_state_t *state, bool retry);

struct aeron_feedback_delay_generator_state_stct
{
    struct static_delay_stct
    {
        int64_t delay_ns;
        int64_t retry_ns;
    }
    static_delay;

    struct optimal_delay_stct
    {
        double rand_max;
        double base_x;
        double constant_t;
        double factor_t;
    }
    optimal_delay;

    aeron_feedback_delay_generator_func_t delay_generator;
};

void aeron_driver_subscribable_remove_position(aeron_subscribable_t *subscribable, int32_t counter_id);

inline void aeron_driver_subscribable_null_hook(void *clientd, volatile int64_t *value_addr)
{
}

typedef void (*aeron_on_remove_publication_cleanup_func_t)(
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

typedef void (*aeron_on_remove_subscription_cleanup_func_t)(
    int64_t id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

typedef void (*aeron_on_remove_image_cleanup_func_t)(
    int64_t id,
    int32_t session_id,
    int32_t stream_id,
    size_t channel_length,
    const char *channel);

typedef void (*aeron_on_endpoint_change_func_t)(const void *channel);

#endif //AERON_DRIVER_COMMON_H
