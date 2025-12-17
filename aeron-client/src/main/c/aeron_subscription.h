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

#ifndef AERON_C_SUBSCRIPTION_H
#define AERON_C_SUBSCRIPTION_H

#include "aeronc.h"
#include "aeron_agent.h"
#include "aeron_context.h"
#include "aeron_client_conductor.h"

typedef struct aeron_image_list_stct
{
    int64_t change_number;
    uint32_t length;
    struct aeron_image_list_stct *volatile next_list;
    aeron_image_t **array;
}
aeron_image_list_t;

#define AERON_IMAGE_LIST_ALLOC_SIZE(l) (sizeof(aeron_image_list_t) + ((l) * sizeof(aeron_image_t *)))

typedef struct aeron_subscription_stct
{
    aeron_client_command_base_t command_base;
    aeron_client_conductor_t *conductor;
    const char *channel;

    struct subscription_conductor_fields_stct
    {
        uint8_t pre_fields_padding[AERON_CACHE_LINE_LENGTH];
        aeron_image_list_t image_lists_head;
        int64_t next_change_number;
        uint8_t post_fields_padding[AERON_CACHE_LINE_LENGTH];
    }
    conductor_fields;

    volatile int64_t *channel_status_indicator;

    volatile int64_t last_image_list_change_number;

    aeron_on_available_image_t on_available_image;
    void *on_available_image_clientd;
    aeron_on_unavailable_image_t on_unavailable_image;
    void *on_unavailable_image_clientd;
    aeron_notification_t on_close_complete;
    void *on_close_complete_clientd;

    int64_t registration_id;
    int32_t stream_id;
    int32_t channel_status_indicator_id;
    size_t round_robin_index;

    volatile bool is_closed;
    uint8_t post_fields_padding[AERON_CACHE_LINE_LENGTH];
}
aeron_subscription_t;

int aeron_subscription_create(
    aeron_subscription_t **subscription,
    aeron_client_conductor_t *conductor,
    const char *channel,
    int32_t stream_id,
    int64_t registration_id,
    int32_t channel_status_indicator_id,
    int64_t *channel_status_indicator_addr,
    aeron_on_available_image_t on_available_image,
    void *on_available_image_clientd,
    aeron_on_unavailable_image_t on_unavailable_image,
    void *on_unavailable_image_clientd);

int aeron_subscription_delete(aeron_subscription_t *subscription);
void aeron_subscription_force_close(aeron_subscription_t *subscription);

int aeron_subscription_alloc_image_list(aeron_image_list_t *volatile *image_list, size_t length);

int aeron_client_conductor_subscription_add_image(aeron_subscription_t *subscription, aeron_image_t *image);
int aeron_client_conductor_subscription_remove_image(aeron_subscription_t *subscription, aeron_image_t *image);

inline volatile aeron_image_list_t *aeron_client_conductor_subscription_image_list(aeron_subscription_t *subscription)
{
    return subscription->conductor_fields.image_lists_head.next_list;
}

int aeron_client_conductor_subscription_install_new_image_list(
    aeron_subscription_t *subscription, aeron_image_list_t *volatile image_list);

int aeron_client_conductor_subscription_prune_image_lists(aeron_subscription_t *subscription);

int aeron_subscription_reject_image(
    aeron_subscription_t *subscription, int64_t image_correlation_id, int64_t position, const char *reason);

inline int aeron_subscription_find_image_index(aeron_image_list_t *volatile image_list, aeron_image_t *image)
{
    size_t length = NULL == image_list ? 0 : image_list->length;

    for (size_t i = 0; i < length; i++)
    {
        if (image == image_list->array[i])
        {
            return (int)i;
        }
    }

    return -1;
}

inline int64_t aeron_subscription_last_image_list_change_number(aeron_subscription_t *subscription)
{
    int64_t last_image_list_change_number;

    AERON_GET_ACQUIRE(last_image_list_change_number, subscription->last_image_list_change_number);

    return last_image_list_change_number;
}

inline void aeron_subscription_propose_last_image_change_number(
    aeron_subscription_t *subscription, int64_t change_number)
{
    if (change_number > subscription->last_image_list_change_number)
    {
        AERON_SET_RELEASE(subscription->last_image_list_change_number, change_number);
    }
}

#endif //AERON_C_SUBSCRIPTION_H
