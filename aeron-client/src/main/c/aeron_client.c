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

#if defined(__linux__)
#define _BSD_SOURCE
#define _GNU_SOURCE
#ifdef HAVE_BSDSTDLIB_H
#include <bsd/stdlib.h>
#endif
#endif

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <inttypes.h>

#include "aeronc.h"
#include "aeron_client.h"

inline static int aeron_do_work(void *clientd)
{
    aeron_t *client = (aeron_t *)clientd;
    return aeron_client_conductor_do_work(&client->conductor);
}

inline static void aeron_on_close(void *clientd)
{
    aeron_t *client = (aeron_t *)clientd;

    aeron_client_conductor_on_close(&client->conductor);
}

int aeron_init(aeron_t **client, aeron_context_t *context)
{
    aeron_t *_client = NULL;

    if (NULL == client || NULL == context)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, client: %s, context: %d",
            AERON_NULL_STR(client),
            AERON_NULL_STR(context));
        goto error;
    }

    if (aeron_alloc((void **)&_client, sizeof(aeron_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to allocate aeron_client");
        goto error;
    }

    _client->context = context;

    _client->runner.agent_state = AERON_AGENT_STATE_UNUSED;
    _client->runner.role_name = NULL;
    _client->runner.on_close = NULL;

    if (aeron_client_connect_to_driver(&context->cnc_map, context) < 0)
    {
        goto error;
    }

    if (aeron_client_conductor_init(&_client->conductor, context) < 0)
    {
        goto error;
    }

    if (aeron_agent_init(
        &_client->runner,
        "aeron-client-conductor",
        _client,
        _client->context->agent_on_start_func,
        _client->context->agent_on_start_state,
        aeron_do_work,
        aeron_on_close,
        _client->context->idle_strategy_func,
        _client->context->idle_strategy_state) < 0)
    {
        goto error;
    }

    *client = _client;
    return 0;

    error:

    if (NULL != _client)
    {
        aeron_free(_client);
    }

    return -1;
}

int aeron_start(aeron_t *client)
{
    if (NULL == client)
    {
        AERON_SET_ERR(EINVAL, "%s", "client must not be null");
        return -1;
    }

    if (!client->context->use_conductor_agent_invoker)
    {
        if (aeron_agent_start(&client->runner) < 0)
        {
            return -1;
        }
    }
    else
    {
        if (NULL != client->runner.on_start)
        {
            client->runner.on_start(client->runner.on_start_state, client->runner.role_name);
        }

        client->runner.state = AERON_AGENT_STATE_MANUAL;
    }

    return 0;
}

int aeron_main_do_work(aeron_t *client)
{
    if (NULL == client)
    {
        AERON_SET_ERR(EINVAL, "%s", "client is null");
        return -1;
    }

    if (!client->context->use_conductor_agent_invoker)
    {
        AERON_SET_ERR(EINVAL, "%s", "client is not configured to use agent invoker");
        return -1;
    }

    return aeron_agent_do_work(&client->runner);
}

void aeron_main_idle_strategy(aeron_t *client, int work_count)
{
    if (NULL != client)
    {
        aeron_agent_idle(&client->runner, work_count);
    }
}

int aeron_close(aeron_t *client)
{
    if (NULL != client)
    {
        if (aeron_agent_stop(&client->runner) < 0)
        {
            return -1;
        }

        if (aeron_agent_close(&client->runner) < 0)
        {
            return -1;
        }

        aeron_free(client);
    }

    return 0;
}

bool aeron_is_closed(aeron_t *client)
{
    return aeron_client_conductor_is_closed(&client->conductor);
}

typedef struct aeron_print_counters_stream_out_stct
{
    void (*stream_out)(const char *);
}
aeron_print_counters_stream_out_t;

void aeron_print_counters_format(
    int64_t value,
    int32_t id,
    int32_t type_id,
    const uint8_t *key,
    size_t key_length,
    const char *label,
    size_t label_length,
    void *clientd)
{
    char buffer[AERON_COUNTERS_MANAGER_METADATA_LENGTH];
    aeron_print_counters_stream_out_t *out = (aeron_print_counters_stream_out_t *)clientd;

    snprintf(buffer, sizeof(buffer), "%3" PRId32 ": %20" PRId64 " - %.*s\r\n",
        id, value, (int)label_length, label);
    out->stream_out(buffer);
}

void aeron_print_counters(aeron_t *client, void (*stream_out)(const char *))
{
    if (NULL == client || NULL == stream_out)
    {
        return;
    }

    aeron_print_counters_stream_out_t out = { .stream_out = stream_out };

    aeron_counters_reader_foreach_counter(&client->conductor.counters_reader, aeron_print_counters_format, &out);
}

aeron_context_t *aeron_context(aeron_t *client)
{
    if (NULL == client)
    {
        errno = EINVAL;
        AERON_SET_ERR(EINVAL, "aeron_context(NULL): %s", strerror(EINVAL));
        return NULL;
    }

    return client->context;
}

int64_t aeron_client_id(aeron_t *client)
{
    if (NULL == client)
    {
        errno = EINVAL;
        AERON_SET_ERR(EINVAL, "aeron_client_id(NULL): %s", strerror(EINVAL));
        return -1;
    }

    return client->conductor.client_id;
}

int64_t aeron_next_correlation_id(aeron_t *client)
{
    if (NULL == client)
    {
        AERON_SET_ERR(EINVAL, "%s", "client is null");
        return -1;
    }

    return aeron_mpsc_rb_next_correlation_id(&client->conductor.to_driver_buffer);
}

aeron_counters_reader_t *aeron_counters_reader(aeron_t *client)
{
    if (NULL == client)
    {
        errno = EINVAL;
        AERON_SET_ERR(EINVAL, "aeron_counters_reader(NULL): %s", strerror(EINVAL));
        return NULL;
    }

    return &client->conductor.counters_reader;
}

inline static void aeron_async_cmd_free(aeron_client_registering_resource_t *async)
{
    if (NULL != async)
    {
        aeron_free(async->error_message);
        aeron_free(async->uri);

        if (AERON_CLIENT_TYPE_COUNTER == async->type)
        {
            aeron_free((void *)async->counter.key_buffer);
            aeron_free((void *)async->counter.label_buffer);
        }

        aeron_free(async);
    }
}

int64_t aeron_async_add_counter_get_registration_id(aeron_async_add_counter_t *add_counter)
{
    return add_counter->registration_id;
}

int64_t aeron_async_add_publication_get_registration_id(aeron_async_add_publication_t *add_publication)
{
    return add_publication->registration_id;
}

int64_t aeron_async_add_exclusive_exclusive_publication_get_registration_id(
    aeron_async_add_exclusive_publication_t *add_exclusive_publication)
{
    return add_exclusive_publication->registration_id;
}

int64_t aeron_async_add_subscription_get_registration_id(aeron_async_add_subscription_t *add_subscription)
{
    return add_subscription->registration_id;
}

int64_t aeron_async_destination_get_registration_id(aeron_async_destination_t *async_destination)
{
    return async_destination->registration_id;
}

int aeron_async_add_publication(
    aeron_async_add_publication_t **async, aeron_t *client, const char *uri, int32_t stream_id)
{
    if (NULL == async || NULL == client || NULL == uri)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, async: %s, client: %s, uri: %s",
            AERON_NULL_STR(async),
            AERON_NULL_STR(client),
            AERON_NULL_STR(uri));
        return -1;
    }

    return aeron_client_conductor_async_add_publication(async, &client->conductor, uri, stream_id);
}

int aeron_async_add_publication_poll(aeron_publication_t **publication, aeron_async_add_publication_t *async)
{
    if (NULL == publication || NULL == async)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, publication: %s, async: %s",
            AERON_NULL_STR(publication),
            AERON_NULL_STR(async));
        return -1;
    }
    if (AERON_CLIENT_TYPE_PUBLICATION != async->type)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must be valid, async->type: %d (expected: %d)",
            (int)(async->type),
            (int)AERON_CLIENT_TYPE_COUNTER);
    }

    *publication = NULL;

    aeron_client_registration_status_t registration_status;
    AERON_GET_ACQUIRE(registration_status, async->registration_status);

    switch (registration_status)
    {
        case AERON_CLIENT_AWAITING_MEDIA_DRIVER:
        {
            return 0;
        }

        case AERON_CLIENT_ERRORED_MEDIA_DRIVER:
        {
            AERON_SET_ERR(
                -async->error_code,
                "async_add_publication registration\n== Driver Error ==\n%.*s",
                (int)async->error_message_length,
                async->error_message);
            aeron_async_cmd_free(async);
            return -1;
        }

        case AERON_CLIENT_REGISTERED_MEDIA_DRIVER:
        {
            *publication = async->resource.publication;
            aeron_async_cmd_free(async);
            return 1;
        }

        case AERON_CLIENT_TIMEOUT_MEDIA_DRIVER:
        {
            AERON_SET_ERR(
                AERON_CLIENT_ERROR_DRIVER_TIMEOUT, "%s", "async_add_publication no response from media driver");
            aeron_async_cmd_free(async);
            return -1;
        }

        default:
        {
            AERON_SET_ERR(EINVAL, "async_add_publication async status %s", "unknown");
            aeron_async_cmd_free(async);
            return -1;
        }
    }
}

int aeron_async_add_exclusive_publication(
    aeron_async_add_exclusive_publication_t **async, aeron_t *client, const char *uri, int32_t stream_id)
{
    if (NULL == async || NULL == client || NULL == uri)
    {
        AERON_SET_ERR(EINVAL, "aeron_async_add_exclusive_publication: %s", strerror(EINVAL));
        return -1;
    }

    return aeron_client_conductor_async_add_exclusive_publication(async, &client->conductor, uri, stream_id);
}

int aeron_async_add_exclusive_publication_poll(
    aeron_exclusive_publication_t **publication, aeron_async_add_exclusive_publication_t *async)
{
    if (NULL == publication || NULL == async || AERON_CLIENT_TYPE_EXCLUSIVE_PUBLICATION != async->type)
    {
        AERON_SET_ERR(EINVAL, "aeron_async_add_exclusive_publication_poll: %s", strerror(EINVAL));
        return -1;
    }

    *publication = NULL;

    aeron_client_registration_status_t registration_status;
    AERON_GET_ACQUIRE(registration_status, async->registration_status);

    switch (registration_status)
    {
        case AERON_CLIENT_AWAITING_MEDIA_DRIVER:
        {
            return 0;
        }

        case AERON_CLIENT_ERRORED_MEDIA_DRIVER:
        {
            AERON_SET_ERR(
                -async->error_code,
                "async_add_exclusive_publication registration\n== Driver Error ==\n%.*s",
                (int)async->error_message_length,
                async->error_message);
            aeron_async_cmd_free(async);
            return -1;
        }

        case AERON_CLIENT_REGISTERED_MEDIA_DRIVER:
        {
            *publication = async->resource.exclusive_publication;
            aeron_async_cmd_free(async);
            return 1;
        }

        case AERON_CLIENT_TIMEOUT_MEDIA_DRIVER:
        {
            AERON_SET_ERR(
                AERON_CLIENT_ERROR_DRIVER_TIMEOUT, "%s", "async_add_exclusive_publication no response from media driver");
            aeron_async_cmd_free(async);
            return -1;
        }

        default:
        {
            AERON_SET_ERR(EINVAL, "async_add_exclusive_publication async status %s", "unknown");
            aeron_async_cmd_free(async);
            return -1;
        }
    }
}

int aeron_async_add_subscription(
    aeron_async_add_subscription_t **async,
    aeron_t *client,
    const char *uri,
    int32_t stream_id,
    aeron_on_available_image_t on_available_image_handler,
    void *on_available_image_clientd,
    aeron_on_unavailable_image_t on_unavailable_image_handler,
    void *on_unavailable_image_clientd)
{
    if (NULL == async || NULL == client || NULL == uri)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must be valid, async: %s, client: %s, uri: %s",
            AERON_NULL_STR(async),
            AERON_NULL_STR(client),
            AERON_NULL_STR(uri));
        return -1;
    }

    return aeron_client_conductor_async_add_subscription(
        async,
        &client->conductor,
        uri,
        stream_id,
        on_available_image_handler,
        on_available_image_clientd,
        on_unavailable_image_handler,
        on_unavailable_image_clientd);
}

int aeron_async_add_subscription_poll(aeron_subscription_t **subscription, aeron_async_add_subscription_t *async)
{
    if (NULL == subscription || NULL == async)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, subscription: %s, async: %s",
            AERON_NULL_STR(subscription),
            AERON_NULL_STR(async));
        return -1;
    }
    if (AERON_CLIENT_TYPE_SUBSCRIPTION != async->type)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must be valid, async->type: %d (expected: %d)",
            (int)async->type,
            (int)AERON_CLIENT_TYPE_COUNTER);
        return -1;
    }

    *subscription = NULL;

    aeron_client_registration_status_t registration_status;
    AERON_GET_ACQUIRE(registration_status, async->registration_status);

    switch (registration_status)
    {
        case AERON_CLIENT_AWAITING_MEDIA_DRIVER:
        {
            return 0;
        }

        case AERON_CLIENT_ERRORED_MEDIA_DRIVER:
        {
            AERON_SET_ERR(
                -async->error_code,
                "async_add_subscription registration\n== Driver Error ==\n%.*s",
                (int)async->error_message_length,
                async->error_message);
            aeron_async_cmd_free(async);
            return -1;
        }

        case AERON_CLIENT_REGISTERED_MEDIA_DRIVER:
        {
            *subscription = async->resource.subscription;
            aeron_async_cmd_free(async);
            return 1;
        }

        case AERON_CLIENT_TIMEOUT_MEDIA_DRIVER:
        {
            AERON_SET_ERR(
                AERON_CLIENT_ERROR_DRIVER_TIMEOUT, "%s", "async_add_subscription no response from media driver");
            aeron_async_cmd_free(async);
            return -1;
        }

        default:
        {
            AERON_SET_ERR(EINVAL, "async_add_subscription async status %s", "unknown");
            aeron_async_cmd_free(async);
            return -1;
        }
    }
}

int aeron_async_add_counter(
    aeron_async_add_counter_t **async,
    aeron_t *client,
    int32_t type_id,
    const uint8_t *key_buffer,
    size_t key_buffer_length,
    const char *label_buffer,
    size_t label_buffer_length)
{
    if (NULL == async || NULL == client)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s, client: %s",
            AERON_NULL_STR(async),
            AERON_NULL_STR(client));
        return -1;
    }

    return aeron_client_conductor_async_add_counter(
        async,
        &client->conductor,
        type_id,
        key_buffer,
        key_buffer_length,
        label_buffer,
        label_buffer_length);
}

int aeron_async_add_counter_poll(aeron_counter_t **counter, aeron_async_add_counter_t *async)
{
    if (NULL == counter || NULL == async)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, counter: %s, async: %s",
            AERON_NULL_STR(counter),
            AERON_NULL_STR(async));
        return -1;
    }
    if (AERON_CLIENT_TYPE_COUNTER != async->type)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must be valid, async->type: %d (expected: %d)",
            (int)async->type,
            (int)AERON_CLIENT_TYPE_COUNTER);
        return -1;
    }

    *counter = NULL;

    aeron_client_registration_status_t registration_status;
    AERON_GET_ACQUIRE(registration_status, async->registration_status);

    switch (registration_status)
    {
        case AERON_CLIENT_AWAITING_MEDIA_DRIVER:
        {
            return 0;
        }

        case AERON_CLIENT_ERRORED_MEDIA_DRIVER:
        {
            AERON_SET_ERR(
                -async->error_code,
                "async_add_counter registration\n== Driver Error ==\n%.*s",
                (int)async->error_message_length,
                async->error_message);
            aeron_async_cmd_free(async);
            return -1;
        }

        case AERON_CLIENT_REGISTERED_MEDIA_DRIVER:
        {
            *counter = async->resource.counter;
            aeron_async_cmd_free(async);
            return 1;
        }

        case AERON_CLIENT_TIMEOUT_MEDIA_DRIVER:
        {
            AERON_SET_ERR(
                AERON_CLIENT_ERROR_DRIVER_TIMEOUT, "%s", "async_add_counter no response from media driver");
            aeron_async_cmd_free(async);
            return -1;
        }

        default:
        {
            AERON_SET_ERR(EINVAL, "async_add_counter async status %s", "unknown");
            aeron_async_cmd_free(async);
            return -1;
        }
    }
}

int aeron_async_add_static_counter(
    aeron_async_add_counter_t **async,
    aeron_t *client,
    int32_t type_id,
    const uint8_t *key_buffer,
    size_t key_buffer_length,
    const char *label_buffer,
    size_t label_buffer_length,
    int64_t registration_id)
{
    if (NULL == async || NULL == client)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s, client: %s",
            AERON_NULL_STR(async),
            AERON_NULL_STR(client));
        return -1;
    }

    return aeron_client_conductor_async_add_static_counter(
        async,
        &client->conductor,
        type_id,
        key_buffer,
        key_buffer_length,
        label_buffer,
        label_buffer_length,
        registration_id);
}

static int aeron_async_destination_poll(aeron_async_destination_t *async)
{
    if (NULL == async)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s",
            AERON_NULL_STR(async));
        return -1;
    }
    if (AERON_CLIENT_TYPE_DESTINATION != async->type)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must be valid, async->type: %d (expected: %d)",
            (int)async->type,
            (int)AERON_CLIENT_TYPE_COUNTER);
        return -1;
    }

    aeron_client_registration_status_t registration_status;
    AERON_GET_ACQUIRE(registration_status, async->registration_status);

    switch (registration_status)
    {
        case AERON_CLIENT_AWAITING_MEDIA_DRIVER:
        {
            return 0;
        }

        case AERON_CLIENT_ERRORED_MEDIA_DRIVER:
        {
            AERON_SET_ERR(
                -async->error_code,
                "async_add_destination registration\n== Driver Error ==\n%.*s",
                (int)async->error_message_length,
                async->error_message);
            aeron_async_cmd_free(async);
            return -1;
        }

        case AERON_CLIENT_REGISTERED_MEDIA_DRIVER:
        {
            aeron_async_cmd_free(async);
            return 1;
        }

        case AERON_CLIENT_TIMEOUT_MEDIA_DRIVER:
        {
            AERON_SET_ERR(
                AERON_CLIENT_ERROR_DRIVER_TIMEOUT, "%s", "async_add_destination no response from media driver");
            aeron_async_cmd_free(async);
            return -1;
        }

        default:
        {
            AERON_SET_ERR(EINVAL, "async_add_destination async status %s", "unknown");
            aeron_async_cmd_free(async);
            return -1;
        }
    }
}

int aeron_publication_async_add_destination(
    aeron_async_destination_t **async, aeron_t *client, aeron_publication_t *publication, const char *uri)
{
    if (NULL == async || NULL == client || NULL == publication || uri == NULL)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s, client: %d, publication: %s, uri: %s",
            AERON_NULL_STR(async),
            AERON_NULL_STR(client),
            AERON_NULL_STR(publication),
            AERON_NULL_STR(uri));
        return -1;
    }

    return aeron_client_conductor_async_add_publication_destination(async, &client->conductor, publication, uri);
}

int aeron_publication_async_destination_poll(aeron_async_destination_t *async)
{
    return aeron_async_destination_poll(async);
}

int aeron_publication_async_remove_destination(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_publication_t *publication,
    const char *uri)
{
    if (NULL == async || NULL == client || NULL == publication || uri == NULL)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s, client: %s, publication: %s, uri: %s",
            AERON_NULL_STR(async),
            AERON_NULL_STR(client),
            AERON_NULL_STR(publication),
            AERON_NULL_STR(uri));
        return -1;
    }

    return aeron_client_conductor_async_remove_publication_destination(async, &client->conductor, publication, uri);
}

int aeron_publication_async_remove_destination_by_id(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_publication_t *publication,
    int64_t destination_registration_id)
{
    if (NULL == async || NULL == client || NULL == publication)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s, client: %s, publication: %s, destination_registration_id: %" PRId64,
            AERON_NULL_STR(async),
            AERON_NULL_STR(client),
            AERON_NULL_STR(publication),
            destination_registration_id);
        return -1;
    }

    return aeron_client_conductor_async_remove_publication_destination_by_id(
        async, &client->conductor, publication, destination_registration_id);
}

int aeron_subscription_async_add_destination(
    aeron_async_destination_t **async, aeron_t *client, aeron_subscription_t *subscription, const char *uri)
{
    if (NULL == async || NULL == client || NULL == subscription || uri == NULL)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s, client: %d, subscription: %s, uri: %s",
            AERON_NULL_STR(async),
            AERON_NULL_STR(client),
            AERON_NULL_STR(subscription),
            AERON_NULL_STR(uri));
        return -1;
    }

    return aeron_client_conductor_async_add_subscription_destination(async, &client->conductor, subscription, uri);
}

int aeron_subscription_async_destination_poll(aeron_async_destination_t *async)
{
    return aeron_async_destination_poll(async);
}

int aeron_subscription_async_remove_destination(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_subscription_t *subscription,
    const char *uri)
{
    if (NULL == async || NULL == client || NULL == subscription || uri == NULL)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s, client: %d, subscription: %s, uri: %s",
            AERON_NULL_STR(async),
            AERON_NULL_STR(client),
            AERON_NULL_STR(subscription),
            AERON_NULL_STR(uri));
        return -1;
    }

    return aeron_client_conductor_async_remove_subscription_destination(async, &client->conductor, subscription, uri);
}

int aeron_exclusive_publication_async_add_destination(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_exclusive_publication_t *publication,
    const char *uri)
{
    if (NULL == async || NULL == client || NULL == publication || uri == NULL)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s, client: %s, publication: %s, uri: %s",
            AERON_NULL_STR(async),
            AERON_NULL_STR(client),
            AERON_NULL_STR(publication),
            AERON_NULL_STR(uri));
        return -1;
    }

    return aeron_client_conductor_async_add_exclusive_publication_destination(
        async, &client->conductor, publication, uri);
}

int aeron_exclusive_publication_async_destination_poll(aeron_async_destination_t *async)
{
    return aeron_async_destination_poll(async);
}

int aeron_exclusive_publication_async_remove_destination(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_exclusive_publication_t *publication,
    const char *uri)
{
    if (NULL == async || NULL == client || NULL == publication || uri == NULL)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s, client: %s, publication: %s, uri: %s",
            AERON_NULL_STR(async),
            AERON_NULL_STR(client),
            AERON_NULL_STR(publication),
            AERON_NULL_STR(uri));
        return -1;
    }

    return aeron_client_conductor_async_remove_exclusive_publication_destination(
        async, &client->conductor, publication, uri);
}

int aeron_exclusive_publication_async_remove_destination_by_id(
    aeron_async_destination_t **async,
    aeron_t *client,
    aeron_exclusive_publication_t *publication,
    int64_t destination_registration_id)
{
    if (NULL == async || NULL == client || NULL == publication)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s, client: %s, publication: %s, destination_registration_id: %" PRId64,
            AERON_NULL_STR(async),
            AERON_NULL_STR(client),
            AERON_NULL_STR(publication),
            destination_registration_id);
        return -1;
    }

    return aeron_client_conductor_async_remove_exclusive_publication_destination_by_id(
        async, &client->conductor, publication, destination_registration_id);
}

int aeron_client_handler_cmd_await_processed(aeron_client_handler_cmd_t *cmd, uint64_t timeout_ms)
{
    bool processed = cmd->processed;
    int64_t deadline_ms = (int64_t)(aeron_epoch_clock() + timeout_ms);
    
    while (!processed)
    {
        if (deadline_ms <= aeron_epoch_clock())
        {
            AERON_SET_ERR(ETIMEDOUT, "%s", "time out waiting for client conductor thread to process message");
            return -1;
        }

        sched_yield();
        AERON_GET_ACQUIRE(processed, cmd->processed);
    }

    return 0;
}

int aeron_add_available_counter_handler(aeron_t *client, aeron_on_available_counter_pair_t *pair)
{
    aeron_client_handler_cmd_t cmd;

    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    cmd.type = AERON_CLIENT_HANDLER_ADD_AVAILABLE_COUNTER;
    cmd.handler.on_available_counter = pair->handler;
    cmd.clientd = pair->clientd;
    cmd.processed = false;

    if (aeron_client_conductor_async_handler(&client->conductor, &cmd) < 0)
    {
        return -1;
    }

    return aeron_client_handler_cmd_await_processed(&cmd, aeron_context_get_driver_timeout_ms(client->context));
}

int aeron_remove_available_counter_handler(aeron_t *client, aeron_on_available_counter_pair_t *pair)
{
    aeron_client_handler_cmd_t cmd;

    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    cmd.type = AERON_CLIENT_HANDLER_REMOVE_AVAILABLE_COUNTER;
    cmd.handler.on_available_counter = pair->handler;
    cmd.clientd = pair->clientd;
    cmd.processed = false;

    if (aeron_client_conductor_async_handler(&client->conductor, &cmd) < 0)
    {
        return -1;
    }

    return aeron_client_handler_cmd_await_processed(&cmd, aeron_context_get_driver_timeout_ms(client->context));
}

int aeron_add_unavailable_counter_handler(aeron_t *client, aeron_on_unavailable_counter_pair_t *pair)
{
    aeron_client_handler_cmd_t cmd;

    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    cmd.type = AERON_CLIENT_HANDLER_ADD_UNAVAILABLE_COUNTER;
    cmd.handler.on_unavailable_counter = pair->handler;
    cmd.clientd = pair->clientd;
    cmd.processed = false;

    if (aeron_client_conductor_async_handler(&client->conductor, &cmd) < 0)
    {
        return -1;
    }

    return aeron_client_handler_cmd_await_processed(&cmd, aeron_context_get_driver_timeout_ms(client->context));
}

int aeron_remove_unavailable_counter_handler(aeron_t *client, aeron_on_unavailable_counter_pair_t *pair)
{
    aeron_client_handler_cmd_t cmd;

    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    cmd.type = AERON_CLIENT_HANDLER_REMOVE_UNAVAILABLE_COUNTER;
    cmd.handler.on_unavailable_counter = pair->handler;
    cmd.clientd = pair->clientd;
    cmd.processed = false;

    if (aeron_client_conductor_async_handler(&client->conductor, &cmd) < 0)
    {
        return -1;
    }

    return aeron_client_handler_cmd_await_processed(&cmd, aeron_context_get_driver_timeout_ms(client->context));
}

int aeron_add_close_handler(aeron_t *client, aeron_on_close_client_pair_t *pair)
{
    aeron_client_handler_cmd_t cmd;

    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    cmd.type = AERON_CLIENT_HANDLER_ADD_CLOSE_HANDLER;
    cmd.handler.on_close_handler = pair->handler;
    cmd.clientd = pair->clientd;
    cmd.processed = false;

    if (aeron_client_conductor_async_handler(&client->conductor, &cmd) < 0)
    {
        return -1;
    }

    return aeron_client_handler_cmd_await_processed(&cmd, aeron_context_get_driver_timeout_ms(client->context));
}

int aeron_remove_close_handler(aeron_t *client, aeron_on_close_client_pair_t *pair)
{
    aeron_client_handler_cmd_t cmd;

    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    cmd.type = AERON_CLIENT_HANDLER_REMOVE_CLOSE_HANDLER;
    cmd.handler.on_close_handler = pair->handler;
    cmd.clientd = pair->clientd;
    cmd.processed = false;

    if (aeron_client_conductor_async_handler(&client->conductor, &cmd) < 0)
    {
        return -1;
    }

    return aeron_client_handler_cmd_await_processed(&cmd, aeron_context_get_driver_timeout_ms(client->context));
}
