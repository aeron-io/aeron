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

#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>

#include "aeronc.h"
#include "aeron_client.h"

static int aeron_do_work(void *clientd)
{
    aeron_t *client = (aeron_t *)clientd;
    return aeron_client_conductor_do_work(&client->conductor);
}

static void aeron_on_close(void *clientd)
{
    aeron_t *client = (aeron_t *)clientd;
    aeron_client_conductor_on_close(&client->conductor);
}

static int aeron_validate_async_resource(
    aeron_client_registering_resource_t *async,
    const aeron_client_managed_resource_type_t expected_type)
{
    if (expected_type != async->type)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must be valid, async->type: %d (expected: %d)",
            (int)async->type,
            (int)expected_type);
        return -1;
    }
    return 0;
}

static int aeron_async_destination_poll(aeron_async_destination_t *async)
{
    void *tmp;
    return aeron_client_conductor_async_resource_poll(&tmp, AERON_CLIENT_MANAGED_RESOURCE_TYPE_DESTINATION, async);
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

    _client->runner.state = AERON_AGENT_STATE_UNUSED;
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
        "aeron-client",
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

int64_t aeron_async_add_counter_get_registration_id(aeron_async_add_counter_t *add_counter)
{
    return add_counter->registration_id;
}

int64_t aeron_async_add_publication_get_registration_id(aeron_async_add_publication_t *add_publication)
{
    return add_publication->registration_id;
}

// deprecated
int64_t aeron_async_add_exclusive_exclusive_publication_get_registration_id(
    aeron_async_add_exclusive_publication_t *add_exclusive_publication)
{
    return add_exclusive_publication->registration_id;
}

int64_t aeron_async_add_exclusive_publication_get_registration_id(
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
    return aeron_client_conductor_async_resource_poll(
        (void **)publication, AERON_CLIENT_MANAGED_RESOURCE_TYPE_PUBLICATION, async);
}

int aeron_async_add_publication_cancel(aeron_t *client, aeron_async_add_publication_t *async)
{
    if (NULL == async)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, async: %s", AERON_NULL_STR(async));
        return -1;
    }

    if (aeron_validate_async_resource(async, AERON_CLIENT_MANAGED_RESOURCE_TYPE_PUBLICATION) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return aeron_async_remove_publication(aeron_async_add_publication_get_registration_id(async), client, NULL, NULL);
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
    return aeron_client_conductor_async_resource_poll(
        (void **)publication, AERON_CLIENT_MANAGED_RESOURCE_TYPE_EXCLUSIVE_PUBLICATION, async);
}

int aeron_async_add_exclusive_publication_cancel(aeron_t *client, aeron_async_add_exclusive_publication_t *async)
{
    if (NULL == async)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, async: %s", AERON_NULL_STR(async));
        return -1;
    }

    if (aeron_validate_async_resource(async, AERON_CLIENT_MANAGED_RESOURCE_TYPE_EXCLUSIVE_PUBLICATION) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return aeron_async_remove_exclusive_publication(
        aeron_async_add_exclusive_publication_get_registration_id(async), client, NULL, NULL);
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
    return aeron_client_conductor_async_resource_poll(
        (void **)subscription, AERON_CLIENT_MANAGED_RESOURCE_TYPE_SUBSCRIPTION, async);
}

int aeron_async_add_subscription_cancel(aeron_t *client, aeron_async_add_subscription_t *async)
{
    if (NULL == async)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, async: %s", AERON_NULL_STR(async));
        return -1;
    }

    if (aeron_validate_async_resource(async, AERON_CLIENT_MANAGED_RESOURCE_TYPE_SUBSCRIPTION) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return aeron_async_remove_subscription(aeron_async_add_subscription_get_registration_id(async), client, NULL, NULL);
}

static int aeron_async_remove_resource(
    int64_t registration_id,
    aeron_client_managed_resource_type_t type,
    aeron_t *client,
    aeron_notification_t on_complete,
    void *on_complete_clientd)
{
    if (NULL == client)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must be valid, client: %s",
            AERON_NULL_STR(client));
        return -1;
    }

    return aeron_client_conductor_async_remove_resource(
        registration_id,
        type,
        &client->conductor,
        on_complete,
        on_complete_clientd);
}

int aeron_async_remove_subscription(
    int64_t registration_id, aeron_t *client, aeron_notification_t on_complete, void *on_complete_clientd)
{
    return aeron_async_remove_resource(
        registration_id,
        AERON_CLIENT_MANAGED_RESOURCE_TYPE_SUBSCRIPTION,
        client,
        on_complete,
        on_complete_clientd);
}

int aeron_async_remove_publication(
    int64_t registration_id, aeron_t *client, aeron_notification_t on_complete, void *on_complete_clientd)
{
    return aeron_async_remove_resource(
        registration_id,
        AERON_CLIENT_MANAGED_RESOURCE_TYPE_PUBLICATION,
        client,
        on_complete,
        on_complete_clientd);
}

int aeron_async_remove_exclusive_publication(
    int64_t registration_id, aeron_t *client, aeron_notification_t on_complete, void *on_complete_clientd)
{
    return aeron_async_remove_resource(
        registration_id,
        AERON_CLIENT_MANAGED_RESOURCE_TYPE_EXCLUSIVE_PUBLICATION,
        client,
        on_complete,
        on_complete_clientd);
}

int aeron_async_remove_counter(
    int64_t registration_id, aeron_t *client, aeron_notification_t on_complete, void *on_complete_clientd)
{
    return aeron_async_remove_resource(
        registration_id,
        AERON_CLIENT_MANAGED_RESOURCE_TYPE_COUNTER,
        client,
        on_complete,
        on_complete_clientd);
}

int aeron_async_next_session_id(aeron_async_get_next_available_session_id_t **async, aeron_t *client, int32_t stream_id)
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

    return aeron_client_conductor_async_get_next_available_session_id(async, &client->conductor, stream_id);
}

int aeron_async_next_session_id_poll(int32_t *next_session_id, aeron_async_get_next_available_session_id_t *async)
{
    return aeron_client_conductor_async_resource_poll(
        (void**)next_session_id, AERON_CLIENT_MANAGED_RESOURCE_TYPE_NEXT_AVAILABLE_SESSION_ID, async);
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
    if (NULL == async)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must not be null, async: %s",
            AERON_NULL_STR(async));
        return -1;
    }

    int resource_type = AERON_CLIENT_MANAGED_RESOURCE_TYPE_STATIC_COUNTER == async->type ?
        AERON_CLIENT_MANAGED_RESOURCE_TYPE_STATIC_COUNTER : AERON_CLIENT_MANAGED_RESOURCE_TYPE_COUNTER;

    return aeron_client_conductor_async_resource_poll((void **)counter, resource_type, async);
}

int aeron_async_add_counter_cancel(aeron_t *client, aeron_async_add_counter_t *async)
{
    if (NULL == async)
    {
        AERON_SET_ERR(EINVAL, "Parameters must not be null, async: %s", AERON_NULL_STR(async));
        return -1;
    }

    if (AERON_CLIENT_MANAGED_RESOURCE_TYPE_COUNTER != async->type &&
        AERON_CLIENT_MANAGED_RESOURCE_TYPE_STATIC_COUNTER != async->type)
    {
        AERON_SET_ERR(
            EINVAL,
            "Parameters must be valid, async->type: %d (expected: %d or %d)",
            (int)(async->type),
            (int)AERON_CLIENT_MANAGED_RESOURCE_TYPE_COUNTER,
            (int)AERON_CLIENT_MANAGED_RESOURCE_TYPE_STATIC_COUNTER);
        return -1;
    }

    return aeron_async_remove_resource(
        aeron_async_add_counter_get_registration_id(async), async->type, client, NULL, NULL);
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

int aeron_add_available_counter_handler(aeron_t *client, aeron_on_available_counter_pair_t *pair)
{
    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    if (aeron_client_conductor_is_closed(&client->conductor))
    {
        AERON_SET_ERR(EPERM, "%s", "client conductor is closed");
        return -1;
    }

    aeron_client_handler_cmd_t cmd;
    cmd.type = AERON_CLIENT_HANDLER_ADD_AVAILABLE_COUNTER;
    cmd.handler.on_available_counter = pair->handler;
    cmd.clientd = pair->clientd;

    return aeron_client_conductor_async_handler(&client->conductor, &cmd);
}

int aeron_remove_available_counter_handler(aeron_t *client, aeron_on_available_counter_pair_t *pair)
{
    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    if (aeron_client_conductor_is_closed(&client->conductor))
    {
        AERON_SET_ERR(EPERM, "%s", "client conductor is closed");
        return -1;
    }

    aeron_client_handler_cmd_t cmd;
    cmd.type = AERON_CLIENT_HANDLER_REMOVE_AVAILABLE_COUNTER;
    cmd.handler.on_available_counter = pair->handler;
    cmd.clientd = pair->clientd;

    return aeron_client_conductor_async_handler(&client->conductor, &cmd);
}

int aeron_add_unavailable_counter_handler(aeron_t *client, aeron_on_unavailable_counter_pair_t *pair)
{
    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    if (aeron_client_conductor_is_closed(&client->conductor))
    {
        AERON_SET_ERR(EPERM, "%s", "client conductor is closed");
        return -1;
    }

    aeron_client_handler_cmd_t cmd;
    cmd.type = AERON_CLIENT_HANDLER_ADD_UNAVAILABLE_COUNTER;
    cmd.handler.on_unavailable_counter = pair->handler;
    cmd.clientd = pair->clientd;

    return aeron_client_conductor_async_handler(&client->conductor, &cmd);
}

int aeron_remove_unavailable_counter_handler(aeron_t *client, aeron_on_unavailable_counter_pair_t *pair)
{
    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    if (aeron_client_conductor_is_closed(&client->conductor))
    {
        AERON_SET_ERR(EPERM, "%s", "client conductor is closed");
        return -1;
    }

    aeron_client_handler_cmd_t cmd;
    cmd.type = AERON_CLIENT_HANDLER_REMOVE_UNAVAILABLE_COUNTER;
    cmd.handler.on_unavailable_counter = pair->handler;
    cmd.clientd = pair->clientd;

    return aeron_client_conductor_async_handler(&client->conductor, &cmd);
}

int aeron_add_close_handler(aeron_t *client, aeron_on_close_client_pair_t *pair)
{
    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    if (aeron_client_conductor_is_closed(&client->conductor))
    {
        AERON_SET_ERR(EPERM, "%s", "client conductor is closed");
        return -1;
    }

    aeron_client_handler_cmd_t cmd;
    cmd.type = AERON_CLIENT_HANDLER_ADD_CLOSE_HANDLER;
    cmd.handler.on_close_handler = pair->handler;
    cmd.clientd = pair->clientd;

    return aeron_client_conductor_async_handler(&client->conductor, &cmd);
}

int aeron_remove_close_handler(aeron_t *client, aeron_on_close_client_pair_t *pair)
{
    if (NULL == client || NULL == pair)
    {
        AERON_SET_ERR(
            EINVAL, "Parameters must not be null, client: %s, pair: %s", AERON_NULL_STR(client), AERON_NULL_STR(pair));
        return -1;
    }

    if (aeron_client_conductor_is_closed(&client->conductor))
    {
        AERON_SET_ERR(EPERM, "%s", "client conductor is closed");
        return -1;
    }

    aeron_client_handler_cmd_t cmd;
    cmd.type = AERON_CLIENT_HANDLER_REMOVE_CLOSE_HANDLER;
    cmd.handler.on_close_handler = pair->handler;
    cmd.clientd = pair->clientd;

    return aeron_client_conductor_async_handler(&client->conductor, &cmd);
}
