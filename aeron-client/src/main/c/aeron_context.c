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

#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>

#include "concurrent/aeron_mpsc_rb.h"
#include "util/aeron_fileutil.h"
#include "aeron_cnc_file_descriptor.h"
#include "command/aeron_control_protocol.h"

#include "aeron_alloc.h"
#include "aeron_context.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "util/aeron_strutil.h"

#define AERON_CONTEXT_USE_CONDUCTOR_AGENT_INVOKER_DEFAULT (false)
#define AERON_CONTEXT_DRIVER_TIMEOUT_MS_DEFAULT (10 * 1000L)
#define AERON_CONTEXT_KEEPALIVE_INTERVAL_NS_DEFAULT (500 * 1000 * 1000LL)
#define AERON_CONTEXT_RESOURCE_LINGER_DURATION_NS_DEFAULT (3 * 1000 * 1000 * 1000LL)
#define AERON_CONTEXT_IDLE_SLEEP_DURATION_NS_DEFAULT (16 * 1000 * 1000LL)
#define AERON_CONTEXT_PRE_TOUCH_MAPPED_MEMORY_DEFAULT (false)

void aeron_default_error_handler(void *clientd, int errcode, const char *message)
{
    fprintf(stderr, "ERROR: (%d): %s\n", errcode, message);
    exit(EXIT_FAILURE);
}

void aeron_default_error_frame_handler(void *clientd, aeron_publication_error_values_t *message)
{
}

int aeron_context_init(aeron_context_t **context)
{
    aeron_context_t *_context = NULL;

    if (NULL == context)
    {
        AERON_SET_ERR(EINVAL, "%s", "aeron_context_init(NULL)");
        goto error;
    }

    if (aeron_alloc((void **)&_context, sizeof(aeron_context_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate aeron_context");
        goto error;
    }

    const size_t command_buffer_capacity = AERON_CLIENT_COMMAND_QUEUE_CAPACITY * 64 + AERON_RB_TRAILER_LENGTH;
    size_t offset;
    if (aeron_alloc_aligned((void **)&_context->command_buffer, &offset, command_buffer_capacity, AERON_CACHE_LINE_LENGTH) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to allocate buffer for commands");
        goto error;
    }

    if (aeron_mpsc_rb_init(&_context->command_rb, _context->command_buffer + offset, command_buffer_capacity) < 0)
    {
        AERON_APPEND_ERR("%s", "Unable to init command_rb");
        goto error;
    }

    if (aeron_default_path(_context->aeron_dir, sizeof(_context->aeron_dir)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to resolve default aeron directory path");
        goto error;
    }

    _context->client_name[0] = '\0';
    _context->error_handler = aeron_default_error_handler;
    _context->error_handler_clientd = NULL;
    _context->error_frame_handler = aeron_default_error_frame_handler;
    _context->on_new_publication = NULL;
    _context->on_new_publication_clientd = NULL;
    _context->on_new_exclusive_publication = NULL;
    _context->on_new_exclusive_publication_clientd = NULL;
    _context->on_new_subscription = NULL;
    _context->on_new_subscription_clientd = NULL;
    _context->on_available_counter = NULL;
    _context->on_available_counter_clientd = NULL;
    _context->on_unavailable_counter = NULL;
    _context->on_unavailable_counter_clientd = NULL;
    _context->on_close_client = NULL;
    _context->on_close_client_clientd = NULL;

    _context->idle_strategy_name = NULL;
    _context->idle_strategy_init_args = NULL;
    _context->idle_strategy_state = NULL;
    _context->idle_strategy_func = NULL;

    _context->use_conductor_agent_invoker = AERON_CONTEXT_USE_CONDUCTOR_AGENT_INVOKER_DEFAULT;
    _context->agent_on_start_func = NULL;
    _context->agent_on_start_state = NULL;

    _context->driver_timeout_ms = AERON_CONTEXT_DRIVER_TIMEOUT_MS_DEFAULT;
    _context->keepalive_interval_ns = AERON_CONTEXT_KEEPALIVE_INTERVAL_NS_DEFAULT;
    _context->resource_linger_duration_ns = AERON_CONTEXT_RESOURCE_LINGER_DURATION_NS_DEFAULT;
    _context->idle_sleep_duration_ns = AERON_CONTEXT_IDLE_SLEEP_DURATION_NS_DEFAULT;

    _context->epoch_clock = aeron_epoch_clock;
    _context->nano_clock = aeron_nano_clock;

    char *value = NULL;

    if ((value = getenv(AERON_DIR_ENV_VAR)))
    {
        if (aeron_context_set_dir(_context, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    if ((value = getenv(AERON_CLIENT_NAME_ENV_VAR)))
    {
        if (aeron_context_set_client_name(_context, value) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }
    }

    if ((value = getenv(AERON_DRIVER_TIMEOUT_ENV_VAR)))
    {
        errno = 0;
        char *end_ptr = NULL;
        uint64_t result = strtoull(value, &end_ptr, 0);

        if ((0 == result && 0 != errno) || '\0' != *end_ptr)
        {
            AERON_SET_ERR(EINVAL, "could not parse driver timeout: %s=%s", AERON_DRIVER_TIMEOUT_ENV_VAR, value);
            goto error;
        }

        _context->driver_timeout_ms = result;
    }

    if ((value = getenv(AERON_CLIENT_RESOURCE_LINGER_DURATION_ENV_VAR)))
    {
        uint64_t result;
        if (aeron_parse_duration_ns(value, &result) < 0)
        {
            AERON_SET_ERR(EINVAL, "could not parse: %s=%s", AERON_CLIENT_RESOURCE_LINGER_DURATION_ENV_VAR, value);
            goto error;
        }

        _context->resource_linger_duration_ns = result;
    }

    if ((value = getenv(AERON_CLIENT_IDLE_SLEEP_DURATION_ENV_VAR)))
    {
        uint64_t result;
        if (aeron_parse_duration_ns(value, &result) < 0)
        {
            AERON_SET_ERR(EINVAL, "could not parse: %s=%s", AERON_CLIENT_IDLE_SLEEP_DURATION_ENV_VAR, value);
            goto error;
        }

        _context->idle_sleep_duration_ns = result;
    }

    _context->pre_touch_mapped_memory = aeron_parse_bool(
        getenv(AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY_ENV_VAR), AERON_CONTEXT_PRE_TOUCH_MAPPED_MEMORY_DEFAULT);

    _context->idle_strategy_name = aeron_strndup("sleep-ns", AERON_MAX_PATH);

    size_t init_args_size = 21; // 20 (UINT64_MAX) + 1
    if (aeron_alloc((void**)&_context->idle_strategy_init_args, init_args_size) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    int rc = snprintf(_context->idle_strategy_init_args, init_args_size, "%" PRIu64, _context->idle_sleep_duration_ns);
    if (rc < 0 || rc >= (int)init_args_size)
    {
        AERON_SET_ERR(EINVAL, "error formatting idle_strategy_init_args=%d", rc);
        goto error;
    }

    if ((_context->idle_strategy_func = aeron_idle_strategy_load(
        _context->idle_strategy_name,
        &_context->idle_strategy_state,
        NULL,
        _context->idle_strategy_init_args)) == NULL)
    {
        goto error;
    }

    *context = _context;
    return 0;

error:
    aeron_context_close(_context);
    return -1;
}

int aeron_context_close(aeron_context_t *context)
{
    if (NULL != context)
    {
        aeron_unmap(&context->cnc_map);

        aeron_free(context->command_buffer);
        aeron_free(context->idle_strategy_state);
        aeron_free(context->idle_strategy_init_args);
        aeron_free((void*)context->idle_strategy_name);
        aeron_free(context);
    }

    return 0;
}

#define AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(r, a) \
do \
{ \
    if (NULL == (a)) \
    { \
        AERON_SET_ERR(EINVAL, "%s is null", "##a"); \
        return (r); \
    } \
} \
while (false) \


int aeron_context_set_dir(aeron_context_t *context, const char *value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    snprintf(context->aeron_dir, sizeof(context->aeron_dir), "%s", value);
    return 0;
}

const char *aeron_context_get_dir(aeron_context_t *context)
{
    return NULL != context ? context->aeron_dir : NULL;
}

int aeron_context_set_client_name(aeron_context_t *context, const char *value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    size_t copy_length = 0;
    if (!aeron_str_length(value, AERON_COUNTER_MAX_CLIENT_NAME_LENGTH + 1, &copy_length))
    {
        AERON_SET_ERR(EINVAL, "client_name length must <= %d", AERON_COUNTER_MAX_CLIENT_NAME_LENGTH);
        return -1;
    }

    memcpy(context->client_name, value, copy_length);
    context->client_name[copy_length] = '\0';
    return 0;
}

const char *aeron_context_get_client_name(aeron_context_t *context)
{
    return NULL != context ? context->client_name : "";
}

int aeron_context_set_driver_timeout_ms(aeron_context_t *context, uint64_t value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->driver_timeout_ms = value;
    return 0;
}

uint64_t aeron_context_get_driver_timeout_ms(aeron_context_t *context)
{
    return NULL == context ? AERON_CONTEXT_DRIVER_TIMEOUT_MS_DEFAULT : context->driver_timeout_ms;
}

int aeron_context_set_keepalive_interval_ns(aeron_context_t *context, uint64_t value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->keepalive_interval_ns = value;
    return 0;
}

uint64_t aeron_context_get_keepalive_interval_ns(aeron_context_t *context)
{
    return NULL == context ? AERON_CONTEXT_KEEPALIVE_INTERVAL_NS_DEFAULT : context->keepalive_interval_ns;
}

int aeron_context_set_resource_linger_duration_ns(aeron_context_t *context, uint64_t value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->resource_linger_duration_ns = value;
    return 0;
}

uint64_t aeron_context_get_resource_linger_duration_ns(aeron_context_t *context)
{
    return NULL == context ? AERON_CONTEXT_RESOURCE_LINGER_DURATION_NS_DEFAULT : context->resource_linger_duration_ns;
}

int aeron_context_set_idle_sleep_duration_ns(aeron_context_t *context, uint64_t value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->idle_sleep_duration_ns = value;
    return 0;
}

uint64_t aeron_context_get_idle_sleep_duration_ns(aeron_context_t *context)
{
    return NULL == context ? AERON_CONTEXT_IDLE_SLEEP_DURATION_NS_DEFAULT : context->idle_sleep_duration_ns;
}

int aeron_context_set_idle_strategy_init_args(aeron_context_t *context, const char *value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    aeron_free(context->idle_strategy_init_args);
    context->idle_strategy_init_args = NULL == value ? NULL : aeron_strndup(value, AERON_MAX_PATH);

    return 0;
}

const char *aeron_context_get_idle_strategy_init_args(aeron_context_t *context)
{
    return NULL != context ? context->idle_strategy_init_args : NULL;
}

int aeron_context_set_idle_strategy(aeron_context_t *context, const char *value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, value);

    aeron_free(context->idle_strategy_state);
    aeron_free((void *)context->idle_strategy_name);

    context->idle_strategy_state = NULL;
    context->idle_strategy_name = NULL;

    if ((context->idle_strategy_func = aeron_idle_strategy_load(
        value,
        &context->idle_strategy_state,
        NULL,
        context->idle_strategy_init_args)) == NULL)
    {
        return -1;
    }

    context->idle_strategy_name = aeron_strndup(value, AERON_MAX_PATH);
    return 0;
}

const char *aeron_context_get_idle_strategy(aeron_context_t *context)
{
    return NULL != context ? context->idle_strategy_name : "sleep-ns";
}

int aeron_context_set_pre_touch_mapped_memory(aeron_context_t *context, bool value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->pre_touch_mapped_memory = value;
    return 0;
}

bool aeron_context_get_pre_touch_mapped_memory(aeron_context_t *context)
{
    return NULL != context ? context->pre_touch_mapped_memory : AERON_CONTEXT_PRE_TOUCH_MAPPED_MEMORY_DEFAULT;
}

int aeron_context_set_error_handler(aeron_context_t *context, aeron_error_handler_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->error_handler = handler;
    context->error_handler_clientd = clientd;
    return 0;
}

aeron_error_handler_t aeron_context_get_error_handler(aeron_context_t *context)
{
    return NULL != context ? context->error_handler : aeron_default_error_handler;
}

void *aeron_context_get_error_handler_clientd(aeron_context_t *context)
{
    return NULL != context ? context->error_handler_clientd : NULL;
}

int aeron_context_set_publication_error_frame_handler(aeron_context_t *context, aeron_publication_error_frame_handler_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->error_frame_handler = handler;
    context->error_frame_handler_clientd = clientd;
    return 0;
}

aeron_publication_error_frame_handler_t aeron_context_get_publication_error_frame_handler(aeron_context_t *context)
{
    return NULL != context ? context->error_frame_handler : NULL;
}

void *aeron_context_get_publication_error_frame_handler_clientd(aeron_context_t *context)
{
    return NULL != context ? context->error_frame_handler_clientd : NULL;
}


int aeron_context_set_on_new_publication(aeron_context_t *context, aeron_on_new_publication_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_new_publication = handler;
    context->on_new_publication_clientd = clientd;
    return 0;
}

aeron_on_new_publication_t aeron_context_get_on_new_publication(aeron_context_t *context)
{
    return NULL != context ? context->on_new_publication : NULL;
}

void *aeron_context_get_on_new_publication_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_new_publication_clientd : NULL;
}

int aeron_context_set_on_new_exclusive_publication(
    aeron_context_t *context, aeron_on_new_publication_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_new_exclusive_publication = handler;
    context->on_new_exclusive_publication_clientd = clientd;
    return 0;
}

aeron_on_new_publication_t aeron_context_get_on_new_exclusive_publication(aeron_context_t *context)
{
    return NULL != context ? context->on_new_exclusive_publication : NULL;
}

void *aeron_context_get_on_new_exclusive_publication_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_new_exclusive_publication_clientd : NULL;
}

int aeron_context_set_on_new_subscription(
    aeron_context_t *context, aeron_on_new_subscription_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_new_subscription = handler;
    context->on_new_subscription_clientd = clientd;
    return 0;
}

aeron_on_new_subscription_t aeron_context_get_on_new_subscription(aeron_context_t *context)
{
    return NULL != context ? context->on_new_subscription : NULL;
}

void *aeron_context_get_on_new_subscription_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_new_subscription_clientd : NULL;
}

int aeron_context_set_on_available_counter(
    aeron_context_t *context, aeron_on_available_counter_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_available_counter = handler;
    context->on_available_counter_clientd = clientd;
    return 0;
}

aeron_on_available_counter_t aeron_context_get_on_available_counter(aeron_context_t *context)
{
    return NULL != context ? context->on_available_counter : NULL;
}

void *aeron_context_get_on_available_counter_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_available_counter_clientd : NULL;
}

int aeron_context_set_on_unavailable_counter(
    aeron_context_t *context, aeron_on_unavailable_counter_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_unavailable_counter = handler;
    context->on_unavailable_counter_clientd = clientd;
    return 0;
}

aeron_on_unavailable_counter_t aeron_context_get_on_unavailable_counter(aeron_context_t *context)
{
    return NULL != context ? context->on_unavailable_counter : NULL;
}

void *aeron_context_get_on_unavailable_counter_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_unavailable_counter_clientd : NULL;
}

int aeron_context_set_on_close_client(
    aeron_context_t *context, aeron_on_close_client_t handler, void *clientd)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->on_close_client = handler;
    context->on_close_client_clientd = clientd;
    return 0;
}

aeron_on_close_client_t aeron_context_get_on_close_client(aeron_context_t *context)
{
    return NULL != context ? context->on_close_client : NULL;
}

void *aeron_context_get_on_close_client_clientd(aeron_context_t *context)
{
    return NULL != context ? context->on_close_client_clientd : NULL;
}

int aeron_context_set_use_conductor_agent_invoker(aeron_context_t *context, bool value)
{
    AERON_CONTEXT_SET_CHECK_ARG_AND_RETURN(-1, context);

    context->use_conductor_agent_invoker = value;
    return 0;
}

bool aeron_context_get_use_conductor_agent_invoker(aeron_context_t *context)
{
    return NULL != context ? context->use_conductor_agent_invoker : AERON_CONTEXT_USE_CONDUCTOR_AGENT_INVOKER_DEFAULT;
}

int aeron_context_request_driver_termination(const char *directory, const uint8_t *token_buffer, size_t token_length)
{
    size_t min_length = AERON_CNC_VERSION_AND_META_DATA_LENGTH;

    if (token_length > AERON_MAX_PATH)
    {
        AERON_SET_ERR(EINVAL, "Token too long: %" PRIu64, (uint64_t)token_length);
        return -1;
    }

    char filename[AERON_MAX_PATH] = { 0 };
    if (aeron_cnc_resolve_filename(directory, filename, sizeof(filename)) < 0)
    {
        AERON_APPEND_ERR("%s", "Failed to get cnc filename");
        return -1;
    }

    int64_t file_length_result = aeron_file_length(filename);
    if (file_length_result < 0)
    {
        AERON_SET_ERR(EINVAL, "%s", "Invalid file length");
        return -1;
    }

    size_t file_length = (size_t)file_length_result;
    if (file_length > min_length)
    {
        aeron_mapped_file_t cnc_mmap;
        if (aeron_map_existing_file(&cnc_mmap, filename) < 0)
        {
            AERON_APPEND_ERR("%s", "Failed to map cnc for driver termination");
            return -1;
        }

        if (cnc_mmap.length > min_length)
        {
            int result = -1;

            aeron_cnc_metadata_t *metadata = (aeron_cnc_metadata_t *)cnc_mmap.addr;
            int32_t cnc_version = aeron_cnc_version_volatile(metadata);

            if (cnc_version <= 0)
            {
                AERON_SET_ERR(EINVAL, "%s", "Aeron CnC not initialised");
                goto cleanup;
            }

            if (aeron_semantic_version_major(cnc_version) != aeron_semantic_version_major(AERON_CNC_VERSION))
            {
                AERON_SET_ERR(
                    EINVAL,
                    "Aeron CnC version does not match: client=%" PRIi32 ", file=%" PRIi32,
                    AERON_CNC_VERSION,
                    cnc_version);
                goto cleanup;
            }

            if (aeron_semantic_version_minor(cnc_version) < aeron_semantic_version_minor(AERON_CNC_VERSION))
            {
                AERON_SET_ERR(
                    EINVAL,
                    "Driver version insufficient: client=%" PRIi32 ", file=%" PRIi32,
                    AERON_CNC_VERSION,
                    cnc_version);
                goto cleanup;
            }

            if (!aeron_cnc_is_file_length_sufficient(&cnc_mmap))
            {
                AERON_SET_ERR(EINVAL, "Aeron CnC file length not sufficient: length=%" PRIi64, file_length_result);
                goto cleanup;
            }

            aeron_mpsc_rb_t rb;
            if (aeron_mpsc_rb_init(
                &rb, aeron_cnc_to_driver_buffer(metadata), (size_t)metadata->to_driver_buffer_length) < 0)
            {
                AERON_APPEND_ERR("%s", "Failed to setup ring buffer for termination");
                goto cleanup;
            }

            if (token_length > rb.max_message_length)
            {
                AERON_SET_ERR(
                    EINVAL,
                    "%s", "Token length (%" PRIu64 ") > max message length (%" PRIu64 ")",
                    (uint64_t)token_length,
                    (uint64_t)rb.max_message_length);
                goto cleanup;
            }

            const size_t command_length = sizeof(aeron_terminate_driver_command_t) + token_length;
            const int32_t offset = aeron_mpsc_rb_try_claim(&rb, AERON_COMMAND_TERMINATE_DRIVER, command_length);
            if (offset < 0)
            {
                AERON_SET_ERR(-AERON_CLIENT_ERROR_BUFFER_FULL, "%s", "Unable to write to driver ring buffer");
                goto cleanup;
            }

            aeron_terminate_driver_command_t *command = (aeron_terminate_driver_command_t *)(rb.buffer + offset);

            command->correlated.client_id = aeron_mpsc_rb_next_correlation_id(&rb);
            command->correlated.correlation_id = aeron_mpsc_rb_next_correlation_id(&rb);
            command->token_length = (int32_t)token_length;
            memcpy((void *)(command + 1), token_buffer, token_length);

            aeron_mpsc_rb_commit(&rb, offset);
            result = 1;

cleanup:
            aeron_unmap(&cnc_mmap);
            return result;
        }
    }

    return 0;
}
