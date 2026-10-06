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

#include <stdlib.h>
#include <stdio.h>
#include "command/aeron_control_protocol.h"
#include "uri/aeron_uri.h"
#include "util/aeron_arrayutil.h"
#include "util/aeron_parse_util.h"

#define AERON_URI_SCHEME "aeron:"
#define AERON_URI_UDP_TRANSPORT "udp"
#define AERON_URI_IPC_TRANSPORT "ipc"

typedef enum aeron_uri_parser_state_enum
{
    PARAM_KEY, PARAM_VALUE
}
aeron_uri_parser_state_t;

int aeron_uri_parse_params(char *uri, aeron_uri_parse_callback_t param_func, void *clientd)
{
    aeron_uri_parser_state_t state = PARAM_KEY;
    char *param_key = NULL, *param_value = NULL;

    for (size_t i = 0; uri[i] != '\0'; i++)
    {
        char c = uri[i];

        switch (state)
        {
            case PARAM_KEY:
                switch (c)
                {
                    case '=':
                        if (NULL == param_key)
                        {
                            AERON_SET_ERR(-AERON_ERROR_CODE_INVALID_CHANNEL, "%s", "empty key not allowed");
                            return -1;
                        }
                        uri[i] = '\0';
                        param_value = NULL;
                        state = PARAM_VALUE;
                        break;

                    case '|':
                        AERON_SET_ERR(-AERON_ERROR_CODE_INVALID_CHANNEL, "%s", "invalid end of key");
                        return -1;

                    default:
                        if (NULL == param_key)
                        {
                            param_key = &uri[i];
                        }
                        break;
                }
                break;

            case PARAM_VALUE:
                switch (c)
                {
                    case '|':
                        if (NULL == param_value)
                        {
                            AERON_SET_ERR(-AERON_ERROR_CODE_INVALID_CHANNEL, "%s", "empty value not allowed");
                            return -1;
                        }
                        uri[i] = '\0';
                        if (param_func(clientd, param_key, param_value) < 0)
                        {
                            return -1;
                        }

                        param_key = NULL;
                        state = PARAM_KEY;
                        break;

                    default:
                        if (NULL == param_value)
                        {
                            param_value = &uri[i];
                        }
                        break;
                }
                break;
        }
    }

    if (state == PARAM_VALUE)
    {
        if (param_func(clientd, param_key, param_value) < 0)
        {
            return -1;
        }
    }

    return 0;
}

void aeron_uri_close(aeron_uri_t *params)
{
    if (params != NULL)
    {
        if (params->type == AERON_URI_UDP)
        {
            aeron_free(params->params.udp.additional_params.array);
            params->params.udp.additional_params.array = NULL;
        }
        else if (params->type == AERON_URI_IPC)
        {
            aeron_free(params->params.ipc.additional_params.array);
            params->params.ipc.additional_params.array = NULL;
        }
    }
}

int aeron_uri_params_ensure_capacity(aeron_uri_params_t *params)
{
    if (aeron_array_ensure_capacity(
        (uint8_t **)&params->array, sizeof(aeron_uri_param_t), params->length, params->length + 1) >= 0)
    {
        params->length++;
        return 0;
    }

    return -1;
}

static int aeron_udp_uri_params_func(void *clientd, const char *key, const char *value)
{
    aeron_udp_channel_params_t *params = (aeron_udp_channel_params_t *)clientd;

    if (strcmp(key, AERON_UDP_CHANNEL_ENDPOINT_KEY) == 0)
    {
        params->endpoint = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_INTERFACE_KEY) == 0)
    {
        params->bind_interface = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_TTL_KEY) == 0)
    {
        params->ttl = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_CONTROL_KEY) == 0)
    {
        params->control = value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_CONTROL_MODE_KEY) == 0)
    {
        params->control_mode = value;
    }
    else if (strcmp(key, AERON_URI_TAGS_KEY) == 0)
    {
        char *ptr = strchr(value, ',');

        if (NULL != ptr)
        {
            *ptr++ = '\0';
            params->entity_tag = '\0' == *ptr ? NULL : ptr;
        }

        params->channel_tag = '\0' == *value ? NULL : value;
    }
    else
    {
        size_t index = params->additional_params.length;
        if (aeron_uri_params_ensure_capacity(&params->additional_params) < 0)
        {
            return -1;
        }

        aeron_uri_param_t *param = &params->additional_params.array[index];

        param->key = key;
        param->value = value;
    }

    return 0;
}

static int aeron_ipc_uri_params_func(void *clientd, const char *key, const char *value)
{
    aeron_ipc_channel_params_t *params = (aeron_ipc_channel_params_t *)clientd;

    if (strcmp(key, AERON_URI_TAGS_KEY) == 0)
    {
        char *ptr = strchr(value, ',');

        if (NULL != ptr)
        {
            *ptr++ = '\0';
            params->entity_tag = '\0' == *ptr ? NULL : ptr;
        }

        params->channel_tag = '\0' == *value ? NULL : value;
    }
    else if (strcmp(key, AERON_UDP_CHANNEL_CONTROL_MODE_KEY) == 0)
    {
        params->control_mode = value;
    }
    else
    {
        size_t index = params->additional_params.length;
        if (aeron_uri_params_ensure_capacity(&params->additional_params) < 0)
        {
            return -1;
        }

        aeron_uri_param_t *param = &params->additional_params.array[index];

        param->key = key;
        param->value = value;
    }

    return 0;
}

static void aeron_uri_reset(aeron_uri_t *params)
{
    params->mutable_uri[0] = '\0';
    params->type = AERON_URI_UNKNOWN;

    params->params.udp.additional_params.length = 0;
    params->params.udp.additional_params.array = NULL;
    params->params.udp.endpoint = NULL;
    params->params.udp.bind_interface = NULL;
    params->params.udp.ttl = NULL;
    params->params.udp.control = NULL;
    params->params.udp.control_mode = NULL;
    params->params.udp.channel_tag = NULL;
    params->params.udp.entity_tag = NULL;

    params->params.ipc.additional_params.length = 0;
    params->params.ipc.additional_params.array = NULL;
    params->params.ipc.channel_tag = NULL;
    params->params.ipc.entity_tag = NULL;
}

int aeron_uri_parse(size_t uri_length, const char *uri, aeron_uri_t *params)
{
    if (NULL == params)
    {
        AERON_SET_ERR(-AERON_ERROR_CODE_INVALID_CHANNEL, "%s", "params is NULL");
        return -1;
    }

    aeron_uri_reset(params);

    if (NULL == uri)
    {
        AERON_SET_ERR(-AERON_ERROR_CODE_INVALID_CHANNEL, "%s", "channel URI is NULL");
        return -1;
    }

    if (uri_length >= AERON_URI_MAX_LENGTH)
    {
        AERON_SET_ERR(-AERON_ERROR_CODE_INVALID_CHANNEL,
            "URI length (%" PRIu64 ") exceeds max supported length (%d): %.*s",
            uri_length, (int32_t)AERON_URI_MAX_LENGTH - 1, AERON_URI_MAX_LENGTH, uri);
        return -1;
    }

    memcpy(params->mutable_uri, uri, uri_length);
    params->mutable_uri[uri_length] = '\0';

    char *ptr = params->mutable_uri;

    if (strncmp(ptr, AERON_URI_SCHEME, strlen(AERON_URI_SCHEME)) == 0)
    {
        ptr += strlen(AERON_URI_SCHEME);

        if (strncmp(ptr, AERON_URI_UDP_TRANSPORT, strlen(AERON_URI_UDP_TRANSPORT)) == 0)
        {
            ptr += strlen(AERON_URI_UDP_TRANSPORT);

            if (*ptr++ == '?')
            {
                params->type = AERON_URI_UDP;
                int result = aeron_uri_parse_params(ptr, aeron_udp_uri_params_func, &params->params.udp);
                if (result < 0)
                {
                    AERON_APPEND_ERR("%.*s", (int)uri_length, uri);
                }
                return result;
            }
        }
        else if (strncmp(ptr, AERON_URI_IPC_TRANSPORT, strlen(AERON_URI_IPC_TRANSPORT)) == 0)
        {
            ptr += strlen(AERON_URI_IPC_TRANSPORT);

            if (*ptr == '\0' || *ptr++ == '?')
            {
                params->type = AERON_URI_IPC;
                int result = aeron_uri_parse_params(ptr, aeron_ipc_uri_params_func, &params->params.ipc);
                if (result < 0)
                {
                    AERON_APPEND_ERR("%.*s", (int)uri_length, uri);
                }
                return result;
            }
        }
    }

    AERON_SET_ERR(-AERON_ERROR_CODE_INVALID_CHANNEL, "invalid URI scheme or transport: %s", uri);

    return -1;
}

uint8_t aeron_uri_multicast_ttl(aeron_uri_t *uri)
{
    uint8_t result = 0;

    if (AERON_URI_UDP == uri->type && NULL != uri->params.udp.ttl)
    {
        uint64_t value = strtoull(uri->params.udp.ttl, NULL, 0);
        result = value > 255u ? (uint8_t)255 : (uint8_t)value;
    }

    return result;
}

const char *aeron_uri_find_param_value(const aeron_uri_params_t *uri_params, const char *key)
{
    for (size_t i = 0, length = uri_params->length; i < length; i++)
    {
        aeron_uri_param_t *param = &uri_params->array[i];

        if (strcmp(key, param->key) == 0)
        {
            return param->value;
        }
    }

    return NULL;
}

int aeron_uri_get_int32(aeron_uri_params_t *uri_params, const char *key, int32_t *retval)
{
    const char *value_str;
    if ((value_str = aeron_uri_find_param_value(uri_params, key)) == NULL)
    {
        *retval = 0;
        return 0;
    }

    char *end_ptr = "";
    errno = 0;
    const long value = strtol(value_str, &end_ptr, 0);

    if (0 != errno || '\0' != *end_ptr)
    {
        AERON_SET_ERR(EINVAL, "could not parse %s=%s as int32_t in URI: %s", key, value_str, strerror(errno));
        return -1;
    }
    else if (value < INT32_MIN || INT32_MAX < value)
    {
        AERON_SET_ERR(
            EINVAL,
            "could not parse %s=%s as int32_t in URI: Numerical result out of range", key, value_str);
        return -1;
    }

    *retval = (int32_t)value;

    return 1;
}

int aeron_uri_get_int64(aeron_uri_params_t *uri_params, const char *key, int64_t default_val, int64_t *retval)
{
    const char *value_str;
    if ((value_str = aeron_uri_find_param_value(uri_params, key)) == NULL)
    {
        *retval = default_val;
        return 0;
    }

    char *end_ptr;
    int64_t value;

    errno = 0;
    value = strtoll(value_str, &end_ptr, 0);
    if (0 != errno || '\0' != *end_ptr)
    {
        AERON_SET_ERR(EINVAL, "could not parse %s=%s as int64_t in URI: %s", key, value_str, strerror(errno));
        return -1;
    }

    *retval = value;

    return 1;
}

int aeron_uri_get_bool(aeron_uri_params_t *uri_params, const char *key, bool *retval)
{
    const char *value_str = aeron_uri_find_param_value(uri_params, key);
    if (value_str != NULL)
    {
        if (strncmp("true", value_str, strlen("true")) == 0)
        {
            *retval = true;
        }
        else if (strncmp("false", value_str, strlen("false")) == 0)
        {
            *retval = false;
        }
        else
        {
            AERON_SET_ERR(EINVAL, "could not parse %s=%s as bool from URI", key, value_str);
            return -1;
        }
    }

    return 1;
}

int aeron_uri_get_size_t(aeron_uri_params_t *uri_params, const char *key, size_t *value)
{
    const char *value_str = aeron_uri_find_param_value(uri_params, key);
    int result = 0;

    if (NULL != value_str)
    {
        uint64_t temp_value = 0;

        if (-1 == aeron_parse_size64(value_str, &temp_value))
        {
            AERON_SET_ERR(EINVAL, "could not parse %s=%s in URI", key, value_str);
            return -1;
        }

        *value = (size_t)temp_value;

        result = 1;
    }

    return result;
}

int aeron_uri_get_ats(aeron_uri_params_t *uri_params, aeron_uri_ats_status_t *uri_ats_status)
{
    const char *value_str = aeron_uri_find_param_value(uri_params, AERON_URI_ATS_KEY);
    *uri_ats_status = AERON_URI_ATS_STATUS_DEFAULT;
    if (value_str != NULL)
    {
        if (strncmp("true", value_str, strlen("true")) == 0)
        {
            *uri_ats_status = AERON_URI_ATS_STATUS_ENABLED;
        }
        else if (strncmp("false", value_str, strlen("false")) == 0)
        {
            *uri_ats_status = AERON_URI_ATS_STATUS_DISABLED;
        }
        else
        {
            AERON_SET_ERR(EINVAL, "could not parse %s=%s as bool from URI", AERON_URI_ATS_KEY, value_str);
            return -1;
        }
    }

    return 1;
}

int aeron_uri_get_socket_buf_lengths(
    aeron_uri_params_t *uri_params, size_t *socket_sndbuf_length, size_t *socket_rcvbuf_length)
{
    int result = aeron_uri_get_size_t(uri_params, AERON_URI_SOCKET_SNDBUF_KEY, socket_sndbuf_length);

    if (result < 0)
    {
        return result;
    }

    return aeron_uri_get_size_t(uri_params, AERON_URI_SOCKET_RCVBUF_KEY, socket_rcvbuf_length);
}

int aeron_uri_get_receiver_window_length(aeron_uri_params_t *uri_params, size_t *receiver_window_length)
{
    return aeron_uri_get_size_t(uri_params, AERON_URI_RECEIVER_WINDOW_KEY, receiver_window_length);
}

int aeron_uri_get_timeout(aeron_uri_params_t *uri_params, const char *param_name, uint64_t *timeout_ns)
{
    const char *value_str;

    if ((value_str = aeron_uri_find_param_value(uri_params, param_name)) != NULL)
    {
        uint64_t value;

        if (-1 == aeron_parse_duration_ns(value_str, &value))
        {
            AERON_SET_ERR(EINVAL, "could not parse %s=%s in URI", param_name, value_str);
            return -1;
        }

        *timeout_ns = value;
    }

    return 0;
}

int64_t aeron_uri_parse_tag(const char *tag_str)
{
    errno = 0;
    char *end_ptr = NULL;
    unsigned long value = strtoul(tag_str, &end_ptr, 10);

    if ((0 == value && 0 != errno) || end_ptr == tag_str)
    {
        return AERON_URI_INVALID_TAG;
    }

    return (int64_t)value;
}

typedef struct aeron_uri_print_context_stct
{
    char *buffer;
    size_t buffer_len;
    size_t offset;
    const char *delimiter;
}
aeron_uri_print_context_t;

static int aeron_uri_print_next(aeron_uri_print_context_t *print_ctx, const char *key, const char *value)
{
    int result = 0;
    if (print_ctx->offset < print_ctx->buffer_len && NULL != value)
    {
        char *dest = print_ctx->buffer + print_ctx->offset;
        size_t dest_len = print_ctx->buffer_len - print_ctx->offset;
        result = snprintf(dest, dest_len, "%s%s=%s", print_ctx->delimiter, key, value);

        if (0 < result)
        {
            print_ctx->offset += result;
            print_ctx->delimiter = "|";
        }
        else if (result < 0)
        {
            AERON_SET_ERR(result, "Failed to print next uri item: %s", key);
        }
    }

    return result;
}

int aeron_uri_ipc_sprint(aeron_uri_t *uri, char *buffer, size_t buffer_len)
{
    aeron_uri_print_context_t ctx = { 0 };
    ctx.buffer = buffer;
    ctx.buffer_len = buffer_len;
    ctx.offset = 0;
    ctx.delimiter = "?";
    const char *tags = NULL;
    char tag_buffer[64];

    aeron_ipc_channel_params_t *ipc_params = &uri->params.ipc;

    if (ctx.offset < buffer_len)
    {
        int num_chars = snprintf(ctx.buffer, ctx.buffer_len - ctx.offset, "aeron:ipc");
        ctx.offset += num_chars;
    }

    if (NULL != ipc_params->channel_tag)
    {
        if (NULL != ipc_params->entity_tag)
        {
            snprintf(tag_buffer, sizeof(tag_buffer), "%s,%s", ipc_params->channel_tag, ipc_params->entity_tag);
            tags = tag_buffer;
        }
        else
        {
            tags = ipc_params->channel_tag;
        }

        if (aeron_uri_print_next(&ctx, AERON_URI_TAGS_KEY, tags) < 0)
        {
            return -1;
        }
    }

    aeron_uri_params_t *additional = &ipc_params->additional_params;
    for (size_t i = 0, n = additional->length; i < n; i++)
    {
        aeron_uri_print_next(&ctx, additional->array[i].key, additional->array[i].value);
    }

    return (int)ctx.offset;
}

int aeron_uri_udp_sprint(aeron_uri_t *uri, char *buffer, size_t buffer_len)
{
    aeron_uri_print_context_t ctx = { 0 };
    ctx.buffer = buffer;
    ctx.buffer_len = buffer_len;
    ctx.offset = 0;
    ctx.delimiter = "?";
    char tag_buffer[64];
    const char *tags = NULL;

    aeron_udp_channel_params_t *udp_params = &uri->params.udp;

    if (ctx.offset < buffer_len)
    {
        int num_chars = snprintf(ctx.buffer, ctx.buffer_len - ctx.offset, "aeron:udp");
        ctx.offset += num_chars;
    }

    if (aeron_uri_print_next(&ctx, AERON_UDP_CHANNEL_ENDPOINT_KEY, udp_params->endpoint) < 0)
    {
        return -1;
    }

    if (aeron_uri_print_next(&ctx, AERON_UDP_CHANNEL_INTERFACE_KEY, udp_params->bind_interface) < 0)
    {
        return -1;
    }

    if (aeron_uri_print_next(&ctx, AERON_UDP_CHANNEL_CONTROL_KEY, udp_params->control) < 0)
    {
        return -1;
    }

    if (aeron_uri_print_next(&ctx, AERON_UDP_CHANNEL_CONTROL_MODE_KEY, udp_params->control_mode) < 0)
    {
        return -1;
    }
    
    if (NULL != udp_params->channel_tag)
    {
        if (NULL != udp_params->entity_tag)
        {
            snprintf(tag_buffer, sizeof(tag_buffer), "%s,%s", udp_params->channel_tag, udp_params->entity_tag);
            tags = tag_buffer;
        }
        else
        {
            tags = udp_params->channel_tag;
        }
        
        if (aeron_uri_print_next(&ctx, AERON_URI_TAGS_KEY, tags) < 0)
        {
            return -1;
        }
    }

    if (aeron_uri_print_next(&ctx, AERON_UDP_CHANNEL_TTL_KEY, udp_params->ttl) < 0)
    {
        return -1;
    }

    aeron_uri_params_t *additional = &udp_params->additional_params;
    for (size_t i = 0, n = additional->length; i < n; i++)
    {
        aeron_uri_print_next(&ctx, additional->array[i].key, additional->array[i].value);
    }

    return (int)ctx.offset;
}

int aeron_uri_sprint(aeron_uri_t *uri, char *buffer, size_t buffer_len)
{
    switch (uri->type)
    {
        case AERON_URI_UDP:
            return aeron_uri_udp_sprint(uri, buffer, buffer_len);

        case AERON_URI_IPC:
            return aeron_uri_ipc_sprint(uri, buffer, buffer_len);

        case AERON_URI_UNKNOWN:
            return snprintf(buffer, buffer_len, "aeron:unknown");

        default:
            AERON_SET_ERR(EINVAL, "Invalid URI type: %d", (int)uri->type);
            return -1;
    }
}
