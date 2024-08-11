/*
 * Copyright 2014-2024 Real Logic Limited.
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

#include <stdio.h>
#include "uri/aeron_uri_string_builder.h"

typedef struct aeron_uri_string_builder_entry_stct
{
    const char *key;
    const char *value;
}
aeron_uri_string_builder_entry_t;

int aeron_uri_string_builder_init_new(aeron_uri_string_builder_t *builder)
{
    aeron_str_to_ptr_hash_map_init(&builder->params, 64, AERON_MAP_DEFAULT_LOAD_FACTOR);

    return 0;
}

static int aeron_uri_string_builder_params_func(void *clientd, const char *key, const char *value)
{
    return aeron_uri_string_builder_put((aeron_uri_string_builder_t *)clientd, key, value);
}

int aeron_uri_string_builder_init_on_string(aeron_uri_string_builder_t *builder, const char *uri)
{
    aeron_uri_string_builder_init_new(builder);

    size_t uri_length = strlen(uri);

    char buffer[AERON_MAX_PATH + 1];

    size_t max_len = uri_length > AERON_MAX_PATH ? AERON_MAX_PATH : uri_length;

    strncpy(buffer, uri, max_len);
    buffer[uri_length] = '\0';

    char *ptr = buffer;
    char *end_ptr = NULL;

    if (strncmp("aeron:", ptr, 6) != 0)
    {
        end_ptr = strchr(ptr, ':');
        if (end_ptr == NULL)
        {
            AERON_SET_ERR(EINVAL, "%s", "uri must start with '[prefix:]aeron:[media]'");
            return -1;
        }

        // replace ':' after prefix with NULL character
        *end_ptr = '\0';

        aeron_uri_string_builder_put(builder, AERON_URI_STRING_BUILDER_PREFIX_KEY, ptr);

        // move ptr past the prefix
        ptr = end_ptr + 1;
    }

    if (strncmp("aeron:", ptr, 6) != 0)
    {
        AERON_SET_ERR(EINVAL, "%s", "uri found without 'aeron:'");
        return -1;
    }

    // *ptr == "aeron:"
    ptr = strchr(ptr, ':');
    ptr++;

    // *ptr == [media] (up to possible ?)
    end_ptr = strchr(ptr, '?');

    if (NULL != end_ptr)
    {
        // replace '?' after media with NULL character
        *end_ptr = '\0';
    }

    aeron_uri_string_builder_put(builder, AERON_URI_STRING_BUILDER_MEDIA_KEY, ptr);

    if (NULL == end_ptr)
    {
        return 0;
    }

    return aeron_uri_parse_params(end_ptr + 1, aeron_uri_string_builder_params_func, builder);
}

static void aeron_uri_string_builder_entry_delete(void *clientd, const char *key, size_t key_len, void *value)
{
    aeron_free(value);
}

int aeron_uri_string_builder_close(aeron_uri_string_builder_t *builder)
{
    aeron_str_to_ptr_hash_map_for_each(&builder->params, aeron_uri_string_builder_entry_delete, NULL);

    aeron_str_to_ptr_hash_map_delete(&builder->params);

    return 0;
}

int aeron_uri_string_builder_put(aeron_uri_string_builder_t *builder, const char *key, const char *value)
{
    if (NULL == builder)
    {
        AERON_SET_ERR(EINVAL, "%s", "builder must not be NULL");
        return -1;
    }

    if (NULL == key)
    {
        AERON_SET_ERR(EINVAL, "%s", "key must not be NULL");
        return -1;
    }

    if (strchr(key, '?') != NULL ||
        strchr(key, '|') != NULL ||
        strchr(key, '=') != NULL)
    {
        AERON_SET_ERR(EINVAL, "%s", "key cannot contain '?', '|' or '='");
        return -1;
    }

    if (NULL != value)
    {
        if (strchr(value, '?') != NULL ||
            strchr(value, '|') != NULL ||
            strchr(value, '=') != NULL)
        {
            AERON_SET_ERR(EINVAL, "%s", "value cannot contain '?', '|' or '='");
            return -1;
        }
    }

    size_t key_len = strlen(key);

    aeron_uri_string_builder_entry_t *entry = NULL;

    entry = aeron_str_to_ptr_hash_map_remove(&builder->params, key, key_len);
    if (NULL != entry)
    {
        aeron_free(entry);
    }

    if (NULL == value)
    {
        return 0;
    }

    size_t value_len = strlen(value);

    // entry struct + key string + value string (plus trailing '\0' for both strings
    aeron_alloc((void **)&entry, sizeof(aeron_uri_string_builder_entry_t) + key_len + 1 + value_len + 1);

    entry->key = ((const char *)entry + sizeof(aeron_uri_string_builder_entry_t));
    entry->value = entry->key + key_len + 1;

    strncpy((char *)entry->key, key, key_len + 1);
    strncpy((char *)entry->value, value, value_len + 1);

    return aeron_str_to_ptr_hash_map_put(&builder->params, entry->key, key_len, entry);
}

int aeron_uri_string_builder_put_int32(aeron_uri_string_builder_t *builder, const char *key, int32_t value)
{
    char buffer[15];

    snprintf(buffer, 15, "%i", value);

    return aeron_uri_string_builder_put(builder, key, buffer);
}

int aeron_uri_string_builder_put_int64(aeron_uri_string_builder_t *builder, const char *key, int64_t value)
{
    char buffer[15];

    snprintf(buffer, 15, "%llu", value);

    return aeron_uri_string_builder_put(builder, key, buffer);
}

const char *aeron_uri_string_builder_get(aeron_uri_string_builder_t *builder, const char *key)
{
    aeron_uri_string_builder_entry_t *entry = NULL;

    entry = aeron_str_to_ptr_hash_map_get(&builder->params, key, strlen(key));

    if (entry == NULL)
    {
        return NULL;
    }

    return entry->value;
}

typedef struct aeron_uri_string_builder_print_context_stct
{
    char *buffer;
    size_t buffer_len;
    size_t offset;
    const char *delimiter;
}
aeron_uri_string_builder_print_context_t;

static void aeron_uri_string_builder_print(void *clientd, const char *key, size_t key_len, void *value)
{
    aeron_uri_string_builder_print_context_t *ctx = (aeron_uri_string_builder_print_context_t *)clientd;
    aeron_uri_string_builder_entry_t *entry = (aeron_uri_string_builder_entry_t *)value;

    if (strcmp(AERON_URI_STRING_BUILDER_PREFIX_KEY, entry->key) == 0 ||
        strcmp(AERON_URI_STRING_BUILDER_MEDIA_KEY, entry->key) == 0)
    {
        return;
    }

    ctx->offset += snprintf(
        ctx->buffer + ctx->offset,
        ctx->buffer_len - ctx->offset,
        "%s%s=%s",
        ctx->delimiter,
        entry->key,
        entry->value);

    ctx->delimiter = "|";
}

int aeron_uri_string_builder_sprint(aeron_uri_string_builder_t *builder, char *buffer, size_t buffer_len)
{
    aeron_uri_string_builder_print_context_t ctx;

    ctx.buffer = buffer;
    ctx.buffer_len = buffer_len;
    ctx.offset = 0;
    ctx.delimiter = "?";

    aeron_uri_string_builder_entry_t *entry;

    entry = aeron_str_to_ptr_hash_map_get(
        &builder->params,
        AERON_URI_STRING_BUILDER_PREFIX_KEY,
        strlen(AERON_URI_STRING_BUILDER_PREFIX_KEY));

    if (NULL != entry)
    {
        ctx.offset += snprintf(
            ctx.buffer + ctx.offset,
            ctx.buffer_len - ctx.offset,
            "%s:", entry->value);
    }

   entry = aeron_str_to_ptr_hash_map_get(
        &builder->params,
        AERON_URI_STRING_BUILDER_MEDIA_KEY,
        strlen(AERON_URI_STRING_BUILDER_MEDIA_KEY));

    ctx.offset += snprintf(
        ctx.buffer + ctx.offset,
        ctx.buffer_len - ctx.offset,
        "aeron:%s",
        entry == NULL ? "unknown" : entry->value);

    aeron_str_to_ptr_hash_map_for_each(&builder->params, aeron_uri_string_builder_print, &ctx);

    return 0;
}
