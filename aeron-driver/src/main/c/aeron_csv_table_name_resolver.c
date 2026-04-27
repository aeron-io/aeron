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
#endif

#include <string.h>
#include <inttypes.h>

#include "util/aeron_error.h"
#include "util/aeron_strutil.h"
#include "util/aeron_arrayutil.h"
#include "command/aeron_control_protocol.h"
#include "aeron_csv_table_name_resolver.h"

#define AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE (1024)
#define AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS (3)

typedef struct aeron_csv_table_name_resolver_row_stct
{
    const char *name;
    const char *initial_resolution_host;
    const char *re_resolution_host;
    aeron_atomic_counter_t operation_toggle;
}
aeron_csv_table_name_resolver_row_t;

typedef struct aeron_csv_table_name_resolver_stct
{
    aeron_csv_table_name_resolver_row_t *array;
    size_t length;
    size_t capacity;
    char *saved_config_csv;
    aeron_name_resolver_t delegate_resolver;
    int64_t *error_counter;
    aeron_distinct_error_log_t *error_log;
}
aeron_csv_table_name_resolver_t;

int aeron_csv_tablename_resolver_lookup(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_lookup,
    const char **resolved_name)
{
    aeron_csv_table_name_resolver_t *csv_resolver = (aeron_csv_table_name_resolver_t *)resolver->state;
    return csv_resolver->delegate_resolver.lookup_func(
        &csv_resolver->delegate_resolver, name, uri_param_name, is_re_lookup, resolved_name);
}

int aeron_csv_table_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    const char *hostname = name;
    aeron_csv_table_name_resolver_t *csv_resolver = (aeron_csv_table_name_resolver_t *)resolver->state;

    for (size_t i = 0; i < csv_resolver->length; i++)
    {
        if (strncmp(name, csv_resolver->array[i].name, strlen(csv_resolver->array[i].name) + 1) == 0)
        {
            int64_t operation;
            AERON_GET_ACQUIRE(operation, *csv_resolver->array[i].operation_toggle.value_addr);

            if (AERON_NAME_RESOLVER_CSV_DISABLE_RESOLUTION_OP == operation)
            {
                AERON_SET_ERR(-AERON_ERROR_CODE_UNKNOWN_HOST, "Unable to resolve host=(%s): (forced)", hostname);
                return -1;
            }
            else if (AERON_NAME_RESOLVER_CSV_USE_INITIAL_RESOLUTION_HOST_OP == operation)
            {
                hostname = csv_resolver->array[i].initial_resolution_host;
            }
            else if (AERON_NAME_RESOLVER_CSV_USE_RE_RESOLUTION_HOST_OP == operation)
            {
                hostname = csv_resolver->array[i].re_resolution_host;
            }
        }
    }

    int result = csv_resolver->delegate_resolver.resolve_func(
        &csv_resolver->delegate_resolver, hostname, uri_param_name, is_re_resolution, address);

    return result;
}

int aeron_csv_table_name_resolver_on_start(aeron_name_resolver_t *resolver)
{
    aeron_csv_table_name_resolver_t *csv_resolver = (aeron_csv_table_name_resolver_t *)resolver->state;
    return csv_resolver->delegate_resolver.start_func(&csv_resolver->delegate_resolver);
}

int aeron_csv_table_name_resolver_do_work(aeron_name_resolver_t *resolver, int64_t now_ms)
{
    aeron_csv_table_name_resolver_t *csv_resolver = (aeron_csv_table_name_resolver_t *)resolver->state;
    return csv_resolver->delegate_resolver.do_work_func(&csv_resolver->delegate_resolver, now_ms);
}

int aeron_csv_table_name_resolver_free(aeron_csv_table_name_resolver_t *csv_resolver)
{
    aeron_free(csv_resolver->saved_config_csv);
    aeron_free(csv_resolver->array);
    aeron_free(csv_resolver);
    return 0;
}

static void aeron_csv_table_name_resolver_log_and_clear_error(aeron_csv_table_name_resolver_t *resolver)
{
    aeron_distinct_error_log_record(resolver->error_log, aeron_errcode(), aeron_errmsg());
    aeron_counter_increment(resolver->error_counter);
    aeron_err_clear();
}

int aeron_csv_table_name_resolver_close(aeron_name_resolver_t *resolver)
{
    aeron_csv_table_name_resolver_t *resolver_state = (aeron_csv_table_name_resolver_t *)resolver->state;

    if (resolver_state->delegate_resolver.close_func(&resolver_state->delegate_resolver) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_csv_table_name_resolver_log_and_clear_error(resolver_state);
    }

    return aeron_csv_table_name_resolver_free(resolver_state);
}

int aeron_csv_table_name_resolver_init(
    aeron_csv_table_name_resolver_t **csv_resolver,
    aeron_driver_context_t *context,
    const char *args)
{
    char *rows[AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE];
    char *columns[AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS];
    aeron_csv_table_name_resolver_t *_csv_resolver = NULL;

    if (NULL == args)
    {
        AERON_SET_ERR(EINVAL, "No CSV configuration, please specify: %s", AERON_NAME_RESOLVER_INIT_ARGS_ENV_VAR);
        return -1;
    }

    if (aeron_alloc((void **)&_csv_resolver, sizeof(aeron_csv_table_name_resolver_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Allocating lookup table");
        return -1;
    }

    if (aeron_default_name_resolver_supplier(&_csv_resolver->delegate_resolver, NULL, context) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    _csv_resolver->saved_config_csv = strdup(args);
    if (NULL == _csv_resolver->saved_config_csv)
    {
        AERON_SET_ERR(errno, "%s", "Duplicating config string");
        goto error;
    }

    int num_rows = aeron_tokenise(_csv_resolver->saved_config_csv, '|', AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE, rows);
    if (num_rows < 0)
    {
        AERON_SET_ERR(num_rows, "%s", "Failed to parse rows for lookup table");
        goto error;
    }

    for (int i = num_rows; -1 < --i;)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, (*_csv_resolver), aeron_csv_table_name_resolver_row_t)
        if (ensure_capacity_result < 0)
        {
            AERON_APPEND_ERR(
                "Failed to allocate rows for lookup table (%" PRIu64 ",%" PRIu64 ")",
                (uint64_t)_csv_resolver->length,
                (uint64_t)_csv_resolver->capacity);
            goto error;
        }

        int num_columns = aeron_tokenise(rows[i], ',', AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS, columns);
        if (AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS == num_columns)
        {
            // Fields are in reverse order.
            aeron_csv_table_name_resolver_row_t *row = &_csv_resolver->array[_csv_resolver->length];
            row->re_resolution_host = columns[0];
            row->initial_resolution_host = columns[1];
            row->name = columns[2];

            uint8_t key_buffer[512] = { 0 };
            uint32_t name_str_length = (uint32_t)strlen(row->name);
            uint32_t name_length = name_str_length < sizeof(key_buffer) - sizeof(name_str_length) ?
                name_str_length : sizeof(key_buffer) - sizeof(name_str_length);

            memcpy(key_buffer, &name_length, sizeof(name_length));
            memcpy(&key_buffer[sizeof(name_length)], row->name, name_length);

            char value_buffer[512] = { 0 };
            size_t value_buffer_maxlen = sizeof(value_buffer) - 1;

            int value_buffer_result = snprintf(
                value_buffer,
                value_buffer_maxlen,
                "NameEntry{name='%s', initialResolutionHost='%s', reResolutionHost='%s'}",
                row->name,
                row->initial_resolution_host,
                row->re_resolution_host);

            if (value_buffer_result < 0)
            {
                AERON_SET_ERR(EINVAL, "%s", "Failed to create csv resolver counter label");
                goto error;
            }

            size_t key_length = sizeof(name_length) + name_length;
            size_t value_length = (size_t)value_buffer_result < value_buffer_maxlen ?
                (size_t)value_buffer_result : value_buffer_maxlen;

            row->operation_toggle.counter_id = aeron_counters_manager_allocate(
                context->counters_manager,
                AERON_NAME_RESOLVER_CSV_ENTRY_COUNTER_TYPE_ID,
                key_buffer,
                key_length,
                value_buffer,
                value_length);

            if (row->operation_toggle.counter_id < 0)
            {
                AERON_APPEND_ERR("%s", "Failed to allocate csv resolver counter");
                goto error;
            }

            row->operation_toggle.value_addr = aeron_counters_manager_addr(
                context->counters_manager, row->operation_toggle.counter_id);

            _csv_resolver->length++;
        }
    }

    _csv_resolver->error_counter = aeron_system_counter_addr(context->system_counters, AERON_SYSTEM_COUNTER_ERRORS);
    _csv_resolver->error_log = context->error_log;

    *csv_resolver = _csv_resolver;
    return 0;

error:
    aeron_csv_table_name_resolver_free(_csv_resolver);
    return -1;
}

int aeron_csv_table_name_resolver_supplier(
    aeron_name_resolver_t *resolver,
    const char *args,
    aeron_driver_context_t *context)
{
    aeron_csv_table_name_resolver_t *csv_resolver = NULL;

    resolver->state = NULL;
    if (aeron_csv_table_name_resolver_init(&csv_resolver, context, args) < 0)
    {
        return -1;
    }

    resolver->lookup_func = aeron_csv_tablename_resolver_lookup;
    resolver->resolve_func = aeron_csv_table_name_resolver_resolve;
    resolver->start_func = aeron_csv_table_name_resolver_on_start;
    resolver->do_work_func = aeron_csv_table_name_resolver_do_work;
    resolver->close_func = aeron_csv_table_name_resolver_close;
    resolver->state = csv_resolver;
    resolver->name = "csv";

    return 0;
}
