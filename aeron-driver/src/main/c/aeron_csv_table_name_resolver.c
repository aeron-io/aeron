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
}
aeron_csv_table_name_resolver_t;

int aeron_csv_table_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    const char *hostname = name;
    if (NULL != resolver->state)
    {
        aeron_csv_table_name_resolver_t *table = (aeron_csv_table_name_resolver_t *)resolver->state;

        for (size_t i = 0; i < table->length; i++)
        {
            if (strncmp(name, table->array[i].name, strlen(table->array[i].name) + 1) == 0)
            {
                int64_t operation;
                AERON_GET_ACQUIRE(operation, *table->array[i].operation_toggle.value_addr);

                if (AERON_NAME_RESOLVER_CSV_DISABLE_RESOLUTION_OP == operation)
                {
                    AERON_SET_ERR(-AERON_ERROR_CODE_UNKNOWN_HOST, "Unable to resolve host=(%s): (forced)", hostname);
                    return -1;
                }
                else if (AERON_NAME_RESOLVER_CSV_USE_INITIAL_RESOLUTION_HOST_OP == operation)
                {
                    hostname = table->array[i].initial_resolution_host;
                }
                else if (AERON_NAME_RESOLVER_CSV_USE_RE_RESOLUTION_HOST_OP == operation)
                {
                    hostname = table->array[i].re_resolution_host;
                }
            }
        }
    }

    int result = aeron_default_name_resolver_resolve(resolver, hostname, uri_param_name, is_re_resolution, address);

    return result;
}

int aeron_csv_table_name_resolver_close(aeron_name_resolver_t *resolver)
{
    aeron_csv_table_name_resolver_t *resolver_state = (aeron_csv_table_name_resolver_t *)resolver->state;

    if (NULL != resolver_state)
    {
        aeron_free(resolver_state->saved_config_csv);
        aeron_free(resolver_state->array);
        aeron_free(resolver_state);
    }
    return 0;
}

int aeron_csv_table_name_resolver_supplier(
    aeron_name_resolver_t *resolver,
    const char *args,
    aeron_driver_context_t *context)
{
    resolver->lookup_func = aeron_default_name_resolver_lookup;
    resolver->close_func = aeron_csv_table_name_resolver_close;
    resolver->start_func = aeron_default_name_resolver_start;
    resolver->resolve_func = aeron_csv_table_name_resolver_resolve;
    resolver->do_work_func = aeron_default_name_resolver_do_work;
    resolver->name = "csv";

    char *rows[AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE];
    char *columns[AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS];
    aeron_csv_table_name_resolver_t *lookup_table = NULL;

    if (NULL == args)
    {
        AERON_SET_ERR(EINVAL, "No CSV configuration, please specify: %s", AERON_NAME_RESOLVER_INIT_ARGS_ENV_VAR);
        goto error;
    }

    if (aeron_alloc((void **)&lookup_table, sizeof(aeron_csv_table_name_resolver_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "Allocating lookup table");
        goto error;
    }
    resolver->state = lookup_table;

    lookup_table->saved_config_csv = strdup(args);
    if (NULL == lookup_table->saved_config_csv)
    {
        AERON_SET_ERR(errno, "%s", "Duplicating config string");
        goto error;
    }

    int num_rows = aeron_tokenise(lookup_table->saved_config_csv, '|', AERON_NAME_RESOLVER_CSV_TABLE_MAX_SIZE, rows);
    if (num_rows < 0)
    {
        AERON_SET_ERR(num_rows, "%s", "Failed to parse rows for lookup table");
        goto error;
    }

    for (int i = num_rows; -1 < --i;)
    {
        int ensure_capacity_result = 0;
        AERON_ARRAY_ENSURE_CAPACITY(ensure_capacity_result, (*lookup_table), aeron_csv_table_name_resolver_row_t)
        if (ensure_capacity_result < 0)
        {
            AERON_APPEND_ERR(
                "Failed to allocate rows for lookup table (%" PRIu64 ",%" PRIu64 ")",
                (uint64_t)lookup_table->length,
                (uint64_t)lookup_table->capacity);
            goto error;
        }

        int num_columns = aeron_tokenise(rows[i], ',', AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS, columns);
        if (AERON_NAME_RESOLVER_CSV_TABLE_COLUMNS == num_columns)
        {
            // Fields are in reverse order.
            aeron_csv_table_name_resolver_row_t *row = &lookup_table->array[lookup_table->length];
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

            lookup_table->length++;
        }
    }

    resolver->state = lookup_table;

    return 0;

error:
    aeron_csv_table_name_resolver_close(resolver);
    return -1;
}
