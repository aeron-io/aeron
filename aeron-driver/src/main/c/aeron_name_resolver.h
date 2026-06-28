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

#ifndef AERON_NAME_RESOLVER_H
#define AERON_NAME_RESOLVER_H

#include <stdbool.h>
#include <stdio.h>
#include "aeron_driver_common.h"
#include "aeron_driver_context.h"
#include "aeron_system_counters.h"
#include "util/aeron_parse_util.h"

#define AERON_NAME_RESOLVER_CSV_TABLE "csv_table"
#define AERON_NAME_RESOLVER_DRIVER "driver"
#define AERON_NAME_RESOLVER_CSV_TABLE_ARGS_ENV_VAR "AERON_NAME_RESOLVER_CSV_LOOKUP_TABLE_ARGS"

typedef int (*aeron_name_resolver_resolve_func_t)(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address);

/**
 * Resolves a name to a host:port string.
 *
 * @return 0 if not found, 1 if found, -1 on error.
 */
typedef int (*aeron_name_resolver_lookup_func_t)(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_lookup,
    const char **resolved_name);

typedef int (*aeron_name_resolver_on_start_func_t)(aeron_name_resolver_t *resolver);

typedef int (*aeron_name_resolver_do_work_func_t)(aeron_name_resolver_t *resolver, int64_t now_ms);

typedef int (*aeron_name_resolver_close_func_t)(aeron_name_resolver_t *resolver);

typedef struct aeron_name_resolver_stct
{
    const char *name;
    aeron_name_resolver_lookup_func_t lookup_func;
    aeron_name_resolver_resolve_func_t resolve_func;
    aeron_name_resolver_on_start_func_t start_func;
    aeron_name_resolver_do_work_func_t do_work_func;
    aeron_name_resolver_close_func_t close_func;
    void *state;
}
aeron_name_resolver_t;

aeron_name_resolver_supplier_func_t aeron_name_resolver_supplier_load(const char *name);

int aeron_name_resolver_init(aeron_name_resolver_t *resolver, const char *args, aeron_driver_context_t *context);

int aeron_default_name_resolver_supplier(
    aeron_name_resolver_t *resolver, const char *args, aeron_driver_context_t *context);

int aeron_default_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address);

int aeron_default_name_resolver_lookup(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_lookup,
    const char **resolved_name);

int aeron_default_name_resolver_start(aeron_name_resolver_t *resolver);

int aeron_default_name_resolver_do_work(aeron_name_resolver_t *resolver, int64_t now_ms);

int aeron_default_name_resolver_close(aeron_name_resolver_t *resolver);

typedef struct aeron_name_resolver_async_resolve_stct
{
    const char *uri_param_name;
    bool is_re_resolution;
    char endpoint_name[AERON_MAX_HOST_LENGTH + 1];
    struct sockaddr_storage resolved_address;
}
aeron_name_resolver_async_resolve_t;

int aeron_name_resolver_resolve_host_and_port(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *sockaddr);

#endif //AERON_NAME_RESOLVER_H
