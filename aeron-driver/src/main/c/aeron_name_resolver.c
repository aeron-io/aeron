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

#include <errno.h>
#include <string.h>
#include "aeron_socket.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "util/aeron_netutil.h"
#include "util/aeron_dlopen.h"
#include "util/aeron_strutil.h"
#include "util/aeron_symbol_table.h"
#include "aeron_name_resolver.h"
#include "aeron_driver_name_resolver.h"
#include "aeron_csv_table_name_resolver.h"

static const aeron_symbol_table_func_t aeron_name_resolver_table[] =
    {
        {
            "default",
            "aeron_default_name_resolver_supplier",
            (aeron_fptr_t)aeron_default_name_resolver_supplier
        },
        {
            "driver",
            "aeron_driver_name_resolver_supplier",
            (aeron_fptr_t)aeron_driver_name_resolver_supplier
        },
        {
            "csv_table",
            "aeron_csv_table_name_resolver_supplier",
            (aeron_fptr_t)aeron_csv_table_name_resolver_supplier
        }
    };

static const size_t aeron_name_resolver_table_length =
    sizeof(aeron_name_resolver_table) / sizeof(aeron_symbol_table_func_t);

static void aeron_name_resolver_load_function_info(
    aeron_name_resolver_t *resolver,
    char *lookup_name_buffer,
    size_t lookup_name_buffer_len,
    char *resolve_name_buffer,
    size_t resolve_name_buffer_len);

int aeron_name_resolver_init(aeron_name_resolver_t *resolver, const char *args, aeron_driver_context_t *context)
{
    if (context->name_resolver_supplier_func(resolver, args, context) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

int aeron_default_name_resolver_supplier(
    aeron_name_resolver_t *resolver, const char *args, aeron_driver_context_t *context)
{
    resolver->lookup_func = aeron_default_name_resolver_lookup;
    resolver->resolve_func = aeron_default_name_resolver_resolve;
    resolver->start_func = aeron_default_name_resolver_start;
    resolver->do_work_func = aeron_default_name_resolver_do_work;
    resolver->close_func = aeron_default_name_resolver_close;
    resolver->state = NULL;
    resolver->name = "default";

    return 0;
}

int aeron_default_name_resolver_resolve(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *address)
{
    if (0 == aeron_ip_addr_resolver(name, address, AF_INET, IPPROTO_UDP))
    {
        return 0;
    }
    return aeron_ip_addr_resolver(name, address, AF_INET6, IPPROTO_UDP);
}

int aeron_default_name_resolver_lookup(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_lookup,
    const char **resolved_name)
{
    *resolved_name = name;
    return 0;
}

int aeron_default_name_resolver_start(aeron_name_resolver_t *resolver)
{
    return 0;
}

int aeron_default_name_resolver_do_work(aeron_name_resolver_t *resolver, int64_t now_ms)
{
    return 0;
}

int aeron_default_name_resolver_close(aeron_name_resolver_t *resolver)
{
    return 0;
}

int aeron_name_resolver_resolve_host_and_port(
    aeron_name_resolver_t *resolver,
    const char *name,
    const char *uri_param_name,
    bool is_re_resolution,
    struct sockaddr_storage *sockaddr)
{
    aeron_parsed_address_t parsed_address;
    const char *address_str = NULL;
    int result = -1;

    if (resolver->lookup_func(resolver, name, uri_param_name, is_re_resolution, &address_str) < 0)
    {
        goto exit;
    }

    if (aeron_address_split(address_str, &parsed_address) < 0)
    {
        goto exit;
    }

    const int family_hint = 6 == parsed_address.ip_version_hint ? AF_INET6 : AF_INET;

    int port = aeron_udp_port_resolver(parsed_address.port, false);

    if (0 <= port)
    {
        if (AF_INET == family_hint)
        {
            if (aeron_try_parse_ipv4(parsed_address.host, sockaddr))
            {
                result = 0;
            }
            else
            {
                result = resolver->resolve_func(
                    resolver, parsed_address.host, uri_param_name, is_re_resolution, sockaddr);
            }
        }
        else if (AF_INET6 == family_hint)
        {
            if (aeron_try_parse_ipv6(parsed_address.host, sockaddr))
            {
                result = 0;
            }
            else
            {
                result = resolver->resolve_func(
                    resolver, parsed_address.host, uri_param_name, is_re_resolution, sockaddr);
            }
        }

        if (0 == result)
        {
            if (AF_INET == sockaddr->ss_family)
            {
                ((struct sockaddr_in *)sockaddr)->sin_port = htons((uint16_t)port);
            }
            else if (AF_INET6 == sockaddr->ss_family)
            {
                ((struct sockaddr_in6 *)sockaddr)->sin6_port = htons((uint16_t)port);
            }
        }
    }

exit:
    if (result < 0)
    {
        char lookup_info[128];
        char resolve_info[128];

        const char *address_or_null = NULL != address_str ? address_str : "null";
        aeron_name_resolver_load_function_info(
            resolver, lookup_info, sizeof(resolve_info), resolve_info, sizeof(resolve_info));

        AERON_APPEND_ERR(
            "Unresolved - %s=%s, name-and-port=%s",
            uri_param_name,
            name,
            address_or_null);
    }

    return result;
}

aeron_name_resolver_supplier_func_t aeron_name_resolver_supplier_load(const char *name)
{
    return (aeron_name_resolver_supplier_func_t)aeron_symbol_table_func_load(
        aeron_name_resolver_table, aeron_name_resolver_table_length, name, "name resolver");
}

static void aeron_name_resolver_load_function_info(
    aeron_name_resolver_t *resolver,
    char *lookup_name_buffer,
    size_t lookup_name_buffer_len,
    char *resolve_name_buffer,
    size_t resolve_name_buffer_len)
{
    aeron_dlinfo_func((aeron_fptr_t)resolver->lookup_func, lookup_name_buffer, lookup_name_buffer_len);
    aeron_dlinfo_func((aeron_fptr_t)resolver->resolve_func, resolve_name_buffer, resolve_name_buffer_len);
}
