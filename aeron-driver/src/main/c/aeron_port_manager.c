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

#include "aeron_port_manager.h"
#include "media/aeron_udp_channel.h"
#include "util/aeron_error.h"
#include "inttypes.h"

uint16_t aeron_wildcard_port_manager_get_port(struct sockaddr_storage *addr)
{
    if (addr->ss_family == AF_INET)
    {
        return ntohs(((struct sockaddr_in *)addr)->sin_port);
    }
    else if (addr->ss_family == AF_INET6)
    {
        return ntohs(((struct sockaddr_in6 *)addr)->sin6_port);
    }

    return 0;
}

int aeron_wildcard_port_manager_init(aeron_wildcard_port_manager_t *port_manager, bool is_sender)
{
    port_manager->port_manager.get_managed_port = aeron_wildcard_port_manager_get_managed_port;
    port_manager->port_manager.free_managed_port = aeron_wildcard_port_manager_free_managed_port;
    port_manager->port_manager.state = port_manager;

    if (aeron_int64_counter_map_init(
        &port_manager->port_table, 0, 16, AERON_MAP_DEFAULT_LOAD_FACTOR) < 0)
    {
        AERON_APPEND_ERR("%s", "could not init wildcard port manager map");
        return -1;
    }

    port_manager->low_port = 0;
    port_manager->high_port = 0;
    port_manager->next_port = 0;
    port_manager->is_os_wildcard = true;
    port_manager->is_sender = is_sender;

    return 0;
}

void aeron_wildcard_port_manager_set_range(
    aeron_wildcard_port_manager_t *port_manager, uint16_t low_port, uint16_t high_port)
{
    port_manager->low_port = low_port;
    port_manager->high_port = high_port;
    port_manager->next_port = low_port;
    port_manager->is_os_wildcard = low_port == high_port && 0 == low_port;
}

void aeron_wildcard_port_manager_delete(aeron_wildcard_port_manager_t *port_manager)
{
    aeron_int64_counter_map_delete(&port_manager->port_table);
}

uint16_t aeron_wildcard_port_manager_find_open_port(aeron_wildcard_port_manager_t *port_manager)
{
    for (uint16_t i = port_manager->next_port; i <= port_manager->high_port; i++)
    {
        if (aeron_int64_counter_map_get(&port_manager->port_table, i) == 0)
        {
            return i;
        }
    }

    for (uint16_t i = port_manager->low_port; i <= port_manager->next_port; i++)
    {
        if (aeron_int64_counter_map_get(&port_manager->port_table, i) == 0)
        {
            return i;
        }
    }

    return 0;
}

int aeron_wildcard_port_manager_allocate_open_port(aeron_wildcard_port_manager_t *port_manager)
{
    uint16_t port = aeron_wildcard_port_manager_find_open_port(port_manager);

    if (0 == port)
    {
        AERON_SET_ERR(
            EINVAL,
            "no available ports in range %" PRIu16 " %" PRIu16,
            port_manager->low_port, port_manager->high_port);
        return -1;
    }

    port_manager->next_port = port + 1;
    if (port_manager->next_port > port_manager->high_port)
    {
        port_manager->next_port = port_manager->low_port;
    }

    if (aeron_int64_counter_map_add_and_get(&port_manager->port_table, port, 1, NULL) == -1)
    {
        AERON_APPEND_ERR("%s", "could not add to wildcard port manager map");
        return -1;
    }

    return port;
}

int aeron_wildcard_port_manager_get_managed_port(
    void *state,
    struct sockaddr_storage *bind_addr_out,
    aeron_udp_channel_t *udp_channel,
    struct sockaddr_storage *bind_addr)
{
    aeron_wildcard_port_manager_t *port_manager = (aeron_wildcard_port_manager_t *)state;
    uint16_t bind_port_in = aeron_wildcard_port_manager_get_port(bind_addr);

    memcpy(bind_addr_out, bind_addr, sizeof(struct sockaddr_storage));

    if (0 != bind_port_in)
    {
        if (aeron_int64_counter_map_add_and_get(&port_manager->port_table, bind_port_in, 1, NULL) == -1)
        {
            AERON_APPEND_ERR("%s", "could not add to wildcard port manager map");
            return -1;
        }
    }
    else if (!port_manager->is_os_wildcard)
    {
        if (!port_manager->is_sender || udp_channel->has_explicit_control)
        {
            int bind_port_out = aeron_wildcard_port_manager_allocate_open_port(port_manager);

            if (bind_port_out < 0)
            {
                return -1;
            }

            if (bind_addr_out->ss_family == AF_INET)
            {
                ((struct sockaddr_in *)bind_addr_out)->sin_port = htons((uint16_t)bind_port_out);
            }
            else if (bind_addr_out->ss_family == AF_INET6)
            {
                ((struct sockaddr_in6 *)bind_addr_out)->sin6_port = htons((uint16_t)bind_port_out);
            }
        }
    }

    return 0;
}

void aeron_wildcard_port_manager_free_managed_port(void *state, struct sockaddr_storage *bind_addr)
{
    aeron_wildcard_port_manager_t *port_manager = (aeron_wildcard_port_manager_t *)state;
    uint16_t bind_port_in = aeron_wildcard_port_manager_get_port(bind_addr);

    if (0 != bind_port_in)
    {
        aeron_int64_counter_map_remove(&port_manager->port_table, bind_port_in);
    }
}

int aeron_parse_port_range(const char *range_str, uint16_t *low_port, uint16_t *high_port)
{
    errno = 0;
    char *end = "";
    int64_t v = strtoll(range_str, &end, 10);
    if ((0 == v && 0 != errno) || v < 0 || v > 65535 || end == range_str)
    {
        const int err = 0 != errno ? errno : EINVAL;
        AERON_SET_ERR(err, "%s", "failed to parse first part of port range");
        return -1;
    }

    const uint16_t low = (uint16_t)v;
    range_str = end;
    end = "";

    v = strtoll(range_str, &end, 10);
    if ((0 == v && 0 != errno) || v < 0 || v > 65535 || end == range_str)
    {
        const int err = 0 != errno ? errno : EINVAL;
        AERON_SET_ERR(err, "%s", "failed to parse second part of port range");
        return -1;
    }

    const uint16_t high = (uint16_t)v;

    if (low > high)
    {
        AERON_SET_ERR(EINVAL, "%s", "low port should be less than or equal to high port");
        return -1;
    }

    *low_port = low;
    *high_port = high;
    return 0;
}
