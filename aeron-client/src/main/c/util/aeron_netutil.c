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
#include <errno.h>
#include <stdlib.h>
#include "util/aeron_netutil.h"
#include "util/aeron_error.h"
#include "util/aeron_parse_util.h"
#include "aeron_socket.h"
#include "command/aeron_control_protocol.h"

#if defined(AERON_COMPILER_GCC)

#elif defined(AERON_COMPILER_MSVC)

#include <intrin.h>

#define __builtin_bswap32 _byteswap_ulong
#define __builtin_bswap64 _byteswap_uint64
#define __builtin_popcount __popcnt

#if defined(AERON_CPU_X64)
#define __builtin_popcountll __popcnt64
#else
__inline DWORD64 __builtin_popcountll(DWORD64 operand)
{
    return __popcnt((DWORD)(operand >> 32)) + __popcnt((DWORD)(operand & UINT32_MAX));
}
#endif

#else
#error Unsupported platform!
#endif

#define AERON_NETUTIL_NAMED_INTERFACE_OPENING_CHAR '{'

int aeron_ip_addr_resolver(const char *host, struct sockaddr_storage *sockaddr, int family_hint, int protocol)
{
    if (-1 == aeron_net_init())
    {
        AERON_APPEND_ERR("%s", "failed to init networking");
        return -1;
    }

    struct addrinfo hints;
    struct addrinfo *info = NULL;

    memset(&hints, 0, sizeof(struct addrinfo));
    hints.ai_family = family_hint;
    hints.ai_socktype = IPPROTO_UDP == protocol ? SOCK_DGRAM : SOCK_STREAM;
    hints.ai_protocol = protocol;

    int error, result = -1;
    if ((error = getaddrinfo(host, NULL, &hints, &info)) != 0)
    {
        AERON_SET_ERR(
            -AERON_ERROR_CODE_UNKNOWN_HOST, "Unable to resolve host=(%s): (%d) %s", host, error, gai_strerror(error));
        return -1;
    }

    if (AF_INET == info->ai_family)
    {
        memcpy(sockaddr, info->ai_addr, sizeof(struct sockaddr_in));
        sockaddr->ss_family = AF_INET;
        result = 0;
    }
    else if (AF_INET6 == info->ai_family)
    {
        memcpy(sockaddr, info->ai_addr, sizeof(struct sockaddr_in6));
        sockaddr->ss_family = AF_INET6;
        result = 0;
    }
    else
    {
        AERON_SET_ERR(EINVAL, "Only IPv4 and IPv6 hosts are supported: family=%d", info->ai_family);
    }

    freeaddrinfo(info);

    return result;
}

bool aeron_try_parse_ipv4(const char *host, struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in *addr = (struct sockaddr_in *)sockaddr;

    if (inet_pton(AF_INET, host, &addr->sin_addr))
    {
        sockaddr->ss_family = AF_INET;
        return true;
    }

    return false;
}

int aeron_ipv4_addr_resolver(const char *host, int protocol, struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in *addr = (struct sockaddr_in *)sockaddr;

    if (inet_pton(AF_INET, host, &addr->sin_addr))
    {
        sockaddr->ss_family = AF_INET;
        return 0;
    }

    return aeron_ip_addr_resolver(host, sockaddr, AF_INET, protocol);
}

bool aeron_try_parse_ipv6(const char *host, struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in6 *addr = (struct sockaddr_in6 *)sockaddr;

    if (inet_pton(AF_INET6, host, &addr->sin6_addr))
    {
        sockaddr->ss_family = AF_INET6;
        return true;
    }

    return false;
}

int aeron_ipv6_addr_resolver(const char *host, int protocol, struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in6 *addr = (struct sockaddr_in6 *)sockaddr;

    if (inet_pton(AF_INET6, host, &addr->sin6_addr))
    {
        sockaddr->ss_family = AF_INET6;
        return 0;
    }

    return aeron_ip_addr_resolver(host, sockaddr, AF_INET6, protocol);
}

int aeron_udp_port_resolver(const char *port_str, bool optional)
{
    if (':' == *port_str)
    {
        port_str++;
    }

    if ('\0' == *port_str)
    {
        if (optional)
        {
            return 0;
        }
    }

    errno = 0;
    char *end_ptr = NULL;
    unsigned long value = strtoul(port_str, &end_ptr, 0);

    if ((0 == value && 0 != errno) || end_ptr == port_str)
    {
        AERON_SET_ERR(EINVAL, "port invalid: %s", port_str);
        return -1;
    }
    else if (value > UINT16_MAX)
    {
        AERON_SET_ERR(EINVAL, "port out of range: %s", port_str);
        return -1;
    }

    return (int)value;
}

int aeron_prefixlen_resolver(const char *prefixlen, unsigned long max)
{
    if ('\0' == *prefixlen)
    {
        return (int)max;
    }

    if ('/' == *prefixlen)
    {
        prefixlen++;
    }

    if (strcmp("0", prefixlen) == 0)
    {
        return 0;
    }

    errno = 0;
    char *end_ptr = NULL;
    unsigned long value = strtoul(prefixlen, &end_ptr, 0);

    if ((0 == value && 0 != errno) || end_ptr == prefixlen)
    {
        AERON_SET_ERR(EINVAL, "prefixlen invalid: %s", prefixlen);
        return -1;
    }
    else if (value > max)
    {
        AERON_SET_ERR(EINVAL, "prefixlen out of range: %s", prefixlen);
        return -1;
    }

    return (int)value;
}

int aeron_host_port_prefixlen_resolver(
    const char *host_str,
    const char *port_str,
    const char *prefixlen_str,
    struct sockaddr_storage *sockaddr,
    size_t *prefixlen,
    int family_hint)
{
    int host_result = -1, prefixlen_result = -1, port_result = aeron_udp_port_resolver(port_str, true);

    if (AF_INET == family_hint)
    {
        host_result = aeron_ipv4_addr_resolver(host_str, IPPROTO_UDP, sockaddr);
        ((struct sockaddr_in *)sockaddr)->sin_port = htons((uint16_t)port_result);
    }
    else if (AF_INET6 == family_hint)
    {
        host_result = aeron_ipv6_addr_resolver(host_str, IPPROTO_UDP, sockaddr);
        ((struct sockaddr_in6 *)sockaddr)->sin6_port = htons((uint16_t)port_result);
    }

    if (host_result >= 0 && port_result >= 0)
    {
        prefixlen_result = aeron_prefixlen_resolver(prefixlen_str, sockaddr->ss_family == AF_INET6 ? 128 : 32);
        if (prefixlen_result >= 0)
        {
            *prefixlen = (size_t)prefixlen_result;
        }
    }

    return prefixlen_result >= 0 ? 0 : prefixlen_result;
}

int aeron_interface_parse_and_resolve(const char *interface_str, struct sockaddr_storage *sockaddr, size_t *prefixlen)
{
    aeron_parsed_interface_t parsed_interface;

    if (-1 == aeron_interface_split(interface_str, &parsed_interface))
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (6 == parsed_interface.ip_version_hint)
    {
        return aeron_host_port_prefixlen_resolver(
            parsed_interface.host, parsed_interface.port, parsed_interface.prefix, sockaddr, prefixlen, AF_INET6);
    }

    return aeron_host_port_prefixlen_resolver(
        parsed_interface.host, parsed_interface.port, parsed_interface.prefix, sockaddr, prefixlen, AF_INET);
}

static aeron_getifaddrs_func_t aeron_getifaddrs_func = aeron_getifaddrs;

static aeron_freeifaddrs_func_t aeron_freeifaddrs_func = aeron_freeifaddrs;

void aeron_set_getifaddrs(aeron_getifaddrs_func_t get_func, aeron_freeifaddrs_func_t free_func)
{
    aeron_getifaddrs_func = get_func;
    aeron_freeifaddrs_func = free_func;
}

int aeron_lookup_interfaces(aeron_ifaddr_func_t func, void *clientd)
{
    struct ifaddrs *ifaddrs = NULL;
    int result = -1;

    if (aeron_getifaddrs_func(&ifaddrs) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return result;
    }

    result = aeron_lookup_interfaces_from_ifaddrs(func, clientd, ifaddrs);
    aeron_freeifaddrs_func(ifaddrs);

    return result;
}

int aeron_lookup_interfaces_from_ifaddrs(aeron_ifaddr_func_t func, void *clientd, struct ifaddrs *ifaddrs)
{
    int result = 0;
    for (struct ifaddrs *ifa = ifaddrs; ifa != NULL; ifa = ifa->ifa_next)
    {
        if (NULL == ifa->ifa_addr)
        {
            continue;
        }

        result += func(
            clientd,
            ifa->ifa_name,
            ifa->ifa_addr,
            ifa->ifa_netmask,
            ifa->ifa_flags);
    }

    return result;
}

uint32_t aeron_ipv4_netmask_from_prefixlen(size_t prefixlen)
{
    uint32_t value;

    if (0 == prefixlen)
    {
        value = ~(-1);
    }
    else
    {
        value = ~((UINT32_C(1) << (32 - prefixlen)) - 1);
    }

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    value = __builtin_bswap32(value);
#endif

    return value;
}

bool aeron_ipv4_does_prefix_match(struct in_addr *in_addr1, struct in_addr *in_addr2, size_t prefixlen)
{
    uint32_t addr1;
    uint32_t addr2;
    uint32_t netmask = aeron_ipv4_netmask_from_prefixlen(prefixlen);

    memcpy(&addr1, in_addr1, sizeof(addr1));
    memcpy(&addr2, in_addr2, sizeof(addr2));

    return (addr1 & netmask) == (addr2 & netmask);
}

size_t aeron_ipv4_netmask_to_prefixlen(struct in_addr *netmask)
{
    return __builtin_popcount(netmask->s_addr);
}

void aeron_set_ipv4_wildcard_host_and_port(struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in *addr = (struct sockaddr_in *)sockaddr;

    sockaddr->ss_family = AF_INET;
    addr->sin_port = htons(0);
    addr->sin_addr.s_addr = INADDR_ANY;
}

void aeron_set_ipv6_wildcard_host_and_port(struct sockaddr_storage *sockaddr)
{
    struct sockaddr_in6 *addr = (struct sockaddr_in6 *)sockaddr;
    memset(addr, 0, sizeof(struct sockaddr_in6));

    sockaddr->ss_family = AF_INET6;
    addr->sin6_port = htons(0);
    addr->sin6_addr = in6addr_any;
}

#if defined(AERON_COMPILER_GCC)
union aeron_128b_as_64b
{
    __uint128_t value;
    uint64_t q[2];
};

__uint128_t aeron_ipv6_netmask_from_prefixlen(size_t prefixlen)
{
    union aeron_128b_as_64b netmask;

    if (0 == prefixlen)
    {
        netmask.value = ~(-1);
    }
    else
    {
        netmask.value = ~(((__uint128_t)1 << (128 - prefixlen)) - (__uint128_t)1);
    }

#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    uint64_t q1 = netmask.q[1];
    netmask.q[1] = __builtin_bswap64(netmask.q[0]);
    netmask.q[0] = __builtin_bswap64(q1);
#endif

    return netmask.value;
}

bool aeron_ipv6_does_prefix_match(struct in6_addr *in6_addr1, struct in6_addr *in6_addr2, size_t prefixlen)
{
    __uint128_t addr1;
    __uint128_t addr2;
    __uint128_t netmask = aeron_ipv6_netmask_from_prefixlen(prefixlen);

    memcpy(&addr1, in6_addr1, sizeof(addr1));
    memcpy(&addr2, in6_addr2, sizeof(addr2));

    return (addr1 & netmask) == (addr2 & netmask);
}
#else
union aeron_128b_as_64b
{
    uint64_t q[2];
};
#endif

size_t aeron_ipv6_netmask_to_prefixlen(struct in6_addr *netmask)
{
    union aeron_128b_as_64b value;

    memcpy(&value, netmask, sizeof(value));

    return __builtin_popcountll(value.q[0]) + __builtin_popcountll(value.q[1]);
}

bool aeron_ip_does_prefix_match(struct sockaddr *addr1, struct sockaddr *addr2, size_t prefixlen)
{
    bool result = false;

    if (addr1->sa_family == addr2->sa_family)
    {
        if (AF_INET6 == addr1->sa_family)
        {
            result = aeron_ipv6_does_prefix_match(
                &((struct sockaddr_in6 *)addr1)->sin6_addr,
                &((struct sockaddr_in6 *)addr2)->sin6_addr,
                prefixlen);
        }
        else if (AF_INET == addr1->sa_family)
        {
            result = aeron_ipv4_does_prefix_match(
                &((struct sockaddr_in *)addr1)->sin_addr,
                &((struct sockaddr_in *)addr2)->sin_addr,
                prefixlen);
        }
    }

    return result;
}

size_t aeron_ip_netmask_to_prefixlen(struct sockaddr *netmask)
{
    return AF_INET6 == netmask->sa_family ?
        aeron_ipv6_netmask_to_prefixlen(&((struct sockaddr_in6 *)netmask)->sin6_addr) :
        aeron_ipv4_netmask_to_prefixlen(&((struct sockaddr_in *)netmask)->sin_addr);
}

struct lookup_state
{
    struct sockaddr_storage lookup_addr;
    struct sockaddr_storage *if_addr;
    unsigned int if_index;
    unsigned int if_flags;
    size_t prefixlen;
    size_t if_prefixlen;
    bool found;
};

int aeron_ip_lookup_func(
    void *clientd, const char *name, struct sockaddr *addr, struct sockaddr *netmask, unsigned int flags)
{
    if (flags & IFF_UP)
    {
        struct lookup_state *state = (struct lookup_state *)clientd;

        if (aeron_ip_does_prefix_match((struct sockaddr *)&state->lookup_addr, addr, state->prefixlen))
        {
            size_t addr_len = AF_INET6 == addr->sa_family ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);

            if ((flags & IFF_LOOPBACK) && !state->found)
            {
                memcpy(state->if_addr, addr, addr_len);
                state->if_index = if_nametoindex(name);
                state->found = true;
                return 1;
            }
            else if (flags & IFF_MULTICAST)
            {
                size_t current_if_prefixlen = aeron_ip_netmask_to_prefixlen(netmask);

                if (current_if_prefixlen > state->if_prefixlen)
                {
                    memcpy(state->if_addr, addr, addr_len);
                    state->if_index = if_nametoindex(name);
                    state->if_prefixlen = current_if_prefixlen;
                }

                state->found = true;
                return 1;
            }
        }
    }

    return 0;
}

struct lookup_by_name_and_family_state
{
    char *name;
    int family;
    struct sockaddr_storage *if_addr;
    unsigned int if_index;
    bool found;
    unsigned int name_matches;
};

static int aeron_ip_lookup_by_name_and_family_func(
    void *clientd, const char *name, struct sockaddr *addr, struct sockaddr *netmask, unsigned int flags)
{
    if (flags & IFF_UP)
    {
        struct lookup_by_name_and_family_state *state = (struct lookup_by_name_and_family_state *)clientd;

        if (0 == strcmp(name, state->name))
        {
            state->name_matches++;

            if (!state->found && state->family == addr->sa_family)
            {
                size_t addr_len = AF_INET6 == addr->sa_family ? sizeof(struct sockaddr_in6) : sizeof(struct sockaddr_in);

                memcpy(state->if_addr, addr, addr_len);
                state->if_index = if_nametoindex(name);
                state->found = true;

                return 1;
            }
        }
    }

    return 0;
}

void aeron_ip_copy_port(struct sockaddr_storage *dest_addr, struct sockaddr_storage *src_addr)
{
    if (AF_INET6 == src_addr->ss_family)
    {
        struct sockaddr_in6 *dest = (struct sockaddr_in6 *)dest_addr;
        struct sockaddr_in6 *src = (struct sockaddr_in6 *)src_addr;

        dest->sin6_port = src->sin6_port;
    }
    else if (AF_INET == src_addr->ss_family)
    {
        struct sockaddr_in *dest = (struct sockaddr_in *)dest_addr;
        struct sockaddr_in *src = (struct sockaddr_in *)src_addr;

        dest->sin_port = src->sin_port;
    }
}

int aeron_parse_named_interface(const char *interface_str, aeron_named_interface_t *out)
{
    if (interface_str[0] != AERON_NETUTIL_NAMED_INTERFACE_OPENING_CHAR)
    {
        return -1;
    }

    int close_idx = 0;
    int len = 1;
    while (true)
    {
        char c = interface_str[len];
        if (c == '\0')
        {
            break;
        }
        else if (c == '}')
        {
            close_idx = len;
        }
        len++;
    }

    int name_len = close_idx - 1;
    if (name_len <= 0 || name_len >= IF_NAMESIZE)
    {
        AERON_SET_ERR(EINVAL, "name length %d out of bounds", name_len);
        return -1;
    }

    int port = 0;
    int idx = close_idx + 1;
    if (idx < len)
    {
        if (interface_str[idx] != ':')
        {
            AERON_SET_ERR(EINVAL, "%s", "unexpected character after name closing brace");
            return -1;
        }

        idx++;

        int port_len = len - idx;
        if (port_len <= 0 || port_len > 5)
        {
            AERON_SET_ERR(EINVAL, "port length %d out of bounds", port_len);
            return -1;
        }

        port = aeron_udp_port_resolver(interface_str + idx, false);
        if (port < 0)
        {
            return -1;
        }
    }

    strncpy(out->name, interface_str + 1, name_len);
    out->name[name_len] = '\0';
    out->port = port;

    return 0;
}

static int aeron_find_interface_by_name_and_family(
    int family, const char *interface_str, struct sockaddr_storage *if_addr, unsigned int *if_index)
{
    aeron_named_interface_t named_interface;

    if (0 > aeron_parse_named_interface(interface_str, &named_interface))
    {
        AERON_APPEND_ERR("could not parse interface='%s'", interface_str);
        return -1;
    }

    struct lookup_by_name_and_family_state state = {
        .name = named_interface.name,
        .family = family,
        .if_addr = if_addr,
    };

    int result = aeron_lookup_interfaces(aeron_ip_lookup_by_name_and_family_func, &state);
    if (0 > result)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (0 == result)
    {
        if (!state.name_matches)
        {
            AERON_SET_ERR(EINVAL, "unknown interface %s", named_interface.name);
        }
        else
        {
            char buffer[20];
            switch (family)
            {
                case AF_INET:
                    sprintf(buffer, "INET");
                    break;
                case AF_INET6:
                    sprintf(buffer, "INET6");
                    break;
                default:
                    sprintf(buffer, "family %d", family);
                    break;
            }
            AERON_SET_ERR(EINVAL, "no %s addresses found on interface %s", buffer, named_interface.name);
        }
        return -1;
    }

    if (AF_INET6 == if_addr->ss_family)
    {
        struct sockaddr_in6 *dest = (struct sockaddr_in6 *)if_addr;

        dest->sin6_port = htons(named_interface.port);
    }
    else if (AF_INET == if_addr->ss_family)
    {
        struct sockaddr_in *dest = (struct sockaddr_in *)if_addr;

        dest->sin_port = htons(named_interface.port);
    }

    *if_index = state.if_index;

    return 0;
}

static int aeron_find_interface_by_address(
    const char *interface_str, struct sockaddr_storage *if_addr, unsigned int *if_index)
{
    struct lookup_state state = { 0 };

    if (aeron_interface_parse_and_resolve(interface_str, &state.lookup_addr, &state.prefixlen) < 0)
    {
        AERON_APPEND_ERR("could not parse interface='%s'", interface_str);
        return -1;
    }

    state.if_addr = if_addr;

    int result = aeron_lookup_interfaces(aeron_ip_lookup_func, &state);
    if (result < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (0 == result)
    {
        AERON_SET_ERR(EINVAL, "could not find matching interface='%s'", interface_str);
        return -1;
    }

    aeron_ip_copy_port(if_addr, &state.lookup_addr);
    *if_index = state.if_index;

    return 0;
}

static bool aeron_is_named_interface(const char *interface_str)
{
    return interface_str[0] == AERON_NETUTIL_NAMED_INTERFACE_OPENING_CHAR;
}

int aeron_find_interface(
    int family, const char *interface_str, struct sockaddr_storage *if_addr, unsigned int *if_index)
{
    if (aeron_is_named_interface(interface_str))
    {
        return aeron_find_interface_by_name_and_family(family, interface_str, if_addr, if_index);
    }

    return aeron_find_interface_by_address(interface_str, if_addr, if_index);
}

int aeron_find_unicast_interface(
    int family, const char *interface_str, struct sockaddr_storage *interface_addr, unsigned int *interface_index)
{
    *interface_index = 0;

    if (NULL != interface_str)
    {
        struct sockaddr_storage tmp_addr;
        size_t prefixlen = 0;

        if (!aeron_is_named_interface(interface_str) &&
            aeron_interface_parse_and_resolve(interface_str, &tmp_addr, &prefixlen) >= 0 &&
            aeron_is_wildcard_addr(&tmp_addr))
        {
            memcpy(interface_addr, &tmp_addr, sizeof(tmp_addr));
            return 0;
        }

        return aeron_find_interface(family, interface_str, interface_addr, interface_index);
    }
    else if (AF_INET6 == family)
    {
        interface_addr->ss_family = AF_INET6;
        struct sockaddr_in6 *addr = (struct sockaddr_in6 *)interface_addr;
        addr->sin6_addr = in6addr_any;
        addr->sin6_port = htons(0);
    }
    else
    {
        interface_addr->ss_family = AF_INET;
        struct sockaddr_in *addr = (struct sockaddr_in *)interface_addr;
        addr->sin_addr.s_addr = INADDR_ANY;
        addr->sin_port = htons(0);
    }

    return 0;
}

bool aeron_is_addr_multicast(struct sockaddr_storage *addr)
{
    bool result = false;

    if (AF_INET6 == addr->ss_family)
    {
        struct sockaddr_in6 *a = (struct sockaddr_in6 *)addr;

        result = IN6_IS_ADDR_MULTICAST(&a->sin6_addr);
    }
    else if (AF_INET == addr->ss_family)
    {
        struct sockaddr_in *a = (struct sockaddr_in *)addr;

        result = IN_MULTICAST(ntohl(a->sin_addr.s_addr));
    }

    return result;
}

bool aeron_is_wildcard_addr(struct sockaddr_storage *addr)
{
    bool result = false;

    if (AF_INET6 == addr->ss_family)
    {
        struct sockaddr_in6 *a = (struct sockaddr_in6 *)addr;

        return memcmp(&a->sin6_addr, &in6addr_any, sizeof(in6addr_any)) == 0 ? true : false;
    }
    else if (AF_INET == addr->ss_family)
    {
        struct sockaddr_in *a = (struct sockaddr_in *)addr;

        result = a->sin_addr.s_addr == INADDR_ANY;
    }

    return result;
}

bool aeron_is_wildcard_port(struct sockaddr_storage *addr)
{
    bool result = false;

    if (AF_INET6 == addr->ss_family)
    {
        struct sockaddr_in6 *a = (struct sockaddr_in6 *)addr;

        return 0 == a->sin6_port;
    }
    else if (AF_INET == addr->ss_family)
    {
        struct sockaddr_in *a = (struct sockaddr_in *)addr;

        result = 0 == a->sin_port;
    }

    return result;
}

int aeron_format_source_identity(char *buffer, size_t length, struct sockaddr_storage *addr)
{
    char addr_str[INET6_ADDRSTRLEN] = "";

    if (length < AERON_NETUTIL_FORMATTED_MAX_LENGTH)
    {
        return -ENOSPC;
    }

    int total = 0;
    if (AF_INET6 == addr->ss_family)
    {
        struct sockaddr_in6 *in6 = (struct sockaddr_in6 *)addr;

        inet_ntop(addr->ss_family, &in6->sin6_addr, addr_str, sizeof(addr_str));
        unsigned short port = ntohs(in6->sin6_port);
        total = snprintf(buffer, length, "[%s]:%d", addr_str, port);
    }
    else if (AF_INET == addr->ss_family)
    {
        struct sockaddr_in *in4 = (struct sockaddr_in *)addr;

        inet_ntop(addr->ss_family, &in4->sin_addr, addr_str, sizeof(addr_str));
        unsigned short port = ntohs(in4->sin_port);
        total = snprintf(buffer, length, "%s:%d", addr_str, port);
    }

    if (total < 0)
    {
        return 0;
    }

    return total;
}

int aeron_netutil_get_so_buf_lengths(size_t *default_so_rcvbuf, size_t *default_so_sndbuf)
{
    int result = -1;

    aeron_socket_t fd = aeron_socket(PF_INET, SOCK_DGRAM, 0);
    if (fd < 0)
    {
        AERON_APPEND_ERR("%s", "failed to probe socket for buffer lengths");
        goto done;
    }

    socklen_t optlen = sizeof(size_t);

    if (aeron_getsockopt(fd, SOL_SOCKET, SO_RCVBUF, default_so_rcvbuf, &optlen) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to get SOL_SOCKET/SO_RCVBUF option");
        goto done;
    }

    if (aeron_getsockopt(fd, SOL_SOCKET, SO_SNDBUF, default_so_sndbuf, &optlen) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to get SOL_SOCKET/SO_SNDBUF option");
        goto done;
    }

    result = 0;

done:
    if (fd > 0)
    {
        aeron_close_socket(fd);
    }

    return result;
}

int aeron_sockaddr_storage_cmp(struct sockaddr_storage *a, struct sockaddr_storage *b, bool *result)
{
    if (a->ss_family != b->ss_family)
    {
        *result = false;
    }
    else if (AF_INET == a->ss_family)
    {
        struct sockaddr_in *a_in = (struct sockaddr_in *)a;
        struct sockaddr_in *b_in = (struct sockaddr_in *)b;

        *result = (a_in->sin_addr.s_addr == b_in->sin_addr.s_addr) && (a_in->sin_port == b_in->sin_port);
    }
    else if (AF_INET6 == a->ss_family)
    {
        struct sockaddr_in6 *a_in6 = (struct sockaddr_in6 *)a;
        struct sockaddr_in6 *b_in6 = (struct sockaddr_in6 *)b;

        *result = 0 == memcmp(&a_in6->sin6_addr, &b_in6->sin6_addr, sizeof(struct sockaddr_in6)) &&
            (a_in6->sin6_port == b_in6->sin6_port);
    }
    else
    {
        *result = false;
        AERON_SET_ERR(EINVAL, "%s", "Unsupported address family");
        return -1;
    }

    return 0;
}
