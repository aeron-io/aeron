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

#ifndef AERON_SOCKET_H
#define AERON_SOCKET_H

#include <stdint.h>
#include "util/aeron_platform.h"

#if defined(AERON_COMPILER_GCC)

#include <netinet/in.h>
#include <sys/socket.h>
#include <net/if.h>
#include <netinet/ip.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <poll.h>

typedef int aeron_socket_t;

#elif defined(AERON_COMPILER_MSVC)

#include <WinSock2.h>
#include <Windows.h>
#include <ws2ipdef.h>
#include <WS2tcpip.h>
#include <iphlpapi.h>

// SOCKET is uint64_t but we need a signed type to match the Linux version
typedef int64_t aeron_socket_t;

struct iovec
{
    ULONG iov_len;
    void *iov_base;
};

// must match _WSAMSG
struct msghdr {
    void *msg_name;
    INT msg_namelen;
    struct iovec *msg_iov;
    ULONG msg_iovlen;
    ULONG msg_controllen;
    void *msg_control;
    ULONG msg_flags;
};

struct ifaddrs
{
    struct ifaddrs *ifa_next;
    char *ifa_name;
    unsigned int ifa_flags;

    struct sockaddr *ifa_addr;
    struct sockaddr *ifa_netmask;
    union
    {
        struct sockaddr *ifu_broadaddr;
        struct sockaddr *ifu_dstaddr;
    }
    ifa_ifu;

# ifndef ifa_broadaddr
#  define ifa_broadaddr      ifa_ifu.ifu_broadaddr
# endif
# ifndef ifa_dstaddr
#  define ifa_dstaddr        ifa_ifu.ifu_dstaddr
# endif

    void *ifa_data;
};

typedef SSIZE_T ssize_t;

#else
#error Unsupported platform!
#endif

int aeron_set_socket_non_blocking(aeron_socket_t fd);

aeron_socket_t aeron_socket(int domain, int type, int protocol);

void aeron_close_socket(aeron_socket_t socket);

int aeron_connect(aeron_socket_t fd, struct sockaddr *address, socklen_t address_length);

int aeron_bind(aeron_socket_t fd, struct sockaddr *address, socklen_t address_length);

int aeron_net_init(void);

int aeron_getsockopt(aeron_socket_t fd, int level, int optname, void *optval, socklen_t *optlen);

int aeron_setsockopt(aeron_socket_t fd, int level, int optname, const void *optval, socklen_t optlen);

int aeron_getifaddrs(struct ifaddrs **ifap);

void aeron_freeifaddrs(struct ifaddrs *ifa);

ssize_t aeron_sendmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags);

ssize_t aeron_send(aeron_socket_t fd, const void *buf, size_t len, int flags);

ssize_t aeron_recvmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags);

int aeron_poll(struct pollfd *fds, unsigned long nfds, int timeout);

#endif //AERON_SOCKET_H
