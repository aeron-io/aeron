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

#include "aeron_socket.h"
#include "util/aeron_error.h"
#include "command/aeron_control_protocol.h"
#include "aeron_alloc.h"
#include "util/aeron_netutil.h"

#if defined(AERON_COMPILER_GCC)

#include <fcntl.h>
#include <unistd.h>
#include <sys/socket.h>
#include <errno.h>
#include <poll.h>

int aeron_net_init(void)
{
    return 0;
}

int aeron_set_socket_non_blocking(aeron_socket_t fd)
{
    int flags;
    if ((flags = fcntl(fd, F_GETFL, 0)) < 0)
    {
        AERON_SET_ERR(errno, "failed to fcntl(fd=%d, cmd=F_GETFL, 0)", fd);
        return -1;
    }

    flags |= O_NONBLOCK;
    if (fcntl(fd, F_SETFL, flags) < 0)
    {
        AERON_SET_ERR(errno, "failed to fcntl(fd=%d, cmd=F_SETFL, %d)", fd, flags);
        return -1;
    }

    return 0;
}

aeron_socket_t aeron_socket(int domain, int type, int protocol)
{
    int socket_fd = socket(domain, type, protocol);

    if (socket_fd < 0)
    {
        AERON_SET_ERR(errno, "failed to socket(domain=%d, type=%d, protocol=%d)", domain, type, protocol);
        return -1;
    }

    return socket_fd;
}

void aeron_close_socket(aeron_socket_t socket)
{
    close(socket);
}

int aeron_connect(aeron_socket_t fd, struct sockaddr *address, socklen_t address_length)
{
    if (connect(fd, address, address_length) < 0)
    {
        char addr_str[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
        aeron_format_source_identity(addr_str, sizeof(addr_str), (struct sockaddr_storage *)address);
        AERON_SET_ERR(errno, "failed to connect to address: %s", addr_str);

        return -1;
    }

    return 0;
}

int aeron_bind(aeron_socket_t fd, struct sockaddr *address, socklen_t address_length)
{
    if (bind(fd, address, address_length) < 0)
    {
        char buffer[AERON_NETUTIL_FORMATTED_MAX_LENGTH] = { 0 };
        aeron_format_source_identity(buffer, AERON_NETUTIL_FORMATTED_MAX_LENGTH, (struct sockaddr_storage *)address);
        AERON_SET_ERR(errno, "failed to bind(%d, %s)", fd, buffer);
        return -1;
    }

    return 0;
}


int aeron_getifaddrs(struct ifaddrs **ifap)
{
    if (getifaddrs(ifap) < 0)
    {
        AERON_SET_ERR(errno, "%s", "Failed getifaddrs(...)");
        return -1;
    }

    return 0;
}

void aeron_freeifaddrs(struct ifaddrs *ifa)
{
    freeifaddrs(ifa);
}

ssize_t aeron_sendmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags)
{
    ssize_t result = sendmsg(fd, msghdr, flags);

    if (result < 0)
    {
        if (aeron_is_acceptable_socket_error())
        {
            return 0;
        }
        else
        {
            AERON_SET_ERR(errno, "failed sendmsg(fd=%d,...)", fd);
            return -1;
        }
    }

    return result;
}

ssize_t aeron_send(aeron_socket_t fd, const void *buf, size_t len, int flags)
{
    ssize_t result = send(fd, buf, len, flags);

    if (result < 0)
    {
        if (aeron_is_acceptable_socket_error())
        {
            return 0;
        }
        else
        {
            AERON_SET_ERR(errno, "failed send(fd=%d,...)", fd);
            return -1;
        }
    }

    return result;
}

ssize_t aeron_recvmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags)
{
    ssize_t result = recvmsg(fd, msghdr, flags);

    if (result < 0)
    {
        if (aeron_is_acceptable_socket_error())
        {
            return 0;
        }

        AERON_SET_ERR(errno, "failed recvmsg(fd=%d,...)", fd);
        return -1;
    }

    return result;
}

int aeron_poll(struct pollfd *fds, unsigned long nfds, int timeout)
{
    int result = poll(fds, (nfds_t)nfds, timeout);
    if (result < 0)
    {
        if (EAGAIN == errno || EWOULDBLOCK == errno || EINTR == errno)
        {
            result = 0;
        }
        else
        {
            AERON_SET_ERR(errno, "%s", "failed to poll(...)");
            return -1;
        }
    }

    return result;
}

int aeron_getsockopt(aeron_socket_t fd, int level, int optname, void *optval, socklen_t *optlen)
{
    if (getsockopt(fd, level, optname, optval, optlen) < 0)
    {
        AERON_SET_ERR(errno, "getsockopt(fd=%d,...)", fd);
        return -1;
    }

    return 0;
}

int aeron_setsockopt(aeron_socket_t fd, int level, int optname, const void *optval, socklen_t optlen)
{
    if (setsockopt(fd, level, optname, optval, optlen) < 0)
    {
        AERON_SET_ERR(errno, "setsockopt(fd=%d,...)", fd);
        return -1;
    }

    return 0;
}

#elif defined(AERON_COMPILER_MSVC)

#if _WIN32_WINNT < 0x0600
#error Unsupported windows version
#endif
#if UNICODE
#error Unicode errors not supported
#endif


#include <ws2ipdef.h>
#include <iphlpapi.h>

int aeron_net_init()
{
    static int started = -1;

    if (-1 == started)
    {
        WORD wVersionRequested = MAKEWORD(2, 2);
        WSADATA buffer = { 0 };
        int err = WSAStartup(wVersionRequested, &buffer);

        if (0 != err)
        {
            AERON_SET_ERR_WIN(err, "%s", "WSAStartup(...)");
            return -1;
        }

        started = 0;
    }

    return 0;
}

int aeron_set_socket_non_blocking(aeron_socket_t fd)
{
    u_long mode = 1;
    const int result = ioctlsocket(fd, FIONBIO, &mode);
    if (SOCKET_ERROR == result)
    {
        AERON_SET_ERR_WIN(WSAGetLastError(), "ioctlsocket(fd=%d,...)", fd);
        return -1;
    }

    return 0;
}

int aeron_getifaddrs(struct ifaddrs **ifap)
{
    DWORD max_tries = 2;
    DWORD adapters_addresses_size = 10 * sizeof(IP_ADAPTER_ADDRESSES);
    IP_ADAPTER_ADDRESSES *adapters_addresses = NULL;

    /* loop to handle interfaces coming online causing a buffer overflow
     * between first call to list buffer length and second call to enumerate.
     */
    for (unsigned i = max_tries; i; i--)
    {
        if (aeron_alloc((void **)&adapters_addresses, adapters_addresses_size) < 0)
        {
            AERON_APPEND_ERR("%s", "unable to allocate IP_ADAPTER_ADDRESSES");
            return -1;
        }

        DWORD result = GetAdaptersAddresses(
            AF_UNSPEC,
            GAA_FLAG_INCLUDE_PREFIX |
                GAA_FLAG_SKIP_ANYCAST |
                GAA_FLAG_SKIP_DNS_SERVER |
                GAA_FLAG_SKIP_FRIENDLY_NAME |
                GAA_FLAG_SKIP_MULTICAST,
            NULL,
            adapters_addresses,
            &adapters_addresses_size);

        if (ERROR_BUFFER_OVERFLOW == result)
        {
            aeron_free(adapters_addresses);
            adapters_addresses = NULL;
        }
        else if (ERROR_SUCCESS == result)
        {
            break;
        }
        else
        {
            aeron_free(adapters_addresses);
            AERON_SET_ERR_WIN(result, "%s", "GetAdaptersAddresses(...)");
            return -1;
        }
    }

    struct ifaddrs *head = NULL;
    struct ifaddrs *tail = NULL;

    /* now populate list */
    for (IP_ADAPTER_ADDRESSES *adapter = adapters_addresses; adapter; adapter = adapter->Next)
    {
        int unicast_index = 0;
        for (IP_ADAPTER_UNICAST_ADDRESS *unicast = adapter->FirstUnicastAddress;
            unicast;
            unicast = unicast->Next, ++unicast_index)
        {
            /* ensure IP adapter */
            if (AF_INET != unicast->Address.lpSockaddr->sa_family &&
                AF_INET6 != unicast->Address.lpSockaddr->sa_family)
            {
                continue;
            }

            struct ifaddrs *current = NULL;
            DWORD supplemental_data_length = 2 * unicast->Address.iSockaddrLength + IF_NAMESIZE;

            if (aeron_alloc((void **)&current, sizeof(struct ifaddrs) + supplemental_data_length) < 0)
            {
                AERON_APPEND_ERR("%s", "unable to allocate ifaddrs");
                aeron_freeifaddrs(head);
                aeron_free(adapters_addresses);
                return -1;
            }

            if (NULL == head)
            {
                head = current;
                tail = head;
            }
            else
            {
                tail->ifa_next = current;
                tail = current;
            }

            uint8_t *supplemental_data = (uint8_t *)(current + 1);
            current->ifa_addr = (struct sockaddr *)(supplemental_data);
            current->ifa_netmask = (struct sockaddr *)(supplemental_data + unicast->Address.iSockaddrLength);
            current->ifa_name = (char *)(supplemental_data + (2 * unicast->Address.iSockaddrLength));

            /* address */
            memcpy(current->ifa_addr, unicast->Address.lpSockaddr, unicast->Address.iSockaddrLength);

            /* name */
            strncpy_s(current->ifa_name, IF_NAMESIZE, adapter->AdapterName, _TRUNCATE);

            /* flags */
            current->ifa_flags = 0;
            if (IfOperStatusUp == adapter->OperStatus)
            {
                current->ifa_flags |= IFF_UP;
            }

            if (IF_TYPE_SOFTWARE_LOOPBACK == adapter->IfType)
            {
                current->ifa_flags |= IFF_LOOPBACK;
            }

            if (!(adapter->Flags & IP_ADAPTER_NO_MULTICAST))
            {
                current->ifa_flags |= IFF_MULTICAST;
            }

            /* netmask */
            ULONG prefixLength = unicast->OnLinkPrefixLength;

            /* map prefix to netmask */
            current->ifa_netmask->sa_family = unicast->Address.lpSockaddr->sa_family;

            switch (unicast->Address.lpSockaddr->sa_family)
            {
                case AF_INET:
                    if (0 == prefixLength || prefixLength > 32)
                    {
                        prefixLength = 32;
                    }

                    ULONG Mask;
                    ConvertLengthToIpv4Mask(prefixLength, &Mask);
                    ((struct sockaddr_in *)current->ifa_netmask)->sin_addr.s_addr = htonl(Mask);
                    break;

                case AF_INET6:
                    if (0 == prefixLength || prefixLength > 128)
                    {
                        prefixLength = 128;
                    }

                    for (LONG i = (LONG)prefixLength, j = 0; i > 0; i -= 8, ++j)
                    {
                        ((struct sockaddr_in6 *)current->ifa_netmask)->sin6_addr.s6_addr[j] = i >= 8 ?
                            0xff : (ULONG)((0xffU << (8 - i)) & 0xffU);
                    }
                    break;

                default:
                    break;
            }
        }
    }

    aeron_free(adapters_addresses);
    *ifap = head;

    return 0;
}

void aeron_freeifaddrs(struct ifaddrs *current)
{
    if (NULL != current)
    {
        while (1)
        {
            struct ifaddrs *next = current->ifa_next;
            aeron_free(current);
            current = next;

            if (NULL == current)
            {
                break;
            }
        }
    }
}

ssize_t aeron_recvmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags)
{
    DWORD size = 0;
    const int result = WSARecvFrom(
        fd,
        (LPWSABUF)msghdr->msg_iov,
        msghdr->msg_iovlen,
        &size,
        &msghdr->msg_flags,
        msghdr->msg_name,
        &msghdr->msg_namelen,
        NULL,
        NULL);

    if (SOCKET_ERROR == result)
    {
        const int err = WSAGetLastError();
        if (WSAEWOULDBLOCK == err || WSAEINTR == err || WSAECONNRESET == err)
        {
            return 0;
        }

        AERON_SET_ERR_WIN(err, "WSARecvFrom(fd=%d,...)", fd);
        return -1;
    }

    return size;
}

ssize_t aeron_send(aeron_socket_t fd, const void *buf, size_t len, int flags)
{
    const DWORD size = send(fd, (const char *)buf, (int)len, flags);

    if (SOCKET_ERROR == size)
    {
        const int err = WSAGetLastError();
        if (WSAEWOULDBLOCK == err || WSAEINTR == err)
        {
            return 0;
        }

        AERON_SET_ERR_WIN(err, "send(fd=%d,...)", fd);
        return -1;
    }

    return size;
}

ssize_t aeron_sendmsg(aeron_socket_t fd, struct msghdr *msghdr, int flags)
{
    DWORD size = 0;
    const int result = WSASendTo(
        fd,
        (LPWSABUF)msghdr->msg_iov,
        msghdr->msg_iovlen,
        &size,
        msghdr->msg_flags,
        (const struct sockaddr *)msghdr->msg_name,
        msghdr->msg_namelen,
        NULL,
        NULL);

    if (SOCKET_ERROR == result)
    {
        const int err = WSAGetLastError();
        if (WSAEWOULDBLOCK == err || WSAEINTR == err)
        {
            return 0;
        }

        AERON_SET_ERR_WIN(err, "WSASendTo(fd=%d,...)", fd);
        return -1;
    }

    return size;
}

int aeron_poll(struct pollfd *fds, unsigned long nfds, int timeout)
{
    int result = WSAPoll(fds, (ULONG)nfds, timeout);

    if (SOCKET_ERROR == result)
    {
        const int err = WSAGetLastError();

        AERON_SET_ERR_WIN(err, "%s", "WSAPoll(...)");
        return -1;
    }

    return result;
}

aeron_socket_t aeron_socket(int domain, int type, int protocol)
{
    aeron_net_init();
    const SOCKET handle = socket(domain, type, protocol);
    if (INVALID_SOCKET == handle)
    {
        AERON_SET_ERR_WIN(
            WSAGetLastError(), "failed to socket(domain=%d, type=%d, protocol=%d)", domain, type, protocol);
        return (aeron_socket_t)-1;
    }

    return (aeron_socket_t)handle;
}

void aeron_close_socket(aeron_socket_t socket)
{
    closesocket(socket);
}

int aeron_connect(aeron_socket_t fd, struct sockaddr *address, socklen_t address_length)
{
    if (SOCKET_ERROR == connect(fd, address, address_length))
    {
        char addr_str[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
        aeron_format_source_identity(addr_str, sizeof(addr_str), (struct sockaddr_storage *)address);
        struct sockaddr_in *a = (struct sockaddr_in *) address;
        printf("addr: %lu, %d\n", a->sin_addr.s_addr, a->sin_port);
        AERON_SET_ERR_WIN(WSAGetLastError(), "failed to connect to address: %s", addr_str);

        return -1;
    }

    return 0;
}

int aeron_bind(aeron_socket_t fd, struct sockaddr *address, socklen_t address_length)
{
    if (SOCKET_ERROR == bind(fd, address, address_length))
    {
        char addr_str[AERON_NETUTIL_FORMATTED_MAX_LENGTH];
        aeron_format_source_identity(addr_str, sizeof(addr_str), (struct sockaddr_storage *)address);
        AERON_SET_ERR_WIN(WSAGetLastError(), "failed to bind to address: %s", addr_str);

        return -1;
    }

    return 0;
}

/* aeron_getsockopt and aeron_setsockopt ensure a consistent signature between platforms
 * (MSVC uses char * instead of void * for optval, which causes warnings)
 */
int aeron_getsockopt(aeron_socket_t fd, int level, int optname, void *optval, socklen_t *optlen)
{
    if (SOCKET_ERROR == getsockopt(fd, level, optname, optval, optlen))
    {
        AERON_SET_ERR_WIN(GetLastError(), "getsockopt(fd=%d,...)", fd);
        return -1;
    }

    return 0;
}

int aeron_setsockopt(aeron_socket_t fd, int level, int optname, const void *optval, socklen_t optlen)
{
    if (SOCKET_ERROR == setsockopt(fd, level, optname, optval, optlen))
    {
        AERON_SET_ERR_WIN(GetLastError(), "setsockopt(fd=%d,...)", fd);
        return -1;
    }

    return 0;
}

#else
#error Unsupported platform!
#endif
