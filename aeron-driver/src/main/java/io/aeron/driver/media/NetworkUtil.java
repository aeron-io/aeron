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
package io.aeron.driver.media;

import org.agrona.BufferUtil;

import java.net.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Objects;

import static java.lang.Boolean.compare;
import static java.lang.Integer.compare;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.Collections.sort;

/**
 * Collection of network specific utility functions.
 */
public final class NetworkUtil
{
    private NetworkUtil()
    {
    }

    /**
     * Search for a list of network interfaces that match the specified address and subnet prefix.
     * The results will be ordered by the length of the subnet prefix
     * ({@link InterfaceAddress#getNetworkPrefixLength()}). If no results match, then the collection
     * will be empty.
     *
     * @param address      to search for on the {@link NetworkInterface}s.
     * @param subnetPrefix to limit the search.
     * @return {@link NetworkInterface}s that match the supplied criteria, ordered by the length
     * of the subnet prefix. Empty if none match.
     * @throws SocketException if an error occurs
     */
    public static NetworkInterface[] filterBySubnet(final InetAddress address, final int subnetPrefix)
        throws SocketException
    {
        return filterBySubnet(NetworkInterfaceShim.DEFAULT, address, subnetPrefix);
    }

    /**
     * Allocate a direct {@link ByteBuffer} that is padded at the end with at least alignment bytes.
     *
     * @param capacity  for the buffer.
     * @param alignment for the buffer.
     * @return the direct {@link ByteBuffer}.
     */
    public static ByteBuffer allocateDirectAlignedAndPadded(final int capacity, final int alignment)
    {
        final ByteBuffer buffer = BufferUtil.allocateDirectAligned(capacity + alignment, alignment);

        buffer.limit(buffer.limit() - alignment);

        return buffer.slice();
    }

    /**
     * Format an address and port pair, so they can be used in a URI endpoint.
     *
     * @param address part of the endpoint.
     * @param port    part of the endpoint.
     * @return The formatted string for an address, IPv4 or IPv6, and port separated by a ':'.
     */
    public static String formatAddressAndPort(final InetAddress address, final int port)
    {
        if (address instanceof Inet6Address)
        {
            return "[" + address.getHostAddress() + "]:" + port;
        }
        else
        {
            return address.getHostAddress() + ":" + port;
        }
    }

    /**
     * Get the {@link ProtocolFamily} to which the address belongs.
     *
     * @param address to get the {@link ProtocolFamily} for.
     * @return the {@link ProtocolFamily} to which the address belongs.
     */
    public static ProtocolFamily getProtocolFamily(final InetAddress address)
    {
        if (address instanceof Inet4Address)
        {
            return StandardProtocolFamily.INET;
        }
        else if (address instanceof Inet6Address)
        {
            return StandardProtocolFamily.INET6;
        }
        else
        {
            throw new IllegalStateException("Unknown ProtocolFamily");
        }
    }

    /**
     * Scans through to the local interfaces and returns the first bound InetAddress that matches the locally
     * defined address and network mask. Useful for determining a return path network address for machines with
     * a simple network topology. May not give the ideal result when a machine has multiple interfaces that match
     * an outgoing network address.
     *
     * @param address used to scan for a matching local address
     * @return first matching address bound to a local interface or null if none match.
     * @throws SocketException if an underlying SocketException is thrown, e.g. when getting the network interfaces.
     */
    public static InetAddress findFirstMatchingLocalAddress(final InetAddress address) throws SocketException
    {
        final Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements())
        {
            final NetworkInterface networkInterface = networkInterfaces.nextElement();
            for (final InterfaceAddress interfaceAddress : networkInterface.getInterfaceAddresses())
            {
                if (NetworkUtil.isMatchWithPrefix(
                    address.getAddress(),
                    interfaceAddress.getAddress().getAddress(),
                    interfaceAddress.getNetworkPrefixLength()))
                {
                    return interfaceAddress.getAddress();
                }
            }
        }

        return null;
    }


    static NetworkInterface[] filterBySubnet(
        final NetworkInterfaceShim shim, final InetAddress address, final int subnetPrefix)
        throws SocketException
    {
        final ArrayList<FilterResult> filterResults = new ArrayList<>();
        final byte[] queryAddress = address.getAddress();

        final Enumeration<NetworkInterface> interfaces = shim.getNetworkInterfaces();
        while (interfaces.hasMoreElements())
        {
            final NetworkInterface networkInterface = interfaces.nextElement();
            final InterfaceAddress interfaceAddress = findAddressOnInterface(
                shim, networkInterface, queryAddress, subnetPrefix);

            if (null != interfaceAddress)
            {
                filterResults.add(new FilterResult(
                    interfaceAddress, networkInterface, shim.isLoopback(networkInterface)));
            }
        }

        sort(filterResults);

        final int size = filterResults.size();
        final NetworkInterface[] results = new NetworkInterface[size];
        for (int i = 0; i < size; i++)
        {
            results[i] = filterResults.get(i).networkInterface;
        }

        return results;
    }

    static InetAddress findAddressOnInterface(
        final NetworkInterface networkInterface, final InetAddress address, final int subnetPrefix)
    {
        final InterfaceAddress interfaceAddress = findAddressOnInterface(
            NetworkInterfaceShim.DEFAULT, networkInterface, address.getAddress(), subnetPrefix);

        return null == interfaceAddress ? null : interfaceAddress.getAddress();
    }

    static InterfaceAddress findAddressOnInterface(
        final NetworkInterfaceShim shim,
        final NetworkInterface networkInterface,
        final byte[] queryAddress,
        final int prefixLength)
    {
        for (final InterfaceAddress interfaceAddress : shim.getInterfaceAddresses(networkInterface))
        {
            if (null != interfaceAddress)
            {
                final InetAddress address = interfaceAddress.getAddress();
                if (null != address)
                {
                    if (isMatchWithPrefix(address.getAddress(), queryAddress, prefixLength))
                    {
                        return interfaceAddress;
                    }
                }
            }
        }

        return null;
    }

    /**
     * Matches to network address with the specified prefix length.  Only works with 4 and 16 byte addresses.
     *
     * @param candidate address to be matched
     * @param expected address to match against (could be a network address - with 0s at the end)
     * @param prefixLength the number of bit required to match.
     * @return true if the leading prefixLength number of bits of the two addresses match.
     */
    public static boolean isMatchWithPrefix(final byte[] candidate, final byte[] expected, final int prefixLength)
    {
        if (candidate.length != expected.length)
        {
            return false;
        }

        if (candidate.length == 4)
        {
            final int mask = prefixLengthToIpV4Mask(prefixLength);

            return (toInt(candidate) & mask) == (toInt(expected) & mask);
        }
        else if (candidate.length == 16)
        {
            final long upperMask = prefixLengthToIpV6Mask(min(prefixLength, 64));
            final long lowerMask = prefixLengthToIpV6Mask(max(prefixLength - 64, 0));

            return
                (upperMask & toLong(candidate, 0)) == (upperMask & toLong(expected, 0)) &&
                (lowerMask & toLong(candidate, 8)) == (lowerMask & toLong(expected, 8));
        }

        throw new IllegalArgumentException("how many bytes does an IP address have again?");
    }

    static int prefixLengthToIpV4Mask(final int subnetPrefix)
    {
        return 0 == subnetPrefix ? 0 : -(1 << 32 - subnetPrefix);
    }

    private static long prefixLengthToIpV6Mask(final int subnetPrefix)
    {
        return 0 == subnetPrefix ? 0 : -(1L << 64 - subnetPrefix);
    }

    private static int toInt(final byte[] b)
    {
        return ((b[3] & 0xFF)) + ((b[2] & 0xFF) << 8) + ((b[1] & 0xFF) << 16) + ((b[0]) << 24);
    }

    static long toLong(final byte[] b, final int offset)
    {
        return ((b[offset + 7] & 0xFFL)) +
            ((b[offset + 6] & 0xFFL) << 8) +
            ((b[offset + 5] & 0xFFL) << 16) +
            ((b[offset + 4] & 0xFFL) << 24) +
            ((b[offset + 3] & 0xFFL) << 32) +
            ((b[offset + 2] & 0xFFL) << 40) +
            ((b[offset + 1] & 0xFFL) << 48) +
            (((long)b[offset]) << 56);
    }

    static class FilterResult implements Comparable<FilterResult>
    {
        private final InterfaceAddress interfaceAddress;
        private final NetworkInterface networkInterface;
        private final boolean isLoopback;

        FilterResult(
            final InterfaceAddress interfaceAddress,
            final NetworkInterface networkInterface,
            final boolean isLoopback)
        {
            this.interfaceAddress = interfaceAddress;
            this.networkInterface = networkInterface;
            this.isLoopback = isLoopback;
        }

        public int compareTo(final FilterResult other)
        {
            if (isLoopback == other.isLoopback)
            {
                return -compare(
                    interfaceAddress.getNetworkPrefixLength(),
                    other.interfaceAddress.getNetworkPrefixLength());
            }
            else
            {
                return compare(isLoopback, other.isLoopback);
            }
        }

        public boolean equals(final Object o)
        {
            return o instanceof FilterResult && compareTo((FilterResult)o) == 0;
        }

        public int hashCode()
        {
            return Objects.hash(interfaceAddress, networkInterface, isLoopback);
        }
    }
}
