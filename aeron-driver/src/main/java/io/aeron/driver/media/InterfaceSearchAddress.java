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

import org.agrona.AsciiEncoding;
import org.agrona.Strings;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.ProtocolFamily;
import java.net.SocketException;
import java.net.UnknownHostException;

import static io.aeron.driver.media.NetworkUtil.filterBySubnet;
import static io.aeron.driver.media.NetworkUtil.findAddressOnInterface;
import static java.lang.System.lineSeparator;
import static java.util.Objects.requireNonNull;

record InterfaceSearchAddress(InetSocketAddress address, int subnetPrefix) implements UnresolvedInterface
{
    private static final InterfaceSearchAddress WILDCARD = new InterfaceSearchAddress(new InetSocketAddress(0), 0);

    InterfaceSearchAddress
    {
        requireNonNull(address, "address must not be null");
    }

    public ResolvedInterface resolve(final boolean multicast, final ProtocolFamily protocolFamily)
        throws SocketException
    {
        final NetworkInterface localInterface;
        final InetSocketAddress resolvedAddress;

        if (!multicast && address.getAddress().isAnyLocalAddress())
        {
            localInterface = null;
            resolvedAddress = address;
        }
        else
        {
            localInterface = findInterface();
            resolvedAddress = resolveToAddressOfInterface(localInterface);
        }

        return new ResolvedInterface(localInterface, resolvedAddress);
    }

    private InetSocketAddress resolveToAddressOfInterface(final NetworkInterface localInterface)
    {
        final InetAddress interfaceAddress = findAddressOnInterface(localInterface, address.getAddress(), subnetPrefix);

        if (null == interfaceAddress)
        {
            throw new IllegalStateException("failed to find " + address.getAddress() + "/" + subnetPrefix + " in " +
                                            localInterface.getInterfaceAddresses());
        }

        return new InetSocketAddress(interfaceAddress, address.getPort());
    }

    private NetworkInterface findInterface() throws SocketException
    {
        final NetworkInterface[] filteredInterfaces = filterBySubnet(address.getAddress(), subnetPrefix);

        for (final NetworkInterface networkInterface : filteredInterfaces)
        {
            if (networkInterface.isUp() && (networkInterface.supportsMulticast() || networkInterface.isLoopback()))
            {
                return networkInterface;
            }
        }

        throw new IllegalArgumentException(noMatchingInterfacesError(filteredInterfaces));
    }

    private String noMatchingInterfacesError(final NetworkInterface[] filteredInterfaces) throws SocketException
    {
        final StringBuilder builder = new StringBuilder()
            .append("Unable to find multicast or loopback interface matching criteria: ")
            .append(address.getAddress())
            .append('/')
            .append(subnetPrefix);

        if (filteredInterfaces.length > 0)
        {
            builder.append(lineSeparator()).append("  Candidates:");

            for (final NetworkInterface ifc : filteredInterfaces)
            {
                builder
                    .append(lineSeparator())
                    .append("  - Name: ")
                    .append(ifc.getDisplayName())
                    .append(", addresses: ")
                    .append(ifc.getInterfaceAddresses())
                    .append(", multicast: ")
                    .append(ifc.supportsMulticast())
                    .append(", loopback: ")
                    .append(ifc.isLoopback())
                    .append(", state: ")
                    .append(ifc.isUp() ? "UP" : "DOWN");
            }
        }

        return builder.toString();
    }

    static InterfaceSearchAddress wildcard()
    {
        return WILDCARD;
    }

    static InterfaceSearchAddress parse(final String addressAndPort) throws UnknownHostException
    {
        if (Strings.isEmpty(addressAndPort))
        {
            throw new IllegalArgumentException("search address string is null or empty");
        }

        int slashIndex = -1;
        int colonIndex = -1;
        int rightAngleBraceIndex = -1;

        for (int i = 0, length = addressAndPort.length(); i < length; i++)
        {
            switch (addressAndPort.charAt(i))
            {
                case '/':
                    slashIndex = i;
                    break;

                case ':':
                    colonIndex = i;
                    break;

                case ']':
                    rightAngleBraceIndex = i;
                    break;
            }
        }

        final String addressString = getAddress(addressAndPort, slashIndex, colonIndex, rightAngleBraceIndex);
        final InetAddress hostAddress = InetAddress.getByName(addressString);
        final int port = getPort(addressAndPort, slashIndex, colonIndex, rightAngleBraceIndex);
        final int defaultSubnetPrefix = hostAddress.getAddress().length * 8;
        final int subnetPrefix = getSubnet(addressAndPort, slashIndex, defaultSubnetPrefix);

        return new InterfaceSearchAddress(new InetSocketAddress(hostAddress, port), subnetPrefix);
    }

    private static int getSubnet(final String s, final int slashIndex, final int defaultSubnetPrefix)
    {
        if (slashIndex < 0)
        {
            return defaultSubnetPrefix;
        }
        else if (s.length() - 1 == slashIndex)
        {
            throw new IllegalArgumentException("invalid subnet: " + s);
        }

        final int subnetStringBegin = slashIndex + 1;

        return AsciiEncoding.parseIntAscii(s, subnetStringBegin, s.length() - subnetStringBegin);
    }

    private static int getPort(
        final String s, final int slashIndex, final int colonIndex, final int rightAngleBraceIndex)
    {
        if (colonIndex < 0 || rightAngleBraceIndex > colonIndex)
        {
            return 0;
        }
        else if (s.length() - 1 == colonIndex)
        {
            throw new IllegalArgumentException("invalid port: " + s);
        }

        final int portStringBegin = colonIndex + 1;
        final int portStringEnd = slashIndex > 0 ? slashIndex : s.length();

        return AsciiEncoding.parseIntAscii(s, portStringBegin, portStringEnd - portStringBegin);
    }

    private static String getAddress(
        final String s, final int slashIndex, final int colonIndex, final int rightAngleBraceIndex)
    {
        int addressEnd = s.length();

        if (slashIndex >= 0)
        {
            addressEnd = slashIndex;
        }

        if (colonIndex >= 0 && colonIndex > rightAngleBraceIndex)
        {
            addressEnd = colonIndex;
        }

        return s.substring(0, addressEnd);
    }
}
