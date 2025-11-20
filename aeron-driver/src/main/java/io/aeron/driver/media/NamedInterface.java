/*
 * Copyright 2025 Adaptive Financial Consulting Limited.
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

import org.agrona.Strings;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.ProtocolFamily;
import java.net.SocketException;

import static io.aeron.driver.media.NetworkUtil.getProtocolFamily;

record NamedInterface(String name, int port) implements UnresolvedInterface
{
    static final char OPENING_CHAR = '{';

    public ResolvedInterface resolve(final boolean multicast, final ProtocolFamily protocolFamily)
        throws SocketException
    {
        final NetworkInterface localInterface = NetworkInterface.getByName(name);
        if (null == localInterface)
        {
            throw new IllegalArgumentException("unknown interface " + name);
        }
        final InetSocketAddress address = resolveToFirstAddressOfFamily(localInterface, protocolFamily);
        return new ResolvedInterface(localInterface, address);
    }

    private InetSocketAddress resolveToFirstAddressOfFamily(
        final NetworkInterface localInterface,
        final ProtocolFamily protocolFamily)
    {
        for (final InterfaceAddress interfaceAddress : localInterface.getInterfaceAddresses())
        {
            final InetAddress address = interfaceAddress.getAddress();
            if (getProtocolFamily(address) == protocolFamily)
            {
                return new InetSocketAddress(address, port);
            }
        }

        throw new IllegalStateException(
            "no " + protocolFamily + " addresses found on interface " + localInterface.getName());
    }

    static NamedInterface parse(final String str)
    {
        if (Strings.isEmpty(str) || str.charAt(0) != OPENING_CHAR)
        {
            throw parseException(str);
        }

        final int nameEnd = str.lastIndexOf('}');
        if (nameEnd <= 1)
        {
            throw parseException(str);
        }
        final String name = str.substring(1, nameEnd);

        int port = 0;
        final int trailing = str.length() - nameEnd - 1;
        if (trailing > 0)
        {
            if (trailing == 1 || str.charAt(nameEnd + 1) != ':')
            {
                throw parseException(str);
            }

            try
            {
                port = Integer.parseUnsignedInt(str, nameEnd + 2, str.length(), 10);
            }
            catch (final NumberFormatException e)
            {
                throw parseException(str, e);
            }

            if (port > 0xFFFF)
            {
                throw parseException("port out of range: " + port);
            }
        }

        return new NamedInterface(name, port);
    }

    private static IllegalArgumentException parseException(final String str)
    {
        return parseException(str, null);
    }

    private static IllegalArgumentException parseException(final String str, final Throwable cause)
    {
        return new IllegalArgumentException(
            "expected format is '{interface_name}' or '{interface_name}:port', but got " +
            (str == null ? null : '\'' + str + '\''),
            cause);
    }
}
