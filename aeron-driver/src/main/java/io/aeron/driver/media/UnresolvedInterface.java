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

import java.net.ProtocolFamily;
import java.net.SocketException;
import java.net.UnknownHostException;

/**
 * Interface specification as found, for example, in the {@code interface=} channel parameter.
 */
sealed interface UnresolvedInterface permits InterfaceSearchAddress, NamedInterface
{
    ResolvedInterface resolve(boolean multicast, ProtocolFamily protocolFamily) throws SocketException;

    static UnresolvedInterface parse(final String str) throws UnknownHostException
    {
        if (Strings.isEmpty(str))
        {
            throw new IllegalArgumentException("interface is null or empty");
        }

        if (str.charAt(0) == NamedInterface.OPENING_CHAR)
        {
            return NamedInterface.parse(str);
        }
        else
        {
            return InterfaceSearchAddress.parse(str);
        }
    }
}
