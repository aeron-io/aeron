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

import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.List;

interface NetworkInterfaceShim
{
    Enumeration<NetworkInterface> getNetworkInterfaces() throws SocketException;

    List<InterfaceAddress> getInterfaceAddresses(NetworkInterface ifc);

    boolean isLoopback(NetworkInterface ifc) throws SocketException;

    @SuppressWarnings("JavadocVariable")
    NetworkInterfaceShim DEFAULT = new NetworkInterfaceShim()
    {
        public Enumeration<NetworkInterface> getNetworkInterfaces() throws SocketException
        {
            return NetworkInterface.getNetworkInterfaces();
        }

        public List<InterfaceAddress> getInterfaceAddresses(final NetworkInterface ifc)
        {
            return ifc.getInterfaceAddresses();
        }

        public boolean isLoopback(final NetworkInterface ifc) throws SocketException
        {
            return ifc.isLoopback();
        }
    };
}
