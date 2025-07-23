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
package io.aeron.cluster.client;

import io.aeron.Aeron;
import io.aeron.exceptions.ConfigurationException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.SocketException;
import java.util.Iterator;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class AeronClusterContextTest
{
    static ByteArrayOutputStream outContent;
    static PrintStream mockErr;

    @BeforeAll
    static void setUp()
    {
        outContent = new ByteArrayOutputStream();
        mockErr = new PrintStream(outContent);
    }

    @BeforeEach
    void clearOutContent()
    {
        outContent.reset();
    }

    @ParameterizedTest
    @NullAndEmptySource
    void concludeThrowsConfigurationExceptionIfIngressChannelIsNotSet(final String ingressChannel)
    {
        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context();
        context.aeron(aeron).ingressChannel(ingressChannel);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals("ERROR - ingressChannel must be specified", exception.getMessage());
    }

    @Test
    void concludeThrowsConfigurationExceptionIfIngressChannelIsSetToIpcAndIngressEndpointsSpecified()
    {
        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context();
        context
            .aeron(aeron)
            .ingressChannel("aeron:ipc")
            .ingressEndpoints("0,localhost:1234");

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - AeronCluster.Context ingressEndpoints must be null when using IPC ingress",
            exception.getMessage());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void concludeThrowsConfigurationExceptionIfEgressChannelIsNotSet(final String egressChannel)
    {
        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context();
        context.aeron(aeron).ingressChannel("aeron:udp").egressChannel(egressChannel);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals("ERROR - egressChannel must be specified", exception.getMessage());
    }

    @Test
    void concludeSuccessfulIfEgressLocalhostAndIngressLocalhost() throws SocketException
    {
        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context()
            .aeron(aeron)
            .ingressChannel("aeron:udp")
            .ingressEndpoints("0=localhost:8055")
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .warningLogger(mockErr);

        context.conclude();
        assertEquals(outContent.size(), 0);
    }

    @Test
    void concludeSuccessfulIfEgressLocalhostAndIngressLocal() throws SocketException
    {
        final String localIp = getLocalNetworkIp();

        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context()
            .aeron(aeron)
            .ingressChannel("aeron:udp")
            .ingressEndpoints("0=" + localIp + ":8055")
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .warningLogger(mockErr);

        context.conclude();
        assertEquals(outContent.size(), 0);
    }

    @Test
    void concludeSuccessfulIfEgressLocalAndIngressLocalhost() throws SocketException
    {
        final String localIp = getLocalNetworkIp();

        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context()
            .aeron(aeron)
            .ingressChannel("aeron:udp")
            .ingressEndpoints("0=localhost:8055")
            .egressChannel("aeron:udp?endpoint=" + localIp + ":0")
            .warningLogger(mockErr);

        context.conclude();
        assertEquals(outContent.size(), 0);

    }

    @Test
    void concludeSuccessfulIfEgressLocalAndIngressRemote() throws SocketException
    {
        final String localIp = getLocalNetworkIp();

        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context()
            .aeron(aeron)
            .ingressChannel("aeron:udp")
            .ingressEndpoints("0=142.251.32.14:8055")
            .egressChannel("aeron:udp?endpoint=" + localIp + ":0")
            .warningLogger(mockErr);

        context.conclude();

        assertEquals(outContent.size(), 0);
    }

    @Test
    void concludeWritesToStandardErrorIfEgressLocalAndIngressRemote() throws SocketException
    {
        getLocalNetworkIp();

        // using google public ip to ensure not local
        final String ingressEndpoints = "0=142.251.32.14:8055";

        final Aeron aeron = mock(Aeron.class);
        final AeronCluster.Context context = new AeronCluster.Context()
            .aeron(aeron)
            .ingressChannel("aeron:udp")
            .ingressEndpoints(ingressEndpoints)
            .egressChannel("aeron:udp?endpoint=localhost:0")
            .warningLogger(mockErr);

        context.conclude();
        final String output = outContent.toString();
        assertTrue(output.startsWith(
            "ERROR - must use network interface for local egress host when ingress is remote"));
    }

    private static String getLocalNetworkIp() throws SocketException
    {
        final Set<String> localIps = AeronCluster.Context.getLocalIPAddresses(false);
        final Iterator<String> iterator = localIps.iterator();
        String localIp = null;
        while (iterator.hasNext())
        {
            localIp = iterator.next();
            if (!localIp.contentEquals("localhost") &&
                !localIp.contentEquals("127.0.0.1"))
            {
                break;
            }
        }
        if (null == localIp)
        {
            throw new ConfigurationException("ERROR - no local IP address found");
        }
        return localIp;
    }
}
