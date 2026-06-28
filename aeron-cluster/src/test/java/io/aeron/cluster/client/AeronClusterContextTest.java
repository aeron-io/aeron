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
import io.aeron.RethrowingErrorHandler;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.test.Tests;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class AeronClusterContextTest
{
    private final Aeron aeron = mock(Aeron.class);
    private final Aeron.Context aeronContext = new Aeron.Context();
    private final AeronCluster.Context context = new AeronCluster.Context();

    @BeforeEach
    void before()
    {
        when(aeron.context()).thenReturn(aeronContext);
        aeronContext.subscriberErrorHandler(RethrowingErrorHandler.INSTANCE);

        context
            .aeron(aeron)
            .ingressChannel("aeron:udp")
            .egressChannel("aeron:udp?endpoint=localhost:0");
    }

    @ParameterizedTest
    @NullAndEmptySource
    void concludeThrowsConfigurationExceptionIfIngressChannelIsNotSet(final String ingressChannel)
    {
        context.ingressChannel(ingressChannel);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals("ERROR - ingressChannel must be specified", exception.getMessage());
    }

    @Test
    void concludeThrowsConfigurationExceptionIfIngressChannelIsSetToIpcAndIngressEndpointsSpecified()
    {
        context
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
        context.egressChannel(egressChannel);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals("ERROR - egressChannel must be specified", exception.getMessage());
    }

    @ParameterizedTest
    @NullAndEmptySource
    void clientNameShouldHandleEmptyValue(final String clientName)
    {
        context.clientName(clientName);
        assertEquals("", context.clientName());
    }

    @ParameterizedTest
    @ValueSource(strings = { "test", "Some other name" })
    void clientNameShouldReturnAssignedValue(final String clientName)
    {
        context.clientName(clientName);
        assertEquals(clientName, context.clientName());
    }

    @ParameterizedTest
    @ValueSource(strings = { "some", "42" })
    void clientNameCanBeSetViaSystemProperty(final String clientName)
    {
        System.setProperty(AeronCluster.Configuration.CLIENT_NAME_PROP_NAME, clientName);
        try
        {
            assertEquals(clientName, new AeronCluster.Context().clientName());
        }
        finally
        {
            System.clearProperty(AeronCluster.Configuration.CLIENT_NAME_PROP_NAME);
        }
    }

    @Test
    void clientNameMustNotExceedMaxLength()
    {
        context.clientName(Tests.generateStringWithSuffix("test", "x", Aeron.Configuration.MAX_CLIENT_NAME_LENGTH));

        final ConfigurationException exception =
            assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - AeronCluster.Context.clientName length must be <= " + Aeron.Configuration.MAX_CLIENT_NAME_LENGTH,
            exception.getMessage());
    }
}
