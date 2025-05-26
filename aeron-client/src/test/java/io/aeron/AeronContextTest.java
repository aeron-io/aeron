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
package io.aeron;

import io.aeron.exceptions.ConfigurationException;
import org.agrona.concurrent.AgentInvoker;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.mockito.Mockito.mock;

class AeronContextTest
{
    @NullAndEmptySource
    @ParameterizedTest
    void shouldUseEmptyStringIfNameIsEmpty(final String name)
    {
        final Aeron.Context context = new Aeron.Context();
        assertEquals("", context.clientName());

        context.clientName(name);
        assertEquals("", context.clientName());
    }

    @ParameterizedTest
    @ValueSource(strings = { "a", "gdajsdgajsd", "7326482374hdfy7dsyf8dyf9sd.-)" })
    void shouldAssignClientName(final String clientName)
    {
        final Aeron.Context context = new Aeron.Context();

        context.clientName(clientName);
        assertSame(clientName, context.clientName());
    }

    @Test
    void shouldRejectClientNameThatIsTooLong()
    {
        final String name =
            "this is a very long value that we are hoping with be reject when the value gets " +
            "set on the the context without causing issues will labels";
        final Aeron.Context context = new Aeron.Context().clientName(name);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals("ERROR - clientName length must <= 100", exception.getMessage());
    }

    @Test
    void shouldRequireRunningInTheInvokerModeIfDriverInvokerIsSpecified()
    {
        final AgentInvoker driverAgentInvoker = mock(AgentInvoker.class);
        final Aeron.Context context = new Aeron.Context()
            .useConductorAgentInvoker(false)
            .driverAgentInvoker(driverAgentInvoker);

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - Must use Aeron.Context.useConductorAgentInvoker(true) when " +
            "Aeron.Context.driverAgentInvoker() is set",
            exception.getMessage());
    }
}
