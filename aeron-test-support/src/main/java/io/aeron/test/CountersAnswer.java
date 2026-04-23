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
package io.aeron.test;

import io.aeron.Counter;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.status.CountersManager;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * Provide a Mockito Answer that will allow a mapping of Aeron.addCounter onto a CountersManager.
 */
public final class CountersAnswer implements Answer<Counter>
{
    private final CountersManager countersManager;

    private CountersAnswer(final CountersManager countersManager)
    {
        this.countersManager = countersManager;
    }

    /**
     * {@inheritDoc}
     */
    public Counter answer(final InvocationOnMock invocation)
    {
        final int counterId;
        if (2 == invocation.getArguments().length)
        {
            counterId = countersManager.allocate(
                invocation.getArgument(1, String.class),
                invocation.getArgument(0, Integer.class));
        }
        else if (7 == invocation.getArguments().length)
        {
            counterId = countersManager.allocate(
                invocation.getArgument(0, Integer.class),
                invocation.getArgument(1, DirectBuffer.class),
                invocation.getArgument(2, Integer.class),
                invocation.getArgument(3, Integer.class),
                invocation.getArgument(4, DirectBuffer.class),
                invocation.getArgument(5, Integer.class),
                invocation.getArgument(6, Integer.class));
        }
        else
        {
            throw new RuntimeException(
                "Unexpected number of arguments, should be used for Aeron::addCounter(int, String) or " +
                "Aeron::addCounter(int, DirectBuffer, int, int, DirectBuffer, int, int)");
        }

        return new Counter(countersManager, counterId);
    }

    /**
     * Set up an answer that will map addCounter requests from addCounter methods (generally from a mocked
     * Aeron instance).
     *
     * @param countersManager delegate to pass addCounter requests to.
     * @return an Answer that will use the supplied {@link CountersManager}.
     */
    public static CountersAnswer mapTo(final CountersManager countersManager)
    {
        return new CountersAnswer(countersManager);
    }
}
