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
package io.aeron.driver;

import io.aeron.CounterProvider;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.status.CountersReader;

/**
 * Interface to allow managing a {@link NameResolver} using an {@link Agent}.
 */
public interface NameResolverAgent extends NameResolver, Agent
{
    /**
     * Do post construction initialisation of the name resolver.
     *
     * @param countersReader for finding existing counters.
     * @param counterProvider for adding counters.
     */
    default void init(final CountersReader countersReader, final CounterProvider counterProvider)
    {
    }

    /**
     * {@inheritDoc}
     */
    default int doWork()
    {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    default String roleName()
    {
        return name();
    }
}
