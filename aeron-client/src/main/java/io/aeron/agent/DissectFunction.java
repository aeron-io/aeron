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
package io.aeron.agent;

import org.agrona.MutableDirectBuffer;

/**
 * Takes an event and serialises it to the supplied {@link StringBuilder}.
 *
 * @param <T> the type of the event
 */
@FunctionalInterface
public interface DissectFunction<T>
{
    /**
     * Dissect an event and serialise it to a {@link StringBuilder}.
     *
     * @param event   to be dissected.
     * @param buffer  with the encoded event.
     * @param offset  at which the encoded event begins.
     * @param builder into which the event will be serialised.
     */
    void dissect(T event, MutableDirectBuffer buffer, int offset, StringBuilder builder);
}
