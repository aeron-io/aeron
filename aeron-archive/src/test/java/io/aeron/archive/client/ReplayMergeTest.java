/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive.client;

import io.aeron.Subscription;
import org.agrona.concurrent.SystemEpochClock;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class ReplayMergeTest
{
    @Test
    void shouldRejectReplayDestinationWithoutEndpoint()
    {
        final Subscription subscription = mock(Subscription.class);
        when(subscription.channel()).thenReturn("aeron:udp?control-mode=manual");
        final AeronArchive archive = mock(AeronArchive.class);
        final String replayDestination = "aeron:udp?interface=localhost";

        final IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> new ReplayMerge(
                subscription,
                archive,
                "aeron:udp?session-id=1",
                replayDestination,
                "aeron:udp?endpoint=localhost:20121|control=localhost:20122",
                0,
                0,
                SystemEpochClock.INSTANCE,
                5_000));

        assertEquals("replay destination must have an endpoint: " + replayDestination, exception.getMessage());
        verifyNoInteractions(archive);
    }
}
