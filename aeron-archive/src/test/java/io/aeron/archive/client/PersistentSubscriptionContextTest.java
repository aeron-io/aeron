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
package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PersistentSubscriptionContextTest
{
    private PersistentSubscription.Context context;

    @BeforeEach
    void setup()
    {
        context = new PersistentSubscription.Context()
            .recordingId(1)
            .liveChannel(IPC_CHANNEL)
            .liveStreamId(1)
            .aeronArchiveContext(new AeronArchive.Context());
    }

    @Test
    void canOnlyConcludeOnce()
    {
        context.conclude();

        assertThrows(ConcurrentConcludeException.class, context::conclude);
    }

    @Test
    void contextMustHaveRecordingId()
    {
        context.recordingId(Aeron.NULL_VALUE);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextMustHaveLiveChannel()
    {
        context.liveChannel(null);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextMustHaveLiveStreamId()
    {
        context.liveStreamId(Aeron.NULL_VALUE);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextMustHaveAnArchiveContext()
    {
        context.aeronArchiveContext(null);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextCanBeCloned()
    {
        final PersistentSubscription.Context clonedCtx = context.clone();

        assertNotSame(context, clonedCtx);

        assertEquals(context.startPosition(), clonedCtx.startPosition());
        assertEquals(context.recordingId(), clonedCtx.recordingId());
        assertEquals(context.liveChannel(), clonedCtx.liveChannel());
        assertEquals(context.liveStreamId(), clonedCtx.liveStreamId());

        assertSame(context.listener(), clonedCtx.listener());
        assertSame(context.aeronArchiveContext(), clonedCtx.aeronArchiveContext());
    }

    @Test
    void contextThrowsIfStartPositionIsInvalid()
    {
        context.startPosition(Aeron.NULL_VALUE);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextThrowsIfSttPositionIsInvalid()
    {
        context.recordingId(-2);
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextShouldCreateListenerIfNoneProvided()
    {
        final PersistentSubscriptionListener nullListener = null;
        context.listener(nullListener);
        context.conclude();

        assertNotSame(nullListener, context.listener());
        assertNotNull(context.listener());
    }
}
