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

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.buffer.TestLogFactory;
import io.aeron.driver.status.SystemCounters;
import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.concurrent.CachedNanoClock;
import org.agrona.concurrent.status.AtomicLongPosition;
import org.agrona.concurrent.status.Position;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

class UntetheredSubscriptionTest
{
    private static final long REGISTRATION_ID = 1;
    private static final int TAG_ID = 0;
    private static final int SESSION_ID = 777;
    private static final int STREAM_ID = 1003;
    private static final int TERM_BUFFER_LENGTH = LogBufferDescriptor.TERM_MIN_LENGTH;
    private static final int TERM_WINDOW_LENGTH = TERM_BUFFER_LENGTH / 2;
    private static final long TIME_NS = 1000;
    private static final long UNTETHERED_WINDOW_LIMIT_TIMEOUT_NS = Configuration.untetheredWindowLimitTimeoutNs();
    private static final long UNTETHERED_LINGER_TIMEOUT_NS = Configuration.untetheredLingerTimeoutNs();
    private static final long UNTETHERED_RESTING_TIMEOUT_NS = Configuration.untetheredRestingTimeoutNs();
    private static final String CHANNEL = CommonContext.IPC_CHANNEL + "?term-length=" + TERM_BUFFER_LENGTH;

    private final RawLog rawLog = TestLogFactory.newLogBuffers(TERM_BUFFER_LENGTH);
    private final AtomicLongPosition publisherLimit = new AtomicLongPosition();
    private final MediaDriver.Context ctx = new MediaDriver.Context()
        .cachedNanoClock(new CachedNanoClock())
        .systemCounters(mock(SystemCounters.class));
    private final DriverConductor conductor = mock(DriverConductor.class);

    private IpcPublication ipcPublication;
    private PublicationParams params;

    @BeforeEach
    void before()
    {
        ctx.cachedNanoClock().update(TIME_NS);
        params = PublicationParams.getPublicationParams(
            ChannelUri.parse(CHANNEL), ctx, conductor, STREAM_ID, "ipc");

        ipcPublication = new IpcPublication(
            REGISTRATION_ID,
            CHANNEL,
            ctx,
            TAG_ID,
            SESSION_ID,
            STREAM_ID,
            mock(Position.class),
            publisherLimit,
            rawLog,
            true,
            params);
    }

    @Test
    void shouldLifeCycleTimeoutsAndRelink()
    {
        final Position tetheredPosition = new AtomicLongPosition();
        tetheredPosition.set(TERM_WINDOW_LENGTH - 1);
        final Position untetheredPosition = new AtomicLongPosition();

        final SubscriptionLink tetheredLink = newLink(true);
        final SubscriptionLink untetheredLink = newLink(false);

        ipcPublication.addSubscriber(tetheredLink, tetheredPosition, ctx.cachedNanoClock().nanoTime());
        ipcPublication.addSubscriber(untetheredLink, untetheredPosition, ctx.cachedNanoClock().nanoTime());

        ipcPublication.updatePublisherPositionAndLimit();

        final long timeNs = TIME_NS + 1;
        ipcPublication.onTimeEvent(timeNs, 0, conductor);
        verify(conductor, never()).notifyUnavailableImageLink(REGISTRATION_ID, untetheredLink);

        final long windowLimitTimeoutNs = timeNs + params.untetheredWindowLimitTimeoutNs;
        ipcPublication.onTimeEvent(windowLimitTimeoutNs, 0, conductor);
        verify(conductor, times(1)).notifyUnavailableImageLink(REGISTRATION_ID, untetheredLink);

        ipcPublication.updatePublisherPositionAndLimit();
        assertEquals(TERM_WINDOW_LENGTH, publisherLimit.get());

        final long afterLingerTimeoutNs = windowLimitTimeoutNs + params.untetheredLingerTimeoutNs;
        ipcPublication.onTimeEvent(afterLingerTimeoutNs, 0, conductor);
        ipcPublication.updatePublisherPositionAndLimit();
        assertEquals(tetheredPosition.get() + TERM_WINDOW_LENGTH, publisherLimit.get());

        final long afterRestingTimeoutNs = afterLingerTimeoutNs + UNTETHERED_RESTING_TIMEOUT_NS;
        ipcPublication.onTimeEvent(afterRestingTimeoutNs, 0, conductor);

        verify(conductor, times(1)).notifyAvailableImageLink(
            eq(REGISTRATION_ID),
            eq(SESSION_ID),
            eq(untetheredLink),
            anyInt(),
            eq(tetheredPosition.get()),
            eq(rawLog.fileName()),
            eq(CommonContext.IPC_CHANNEL));
    }

    IpcSubscriptionLink newLink(final boolean isTether)
    {
        final SubscriptionParams params = new SubscriptionParams();
        params.isTether = isTether;

        return new IpcSubscriptionLink(1, STREAM_ID, CHANNEL, null, params);
    }
}
