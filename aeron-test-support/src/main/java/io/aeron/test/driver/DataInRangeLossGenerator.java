/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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
package io.aeron.test.driver;

import io.aeron.driver.ext.LossGenerator;
import io.aeron.protocol.DataHeaderFlyweight;
import io.aeron.protocol.HeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Drops incoming DATA frames whose {@code (streamId, termId)} matches and whose
 * {@code termOffset} falls in {@code [termOffsetInclusiveMin, termOffsetExclusiveMax)}.
 * Used to deterministically punch a contiguous gap into one subscriber's reception
 * (e.g. a persistent subscription on a dedicated test driver) while another
 * subscriber on the publisher's own driver receives the same range normally.
 *
 * <p>SETUP frames (handled by the simple {@code shouldDropFrame} variant) always pass.
 * Heartbeat DATA frames (frame-length == 0) also always pass — they carry the EOS and
 * REVOKED flags, which the subscriber needs to see to detect a revoked publication
 * and close its image. Without that the subscriber would never refresh-replay.
 */
public final class DataInRangeLossGenerator implements LossGenerator
{
    private static final VarHandle ENABLED_VH;

    static
    {
        try
        {
            ENABLED_VH = MethodHandles.lookup()
                .findVarHandle(DataInRangeLossGenerator.class, "enabled", boolean.class);
        }
        catch (final NoSuchFieldException | IllegalAccessException e)
        {
            throw new Error(e);
        }
    }

    private int streamId;
    private int activeTermId;
    private int termOffsetInclusiveMin;
    private int termOffsetExclusiveMax;
    private volatile boolean enabled;
    private final AtomicInteger framesDropped = new AtomicInteger();

    public void setTarget(
        final int streamId,
        final int activeTermId,
        final int termOffsetInclusiveMin,
        final int termOffsetExclusiveMax)
    {
        this.streamId = streamId;
        this.activeTermId = activeTermId;
        this.termOffsetInclusiveMin = termOffsetInclusiveMin;
        this.termOffsetExclusiveMax = termOffsetExclusiveMax;
    }

    public void enable()
    {
        ENABLED_VH.setRelease(this, true);
    }

    public void disable()
    {
        ENABLED_VH.setRelease(this, false);
    }

    public int framesDropped()
    {
        return framesDropped.get();
    }

    public boolean shouldDropFrame(
        final InetSocketAddress address,
        final UnsafeBuffer buffer,
        final int streamId,
        final int sessionId,
        final int termId,
        final int termOffset,
        final int length)
    {
        if (!(boolean)ENABLED_VH.getAcquire(this))
        {
            return false;
        }

        // Heartbeats (frame_length == 0, length == DATA header size) carry EOS / REVOKED flags.
        // Dropping them would prevent the subscriber from seeing publisher revoke and closing
        // its image, which would block the refresh-replay path this generator is meant to drive.
        if (length == DataHeaderFlyweight.HEADER_LENGTH &&
            0 == buffer.getInt(HeaderFlyweight.FRAME_LENGTH_FIELD_OFFSET, ByteOrder.LITTLE_ENDIAN))
        {
            return false;
        }

        if (streamId != this.streamId ||
            termId != this.activeTermId ||
            termOffset < this.termOffsetInclusiveMin ||
            termOffset >= this.termOffsetExclusiveMax)
        {
            return false;
        }

        framesDropped.incrementAndGet();
        return true;
    }

    public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
    {
        return false;
    }
}
