/*
 * Copyright 2025 Adaptive Financial Consulting Limited.
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
import org.agrona.concurrent.UnsafeBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;

public final class StreamIdLossGenerator implements LossGenerator
{
    private static final VarHandle ENABLED_VH;

    static
    {
        try
        {
            ENABLED_VH = MethodHandles.lookup().findVarHandle(StreamIdLossGenerator.class, "enabled", boolean.class);
        }
        catch (final NoSuchFieldException | IllegalAccessException e)
        {
            throw new Error(e);
        }
    }

    private int streamId;
    private volatile boolean enabled;

    public void enable(final int streamId)
    {
        this.streamId = streamId;
        ENABLED_VH.setRelease(this, true);
    }

    public void disable()
    {
        ENABLED_VH.setRelease(this, false);
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
        return (boolean)ENABLED_VH.getAcquire(this) && streamId == this.streamId;
    }

    public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
    {
        return false;
    }
}
