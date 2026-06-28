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
import org.agrona.concurrent.UnsafeBuffer;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.net.InetSocketAddress;
import java.util.function.Predicate;

public final class FrameDataLossGenerator implements LossGenerator
{
    private static final VarHandle ENABLED_VH;

    static
    {
        try
        {
            ENABLED_VH = MethodHandles.lookup()
                .findVarHandle(FrameDataLossGenerator.class, "enabled", boolean.class);
        }
        catch (final NoSuchFieldException | IllegalAccessException e)
        {
            throw new Error(e);
        }
    }

    private volatile boolean enabled;
    private Predicate<byte[]> dropPredicate;

    public void enable(final Predicate<byte[]> dropPredicate)
    {
        this.dropPredicate = dropPredicate;
        ENABLED_VH.setRelease(this, true);
    }

    public void disable()
    {
        ENABLED_VH.setRelease(this, false);
    }


    public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
    {
        if ((boolean)ENABLED_VH.getAcquire(this))
        {
            final byte[] bytes = new byte[length];
            buffer.getBytes(0, bytes);
            return dropPredicate.test(bytes);
        }
        return false;
    }
}
