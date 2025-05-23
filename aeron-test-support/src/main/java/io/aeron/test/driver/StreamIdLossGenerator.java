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

import java.net.InetSocketAddress;

public class StreamIdLossGenerator implements LossGenerator
{
    private int streamId;
    private long dropUntil;
    private volatile boolean drop;

    public void startDropping(final int streamId, final long durationNs)
    {
        this.streamId = streamId;
        dropUntil = System.nanoTime() + durationNs;
        drop = true;
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
        if (drop && streamId == this.streamId)
        {
            if (System.nanoTime() - dropUntil < 0)
            {
                return true;
            }
            else
            {
                drop = false;
            }
        }

        return false;
    }

    public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
    {
        return false;
    }
}
