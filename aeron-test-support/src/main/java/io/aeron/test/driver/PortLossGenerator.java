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

public class PortLossGenerator implements LossGenerator
{
    private int port;
    private long dropUntil;
    private volatile boolean drop;

    public void startDropping(final int port, final long durationNs)
    {
        this.port = port;
        dropUntil = System.nanoTime() + durationNs;
        drop = true;
    }

    public boolean shouldDropFrame(final InetSocketAddress address, final UnsafeBuffer buffer, final int length)
    {
        if (drop && port == address.getPort())
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
}
