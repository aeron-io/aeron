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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.driver.status.StatusUtil;
import org.agrona.concurrent.status.CountersReader;
import org.agrona.concurrent.status.StatusIndicator;

/**
 * Allows a {@link org.agrona.concurrent.ControllableIdleStrategy} to be set via the command line.
 * <p>
 * The first command line arg should be an integer value representing one of constants in
 * {@link org.agrona.concurrent.ControllableIdleStrategy}.
 */
public class SetControllableIdleStrategy
{
    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        if (args.length != 1)
        {
            System.out.format("Usage: SetControllableIdleStrategy <n>");
            System.exit(0);
        }

        try (Aeron aeron = Aeron.connect())
        {
            final CountersReader countersReader = aeron.countersReader();
            final StatusIndicator statusIndicator = StatusUtil.controllableIdleStrategy(countersReader);

            if (null != statusIndicator)
            {
                final int status = Integer.parseInt(args[0]);
                statusIndicator.setRelease(status);
                System.out.println("Set ControllableIdleStrategy status to " + status);
            }
            else
            {
                System.out.println("Could not find ControllableIdleStrategy status.");
            }
        }
    }
}
