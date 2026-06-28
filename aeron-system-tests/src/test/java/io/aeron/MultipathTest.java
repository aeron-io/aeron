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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

@ExtendWith(InterruptingTestCallback.class)
public class MultipathTest
{
    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    @Test
    @InterruptAfter(5)
    void shouldEndMultipath()
    {
        final String mdcUri = "aeron:udp?control-mode=manual";
        final String endpoint1 = "aeron:udp?endpoint=localhost:10001|group=true";
        final String endpoint2 = "aeron:udp?endpoint=localhost:10002|group=true";
        final int streamId = 10000;

        try (TestMediaDriver driver = TestMediaDriver.launch(
            new MediaDriver.Context().aeronDirectoryName(CommonContext.generateRandomDirName()), testWatcher);
            Aeron aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(driver.aeronDirectoryName()));
            Publication multipathPub = aeron.addPublication(mdcUri, streamId);
            Subscription multipathSub = aeron.addSubscription(mdcUri, streamId))
        {
            multipathPub.addDestination(endpoint1);
            multipathPub.addDestination(endpoint2);
            multipathSub.addDestination(endpoint1);
            multipathSub.addDestination(endpoint2);

            Tests.awaitConnected(multipathPub);
            Tests.awaitConnected(multipathSub);

            final MutableDirectBuffer msg = new UnsafeBuffer(new byte[1024]);
            final int numMessages = 10;
            for (int i = 0; i < numMessages; i++)
            {
                msg.putInt(0, i);

                while (multipathPub.offer(msg) < 0)
                {
                    Tests.yield();
                }
            }

            final MutableInteger counter = new MutableInteger();

            while (counter.get() < numMessages)
            {
                multipathSub.poll(
                    (buffer, offset, length, header) ->
                    {
                        if (buffer.getInt(offset) == counter.get())
                        {
                            counter.increment();
                        }
                    }, 10);

                Tests.yield();
            }
        }
    }
}
