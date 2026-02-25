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
package io.aeron;

import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import io.aeron.logbuffer.LogBufferDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(InterruptingTestCallback.class)
class ControlledAssemblyTest
{
    private static final int STREAM_ID = 1001;

    @RegisterExtension
    final SystemTestWatcher testWatcher = new SystemTestWatcher();

    private final MediaDriver.Context driverContext = new MediaDriver.Context()
        .publicationTermBufferLength(LogBufferDescriptor.TERM_MIN_LENGTH)
        .errorHandler(Tests::onError)
        .threadingMode(ThreadingMode.SHARED);

    private TestMediaDriver driver;

    private Aeron aeron;

    @BeforeEach
    void setUp()
    {
        driver = TestMediaDriver.launch(driverContext, testWatcher);
        testWatcher.dataCollector().add(driver.context().aeronDirectory());

        aeron = Aeron.connect();
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    @InterruptAfter(10)
    void testHeaderInRepeatedCallbacksAfterAborting()
    {
        try (Subscription subscription1 = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
            Subscription subscription2 = aeron.addSubscription(IPC_CHANNEL, STREAM_ID);
            ExclusivePublication publication = aeron.addExclusivePublication(IPC_CHANNEL, STREAM_ID))
        {
            final UnsafeBuffer buffer = new UnsafeBuffer(new byte[publication.maxPayloadLength() + 1]);

            Tests.awaitConnected(subscription1);
            Tests.awaitConnected(subscription2);

            final int length1 = 17;
            final long position1 = offer(publication, buffer, length1);

            final int length2 = buffer.capacity();
            final long position2 = offer(publication, buffer, length2);

            final int length3 = 64;
            final long position3 = offer(publication, buffer, length3);

            final TestHandler subscriptionHandler = new TestHandler();
            final ControlledFragmentAssembler subscriptionAssembler =
                new ControlledFragmentAssembler(subscriptionHandler);

            final Image image = subscription2.imageAtIndex(0);
            final TestHandler imageHandler = new TestHandler();
            final ImageControlledFragmentAssembler imageAssembler = new ImageControlledFragmentAssembler(imageHandler);

            final BiConsumer<Action, Integer> pollUntilActionReturnedNTimes = (action, n) ->
            {
                subscriptionHandler.action = action;
                final int subscriptionExpected = subscriptionHandler.fragments.size() + n;
                while (subscriptionHandler.fragments.size() < subscriptionExpected)
                {
                    if (subscription1.controlledPoll(subscriptionAssembler, 1) == 0)
                    {
                        Tests.yield();
                    }
                }

                imageHandler.action = action;
                final int imageExpected = imageHandler.fragments.size() + n;
                while (imageHandler.fragments.size() < imageExpected)
                {
                    if (image.controlledPoll(imageAssembler, 1) == 0)
                    {
                        Tests.yield();
                    }
                }
            };

            pollUntilActionReturnedNTimes.accept(ABORT, 1);
            pollUntilActionReturnedNTimes.accept(CONTINUE, 1);
            pollUntilActionReturnedNTimes.accept(ABORT, 2);
            pollUntilActionReturnedNTimes.accept(CONTINUE, 2);

            assertEquals(List.of(
                new Fragment(length1, length1 + HEADER_LENGTH, NULL_VALUE, position1),
                new Fragment(length1, length1 + HEADER_LENGTH, NULL_VALUE, position1),
                new Fragment(length2, length2 + HEADER_LENGTH, (int)(position2 - position1), position2),
                new Fragment(length2, length2 + HEADER_LENGTH, (int)(position2 - position1), position2),
                new Fragment(length2, length2 + HEADER_LENGTH, (int)(position2 - position1), position2),
                new Fragment(length3, length3 + HEADER_LENGTH, NULL_VALUE, position3)
            ), subscriptionHandler.fragments);

            assertEquals(subscriptionHandler.fragments, imageHandler.fragments);
        }
    }

    private static long offer(final Publication publication, final DirectBuffer buffer, final int length)
    {
        while (true)
        {
            final long position = publication.offer(buffer, 0, length);
            if (position > 0)
            {
                return position;
            }
            Tests.yield();
        }
    }

    private static final class TestHandler implements ControlledFragmentHandler
    {
        private final List<Fragment> fragments = new ArrayList<>();
        private Action action;

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            fragments.add(new Fragment(
                length,
                header.frameLength(),
                header.fragmentedFrameLength(),
                header.position()));

            return action;
        }
    }

    private record Fragment(int length, int frameLength, int fragmentedFrameLength, long position)
    {
    }
}
