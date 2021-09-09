/*
 * Copyright 2014-2021 Real Logic Limited.
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
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.Tests;
import io.aeron.test.driver.MediaDriverTestWatcher;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.collections.MutableReference;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(InterruptingTestCallback.class)
public class ErrorHandlerTest
{
    @RegisterExtension
    public final MediaDriverTestWatcher watcher = new MediaDriverTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context();
    {
        context
            .errorHandler(ignore -> {})
            .dirDeleteOnStart(true)
            .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500))
            .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100));
    }

    private final ArrayList<AutoCloseable> closeables = new ArrayList<>();

    private Aeron aeron;
    private TestMediaDriver driver;

    private void launch()
    {
        driver = TestMediaDriver.launch(context, watcher);
        aeron = Aeron.connect();
    }

    @AfterEach
    public void after()
    {
        CloseHelper.closeAll(closeables);
        CloseHelper.closeAll(aeron, driver);
        if (null != driver)
        {
            driver.context().deleteDirectory();
        }
    }

    @Test
    @InterruptAfter(5)
    void shouldReportToErrorHandlerAndDistinctErrorLog() throws IOException
    {
        TestMediaDriver.notSupportedOnCMediaDriver("C driver doesn't support ErrorHandler callbacks");

        final MutableReference<Throwable> throwableRef = new MutableReference<>(null);
        context.errorHandler(throwableRef::set);

        launch();

        final long initialErrorCount = aeron.countersReader().getCounterValue(SystemCounterDescriptor.ERRORS.id());

        addPublication("aeron:udp?endpoint=localhost:9999|mtu=1408", 1000);
        addSubscription("aeron:udp?endpoint=localhost:9999|rcv-wnd=1376", 1000);

        Tests.awaitCounterDelta(aeron.countersReader(), SystemCounterDescriptor.ERRORS.id(), initialErrorCount, 1);

        final Matcher<String> exceptionMessageMatcher = allOf(
            containsString("mtuLength="),
            containsString("> initialWindowLength="));

        SystemTests.waitForErrorToOccur(driver.aeronDirectoryName(), exceptionMessageMatcher, Tests.SLEEP_1_MS);

        assertNotNull(throwableRef.get());
    }

    private Publication addPublication(final String channel, final int streamId)
    {
        final Publication pub = aeron.addPublication(channel, streamId);
        closeables.add(pub);
        return pub;
    }

    private Subscription addSubscription(final String channel, final int streamId)
    {
        final Subscription sub = aeron.addSubscription(channel, streamId);
        closeables.add(sub);
        return sub;
    }
}
