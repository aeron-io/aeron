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
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.driver.TestMediaDriver;
import org.agrona.CloseHelper;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.containsString;

@ExtendWith(InterruptingTestCallback.class)
class ErrorHandlerTest
{
    @RegisterExtension
    final SystemTestWatcher watcher = new SystemTestWatcher();

    private final MediaDriver.Context context = new MediaDriver.Context()
        .errorHandler((ignore) -> {})
        .dirDeleteOnStart(true)
        .publicationConnectionTimeoutNs(TimeUnit.MILLISECONDS.toNanos(500))
        .timerIntervalNs(TimeUnit.MILLISECONDS.toNanos(100));

    private final ArrayList<AutoCloseable> closeables = new ArrayList<>();

    private Aeron aeron;
    private TestMediaDriver driver;

    private void launch()
    {
        driver = TestMediaDriver.launch(context, watcher);
        watcher.dataCollector().add(driver.context().aeronDirectory());
        watcher.ignoreErrorsMatching((s) -> true);

        aeron = Aeron.connect();
    }

    @AfterEach
    void after()
    {
        CloseHelper.closeAll(closeables);
        CloseHelper.closeAll(aeron, driver);
    }

    @Test
    @InterruptAfter(5)
    void shouldReportToErrorHandlerAndDistinctErrorLog() throws IOException
    {
        TestMediaDriver.notSupportedOnCMediaDriver("C driver doesn't support ErrorHandler callbacks");

        final AtomicReference<Throwable> throwableRef = new AtomicReference<>(null);
        context.errorHandler(throwableRef::set);

        launch();

        final long initialErrorCount = aeron.countersReader().getCounterValue(SystemCounterDescriptor.ERRORS.id());

        addPublication("aeron:udp?endpoint=localhost:9999|mtu=1408", 1000);
        addSubscription("aeron:udp?endpoint=localhost:9999|rcv-wnd=1376", 1000);

        Tests.awaitCounterDelta(aeron.countersReader(), SystemCounterDescriptor.ERRORS.id(), initialErrorCount, 1);

        final Matcher<String> exceptionMessageMatcher =
            containsString("rcv-wnd=1376 must be at least two times larger than the mtu=1408");

        Tests.await(() -> null != throwableRef.get());
        SystemTests.waitForErrorToOccur(driver.aeronDirectoryName(), exceptionMessageMatcher, Tests.SLEEP_1_MS);
    }

    private void addPublication(final String channel, final int streamId)
    {
        final Publication pub = aeron.addPublication(channel, streamId);
        closeables.add(pub);
    }

    private void addSubscription(final String channel, final int streamId)
    {
        final Subscription sub = aeron.addSubscription(channel, streamId);
        closeables.add(sub);
    }
}
