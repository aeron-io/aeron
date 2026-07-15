/*
 * Copyright 2014-2026 Real Logic Limited.
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
package io.aeron.test;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks a test class that restarts the event logging reader with a given class name and set of enabled events
 * before each test. Intended to be run in its own forked JVM, one per test class.
 */
@Target({ ElementType.TYPE })
@Retention(RetentionPolicy.RUNTIME)
@Tag("logging")
public @interface LoggingTest
{
    /**
     * The class name of the event log reader agent to install, e.g. a test-local stub reader.
     *
     * @return the event log reader agent class.
     */
    Class<?> readerClassname();

    /**
     * The system property key used to enable events for this test class, e.g. {@code aeron.event.log} for driver
     * events, {@code aeron.event.archive.log} for archive events, or {@code aeron.event.cluster.log} for cluster
     * events.
     *
     * @return the enabled-events system property key.
     */
    String enabledEventsKey();

    /**
     * The value assigned to {@link #enabledEventsKey()}.
     *
     * @return the enabled-events value.
     */
    String enabledEvents() default "all";
}
