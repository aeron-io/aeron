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

import io.aeron.agent.EventConfiguration;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.util.Properties;

/**
 * JUnit framework extension backing {@link LoggingTest} that restarts the event log reader with the reader
 * class name and enabled events declared on the annotation before each test.
 */
public class LoggingTestExtension implements BeforeEachCallback
{
    private static final String READER_CLASSNAME_PROPERTY = "aeron.event.log.reader.classname";

    /**
     * {@inheritDoc}
     */
    public void beforeEach(final ExtensionContext context)
    {
        final LoggingTest loggingTest = context.getRequiredTestClass().getAnnotation(LoggingTest.class);

        final Properties configOptions = new Properties();
        configOptions.put(READER_CLASSNAME_PROPERTY, loggingTest.readerClassname().getName());
        configOptions.put(loggingTest.enabledEventsKey(), loggingTest.enabledEvents());

        EventConfiguration.restartReader(configOptions);
    }
}
