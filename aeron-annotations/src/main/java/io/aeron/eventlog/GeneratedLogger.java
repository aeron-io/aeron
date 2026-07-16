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
package io.aeron.eventlog;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Marks an interface whose {@link LoggerMethod}-annotated methods should have a ring-buffer-backed
 * implementation class generated at compile time by {@code EventLoggerProcessor}.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.SOURCE)
public @interface GeneratedLogger
{
    /**
     * Fully qualified name of the static helper class holding the {@code xxxLength}/{@code encodeXxx}
     * method pairs referenced by this interface's {@link LoggerMethod}-annotated methods.
     *
     * @return the encoder class name.
     */
    String encoder();

    /**
     * Fully qualified name of the event-code enum used to identify each event claimed on the ring buffer.
     *
     * @return the event code enum name.
     */
    String eventCodeType();
}
