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
 * Declares how a {@link GeneratedLogger}-annotated interface method's ring-buffer claim/encode/commit
 * body should be generated. Every attribute is either a name resolved against real, already-compiled
 * elements (and validated as such) or an ordinary compile-time constant expression - never a raw code
 * snippet pasted verbatim.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface LoggerMethod
{
    /**
     * Simple name of the event-code enum constant to use, e.g. {@code "CANVASS_POSITION"}. Leave blank
     * to auto-detect the event code from this method's sole parameter of type
     * {@link GeneratedLogger#eventCodeType()} (e.g. a method that takes the event code as an argument).
     *
     * @return the event code constant name, or empty string to auto-detect from a parameter.
     */
    String eventCode() default "";

    /**
     * Identifies the offset and length parameters (if any) that describe a sub-range of a
     * {@code DirectBuffer}/{@code ByteBuffer}-typed parameter, for the CBOR logger processor
     * ({@code CborEventLoggerProcessor}) only - ignored by the SBE {@code EventLoggerProcessor}.
     * When set, must be exactly {@code {bufferParamName, offsetParamName, lengthParamName}}, each
     * validated against this method's real parameters (and their types) at compile time. Leave
     * empty when the {@code DirectBuffer}/{@code ByteBuffer} parameter represents the buffer's own
     * full extent - its {@code capacity()} for a bare {@code DirectBuffer}, or its
     * {@code position()}/{@code remaining()} for a bare {@code ByteBuffer}.
     *
     * @return {@code {bufferParamName, offsetParamName, lengthParamName}}, or empty to use the
     * buffer's own extent.
     */
    String[] bufferView() default {};
}
