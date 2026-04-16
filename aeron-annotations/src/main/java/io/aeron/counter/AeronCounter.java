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
package io.aeron.counter;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark a field as an Aeron counter definition.
 *
 * <p>There are two distinct kinds of counter field that carry this annotation, and they must be
 * kept separate from each other because their integer IDs occupy independent namespaces:</p>
 *
 * <ol>
 *   <li><b>Counter type-ID fields</b> — named with a {@code _TYPE_ID} suffix
 *       (e.g. {@code DRIVER_PUBLISHER_LIMIT_TYPE_ID = 1}).  Each value is a unique integer that
 *       identifies a <em>kind</em> of counter in the counters buffer.  The C driver exposes these
 *       as {@code AERON_COUNTER_*_TYPE_ID} defines in {@code aeron_counters.h}.</li>
 *
 *   <li><b>System counter sub-type fields</b> — named with a {@code SYSTEM_COUNTER_ID_} prefix
 *       (e.g. {@code SYSTEM_COUNTER_ID_BYTES_SENT = 0}).  These are <em>not</em> independent
 *       counter types; they are discriminator values used <em>within</em> the single
 *       {@code DRIVER_SYSTEM_COUNTER_TYPE_ID} (type-ID 0) counter to identify individual
 *       system-wide statistics (bytes sent/received, NAKs, errors, etc.).  The C driver exposes
 *       these as {@code AERON_SYSTEM_COUNTER_ID_*} defines.  Because they share the same counter
 *       type, their IDs restart from 0 and overlap with counter type-IDs.</li>
 * </ol>
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.SOURCE)
public @interface AeronCounter
{
    /**
     * Whether this counter exists in the C code.
     *
     * @return whether this counter exists in the C code.
     */
    boolean existsInC() default true;

    /**
     * The name of the #define in C.
     *
     * @return the name of the #define in C.
     */
    String expectedCName() default "";
}
