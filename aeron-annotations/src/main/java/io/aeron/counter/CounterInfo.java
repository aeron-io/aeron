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

import java.io.Serializable;

/**
 * Holds all metadata collected by {@link CounterProcessor} for a single {@code @AeronCounter}
 * annotated field, serialised to {@code counter-info.dat} for use by downstream Gradle tasks.
 *
 * <p>The {@link #isSystemCounter} flag distinguishes the two namespaces:</p>
 * <ul>
 *   <li>{@code false} — a counter <em>type-ID</em> ({@code *_TYPE_ID} field).  {@link #id} is
 *       unique across all type-IDs and matches {@code AERON_COUNTER_*_TYPE_ID} in C.</li>
 *   <li>{@code true} — a system counter <em>sub-type</em> ({@code SYSTEM_COUNTER_ID_*} field).
 *       {@link #id} is an index within {@code DRIVER_SYSTEM_COUNTER_TYPE_ID} (type-ID 0) and
 *       matches {@code AERON_SYSTEM_COUNTER_ID_*} in C.  These IDs overlap with type-IDs and
 *       must never be sorted or compared alongside them.</li>
 * </ul>
 */
public class CounterInfo implements Serializable
{
    private static final long serialVersionUID = -5863246029522577056L;

    /**
     * Counter name.
     */
    public final String name;

    /**
     * Counter id.
     */
    public int id;

    /**
     * Counter description.
     */
    public String counterDescription;

    /**
     * Counter description with Javadoc markup converted to Markdown.
     */
    public String counterDescriptionClean;

    /**
     * Whether counter exists in the C media driver.
     */
    public boolean existsInC = true;

    /**
     * Whether this is a system counter sub-type (SYSTEM_COUNTER_ID_* field).
     */
    public boolean isSystemCounter = false;

    /**
     * Fully-qualified Java field reference, e.g. {@code AeronCounters.SYSTEM_COUNTER_ID_BYTES_SENT}.
     */
    public String javaFieldName;

    /**
     * Expected name in the C media driver.
     */
    public String expectedCName;

    /**
     * Default constructor.
     */
    public CounterInfo()
    {
        this.name = null;
    }

    /**
     * Create the CounterInfo given a name.
     *
     * @param name the name of the counter
     */
    public CounterInfo(final String name)
    {
        this.name = name;
    }
}
