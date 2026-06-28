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
 * A handy class for storing data that gets serialized into json.
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
     * Whether counter exists in the C media driver.
     */
    public boolean existsInC = true;

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
