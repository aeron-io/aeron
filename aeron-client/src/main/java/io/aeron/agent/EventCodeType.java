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
package io.aeron.agent;

/**
 * Specifies the type of EventCode that can be handled by the logging agent.
 */
public enum EventCodeType
{
    /**
     * Events related to media driver operation.
     */
    DRIVER(0),

    /**
     * Events related to archive service operation.
     */
    ARCHIVE(1),

    /**
     * Events related to cluster election and consensus operation.
     */
    CLUSTER(2),

    /**
     * Events related to cluster standby operations including standby consensus and transitioning.
     */
    STANDBY(3),

    /**
     * Events related to the sequencer operations.
     */
    SEQUENCER(4),

    /**
     * User defined events for third party usage.
     */
    USER(0xFFFF);

    private final int typeCode;

    EventCodeType(final int typeCode)
    {
        this.typeCode = typeCode;
    }

    /**
     * The type code which classifies the events to identify one of the {@link EventCodeType} enum value.
     *
     * @return type code which classifies the events to identify one of the {@link EventCodeType} enum value.
     */
    public int getTypeCode()
    {
        return typeCode;
    }
}
