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
package io.aeron.driver.status;

import io.aeron.status.HeartbeatTimestamp;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;

import static io.aeron.status.HeartbeatTimestamp.HEARTBEAT_TYPE_ID;

/**
 * Counter for tracking the timestamp of a last heartbeat from an Aeron client.
 *
 * @see HeartbeatTimestamp
 */
public final class ClientHeartbeatTimestamp
{
    /**
     * Human-readable name for the counter.
     */
    public static final String NAME = "client-heartbeat";

    private ClientHeartbeatTimestamp()
    {
    }

    /**
     * Allocate a new counter by delegating to
     * {@link HeartbeatTimestamp#allocate(MutableDirectBuffer, String, int, CountersManager, long)}.
     *
     * @param tempBuffer      for writing the metadata.
     * @param countersManager for the counter.
     * @param registrationId  for the client.
     * @return the allocated counter.
     */
    public static AtomicCounter allocate(
        final MutableDirectBuffer tempBuffer, final CountersManager countersManager, final long registrationId)
    {
        return HeartbeatTimestamp.allocate(tempBuffer, NAME, HEARTBEAT_TYPE_ID, countersManager, registrationId);
    }
}
