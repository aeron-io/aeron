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
package io.aeron.status;

import io.aeron.AeronCounters;
import org.agrona.BitUtil;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersManager;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static org.agrona.concurrent.status.CountersReader.*;

/**
 * Allocate a counter for tracking the last heartbeat of an entity with a given registration id.
 */
public final class HeartbeatTimestamp
{
    private HeartbeatTimestamp()
    {
    }

    /**
     * Type id of a heartbeat counter.
     */
    public static final int HEARTBEAT_TYPE_ID = AeronCounters.DRIVER_HEARTBEAT_TYPE_ID;

    /**
     * Offset in the key metadata for the registration id of the counter.
     */
    public static final int REGISTRATION_ID_OFFSET = 0;

    /**
     * Allocate a counter for tracking the last heartbeat of an entity.
     *
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which the underlying storage is allocated.
     * @param registrationId  to be associated with the counter.
     * @return a new {@link AtomicCounter} for tracking the last heartbeat.
     */
    public static AtomicCounter allocate(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId)
    {
        return new AtomicCounter(
            countersManager.valuesBuffer(),
            allocateCounterId(tempBuffer, name, typeId, countersManager, registrationId),
            countersManager);
    }

    /**
     * Allocate a counter id for tracking the last heartbeat of an entity.
     *
     * @param tempBuffer      to be used for labels and key.
     * @param name            of the counter for the label.
     * @param typeId          of the counter for classification.
     * @param countersManager from which the underlying storage is allocated.
     * @param registrationId  to be associated with the counter.
     * @return the counter id to be used.
     */
    public static int allocateCounterId(
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final CountersManager countersManager,
        final long registrationId)
    {
        tempBuffer.putLong(REGISTRATION_ID_OFFSET, registrationId);
        final int keyLength = REGISTRATION_ID_OFFSET + SIZE_OF_LONG;

        final int labelOffset = BitUtil.align(keyLength, SIZE_OF_INT);
        int labelLength = 0;
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, name);
        labelLength += tempBuffer.putStringWithoutLengthAscii(labelOffset + labelLength, ": id=");
        labelLength += tempBuffer.putLongAscii(labelOffset + labelLength, registrationId);

        return countersManager.allocate(
            typeId,
            tempBuffer,
            0,
            keyLength,
            tempBuffer,
            labelOffset,
            labelLength);
    }

    /**
     * Find the active counter id for a heartbeat timestamp.
     *
     * @param countersReader to search within.
     * @param counterTypeId  to match on.
     * @param registrationId for the active client.
     * @return the counter id if found otherwise {@link CountersReader#NULL_COUNTER_ID}.
     */
    public static int findCounterIdByRegistrationId(
        final CountersReader countersReader, final int counterTypeId, final long registrationId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();

        for (int counterId = 0, maxId = countersReader.maxCounterId(); counterId <= maxId; counterId++)
        {
            final int counterState = countersReader.getCounterState(counterId);
            if (counterState == RECORD_ALLOCATED)
            {
                if (countersReader.getCounterTypeId(counterId) == counterTypeId &&
                    buffer.getLong(metaDataOffset(counterId) + KEY_OFFSET + REGISTRATION_ID_OFFSET) == registrationId)
                {
                    return counterId;
                }
            }
            else if (RECORD_UNUSED == counterState)
            {
                break;
            }
        }

        return NULL_COUNTER_ID;
    }

    /**
     * Is the counter active for usage? Checks to see if reclaimed or reused and matches registration id.
     *
     * @param countersReader to search within.
     * @param counterId      to test.
     * @param counterTypeId  to validate type.
     * @param registrationId for the entity.
     * @return true if still valid otherwise false.
     */
    public static boolean isActive(
        final CountersReader countersReader, final int counterId, final int counterTypeId, final long registrationId)
    {
        final DirectBuffer buffer = countersReader.metaDataBuffer();
        final int recordOffset = CountersReader.metaDataOffset(counterId);

        return countersReader.getCounterTypeId(counterId) == counterTypeId &&
            buffer.getLong(recordOffset + KEY_OFFSET + REGISTRATION_ID_OFFSET) == registrationId &&
            countersReader.getCounterState(counterId) == RECORD_ALLOCATED;
    }
}
