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

import io.aeron.Aeron;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Readonly view of an associated {@link io.aeron.Counter}.
 * <p>
 * <b>Note:</b>The user should call {@link #isClosed()} and ensure the result is false to avoid a race on reading a
 * closed {@link io.aeron.Counter}.
 */
public final class ReadableCounter implements AutoCloseable
{
    private final CountersReader countersReader;
    private final UnsafeBuffer valueBuffer;
    private final long registrationId;
    private final int counterId;
    private volatile boolean isClosed = false;

    /**
     * Construct a view of an existing counter.
     *
     * @param countersReader for getting access to the buffers.
     * @param registrationId assigned by the driver for the counter or {@link Aeron#NULL_VALUE} if not known.
     * @param counterId      for the counter to be viewed.
     * @throws IllegalStateException if the id has for the counter has not been allocated.
     */
    public ReadableCounter(final CountersReader countersReader, final long registrationId, final int counterId)
    {
        final int counterState = countersReader.getCounterState(counterId);
        if (counterState != CountersReader.RECORD_ALLOCATED)
        {
            throw new IllegalStateException("Counter not allocated: id=" + counterId + " state=" + counterState);
        }

        this.countersReader = countersReader;
        this.counterId = counterId;
        this.registrationId = registrationId;

        final AtomicBuffer valuesBuffer = countersReader.valuesBuffer();
        final int counterOffset = CountersReader.counterOffset(counterId);
        valuesBuffer.boundsCheck(counterOffset, SIZE_OF_LONG);

        valueBuffer = new UnsafeBuffer(valuesBuffer, counterOffset, SIZE_OF_LONG);
    }

    /**
     * Construct a view of an existing counter.
     *
     * @param countersReader for getting access to the buffers.
     * @param counterId      for the counter to be viewed.
     * @throws IllegalStateException if the id has for the counter has not been allocated.
     */
    public ReadableCounter(final CountersReader countersReader, final int counterId)
    {
        this(countersReader, Aeron.NULL_VALUE, counterId);
    }

    /**
     * Return the registration id for the counter.
     *
     * @return registration id.
     */
    public long registrationId()
    {
        return registrationId;
    }

    /**
     * Return the counter id.
     *
     * @return counter id.
     */
    public int counterId()
    {
        return counterId;
    }

    /**
     * Return the state of the counter.
     *
     * @return state for the counter.
     * @see CountersReader#RECORD_ALLOCATED
     * @see CountersReader#RECORD_RECLAIMED
     * @see CountersReader#RECORD_UNUSED
     */
    public int state()
    {
        return countersReader.getCounterState(counterId);
    }

    /**
     * Return the counter label.
     *
     * @return the counter label.
     */
    public String label()
    {
        return countersReader.getCounterLabel(counterId);
    }

    /**
     * Get the latest value for the counter with volatile semantics.
     * <p>
     * <b>Note:</b>The user should call {@link #isClosed()} and ensure the result is false to avoid a race on reading
     * a closed counter.
     *
     * @return the latest value for the counter.
     */
    public long get()
    {
        return valueBuffer.getLongVolatile(0);
    }

    /**
     * Get the value of the counter using weak ordering semantics. This is the same a standard read of a field.
     * <p>
     * This call is identical to {@link #getPlain()} and that method is preferred.
     *
     * @return the  value for the counter.
     */
    public long getWeak()
    {
        return getPlain();
    }

    /**
     * Get the value of the counter using plain memory semantics. This is the same a standard read of a field.
     *
     * @return the value for the counter.
     */
    public long getPlain()
    {
        return valueBuffer.getLong(0);
    }

    /**
     * Close this counter. This has no impact on the {@link io.aeron.Counter} it is viewing.
     */
    public void close()
    {
        isClosed = true;
    }

    /**
     * Has this counter been closed and should it no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    public boolean isClosed()
    {
        return isClosed;
    }
}
