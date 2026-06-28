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
package io.aeron;

import io.aeron.exceptions.AeronException;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

/**
 * Counter stored in a file managed by the media driver which can be observed with AeronStat.
 */
public final class Counter extends AtomicCounter
{
    private static final VarHandle IS_CLOSED_VH;

    static
    {
        try
        {
            IS_CLOSED_VH = MethodHandles.lookup().findVarHandle(Counter.class, "isClosed", boolean.class);
        }
        catch (final ReflectiveOperationException ex)
        {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private volatile boolean isClosed;
    private final long correlationId;
    private final long registrationId;
    private final ClientConductor clientConductor;
    private boolean clientOwned;

    Counter(
        final long correlationId,
        final long registrationId,
        final boolean clientOwned,
        final ClientConductor clientConductor,
        final AtomicBuffer buffer,
        final int counterId)
    {
        super(buffer, counterId);

        this.correlationId = correlationId;
        this.registrationId = registrationId;
        this.clientConductor = clientConductor;
        this.clientOwned = clientOwned;
    }

    /**
     * Construct a read-write view of an existing counter.
     *
     * @param countersReader for getting access to the buffers.
     * @param counterId      for the counter to be viewed.
     * @throws AeronException if the id has for the counter has not been allocated.
     */
    public Counter(final CountersReader countersReader, final int counterId)
    {
        super(countersReader.valuesBuffer(), counterId);

        if (countersReader.getCounterState(counterId) != CountersReader.RECORD_ALLOCATED)
        {
            throw new AeronException("Counter id is not allocated: " + counterId);
        }

        correlationId = Aeron.NULL_VALUE;
        registrationId = countersReader.getCounterRegistrationId(counterId);
        clientConductor = null;
        clientOwned = true;
    }

    /**
     * Return the correlation id of the counter creation command sent to the media driver.
     *
     * @return the correlation id of the command or {@link Aeron#NULL_VALUE} if unknown.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * Return the registration id used to register this counter with the media driver. Can also be retrieved by calling
     * {@link CountersReader#getCounterRegistrationId(int)}.
     *
     * <p>
     * For non-static counters this is the same as {@link #correlationId()}. For static counters this will be a
     * user-defined value specified at creation time.
     *
     * @return the registration id used to register this counter with the media driver.
     * @see #correlationId()
     * @see CountersReader#getCounterRegistrationId(int)
     */
    public long registrationId()
    {
        return registrationId;
    }

    /**
     * Close the counter, releasing the resource managed by the media driver if this was the creator of the Counter.
     * <p>
     * This method is idempotent and thread safe.
     */
    public void close()
    {
        if (IS_CLOSED_VH.compareAndSet(this, false, true))
        {
            super.close();
            if (null != clientConductor)
            {
                clientConductor.removeCounter(this);
            }
        }
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    public boolean isClosed()
    {
        return isClosed;
    }

    void internalClose()
    {
        super.close();
        isClosed = true;
    }

    boolean clientOwned()
    {
        return clientOwned;
    }
}
