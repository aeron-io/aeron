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
package io.aeron.samples.echo;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import io.aeron.samples.echo.api.EchoMonitorMBean;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

/**
 * Pub/sub pair that will copy all incoming fragments from the subscription onto the publication.
 */
public class EchoPair implements ControlledFragmentHandler, AutoCloseable
{
    private static final int FRAGMENT_LIMIT = 10;

    private final long correlationId;
    private final Subscription subscription;
    private final Publication publication;

    private long notConnectedCount = 0;
    private long backPressureCount = 0;
    private long adminActionCount = 0;
    private long closedCount = 0;
    private long maxSessionExceededCount = 0;

    private long fragmentCount = 0;
    private long byteCount = 0;

    /**
     * Construct the echo pair.
     *
     * @param correlationId user supplied correlation id.
     * @param subscription  to read fragments from.
     * @param publication   to send fragments back to.
     */
    public EchoPair(final long correlationId, final Subscription subscription, final Publication publication)
    {
        this.correlationId = correlationId;
        this.subscription = subscription;
        this.publication = publication;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        final long offerPosition = publication.offer(buffer, offset, length);
        if (Publication.NOT_CONNECTED == offerPosition)
        {
            notConnectedCount++;
            return Action.ABORT;
        }
        else if (Publication.BACK_PRESSURED == offerPosition)
        {
            backPressureCount++;
            return Action.ABORT;
        }
        else if (Publication.ADMIN_ACTION == offerPosition)
        {
            adminActionCount++;
            return Action.ABORT;
        }
        else if (Publication.CLOSED == offerPosition)
        {
            closedCount++;
            return Action.CONTINUE;
        }
        else if (Publication.MAX_POSITION_EXCEEDED == offerPosition)
        {
            maxSessionExceededCount++;
            return Action.CONTINUE;
        }
        else
        {
            fragmentCount++;
            byteCount += length;
            return Action.COMMIT;
        }
    }

    /**
     * Poll subscription of the echo pair.
     *
     * @return number of fragments processed.
     */
    public int poll()
    {
        return subscription.controlledPoll(this, FRAGMENT_LIMIT);
    }

    /**
     * Get the correlationId.
     *
     * @return user supplied correlationId.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * Get the monitoring MBean for this echo pair.
     *
     * @return An instance of the monitoring MBean that can be installed into a JMX container.
     */
    public EchoMonitorMBean monitor()
    {
        return new EchoMonitor();
    }

    /**
     * Close the echo pair.
     */
    @Override
    public void close()
    {
        CloseHelper.quietCloseAll(publication, subscription);
    }

    private final class EchoMonitor implements EchoMonitorMBean
    {
        /**
         * Get the correlationId.
         *
         * @return correlationId.
         */
        @Override
        public long getCorrelationId()
        {
            return correlationId;
        }

        /**
         * Number of times echo pair has experienced back pressure when copying to the publication.
         *
         * @return number of back pressure events.
         */
        @Override
        public long getBackPressureCount()
        {
            return backPressureCount;
        }

        /**
         * Number of fragments processed.
         *
         * @return number of fragments.
         */
        @Override
        public long getFragmentCount()
        {
            return fragmentCount;
        }

        /**
         * Number of bytes processed.
         *
         * @return number of bytes.
         */
        @Override
        public long getByteCount()
        {
            return byteCount;
        }
    }
}
