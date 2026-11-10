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

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.Subscription;
import io.aeron.samples.echo.api.EchoMonitorMBean;
import io.aeron.samples.echo.api.ProvisioningConstants;
import io.aeron.samples.echo.api.ProvisioningMBean;
import org.agrona.CloseHelper;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import javax.management.*;
import java.lang.management.ManagementFactory;

/**
 * Implementation of the ProvisionMBean to manage pub/sub echo pairs for testing.
 */
public class Provisioning implements ProvisioningMBean
{
    private final Aeron aeron;
    private final Long2ObjectHashMap<EchoPair> echoPairByCorrelationId = new Long2ObjectHashMap<>();
    private final ManyToOneConcurrentArrayQueue<ProvisioningMessage> provisioningMessageQ =
        new ManyToOneConcurrentArrayQueue<>(1024);

    /**
     * Construct using the specified Aeron instance.
     *
     * @param aeron Aeron client instance.
     */
    public Provisioning(final Aeron aeron)
    {
        this.aeron = aeron;
    }

    /**
     * poll and send messages.
     *
     * @return amount of work done.
     */
    public int doWork()
    {
        int workDone = 0;

        workDone += pollProvisioningQueue();
        workDone += pollEchoPairs();

        return workDone;
    }

    /**
     * Remove all existing echo pairs.
     */
    @Override
    public void removeAll()
    {
        final RemoveAllEchoPairs removeAllEchoPairs = new RemoveAllEchoPairs();

        provisioningMessageQ.add(removeAllEchoPairs);

        try
        {
            removeAllEchoPairs.await();
        }
        catch (final InterruptedException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Create a pub/sub echo pair.
     *
     * @param correlationId user specified correlationId to track the echo pair.
     * @param subChannel    channel used for subscription.
     * @param subStreamId   stream id used for subscription.
     * @param pubChannel    channel used for publication.
     * @param pubStreamId   stream id used for publication.
     */
    @Override
    public void createEchoPair(
        final long correlationId,
        final String subChannel,
        final int subStreamId,
        final String pubChannel,
        final int pubStreamId)
    {
        final CreateEchoPair createEchoPair = new CreateEchoPair(
            correlationId,
            subChannel,
            subStreamId,
            pubChannel,
            pubStreamId);

        provisioningMessageQ.add(createEchoPair);

        try
        {
            createEchoPair.await();
        }
        catch (final InterruptedException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private void handleCreateEchoPair(final CreateEchoPair create) throws Exception
    {
        final ConcurrentPublication publication = aeron.addPublication(
            create.publicationChannel,
            create.publicationStream);

        final Subscription subscription;
        try
        {
            subscription = aeron.addSubscription(
                create.subscriptionChannel,
                create.subscriptionStream);
        }
        catch (final Exception ex)
        {
            CloseHelper.quietClose(publication);
            throw ex;
        }

        final EchoPair echoPair = new EchoPair(create.correlationId, subscription, publication);
        try
        {
            ManagementFactory.getPlatformMBeanServer().registerMBean(
                new StandardMBean(echoPair.monitor(), EchoMonitorMBean.class),
                new ObjectName(ProvisioningConstants.echoPairObjectName(create.correlationId)));
        }
        catch (final InstanceAlreadyExistsException |
            MBeanRegistrationException |
            NotCompliantMBeanException |
            MalformedObjectNameException ex)
        {
            CloseHelper.quietCloseAll(subscription, publication);
            throw ex;
        }

        echoPairByCorrelationId.put(echoPair.correlationId(), echoPair);
    }

    private int pollEchoPairs()
    {
        int workDone = 0;

        for (final EchoPair echoPair : echoPairByCorrelationId.values())
        {
            workDone += echoPair.poll();
        }

        return workDone;
    }

    private int pollProvisioningQueue()
    {
        int workDone = 0;
        ProvisioningMessage poll;

        while (null != (poll = provisioningMessageQ.poll()))
        {
            workDone++;

            try
            {
                if (poll instanceof CreateEchoPair)
                {
                    handleCreateEchoPair((CreateEchoPair)poll);
                }
                else if (poll instanceof RemoveAllEchoPairs)
                {
                    handleRemoveAll();
                }

                poll.complete("OK");
            }
            catch (final Exception ex)
            {
                poll.complete(ex);
            }
        }

        return workDone;
    }

    private void handleRemoveAll()
    {
        for (final EchoPair echoPair : echoPairByCorrelationId.values())
        {
            try
            {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(
                    new ObjectName(ProvisioningConstants.echoPairObjectName(echoPair.correlationId())));
            }
            catch (final InstanceNotFoundException ignore)
            {
            }
            catch (final MBeanRegistrationException | MalformedObjectNameException ex)
            {
                ex.printStackTrace();
            }

            CloseHelper.quietClose(echoPair);
        }

        echoPairByCorrelationId.clear();
    }
}
