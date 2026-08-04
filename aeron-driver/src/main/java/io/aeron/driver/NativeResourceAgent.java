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
package io.aeron.driver;

import io.aeron.ChannelUri;
import io.aeron.driver.buffer.LogFactory;
import io.aeron.driver.buffer.RawLog;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.status.SystemCounterDescriptor;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.OneToOneConcurrentArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.util.ArrayDeque;

import static io.aeron.driver.MediaDriver.AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME;

final class NativeResourceAgent implements Agent
{
    private final ArrayDeque<RawLog> logBuffersToFree = new ArrayDeque<>();
    private final TimeTrackingNameResolver nameResolver;
    private final OneToOneConcurrentArrayQueue<Runnable> taskQueue;
    private final AtomicCounter freeFailsCounter;
    private final int freeLimit;
    private final LogFactory logFactory;
    private final MediaDriver.Context ctx;

    NativeResourceAgent(final MediaDriver.Context ctx)
    {
        this.nameResolver = new TimeTrackingNameResolver(
            ctx.nameResolver(),
            ctx.nanoClock(),
            ctx.nameResolverTimeTracker());
        this.taskQueue = ctx.nativeResourceAgentCommandQueue();
        this.freeFailsCounter = ctx.systemCounters().get(SystemCounterDescriptor.FREE_FAILS);
        this.freeLimit = ctx.resourceFreeLimit();
        logFactory = ctx.logFactory();
        this.ctx = ctx;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onStart()
    {
        nameResolver.onStart();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int doWork()
    {
        int workCount = 0;
        workCount += nameResolver.doWork();
        workCount += runTasks();
        workCount += freeEndOfLifeResources();

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onClose()
    {
        nameResolver.onClose();

        RawLog rawLog;
        while (null != (rawLog = logBuffersToFree.pollFirst()))
        {
            rawLog.free();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String roleName()
    {
        return AERON_DRIVER_NATIVE_RESOURCE_THREAD_NAME;
    }

    void onParseChannel(final CommandResult<UdpChannel> result, final String channel, final boolean isDestination)
    {
        try
        {
            result.ok(UdpChannel.parse(channel, nameResolver, isDestination));
        }
        catch (final Exception ex)
        {
            result.error(ex);
        }
    }

    void onReResolveAddress(
        final CommandResult<InetSocketAddress> result, final String endpoint, final String uriParamName)
    {
        try
        {
            result.ok(UdpChannel.resolve(endpoint, uriParamName, true, nameResolver));
        }
        catch (final Exception ex)
        {
            result.error(ex);
        }
    }

    void onDestinationAddress(final CommandResult<InetSocketAddress> result, final ChannelUri channel)
    {
        try
        {
            result.ok(UdpChannel.destinationAddress(channel, nameResolver));
        }
        catch (final Exception ex)
        {
            result.error(ex);
        }
    }

    void onFreeLogBuffer(final RawLog rawLog)
    {
        if (!rawLog.free())
        {
            onFreeFail(rawLog);
        }
    }

    void onNewPublicationLog(
        final CommandResult<RawLog> result,
        final long registrationId,
        final int termBufferLength,
        final boolean useSparseFiles)
    {
        try
        {
            result.ok(logFactory.newPublication(registrationId, termBufferLength, useSparseFiles));
        }
        catch (final Exception ex)
        {
            result.error(ex);
        }
    }

    void onNewImageLog(
        final CommandResult<RawLog> result,
        final long registrationId,
        final int termBufferLength,
        final boolean useSparseFiles)
    {
        try
        {
            result.ok(logFactory.newImage(registrationId, termBufferLength, useSparseFiles));
        }
        catch (final Exception ex)
        {
            result.error(ex);
        }
    }

    private int runTasks()
    {
        final Runnable task = taskQueue.poll();
        if (null != task)
        {
            try
            {
                task.run();
            }
            catch (final Exception ex)
            {
                ctx.countedErrorHandler().onError(ex);
            }
            return 1;
        }
        return 0;
    }

    private int freeEndOfLifeResources()
    {
        int workCount = 0;

        for (int i = 0; i < freeLimit; i++)
        {
            final RawLog rawLog = logBuffersToFree.pollFirst();
            if (null == rawLog)
            {
                break;
            }

            if (rawLog.free())
            {
                workCount++;
            }
            else
            {
                onFreeFail(rawLog);
            }
        }

        return workCount;
    }

    private void onFreeFail(final RawLog rawLog)
    {
        if (!freeFailsCounter.isClosed())
        {
            freeFailsCounter.incrementRelease();
        }
        logBuffersToFree.addLast(rawLog);
    }
}
