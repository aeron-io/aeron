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

import io.aeron.config.Config;
import io.aeron.config.DefaultType;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import io.aeron.exceptions.DriverTimeoutException;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.version.Versioned;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.Strings;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.agrona.concurrent.CountedErrorHandler;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.SystemNanoClock;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.agrona.concurrent.broadcast.BroadcastReceiver;
import org.agrona.concurrent.broadcast.CopyBroadcastReceiver;
import org.agrona.concurrent.ringbuffer.ManyToOneRingBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static io.aeron.Aeron.Configuration.MAX_CLIENT_NAME_LENGTH;
import static org.agrona.SystemUtil.getDurationInNanos;
import static org.agrona.SystemUtil.getProperty;

/**
 * Aeron entry point for communicating to the Media Driver for creating {@link Publication}s and {@link Subscription}s.
 * Use an {@link Aeron.Context} to configure the Aeron object.
 * <p>
 * A client application requires only one Aeron object per Media Driver.
 * <p>
 * <b>Note:</b> If {@link Aeron.Context#errorHandler(ErrorHandler)} is not set and a {@link DriverTimeoutException}
 * occurs then the process will face the wrath of {@link System#exit(int)}.
 * See {@link Aeron.Configuration#DEFAULT_ERROR_HANDLER}.
 */
@Versioned
public class Aeron implements AutoCloseable
{
    /**
     * Used to represent a null value for when some value is not yet set.
     */
    public static final int NULL_VALUE = -1;

    private static final VarHandle IS_CLOSED_VH;

    static
    {
        try
        {
            IS_CLOSED_VH = MethodHandles.lookup().findVarHandle(Aeron.class, "isClosed", boolean.class);
        }
        catch (final ReflectiveOperationException ex)
        {
            throw new ExceptionInInitializerError(ex);
        }
    }

    private volatile boolean isClosed;
    private final long clientId;
    private final ClientConductor conductor;
    private final RingBuffer commandBuffer;
    private final AgentInvoker conductorInvoker;
    private final AgentRunner conductorRunner;
    private final Context ctx;

    Aeron(final Context ctx)
    {
        try
        {
            ctx.conclude();

            this.ctx = ctx;
            clientId = ctx.clientId();
            commandBuffer = ctx.toDriverBuffer();
            conductor = new ClientConductor(ctx, this);

            if (ctx.useConductorAgentInvoker())
            {
                conductorInvoker = new AgentInvoker(ctx.errorHandler(), null, conductor);
                conductorRunner = null;
            }
            else
            {
                conductorInvoker = null;
                conductorRunner = new AgentRunner(ctx.idleStrategy(), ctx.errorHandler(), null, conductor);
            }
        }
        catch (final ConcurrentConcludeException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            CloseHelper.quietClose(ctx::close);
            throw ex;
        }
    }

    /**
     * Create an Aeron instance and connect to the media driver with a default {@link Context}.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     *
     * @return the new {@link Aeron} instance connected to the Media Driver.
     */
    public static Aeron connect()
    {
        return connect(new Context());
    }

    /**
     * Create an Aeron instance and connect to the media driver.
     * <p>
     * Threads required for interacting with the media driver are created and managed within the Aeron instance.
     * <p>
     * If an exception occurs while trying to establish a connection then the {@link Context#close()} method
     * will be called on the passed context.
     *
     * @param ctx for configuration of the client.
     * @return the new {@link Aeron} instance connected to the Media Driver.
     */
    public static Aeron connect(final Context ctx)
    {
        try
        {
            final Aeron aeron = new Aeron(ctx);

            if (ctx.useConductorAgentInvoker())
            {
                aeron.conductorInvoker.start();
            }
            else
            {
                AgentRunner.startOnThread(aeron.conductorRunner, ctx.threadFactory());
            }

            return aeron;
        }
        catch (final ConcurrentConcludeException ex)
        {
            throw ex;
        }
        catch (final Exception ex)
        {
            ctx.close();
            throw ex;
        }
    }

    /**
     * Print out the values from {@link #countersReader()} which can be useful for debugging.
     *
     * @param out to where the counters get printed.
     */
    public void printCounters(final PrintStream out)
    {
        final CountersReader counters = countersReader();
        counters.forEach((value, id, label) -> out.format("%3d: %,20d - %s%n", id, value, label));
    }

    /**
     * Has the client been closed? If not then the CnC file may not be unmapped.
     *
     * @return true if the client has been explicitly closed otherwise false.
     */
    public boolean isClosed()
    {
        return isClosed;
    }

    /**
     * Get the {@link Aeron.Context} that is used by this client.
     *
     * @return the {@link Aeron.Context} that is use by this client.
     */
    public Context context()
    {
        return ctx;
    }

    /**
     * Get the client identity that has been allocated for communicating with the media driver.
     *
     * @return the client identity that has been allocated for communicating with the media driver.
     */
    public long clientId()
    {
        return clientId;
    }

    /**
     * Get the {@link AgentInvoker} for the client conductor.
     *
     * @return the {@link AgentInvoker} for the client conductor.
     */
    public AgentInvoker conductorAgentInvoker()
    {
        return conductorInvoker;
    }

    /**
     * Is the command still active for a given correlation id.
     *
     * @param correlationId to check if it is still active.
     * @return true in the command is still in active processing or false if completed successfully or errored.
     * @see Publication#asyncAddDestination(String)
     * @see Subscription#asyncAddDestination(String)
     * @see #hasActiveCommands()
     */
    public boolean isCommandActive(final long correlationId)
    {
        return conductor.isCommandActive(correlationId);
    }

    /**
     * Does the client have any active asynchronous commands?
     * <p>
     * When close operations are performed on {@link Publication}s, {@link Subscription}s, and {@link Counter}s the
     * commands are sent asynchronously to the driver. The client tracks active commands in case errors need to be
     * reported. If you wish to wait for acknowledgement of close operations then wait for this method to return false.
     *
     * @return true if any commands are currently active otherwise false.
     */
    public boolean hasActiveCommands()
    {
        return conductor.hasActiveCommands();
    }

    /**
     * Clean up and release all Aeron client resources and shutdown conductor thread if not using
     * {@link Context#useConductorAgentInvoker(boolean)}.
     * <p>
     * This will close all currently open {@link Publication}s, {@link Subscription}s, and {@link Counter}s created
     * from this client. To check for the command being acknowledged by the driver
     */
    public void close()
    {
        if (IS_CLOSED_VH.compareAndSet(this, false, true))
        {
            final ErrorHandler errorHandler = ctx.errorHandler();
            if (null != conductorRunner)
            {
                CloseHelper.close(errorHandler, conductorRunner);
            }
            else
            {
                CloseHelper.close(errorHandler, conductorInvoker);
            }
        }
    }

    /**
     * Add a {@link Publication} for publishing messages to subscribers. The publication returned is threadsafe.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return a new {@link ConcurrentPublication}.
     */
    public ConcurrentPublication addPublication(final String channel, final int streamId)
    {
        return conductor.addPublication(channel, streamId);
    }

    /**
     * Add an {@link ExclusivePublication} for publishing messages to subscribers from a single thread.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return a new {@link ExclusivePublication}.
     */
    public ExclusivePublication addExclusivePublication(final String channel, final int streamId)
    {
        return conductor.addExclusivePublication(channel, streamId);
    }

    /**
     * Asynchronously add a {@link Publication} for publishing messages to subscribers. The added publication returned
     * is threadsafe.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return the registration id of the publication which can be used to get the added publication.
     * @see #getPublication(long)
     */
    public long asyncAddPublication(final String channel, final int streamId)
    {
        return conductor.asyncAddPublication(channel, streamId);
    }

    /**
     * Asynchronously add a {@link Publication} for publishing messages to subscribers from a single thread.
     *
     * @param channel  for sending the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return the registration id of the publication which can be used to get the added exclusive publication.
     * @see #getExclusivePublication(long)
     */
    public long asyncAddExclusivePublication(final String channel, final int streamId)
    {
        return conductor.asyncAddExclusivePublication(channel, streamId);
    }

    /**
     * Asynchronously remove a {@link Publication}.
     *
     * @param registrationId to be of the publication removed.
     * @see #asyncAddPublication(String, int)
     * @see #asyncAddExclusivePublication(String, int)
     */
    public void asyncRemovePublication(final long registrationId)
    {
        conductor.removePublication(registrationId);
    }

    /**
     * Get a {@link Publication} for publishing messages to subscribers. The publication returned is threadsafe.
     *
     * @param registrationId returned from {@link #asyncAddPublication(String, int)}.
     * @return a new {@link ConcurrentPublication} when available otherwise null.
     * @see #asyncAddPublication(String, int)
     */
    public ConcurrentPublication getPublication(final long registrationId)
    {
        return conductor.getPublication(registrationId);
    }

    /**
     * Get a single threaded {@link Publication} for publishing messages to subscribers.
     *
     * @param registrationId returned from {@link #asyncAddExclusivePublication(String, int)}.
     * @return a new {@link ExclusivePublication} when available otherwise null.
     * @see #asyncAddExclusivePublication(String, int)
     */
    public ExclusivePublication getExclusivePublication(final long registrationId)
    {
        return conductor.getExclusivePublication(registrationId);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     * <p>
     * The method will set up the {@link Subscription} to use the
     * {@link Aeron.Context#availableImageHandler(AvailableImageHandler)} and
     * {@link Aeron.Context#unavailableImageHandler(UnavailableImageHandler)} from the {@link Aeron.Context}.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return the {@link Subscription} for the channel and streamId pair.
     */
    public Subscription addSubscription(final String channel, final int streamId)
    {
        return conductor.addSubscription(channel, streamId);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     * <p>
     * This method will override the default handlers from the {@link Aeron.Context}, i.e.
     * {@link Aeron.Context#availableImageHandler(AvailableImageHandler)} and
     * {@link Aeron.Context#unavailableImageHandler(UnavailableImageHandler)}. Null values are valid and will
     * result in no action being taken.
     *
     * @param channel                 for receiving the messages known to the media layer.
     * @param streamId                within the channel scope.
     * @param availableImageHandler   called when {@link Image}s become available for consumption. Null is valid if no
     *                                action is to be taken.
     * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption. Null is valid if no
     *                                action is to be taken.
     * @return the {@link Subscription} for the channel and streamId pair.
     */
    public Subscription addSubscription(
        final String channel,
        final int streamId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        return conductor.addSubscription(channel, streamId, availableImageHandler, unavailableImageHandler);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * @param channel                 for receiving the messages known to the media layer.
     * @param streamId                within the channel scope.
     * @param availableImageHandler   called when {@link Image}s become available for consumption. Null is valid if no
     *                                action is to be taken.
     * @param unavailableImageHandler called when {@link Image}s go unavailable for consumption. Null is valid if no
     *                                action is to be taken.
     * @return the registration id of the subscription which can be used to get the added subscription.
     * @see Aeron#addSubscription(String, int, AvailableImageHandler, UnavailableImageHandler)
     * @see Aeron#getSubscription(long)
     */
    public long asyncAddSubscription(
        final String channel,
        final int streamId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        return conductor.asyncAddSubscription(channel, streamId, availableImageHandler, unavailableImageHandler);
    }

    /**
     * Add a new {@link Subscription} for subscribing to messages from publishers.
     *
     * @param channel  for receiving the messages known to the media layer.
     * @param streamId within the channel scope.
     * @return the registration id of the subscription which can be used to get the added subscription.
     * @see Aeron#addSubscription(String, int)
     * @see Aeron#getSubscription(long)
     */
    public long asyncAddSubscription(final String channel, final int streamId)
    {
        return conductor.asyncAddSubscription(channel, streamId);
    }

    /**
     * Asynchronously remove a {@link Subscription}.
     *
     * @param registrationId to be of the subscription removed.
     * @see #asyncAddSubscription(String, int)
     * @see #asyncAddSubscription(String, int, AvailableImageHandler, UnavailableImageHandler)
     */
    public void asyncRemoveSubscription(final long registrationId)
    {
        conductor.removeSubscription(registrationId);
    }

    /**
     * Get a {@link Subscription} for subscribing to messages from publishers.
     *
     * @param registrationId returned from
     *                       {@link #asyncAddSubscription(String, int, AvailableImageHandler, UnavailableImageHandler)}
     *                       or {@link #asyncAddSubscription(String, int)}
     * @return a new {@link Subscription} when available otherwise null.
     * @see #asyncAddSubscription(String, int)
     * @see #asyncAddSubscription(String, int, AvailableImageHandler, UnavailableImageHandler)
     */
    public Subscription getSubscription(final long registrationId)
    {
        return conductor.getSubscription(registrationId);
    }

    /**
     * Generate the next correlation id that is unique for the connected Media Driver.
     * <p>
     * This is useful generating correlation identifiers for pairing requests with responses in a clients own
     * application protocol.
     * <p>
     * This method is thread safe and will work across processes that all use the same media driver.
     *
     * @return next correlation id that is unique for the Media Driver.
     */
    public long nextCorrelationId()
    {
        if (isClosed)
        {
            throw new AeronException("client is closed");
        }

        return commandBuffer.nextCorrelationId();
    }

    /**
     * Get the {@link CountersReader} for the Aeron media driver counters.
     *
     * @return new {@link CountersReader} for the Aeron media driver in use.
     */
    public CountersReader countersReader()
    {
        if (isClosed)
        {
            throw new AeronException("client is closed");
        }

        return conductor.countersReader();
    }

    /**
     * Allocate a counter on the media driver and return a {@link Counter} for it.
     * <p>
     * The counter should be freed by calling {@link Counter#close()}.
     * <p>
     * The typeId should be 1000 or greater. Values lower than that are reserved for use by Aeron.
     *
     * @param typeId      for the counter.
     * @param keyBuffer   containing the optional key for the counter.
     * @param keyOffset   within the keyBuffer at which the key begins.
     * @param keyLength   of the key in the keyBuffer.
     * @param labelBuffer containing the mandatory label for the counter. The label should not be length prefixed.
     * @param labelOffset within the labelBuffer at which the label begins.
     * @param labelLength of the label in the labelBuffer.
     * @return the newly allocated counter.
     * @see org.agrona.concurrent.status.CountersManager#allocate(int, DirectBuffer, int, int, DirectBuffer, int, int)
     */
    public Counter addCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength)
    {
        return conductor.addCounter(typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength);
    }

    /**
     * Allocate a counter on the media driver and return a {@link Counter} for it.
     * <p>
     * The counter should be freed by calling {@link Counter#close()}.
     * <p>
     * The typeId should be 1000 or greater. Values lower than that are reserved for use by Aeron.
     *
     * @param typeId for the counter.
     * @param label  for the counter. It should be US-ASCII.
     * @return the newly allocated counter.
     * @see org.agrona.concurrent.status.CountersManager#allocate(String, int)
     */
    public Counter addCounter(final int typeId, final String label)
    {
        return conductor.addCounter(typeId, label);
    }


    /**
     * Allocates or returns an existing static counter instance using specified {@code typeId} and
     * {@code registrationId} pair. Such a counter cannot be deleted and its lifecycle is decoupled from this
     * {@link Aeron} instance, i.e. won't be closed when this instance is closed or times out.
     * <p>
     * <em><strong>Note:</strong> calling {@link Counter#close()} will only close the counter instance itself but will
     * not free the counter in the CnC file.</em>
     * <p>
     * The typeId should be 1000 or greater. Values lower than that are reserved for use by Aeron.
     *
     * @param typeId         for the counter.
     * @param keyBuffer      containing the optional key for the counter.
     * @param keyOffset      within the keyBuffer at which the key begins.
     * @param keyLength      of the key in the keyBuffer.
     * @param labelBuffer    containing the mandatory label for the counter. The label should not be length prefixed.
     * @param labelOffset    within the labelBuffer at which the label begins.
     * @param labelLength    of the label in the labelBuffer.
     * @param registrationId that uniquely identifies the static counter for a given {@code typeId}.
     * @return the static counter instance.
     * @see org.agrona.concurrent.status.CountersManager#allocate(int, DirectBuffer, int, int, DirectBuffer, int, int)
     * @since 1.45.0
     */
    public Counter addStaticCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength,
        final long registrationId)
    {
        return conductor.addStaticCounter(
            typeId, keyBuffer, keyOffset, keyLength, labelBuffer, labelOffset, labelLength, registrationId);
    }

    /**
     * Allocates or returns an existing static counter instance using specified {@code typeId} and
     * {@code registrationId} pair. Such a counter cannot be deleted and its lifecycle is decoupled from this
     * {@link Aeron} instance, i.e. won't be closed when this instance is closed or times out.
     * <p>
     * <em><strong>Note:</strong> calling {@link Counter#close()} will only close the counter instance itself but will
     * not free the counter in the CnC file.</em>
     * <p>
     * The typeId should be 1000 or greater. Values lower than that are reserved for use by Aeron.
     *
     * @param typeId         for the counter.
     * @param label          for the counter. It should be US-ASCII.
     * @param registrationId that uniquely identifies the static counter for a given {@code typeId}.
     * @return the static counter.
     * @see org.agrona.concurrent.status.CountersManager#allocate(String, int)
     * @since 1.45.0
     */
    public Counter addStaticCounter(final int typeId, final String label, final long registrationId)
    {
        return conductor.addStaticCounter(typeId, label, registrationId);
    }

    /**
     * Add a handler to the list be called when {@link Counter}s become available.
     *
     * @param handler to be called when {@link Counter}s become available.
     * @return registration id for the handler which can be used to remove it.
     */
    public long addAvailableCounterHandler(final AvailableCounterHandler handler)
    {
        return conductor.addAvailableCounterHandler(handler);
    }

    /**
     * Remove a previously added handler to the list be called when {@link Counter}s become available.
     *
     * @param registrationId to be removed which was returned from add method.
     * @return true if found and removed otherwise false.
     */
    public boolean removeAvailableCounterHandler(final long registrationId)
    {
        return conductor.removeAvailableCounterHandler(registrationId);
    }

    /**
     * Remove a previously added handler to the list be called when {@link Counter}s become available.
     *
     * @param handler to be removed.
     * @return true if found and removed otherwise false.
     * @deprecated please use {@link #removeAvailableCounterHandler(long)}.
     */
    @Deprecated
    public boolean removeAvailableCounterHandler(final AvailableCounterHandler handler)
    {
        return conductor.removeAvailableCounterHandler(handler);
    }

    /**
     * Add a handler to the list be called when {@link Counter}s become unavailable.
     *
     * @param handler to be called when {@link Counter}s become unavailable.
     * @return registration id for the handler which can be used to remove it.
     */
    public long addUnavailableCounterHandler(final UnavailableCounterHandler handler)
    {
        return conductor.addUnavailableCounterHandler(handler);
    }

    /**
     * Remove a previously added handler to the list be called when {@link Counter}s become unavailable.
     *
     * @param registrationId to be removed which was returned from add method.
     * @return true if found and removed otherwise false.
     */
    public boolean removeUnavailableCounterHandler(final long registrationId)
    {
        return conductor.removeUnavailableCounterHandler(registrationId);
    }

    /**
     * Remove a previously added handler to the list be called when {@link Counter}s become unavailable.
     *
     * @param handler to be removed.
     * @return true if found and removed otherwise false.
     * @deprecated please use {@link #removeUnavailableCounterHandler(long)}.
     */
    @Deprecated
    public boolean removeUnavailableCounterHandler(final UnavailableCounterHandler handler)
    {
        return conductor.removeUnavailableCounterHandler(handler);
    }

    /**
     * Add a handler to the list be called when the Aeron client is closed.
     *
     * @param handler to be called when the Aeron client is closed.
     * @return registration id for the handler which can be used to remove it.
     */
    public long addCloseHandler(final Runnable handler)
    {
        return conductor.addCloseHandler(handler);
    }

    /**
     * Remove a previously added handler to the list be called when the Aeron client is closed.
     *
     * @param registrationId of the handler from when it was added.
     * @return true if found and removed otherwise false.
     */
    public boolean removeCloseHandler(final long registrationId)
    {
        return conductor.removeCloseHandler(registrationId);
    }

    /**
     * Remove a previously added handler to the list be called when the Aeron client is closed.
     *
     * @param handler to be removed.
     * @return true if found and removed otherwise false.
     * @deprecated please use {@link #removeCloseHandler(long)}.
     */
    @Deprecated
    public boolean removeCloseHandler(final Runnable handler)
    {
        return conductor.removeCloseHandler(handler);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "Aeron{" +
            "isClosed=" + isClosed +
            ", clientId=" + clientId +
            '}';
    }

    /**
     * Called by the {@link ClientConductor} if the client should be terminated due to timeout.
     */
    void internalClose()
    {
        isClosed = true;
    }

    /**
     * Configuration options for the {@link Aeron} client.
     */
    public static class Configuration
    {
        /**
         * Duration in milliseconds for which the client conductor will sleep between duty cycles.
         */
        static final long IDLE_SLEEP_DEFAULT_MS = 16;

        /**
         * Duration in milliseconds for which the client will sleep when awaiting a response from the driver.
         */
        static final long AWAITING_IDLE_SLEEP_MS = 1;

        /**
         * Duration to sleep when idle.
         */
        @Config(expectedCDefaultFieldName = "AERON_CONTEXT_IDLE_SLEEP_DURATION_NS_DEFAULT")
        public static final String IDLE_SLEEP_DURATION_PROP_NAME = "aeron.client.idle.sleep.duration";

        /**
         * Duration in nanoseconds for which the client conductor will sleep between work cycles when idle.
         */
        @Config(id = "IDLE_SLEEP_DURATION", defaultType = DefaultType.LONG, defaultLong = 16_000_000)
        static final long IDLE_SLEEP_DEFAULT_NS = TimeUnit.MILLISECONDS.toNanos(IDLE_SLEEP_DEFAULT_MS);

        /**
         * Default interval between sending keepalive control messages to the driver.
         */
        static final long KEEPALIVE_INTERVAL_NS = TimeUnit.MILLISECONDS.toNanos(500);

        /**
         * Duration to wait while lingering an entity such as an {@link Image} before deleting underlying resources
         * such as memory mapped files.
         */
        @Config(expectedCDefaultFieldName = "AERON_CONTEXT_RESOURCE_LINGER_DURATION_NS_DEFAULT")
        public static final String RESOURCE_LINGER_DURATION_PROP_NAME = "aeron.client.resource.linger.duration";

        /**
         * Default duration a resource should linger before deletion.
         */
        @Config(defaultType = DefaultType.LONG, defaultLong = 3_000_000_000L)
        public static final long RESOURCE_LINGER_DURATION_DEFAULT_NS = TimeUnit.SECONDS.toNanos(3);

        /**
         * Duration to linger on close so that publishers subscribers have time to notice closed resources.
         * This value can be set to a few seconds if the application is likely to experience CPU starvation or
         * long GC pauses.
         */
        @Config(existsInC = false)
        public static final String CLOSE_LINGER_DURATION_PROP_NAME = "aeron.client.close.linger.duration";

        /**
         * Default duration to linger on close so that publishers subscribers have time to notice closed resources.
         */
        @Config
        public static final long CLOSE_LINGER_DURATION_DEFAULT_NS = 0;

        /**
         * Should memory-mapped files be pre-touched so that they are already faulted into a process.
         * <p>
         * Pre-touching files can result in it taking longer for resources to become available in
         * return for avoiding later pauses due to page faults.
         */
        @Config(
            expectedCEnvVarFieldName = "AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY_ENV_VAR",
            expectedCEnvVar = "AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY",
            expectedCDefaultFieldName = "AERON_CONTEXT_PRE_TOUCH_MAPPED_MEMORY_DEFAULT")
        public static final String PRE_TOUCH_MAPPED_MEMORY_PROP_NAME = "aeron.pre.touch.mapped.memory";

        /**
         * Default for if a memory-mapped filed should be pre-touched to fault it into a process.
         */
        @Config
        public static final boolean PRE_TOUCH_MAPPED_MEMORY_DEFAULT = false;

        /**
         * System property to name Aeron client. Default to empty string.
         *
         * @since 1.44.0
         */
        @Config(defaultType = DefaultType.STRING, defaultString = "", skipCDefaultValidation = true)
        public static final String CLIENT_NAME_PROP_NAME = "aeron.client.name";

        /**
         * The Default handler for Aeron runtime exceptions.
         * When a {@link DriverTimeoutException} is encountered, this handler will exit the program.
         * <p>
         * The error handler can be overridden by supplying an {@link Context} with a custom handler.
         *
         * @see Context#errorHandler(ErrorHandler)
         */
        public static final ErrorHandler DEFAULT_ERROR_HANDLER =
            (throwable) ->
            {
                synchronized (System.err)
                {
                    System.err.println(System.currentTimeMillis() + " Exception:");
                    throwable.printStackTrace(System.err);
                }

                if (throwable instanceof DriverTimeoutException)
                {
                    System.err.printf("%n***%n*** Media Driver timeout - is it running? exiting client...%n***%n");

                    final Thread t = new Thread(() -> Runtime.getRuntime().exit(-1));

                    t.setName("runtime-exit-runner");
                    t.setDaemon(true);
                    t.start();
                }
            };

        /**
         * Duration in nanoseconds for which the client conductor will sleep between work cycles when idle.
         *
         * @return duration in nanoseconds to wait when idle in client conductor.
         * @see #IDLE_SLEEP_DURATION_PROP_NAME
         */
        @Config
        public static long idleSleepDurationNs()
        {
            return getDurationInNanos(IDLE_SLEEP_DURATION_PROP_NAME, IDLE_SLEEP_DEFAULT_NS);
        }

        /**
         * Duration to wait while lingering an entity such as an {@link Image} before deleting underlying resources
         * such as memory mapped files.
         *
         * @return duration in nanoseconds to wait before deleting an expired resource.
         * @see #RESOURCE_LINGER_DURATION_PROP_NAME
         */
        @Config
        public static long resourceLingerDurationNs()
        {
            return getDurationInNanos(RESOURCE_LINGER_DURATION_PROP_NAME, RESOURCE_LINGER_DURATION_DEFAULT_NS);
        }

        /**
         * Duration to wait while lingering an entity such as an {@link Image} before deleting underlying resources
         * such as memory mapped files.
         *
         * @return duration in nanoseconds to wait before deleting an expired resource.
         * @see #RESOURCE_LINGER_DURATION_PROP_NAME
         */
        @Config
        public static long closeLingerDurationNs()
        {
            return getDurationInNanos(CLOSE_LINGER_DURATION_PROP_NAME, CLOSE_LINGER_DURATION_DEFAULT_NS);
        }

        /**
         * Should memory-mapped files be pre-touched so that they are already faulted into a process.
         *
         * @return true if memory mappings should be pre-touched, otherwise false.
         * @see #PRE_TOUCH_MAPPED_MEMORY_PROP_NAME
         */
        @Config
        public static boolean preTouchMappedMemory()
        {
            final String value = System.getProperty(PRE_TOUCH_MAPPED_MEMORY_PROP_NAME);
            if (null != value)
            {
                return Boolean.parseBoolean(value);
            }

            return PRE_TOUCH_MAPPED_MEMORY_DEFAULT;
        }

        /**
         * Get the configured client name.
         *
         * @return specified client name or empty string if not set.
         * @see #CLIENT_NAME_PROP_NAME
         */
        @Config
        public static String clientName()
        {
            return getProperty(CLIENT_NAME_PROP_NAME, "");
        }

        /**
         * Limit to the number of characters allowed in the client name.
         */
        public static final int MAX_CLIENT_NAME_LENGTH = 100;
    }

    /**
     * Provides a means to override configuration for an {@link Aeron} client via the
     * {@link Aeron#connect(Aeron.Context)} method and its overloads. It gives applications some control over
     * the interactions with the Aeron Media Driver. It can also set up error handling as well as application
     * callbacks for image information from the Media Driver.
     * <p>
     * A number of the properties are for testing and should not be set by end users.
     * <p>
     * <b>Note:</b> Do not reuse instances of the context across different {@link Aeron} clients.
     * <p>
     * The context will be owned by {@link ClientConductor} after a successful
     * {@link Aeron#connect(Context)} and closed via {@link Aeron#close()}.
     */
    public static class Context extends CommonContext
    {
        private long clientId;
        private String clientName = Configuration.clientName();
        private boolean useConductorAgentInvoker = false;
        private boolean preTouchMappedMemory = Configuration.preTouchMappedMemory();
        private AgentInvoker driverAgentInvoker;
        private Lock clientLock;
        private EpochClock epochClock;
        private NanoClock nanoClock;
        private IdleStrategy idleStrategy;
        private IdleStrategy awaitingIdleStrategy;
        private CopyBroadcastReceiver toClientBuffer;
        private RingBuffer toDriverBuffer;
        private DriverProxy driverProxy;
        private ByteBuffer cncByteBuffer;
        private AtomicBuffer cncMetaDataBuffer;
        private LogBuffersFactory logBuffersFactory;
        private ErrorHandler errorHandler;
        private ErrorHandler subscriberErrorHandler;
        private AvailableImageHandler availableImageHandler;
        private UnavailableImageHandler unavailableImageHandler;
        private AvailableCounterHandler availableCounterHandler;
        private UnavailableCounterHandler unavailableCounterHandler;
        private PublicationErrorFrameHandler publicationErrorFrameHandler = PublicationErrorFrameHandler.NO_OP;
        private Runnable closeHandler;
        private long keepAliveIntervalNs = Configuration.KEEPALIVE_INTERVAL_NS;
        private long interServiceTimeoutNs;
        private long idleSleepDurationNs = Configuration.idleSleepDurationNs();
        private long resourceLingerDurationNs = Configuration.resourceLingerDurationNs();
        private long closeLingerDurationNs = Configuration.closeLingerDurationNs();
        private int filePageSize;

        private ThreadFactory threadFactory = Thread::new;

        /**
         * Perform a shallow copy of the object.
         *
         * @return a shallow copy of the object.
         */
        public Context clone()
        {
            return (Context)super.clone();
        }

        /**
         * This is called automatically by {@link Aeron#connect(Aeron.Context)} and its overloads.
         * There is no need to call it from a client application. It is responsible for providing default
         * values for options that are not individually changed through field setters.
         *
         * @return this for a fluent API.
         */
        @SuppressWarnings("checkstyle:methodlength")
        public Context conclude()
        {
            super.conclude();

            if (null == clientLock)
            {
                clientLock = new ReentrantLock();
            }
            else if (clientLock instanceof NoOpLock && !useConductorAgentInvoker)
            {
                throw new ConfigurationException(
                    "Must use Aeron.Context.useConductorAgentInvoker(true) when Aeron.Context.clientLock(...) " +
                    "is using a NoOpLock");
            }

            if (null != driverAgentInvoker && !useConductorAgentInvoker)
            {
                throw new ConfigurationException(
                    "Must use Aeron.Context.useConductorAgentInvoker(true) when Aeron.Context.driverAgentInvoker() " +
                    "is set");
            }

            if (null != clientName && clientName.length() > MAX_CLIENT_NAME_LENGTH)
            {
                throw new ConfigurationException("clientName length must <= " + MAX_CLIENT_NAME_LENGTH);
            }

            if (null == epochClock)
            {
                epochClock = SystemEpochClock.INSTANCE;
            }

            if (null == nanoClock)
            {
                nanoClock = SystemNanoClock.INSTANCE;
            }

            if (idleSleepDurationNs < 0 || idleSleepDurationNs > TimeUnit.SECONDS.toNanos(1))
            {
                throw new ConfigurationException("Invalid idle sleep duration: " + idleSleepDurationNs + "ns");
            }

            if (null == idleStrategy)
            {
                idleStrategy = new SleepingMillisIdleStrategy(TimeUnit.NANOSECONDS.toMillis(idleSleepDurationNs));
            }

            if (null == awaitingIdleStrategy)
            {
                awaitingIdleStrategy = new SleepingMillisIdleStrategy(Configuration.AWAITING_IDLE_SLEEP_MS);
            }

            connectToDriver();
            filePageSize = driverFilePageSize(cncMetaDataBuffer);

            interServiceTimeoutNs = CncFileDescriptor.clientLivenessTimeoutNs(cncMetaDataBuffer);
            if (interServiceTimeoutNs <= keepAliveIntervalNs)
            {
                throw new ConfigurationException("interServiceTimeoutNs=" + interServiceTimeoutNs +
                    " <= keepAliveIntervalNs=" + keepAliveIntervalNs);
            }

            if (null == toDriverBuffer)
            {
                toDriverBuffer = new ManyToOneRingBuffer(
                    CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));
            }

            if (null == toClientBuffer)
            {
                toClientBuffer = new CopyBroadcastReceiver(new BroadcastReceiver(
                    CncFileDescriptor.createToClientsBuffer(cncByteBuffer, cncMetaDataBuffer)),
                    new ExpandableArrayBuffer(CopyBroadcastReceiver.SCRATCH_BUFFER_LENGTH));
            }

            if (countersMetaDataBuffer() == null)
            {
                countersMetaDataBuffer(
                    CncFileDescriptor.createCountersMetaDataBuffer(cncByteBuffer, cncMetaDataBuffer));
            }

            if (countersValuesBuffer() == null)
            {
                countersValuesBuffer(CncFileDescriptor.createCountersValuesBuffer(cncByteBuffer, cncMetaDataBuffer));
            }

            if (null == logBuffersFactory)
            {
                logBuffersFactory = new MappedLogBuffersFactory();
            }

            if (null == errorHandler)
            {
                errorHandler = Configuration.DEFAULT_ERROR_HANDLER;
            }

            if (null == subscriberErrorHandler)
            {
                subscriberErrorHandler = errorHandler;
            }

            if (null == driverProxy)
            {
                clientId = toDriverBuffer.nextCorrelationId();
                driverProxy = new DriverProxy(toDriverBuffer, clientId);
            }

            return this;
        }

        /**
         * Get the client identity that has been allocated for communicating with the media driver.
         *
         * @return the client identity that has been allocated for communicating with the media driver.
         */
        public long clientId()
        {
            return clientId;
        }

        /**
         * Sets the name used to identify this client among other clients connected to the media driver.
         *
         * @param clientName to use.
         * @return this for a fluent API.
         * @see Configuration#CLIENT_NAME_PROP_NAME
         * @since 1.44.0
         */
        public Context clientName(final String clientName)
        {
            this.clientName = Strings.isEmpty(clientName) ? "" : clientName;
            return this;
        }

        /**
         * Returns the name of this client.
         *
         * @return name of this client or empty String if not configured.
         * @see Configuration#CLIENT_NAME_PROP_NAME
         * @since 1.44.0
         */
        public String clientName()
        {
            return clientName;
        }

        /**
         * Should an {@link AgentInvoker} be used for running the {@link ClientConductor} rather than run it on
         * a thread with a {@link AgentRunner}.
         *
         * @param useConductorAgentInvoker use {@link AgentInvoker} be used for running the {@link ClientConductor}?
         * @return this for a fluent API.
         */
        public Context useConductorAgentInvoker(final boolean useConductorAgentInvoker)
        {
            this.useConductorAgentInvoker = useConductorAgentInvoker;
            return this;
        }

        /**
         * Should an {@link AgentInvoker} be used for running the {@link ClientConductor} rather than run it on
         * a thread with a {@link AgentRunner}.
         *
         * @return true if the {@link ClientConductor} will be run with an {@link AgentInvoker} otherwise false.
         */
        public boolean useConductorAgentInvoker()
        {
            return useConductorAgentInvoker;
        }

        /**
         * Should mapped-memory be pre-touched to avoid soft page faults.
         *
         * @param preTouchMappedMemory true if mapped-memory should be pre-touched otherwise false.
         * @return this for a fluent API.
         * @see Configuration#PRE_TOUCH_MAPPED_MEMORY_PROP_NAME
         */
        public Context preTouchMappedMemory(final boolean preTouchMappedMemory)
        {
            this.preTouchMappedMemory = preTouchMappedMemory;
            return this;
        }

        /**
         * Should mapped-memory be pre-touched to avoid soft page faults.
         *
         * @return true if mapped-memory should be pre-touched otherwise false.
         * @see Configuration#PRE_TOUCH_MAPPED_MEMORY_PROP_NAME
         */
        public boolean preTouchMappedMemory()
        {
            return preTouchMappedMemory;
        }

        /**
         * Set the {@link AgentInvoker} for the Media Driver to be used while awaiting a synchronous response.
         * <p>
         * Useful for when running on a low thread count scenario.
         *
         * @param driverAgentInvoker to be invoked while awaiting a response in the client.
         * @return this for a fluent API.
         */
        public Context driverAgentInvoker(final AgentInvoker driverAgentInvoker)
        {
            this.driverAgentInvoker = driverAgentInvoker;
            return this;
        }

        /**
         * Get the {@link AgentInvoker} that is used to run the Media Driver while awaiting a synchronous response.
         *
         * @return the {@link AgentInvoker} that is used for running the Media Driver.
         */
        public AgentInvoker driverAgentInvoker()
        {
            return driverAgentInvoker;
        }

        /**
         * The {@link Lock} that is used to provide mutual exclusion in the Aeron client.
         * <p>
         * If the {@link #useConductorAgentInvoker()} is set and only one thread accesses the client
         * then the lock can be set to {@link NoOpLock} to elide the lock overhead.
         *
         * @param lock that is used to provide mutual exclusion in the Aeron client.
         * @return this for a fluent API.
         */
        public Context clientLock(final Lock lock)
        {
            clientLock = lock;
            return this;
        }

        /**
         * Get the {@link Lock} that is used to provide mutual exclusion in the Aeron client.
         *
         * @return the {@link Lock} that is used to provide mutual exclusion in the Aeron client.
         */
        public Lock clientLock()
        {
            return clientLock;
        }

        /**
         * Set the {@link EpochClock} to be used for tracking wall clock time when interacting with the driver.
         *
         * @param clock {@link EpochClock} to be used for tracking wall clock time when interacting with the driver.
         * @return this for a fluent API.
         */
        public Context epochClock(final EpochClock clock)
        {
            this.epochClock = clock;
            return this;
        }

        /**
         * Get the {@link EpochClock} used by the client for the epoch time in milliseconds.
         *
         * @return the {@link EpochClock} used by the client for the epoch time in milliseconds.
         */
        public EpochClock epochClock()
        {
            return epochClock;
        }

        /**
         * Set the {@link NanoClock} to be used for tracking high resolution time.
         *
         * @param clock {@link NanoClock} to be used for tracking high resolution time.
         * @return this for a fluent API.
         */
        public Context nanoClock(final NanoClock clock)
        {
            this.nanoClock = clock;
            return this;
        }

        /**
         * Get the {@link NanoClock} to be used for tracking high resolution time.
         *
         * @return the {@link NanoClock} to be used for tracking high resolution time.
         */
        public NanoClock nanoClock()
        {
            return nanoClock;
        }

        /**
         * Provides an {@link IdleStrategy} for the thread responsible for the client duty cycle.
         *
         * @param idleStrategy Thread idle strategy for the client duty cycle.
         * @return this for a fluent API.
         */
        public Context idleStrategy(final IdleStrategy idleStrategy)
        {
            this.idleStrategy = idleStrategy;
            return this;
        }

        /**
         * Get the {@link IdleStrategy} employed by the client for the client duty cycle.
         *
         * @return the {@link IdleStrategy} employed by the client for the client duty cycle.
         */
        public IdleStrategy idleStrategy()
        {
            return idleStrategy;
        }

        /**
         * Provides an {@link IdleStrategy} to be used when awaiting a response from the Media Driver.
         *
         * @param idleStrategy Thread idle strategy for awaiting a response from the Media Driver.
         * @return this for a fluent API.
         */
        public Context awaitingIdleStrategy(final IdleStrategy idleStrategy)
        {
            this.awaitingIdleStrategy = idleStrategy;
            return this;
        }

        /**
         * The {@link IdleStrategy} to be used when awaiting a response from the Media Driver.
         * <p>
         * This can be changed to a {@link BusySpinIdleStrategy} or {@link YieldingIdleStrategy} for lower response
         * time, especially for adding counters or releasing resources, at the expense of CPU usage.
         *
         * @return the {@link IdleStrategy} to be used when awaiting a response from the Media Driver.
         */
        public IdleStrategy awaitingIdleStrategy()
        {
            return awaitingIdleStrategy;
        }

        /**
         * This method is used for testing and debugging.
         *
         * @param toClientBuffer Injected CopyBroadcastReceiver
         * @return this for a fluent API.
         */
        Context toClientBuffer(final CopyBroadcastReceiver toClientBuffer)
        {
            this.toClientBuffer = toClientBuffer;
            return this;
        }

        /**
         * The buffer used for communicating from the media driver to the Aeron client.
         *
         * @return the buffer used for communicating from the media driver to the Aeron client.
         */
        public CopyBroadcastReceiver toClientBuffer()
        {
            return toClientBuffer;
        }

        /**
         * Get the {@link RingBuffer} used for sending commands to the media driver.
         *
         * @return the {@link RingBuffer} used for sending commands to the media driver.
         */
        public RingBuffer toDriverBuffer()
        {
            return toDriverBuffer;
        }

        /**
         * Set the proxy for communicating with the media driver.
         *
         * @param driverProxy for communicating with the media driver.
         * @return this for a fluent API.
         */
        Context driverProxy(final DriverProxy driverProxy)
        {
            this.driverProxy = driverProxy;
            return this;
        }

        /**
         * Get the proxy for communicating with the media driver.
         *
         * @return the proxy for communicating with the media driver.
         */
        public DriverProxy driverProxy()
        {
            return driverProxy;
        }

        /**
         * This method is used for testing and debugging.
         *
         * @param logBuffersFactory Injected LogBuffersFactory
         * @return this for a fluent API.
         */
        Context logBuffersFactory(final LogBuffersFactory logBuffersFactory)
        {
            this.logBuffersFactory = logBuffersFactory;
            return this;
        }

        /**
         * Get the factory for making log buffers.
         *
         * @return the factory for making log buffers.
         */
        LogBuffersFactory logBuffersFactory()
        {
            return logBuffersFactory;
        }

        /**
         * Handle Aeron exceptions in a callback method. The default behavior is defined by
         * {@link Configuration#DEFAULT_ERROR_HANDLER}. This is the error handler which will be used if an error occurs
         * during the callback for poll operations such as {@link Subscription#poll(FragmentHandler, int)}.
         * <p>
         * The error handler can be reset after {@link Aeron#connect()} and the latest version will always be used
         * so that the bootstrapping process can be performed such as replacing the default one with a
         * {@link CountedErrorHandler}.
         *
         * @param errorHandler Method to handle objects of type Throwable.
         * @return this for a fluent API.
         * @see io.aeron.exceptions.DriverTimeoutException
         * @see io.aeron.exceptions.RegistrationException
         */
        public Context errorHandler(final ErrorHandler errorHandler)
        {
            this.errorHandler = errorHandler;
            return this;
        }

        /**
         * Get the error handler that will be called for errors reported back from the media driver or during poll
         * operations.
         *
         * @return the error handler that will be called for errors reported back from the media driver or poll
         * operations.
         */
        public ErrorHandler errorHandler()
        {
            return errorHandler;
        }

        /**
         * The error handler which will be used if an error occurs during the callback for poll operations such as
         * {@link Subscription#poll(FragmentHandler, int)}. The default will be {@link #errorHandler()} if not set.
         *
         * @param errorHandler Method to handle objects of type Throwable.
         * @return this for a fluent API.
         * @see io.aeron.exceptions.DriverTimeoutException
         * @see io.aeron.exceptions.RegistrationException
         */
        public Context subscriberErrorHandler(final ErrorHandler errorHandler)
        {
            this.subscriberErrorHandler = errorHandler;
            return this;
        }

        /**
         * This is the error handler which will be used if an error occurs during the callback for poll operations
         * such as {@link Subscription#poll(FragmentHandler, int)}. The default will be {@link #errorHandler()} if not
         * set. To have {@link Subscription#poll(FragmentHandler, int)} not delegate then set with
         * {@link RethrowingErrorHandler}.
         *
         * @return the error handler that will be called for errors reported back from the media driver.
         */
        public ErrorHandler subscriberErrorHandler()
        {
            return subscriberErrorHandler;
        }

        /**
         * Set up a default callback for when an {@link Image} is available.
         *
         * @param handler Callback method for handling available image notifications.
         * @return this for a fluent API.
         */
        public Context availableImageHandler(final AvailableImageHandler handler)
        {
            this.availableImageHandler = handler;
            return this;
        }

        /**
         * Get the default callback handler for notifying when {@link Image}s become available.
         *
         * @return the callback handler for notifying when {@link Image}s become available.
         */
        public AvailableImageHandler availableImageHandler()
        {
            return availableImageHandler;
        }

        /**
         * Set up a default callback for when an {@link Image} is unavailable.
         *
         * @param handler Callback method for handling unavailable image notifications.
         * @return this for a fluent API.
         */
        public Context unavailableImageHandler(final UnavailableImageHandler handler)
        {
            this.unavailableImageHandler = handler;
            return this;
        }

        /**
         * Get the callback handler for when an {@link Image} is unavailable.
         *
         * @return the callback handler for when an {@link Image} is unavailable.
         */
        public UnavailableImageHandler unavailableImageHandler()
        {
            return unavailableImageHandler;
        }

        /**
         * Set up a callback for when a counter is available. This will be added to the list first before
         * additional handler are added with {@link Aeron#addAvailableCounterHandler(AvailableCounterHandler)}.
         *
         * @param handler to be called for handling available counter notifications.
         * @return this for a fluent API.
         */
        public Context availableCounterHandler(final AvailableCounterHandler handler)
        {
            this.availableCounterHandler = handler;
            return this;
        }

        /**
         * Get the callback handler for when a counter is available.
         *
         * @return the callback handler for when a counter is available.
         */
        public AvailableCounterHandler availableCounterHandler()
        {
            return availableCounterHandler;
        }

        /**
         * Set up a callback for when a counter is unavailable. This will be added to the list first before
         * additional handler are added with {@link Aeron#addUnavailableCounterHandler(UnavailableCounterHandler)}.
         *
         * @param handler to be called for handling unavailable counter notifications.
         * @return this for a fluent API.
         */
        public Context unavailableCounterHandler(final UnavailableCounterHandler handler)
        {
            this.unavailableCounterHandler = handler;
            return this;
        }

        /**
         * Get the callback handler for when a counter is unavailable.
         *
         * @return the callback handler for when a counter is unavailable.
         */
        public UnavailableCounterHandler unavailableCounterHandler()
        {
            return unavailableCounterHandler;
        }

        /**
         * Set a {@link Runnable} that is called when the client is closed by timeout or normal means.
         * <p>
         * It is not safe to call any API functions from any threads after this hook is called. In addition, any
         * in flight calls may still cause faults. Thus treating this as a hard error and terminate the process in
         * this hook as soon as possible is recommended.
         *
         * @param handler that is called when the client is closed.
         * @return this for a fluent API.
         */
        public Context closeHandler(final Runnable handler)
        {
            this.closeHandler = handler;
            return this;
        }

        /**
         * Get the {@link Runnable} that is called when the client is closed by timeout or normal means.
         *
         * @return the {@link Runnable} that is called when the client is closed.
         */
        public Runnable closeHandler()
        {
            return closeHandler;
        }

        /**
         * Set the interval in nanoseconds for which the client will perform keep-alive operations.
         *
         * @param value the interval in nanoseconds for which the client will perform keep-alive operations.
         * @return this for a fluent API.
         */
        public Context keepAliveIntervalNs(final long value)
        {
            keepAliveIntervalNs = value;
            return this;
        }

        /**
         * Get the interval in nanoseconds for which the client will perform keep-alive operations.
         *
         * @return the interval in nanoseconds for which the client will perform keep-alive operations.
         */
        public long keepAliveIntervalNs()
        {
            return keepAliveIntervalNs;
        }

        /**
         * Set the amount of time, in milliseconds, that this client will wait until it determines the
         * Media Driver is unavailable. When this happens a
         * {@link io.aeron.exceptions.DriverTimeoutException} will be generated for the error handler.
         *
         * @param value Number of milliseconds.
         * @return this for a fluent API.
         * @see #errorHandler(ErrorHandler)
         */
        public Context driverTimeoutMs(final long value)
        {
            super.driverTimeoutMs(value);
            return this;
        }

        /**
         * Set the timeout between service calls the to {@link ClientConductor} duty cycles in nanoseconds.
         * <p>
         * <b>Note:</b> the method is used for testing only.
         *
         * @param interServiceTimeout the timeout (ns) between service calls the to {@link ClientConductor} duty cycle.
         * @return this for a fluent API.
         */
        Context interServiceTimeoutNs(final long interServiceTimeout)
        {
            this.interServiceTimeoutNs = interServiceTimeout;
            return this;
        }

        /**
         * Return the timeout between service calls to the duty cycle for the client.
         * <p>
         * When exceeded, {@link #errorHandler()} will be called and the active {@link Publication}s, {@link Image}s,
         * and {@link Counter}s will be closed.
         * <p>
         * This value is controlled by the driver and included in the CnC file. It can be configured by adjusting
         * the {@code aeron.client.liveness.timeout} property set on the media driver.
         *
         * @return the timeout in nanoseconds between service calls as an allowed maximum.
         */
        public long interServiceTimeoutNs()
        {
            return CommonContext.checkDebugTimeout(interServiceTimeoutNs, TimeUnit.NANOSECONDS);
        }

        /**
         * Duration to sleep when conductor is idle.
         *
         * @param idleSleepDurationNs to sleep when conductor is idle.
         * @return this for a fluent API.
         * @see Configuration#IDLE_SLEEP_DURATION_PROP_NAME
         */
        public Context idleSleepDurationNs(final long idleSleepDurationNs)
        {
            this.idleSleepDurationNs = idleSleepDurationNs;
            return this;
        }

        /**
         * Duration to sleep when conductor is idle.
         *
         * @return duration in nanoseconds to sleep when conductor is idle.
         * @see Configuration#IDLE_SLEEP_DURATION_PROP_NAME
         */
        public long idleSleepDurationNs()
        {
            return idleSleepDurationNs;
        }

        /**
         * Duration to wait while lingering an entity such as an {@link Image} before deleting underlying resources
         * such as memory mapped files.
         *
         * @param resourceLingerDurationNs to wait before deleting an expired resource.
         * @return this for a fluent API.
         * @see Configuration#RESOURCE_LINGER_DURATION_PROP_NAME
         */
        public Context resourceLingerDurationNs(final long resourceLingerDurationNs)
        {
            this.resourceLingerDurationNs = resourceLingerDurationNs;
            return this;
        }

        /**
         * Duration to wait while lingering an entity such as an {@link Image} before deleting underlying resources
         * such as memory mapped files.
         *
         * @return duration in nanoseconds to wait before deleting an expired resource.
         * @see Configuration#RESOURCE_LINGER_DURATION_PROP_NAME
         */
        public long resourceLingerDurationNs()
        {
            return resourceLingerDurationNs;
        }

        /**
         * Duration to linger on closing to allow publishers and subscribers time to notice closed resources.
         * <p>
         * This value can be increased from the default to a few seconds to better cope with long GC pauses
         * or resource starved environments. Issues could manifest as seg faults using files after they have
         * been unmapped from publishers or subscribers not noticing the close in a timely fashion.
         *
         * @param closeLingerDurationNs to wait before deleting resources when closing.
         * @return this for a fluent API.
         * @see Configuration#CLOSE_LINGER_DURATION_PROP_NAME
         */
        public Context closeLingerDurationNs(final long closeLingerDurationNs)
        {
            this.closeLingerDurationNs = closeLingerDurationNs;
            return this;
        }

        /**
         * Duration to linger on closing to allow publishers and subscribers time to notice closed resources.
         * <p>
         * This value can be increased from the default to a few seconds to better cope with long GC pauses
         * or resource starved environments. Issues could manifest as seg faults using files after they have
         * been unmapped from publishers or subscribers not noticing the close in a timely fashion.
         *
         * @return duration in nanoseconds to wait before deleting resources when closing.
         * @see Configuration#CLOSE_LINGER_DURATION_PROP_NAME
         */
        public long closeLingerDurationNs()
        {
            return closeLingerDurationNs;
        }

        /**
         * {@inheritDoc}
         */
        public Context aeronDirectoryName(final String dirName)
        {
            super.aeronDirectoryName(dirName);
            return this;
        }

        /**
         * Specify the thread factory to use when starting the conductor thread.
         *
         * @param threadFactory thread factory to construct the thread.
         * @return this for a fluent API.
         */
        public Context threadFactory(final ThreadFactory threadFactory)
        {
            this.threadFactory = threadFactory;
            return this;
        }

        /**
         * The thread factory to be used to construct the conductor thread.
         *
         * @return the specified thread factory or {@link Thread#Thread(Runnable)} if none is provided.
         */
        public ThreadFactory threadFactory()
        {
            return threadFactory;
        }

        /**
         * Set the handler to receive error frames that have been received by the local driver for publications added by
         * this client.
         *
         * @param publicationErrorFrameHandler to be called back when an error frame is received.
         * @return this for a fluent API.
         * @since 1.47.0
         */
        public Context publicationErrorFrameHandler(
            final PublicationErrorFrameHandler publicationErrorFrameHandler)
        {
            this.publicationErrorFrameHandler = Objects.requireNonNull(publicationErrorFrameHandler);
            return this;
        }

        /**
         * Get the handler to receive error frames that have been received by the local driver for publications added by
         * this client.
         *
         * @return the {@link PublicationErrorFrameHandler} to call back on to.
         * @since 1.47.0
         */
        public PublicationErrorFrameHandler publicationErrorFrameHandler()
        {
            return this.publicationErrorFrameHandler;
        }

        /**
         * Get file page size from running media driver.
         *
         * @return file page size or zero (if not connected to the media driver).
         * @since 1.48.0
         */
        public int filePageSize()
        {
            return filePageSize;
        }

        /**
         * Clean up all resources that the client uses to communicate with the Media Driver.
         */
        public void close()
        {
            BufferUtil.free(cncByteBuffer);
            this.cncByteBuffer = null;
            super.close();
        }

        /**
         * {@inheritDoc}
         */
        public String toString()
        {
            return "Aeron.Context" +
                "\n{" +
                "\n    isConcluded=" + isConcluded() +
                "\n    aeronDirectory=" + aeronDirectory() +
                "\n    aeronDirectoryName='" + aeronDirectoryName() + '\'' +
                "\n    cncFile=" + cncFile() +
                "\n    countersMetaDataBuffer=" + countersMetaDataBuffer() +
                "\n    countersValuesBuffer=" + countersValuesBuffer() +
                "\n    driverTimeoutMs=" + driverTimeoutMs() +
                "\n    clientId=" + clientId +
                "\n    clientName=" + clientName +
                "\n    useConductorAgentInvoker=" + useConductorAgentInvoker +
                "\n    preTouchMappedMemory=" + preTouchMappedMemory +
                "\n    driverAgentInvoker=" + driverAgentInvoker +
                "\n    clientLock=" + clientLock +
                "\n    epochClock=" + epochClock +
                "\n    nanoClock=" + nanoClock +
                "\n    idleStrategy=" + idleStrategy +
                "\n    awaitingIdleStrategy=" + awaitingIdleStrategy +
                "\n    toClientBuffer=" + toClientBuffer +
                "\n    toDriverBuffer=" + toDriverBuffer +
                "\n    driverProxy=" + driverProxy +
                "\n    cncByteBuffer=" + cncByteBuffer +
                "\n    cncMetaDataBuffer=" + cncMetaDataBuffer +
                "\n    logBuffersFactory=" + logBuffersFactory +
                "\n    errorHandler=" + errorHandler +
                "\n    subscriberErrorHandler=" + subscriberErrorHandler +
                "\n    availableImageHandler=" + availableImageHandler +
                "\n    unavailableImageHandler=" + unavailableImageHandler +
                "\n    availableCounterHandler=" + availableCounterHandler +
                "\n    unavailableCounterHandler=" + unavailableCounterHandler +
                "\n    closeHandler=" + closeHandler +
                "\n    keepAliveIntervalNs=" + keepAliveIntervalNs +
                "\n    interServiceTimeoutNs=" + interServiceTimeoutNs +
                "\n    resourceLingerDurationNs=" + resourceLingerDurationNs +
                "\n    closeLingerDurationNs=" + closeLingerDurationNs +
                "\n    threadFactory=" + threadFactory +
                "\n}";
        }

        private void connectToDriver()
        {
            final EpochClock clock = epochClock;
            final long deadlineMs = clock.time() + driverTimeoutMs();
            final File cncFile = cncFile();

            while (null == toDriverBuffer)
            {
                cncMetaDataBuffer = awaitCncFileCreation(cncFile, clock, deadlineMs);
                cncByteBuffer = cncMetaDataBuffer.byteBuffer();

                if (!CncFileDescriptor.isCncFileLengthSufficient(cncMetaDataBuffer, cncByteBuffer.capacity()))
                {
                    BufferUtil.free(cncByteBuffer);
                    cncByteBuffer = null;
                    cncMetaDataBuffer = null;

                    sleep(Configuration.AWAITING_IDLE_SLEEP_MS);
                    continue;
                }

                final ManyToOneRingBuffer ringBuffer = new ManyToOneRingBuffer(
                    CncFileDescriptor.createToDriverBuffer(cncByteBuffer, cncMetaDataBuffer));

                while (0 == ringBuffer.consumerHeartbeatTime())
                {
                    if (clock.time() > deadlineMs)
                    {
                        throw new DriverTimeoutException("no driver heartbeat detected");
                    }

                    sleep(Configuration.AWAITING_IDLE_SLEEP_MS);
                }

                final long timeMs = clock.time();
                if (ringBuffer.consumerHeartbeatTime() < (timeMs - driverTimeoutMs()))
                {
                    if (timeMs > deadlineMs)
                    {
                        throw new DriverTimeoutException("no driver heartbeat detected");
                    }

                    BufferUtil.free(cncByteBuffer);
                    cncByteBuffer = null;
                    cncMetaDataBuffer = null;

                    sleep(100);
                    continue;
                }

                toDriverBuffer = ringBuffer;
            }
        }
    }
}
