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
package io.aeron.cluster.bridge.resilience;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Graceful shutdown coordinator for Aeron Bridge components.
 * <p>
 * Features:
 * <ul>
 *   <li>Phased shutdown with configurable timeouts</li>
 *   <li>Component registration with priorities</li>
 *   <li>JVM shutdown hook integration</li>
 *   <li>Drain period for in-flight messages</li>
 * </ul>
 */
public final class GracefulShutdown
{
    /**
     * Shutdown phase.
     */
    public enum Phase
    {
        /** Stop accepting new work */
        STOP_ACCEPTING,
        /** Wait for in-flight work to complete */
        DRAIN,
        /** Close resources */
        CLOSE,
        /** Final cleanup */
        CLEANUP
    }

    /**
     * Component that can be shutdown gracefully.
     */
    public interface ShutdownAware
    {
        /**
         * Get component name for logging.
         *
         * @return component name
         */
        String name();

        /**
         * Called during shutdown.
         *
         * @param phase current shutdown phase
         * @return true if phase completed successfully
         */
        boolean onShutdown(Phase phase);
    }

    /**
     * Callback for shutdown events.
     */
    @FunctionalInterface
    public interface ShutdownListener
    {
        void onShutdown(Phase phase, String componentName, boolean success);
    }

    private static final int DEFAULT_DRAIN_TIMEOUT_MS = 5000;
    private static final int DEFAULT_CLOSE_TIMEOUT_MS = 10000;

    private final String name;
    private final List<PrioritizedComponent> components = Collections.synchronizedList(new ArrayList<>());
    private final AtomicBoolean shutdownInitiated = new AtomicBoolean(false);
    private final AtomicBoolean shutdownComplete = new AtomicBoolean(false);
    private final CountDownLatch completionLatch = new CountDownLatch(1);
    private final AtomicInteger activePhase = new AtomicInteger(-1);

    private volatile ShutdownListener shutdownListener;
    private volatile Thread shutdownThread;
    private volatile boolean installShutdownHook = true;

    private int drainTimeoutMs = DEFAULT_DRAIN_TIMEOUT_MS;
    private int closeTimeoutMs = DEFAULT_CLOSE_TIMEOUT_MS;

    /**
     * Create a new shutdown coordinator.
     *
     * @param name identifier for logging
     */
    public GracefulShutdown(final String name)
    {
        this.name = name;
    }

    /**
     * Configure drain timeout.
     *
     * @param timeoutMs timeout in milliseconds
     * @return this for chaining
     */
    public GracefulShutdown drainTimeout(final int timeoutMs)
    {
        this.drainTimeoutMs = timeoutMs;
        return this;
    }

    /**
     * Configure close timeout.
     *
     * @param timeoutMs timeout in milliseconds
     * @return this for chaining
     */
    public GracefulShutdown closeTimeout(final int timeoutMs)
    {
        this.closeTimeoutMs = timeoutMs;
        return this;
    }

    /**
     * Disable JVM shutdown hook.
     *
     * @return this for chaining
     */
    public GracefulShutdown disableShutdownHook()
    {
        this.installShutdownHook = false;
        return this;
    }

    /**
     * Set shutdown listener.
     *
     * @param listener the listener
     * @return this for chaining
     */
    public GracefulShutdown onShutdown(final ShutdownListener listener)
    {
        this.shutdownListener = listener;
        return this;
    }

    /**
     * Register a component for graceful shutdown.
     *
     * @param component the component
     * @param priority  shutdown priority (lower = earlier shutdown)
     * @return this for chaining
     */
    public GracefulShutdown register(final ShutdownAware component, final int priority)
    {
        components.add(new PrioritizedComponent(component, priority));
        // Sort by priority (ascending)
        components.sort((a, b) -> Integer.compare(a.priority, b.priority));
        return this;
    }

    /**
     * Register a component with default priority (100).
     *
     * @param component the component
     * @return this for chaining
     */
    public GracefulShutdown register(final ShutdownAware component)
    {
        return register(component, 100);
    }

    /**
     * Register a simple AutoCloseable.
     *
     * @param closeable the closeable
     * @param name      name for logging
     * @param priority  shutdown priority
     * @return this for chaining
     */
    public GracefulShutdown register(final AutoCloseable closeable, final String name, final int priority)
    {
        return register(new ShutdownAware()
        {
            @Override
            public String name()
            {
                return name;
            }

            @Override
            public boolean onShutdown(final Phase phase)
            {
                if (phase == Phase.CLOSE)
                {
                    try
                    {
                        closeable.close();
                        return true;
                    }
                    catch (final Exception e)
                    {
                        System.err.println("[" + name + "] Close failed: " + e.getMessage());
                        return false;
                    }
                }
                return true;
            }
        }, priority);
    }

    /**
     * Initialize and install shutdown hook.
     */
    public void initialize()
    {
        if (installShutdownHook)
        {
            Runtime.getRuntime().addShutdownHook(new Thread(this::initiateShutdown, name + "-shutdown-hook"));
        }
        System.out.println("[GracefulShutdown:" + name + "] Initialized with " + components.size() + " components");
    }

    /**
     * Initiate graceful shutdown.
     * <p>
     * This method can be called multiple times safely.
     */
    public void initiateShutdown()
    {
        if (!shutdownInitiated.compareAndSet(false, true))
        {
            System.out.println("[GracefulShutdown:" + name + "] Shutdown already in progress");
            return;
        }

        System.out.println("[GracefulShutdown:" + name + "] Initiating graceful shutdown...");
        shutdownThread = Thread.currentThread();

        try
        {
            // Phase 1: Stop accepting new work
            executePhase(Phase.STOP_ACCEPTING, 0);

            // Phase 2: Drain in-flight work
            executePhase(Phase.DRAIN, drainTimeoutMs);

            // Phase 3: Close resources
            executePhase(Phase.CLOSE, closeTimeoutMs);

            // Phase 4: Final cleanup
            executePhase(Phase.CLEANUP, 0);

            System.out.println("[GracefulShutdown:" + name + "] Shutdown complete");
        }
        catch (final Exception e)
        {
            System.err.println("[GracefulShutdown:" + name + "] Shutdown error: " + e.getMessage());
        }
        finally
        {
            shutdownComplete.set(true);
            completionLatch.countDown();
        }
    }

    /**
     * Initiate shutdown asynchronously.
     *
     * @return thread running shutdown
     */
    public Thread initiateShutdownAsync()
    {
        final Thread thread = new Thread(this::initiateShutdown, name + "-async-shutdown");
        thread.start();
        return thread;
    }

    /**
     * Wait for shutdown to complete.
     *
     * @param timeout timeout value
     * @param unit    timeout unit
     * @return true if shutdown completed within timeout
     * @throws InterruptedException if interrupted
     */
    public boolean awaitShutdown(final long timeout, final TimeUnit unit) throws InterruptedException
    {
        return completionLatch.await(timeout, unit);
    }

    /**
     * Check if shutdown has been initiated.
     *
     * @return true if shutdown started
     */
    public boolean isShutdownInitiated()
    {
        return shutdownInitiated.get();
    }

    /**
     * Check if shutdown is complete.
     *
     * @return true if shutdown finished
     */
    public boolean isShutdownComplete()
    {
        return shutdownComplete.get();
    }

    /**
     * Get current shutdown phase.
     *
     * @return phase ordinal, or -1 if not in shutdown
     */
    public int activePhase()
    {
        return activePhase.get();
    }

    private void executePhase(final Phase phase, final int timeoutMs)
    {
        activePhase.set(phase.ordinal());
        System.out.println("[GracefulShutdown:" + name + "] Entering phase: " + phase);

        final long startTime = System.currentTimeMillis();
        final long deadline = timeoutMs > 0 ? startTime + timeoutMs : Long.MAX_VALUE;

        for (final PrioritizedComponent pc : components)
        {
            if (System.currentTimeMillis() > deadline)
            {
                System.err.println("[GracefulShutdown:" + name + "] Phase " + phase + " timeout exceeded");
                break;
            }

            try
            {
                final boolean success = pc.component.onShutdown(phase);
                notifyListener(phase, pc.component.name(), success);

                if (!success)
                {
                    System.err.println("[GracefulShutdown:" + name + "] Component " +
                        pc.component.name() + " failed phase " + phase);
                }
            }
            catch (final Exception e)
            {
                System.err.println("[GracefulShutdown:" + name + "] Component " +
                    pc.component.name() + " error in phase " + phase + ": " + e.getMessage());
                notifyListener(phase, pc.component.name(), false);
            }
        }

        final long elapsed = System.currentTimeMillis() - startTime;
        System.out.println("[GracefulShutdown:" + name + "] Phase " + phase + " completed in " + elapsed + "ms");
    }

    private void notifyListener(final Phase phase, final String componentName, final boolean success)
    {
        final ShutdownListener listener = shutdownListener;
        if (listener != null)
        {
            try
            {
                listener.onShutdown(phase, componentName, success);
            }
            catch (final Exception e)
            {
                // Ignore listener errors during shutdown
            }
        }
    }

    private static final class PrioritizedComponent
    {
        final ShutdownAware component;
        final int priority;

        PrioritizedComponent(final ShutdownAware component, final int priority)
        {
            this.component = component;
            this.priority = priority;
        }
    }
}
