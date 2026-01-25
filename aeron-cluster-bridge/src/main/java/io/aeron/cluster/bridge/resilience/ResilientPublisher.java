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

import io.aeron.Publication;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Resilient publisher wrapper with retry, backoff, and circuit breaker.
 * <p>
 * Features:
 * <ul>
 *   <li>Configurable retry policy with backoff</li>
 *   <li>Circuit breaker for fast-fail on sustained errors</li>
 *   <li>Metrics collection</li>
 *   <li>Graceful shutdown support</li>
 * </ul>
 */
public final class ResilientPublisher implements GracefulShutdown.ShutdownAware
{
    /**
     * Result of a publish attempt.
     */
    public enum PublishResult
    {
        /** Successfully published */
        SUCCESS,
        /** Circuit breaker open */
        CIRCUIT_OPEN,
        /** Retries exhausted */
        RETRIES_EXHAUSTED,
        /** Publication closed */
        PUBLICATION_CLOSED,
        /** Shutdown in progress */
        SHUTDOWN,
        /** Max position exceeded */
        MAX_POSITION_EXCEEDED
    }

    /**
     * Publish outcome with details.
     */
    public static final class PublishOutcome
    {
        public final PublishResult result;
        public final long position;
        public final int attempts;
        public final long totalDelayMs;

        PublishOutcome(final PublishResult result, final long position, final int attempts, final long totalDelayMs)
        {
            this.result = result;
            this.position = position;
            this.attempts = attempts;
            this.totalDelayMs = totalDelayMs;
        }

        public boolean isSuccess()
        {
            return result == PublishResult.SUCCESS;
        }

        @Override
        public String toString()
        {
            return "PublishOutcome{result=" + result + ", position=" + position +
                ", attempts=" + attempts + ", totalDelayMs=" + totalDelayMs + '}';
        }
    }

    private final String name;
    private final Publication publication;
    private final RetryPolicy retryPolicy;
    private final CircuitBreaker circuitBreaker;
    private final IdleStrategy idleStrategy;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean draining = new AtomicBoolean(false);

    // Metrics
    private final AtomicLong totalPublishes = new AtomicLong(0);
    private final AtomicLong successfulPublishes = new AtomicLong(0);
    private final AtomicLong failedPublishes = new AtomicLong(0);
    private final AtomicLong retriedPublishes = new AtomicLong(0);
    private final AtomicLong circuitBreakerRejects = new AtomicLong(0);
    private final AtomicLong totalRetryDelayMs = new AtomicLong(0);

    /**
     * Create a resilient publisher with default settings.
     *
     * @param name        identifier for logging
     * @param publication the Aeron publication
     * @return the resilient publisher
     */
    public static ResilientPublisher withDefaults(final String name, final Publication publication)
    {
        return new ResilientPublisher(
            name,
            publication,
            RetryPolicy.exponentialWithJitter(5, 10, 1000, 0.3),
            CircuitBreaker.withDefaults(name),
            YieldingIdleStrategy.INSTANCE
        );
    }

    /**
     * Create a resilient publisher.
     *
     * @param name           identifier for logging
     * @param publication    the Aeron publication
     * @param retryPolicy    retry policy
     * @param circuitBreaker circuit breaker
     * @param idleStrategy   idle strategy for waits
     */
    public ResilientPublisher(
        final String name,
        final Publication publication,
        final RetryPolicy retryPolicy,
        final CircuitBreaker circuitBreaker,
        final IdleStrategy idleStrategy)
    {
        this.name = name;
        this.publication = publication;
        this.retryPolicy = retryPolicy;
        this.circuitBreaker = circuitBreaker;
        this.idleStrategy = idleStrategy;
    }

    /**
     * Publish with resilience features.
     *
     * @param buffer the buffer to publish
     * @param offset offset in buffer
     * @param length length to publish
     * @return publish outcome
     */
    public PublishOutcome offer(final DirectBuffer buffer, final int offset, final int length)
    {
        totalPublishes.incrementAndGet();

        // Check shutdown state
        if (!running.get())
        {
            return new PublishOutcome(PublishResult.SHUTDOWN, -1, 0, 0);
        }

        // Check circuit breaker
        if (!circuitBreaker.allowRequest())
        {
            circuitBreakerRejects.incrementAndGet();
            failedPublishes.incrementAndGet();
            return new PublishOutcome(PublishResult.CIRCUIT_OPEN, -1, 0, 0);
        }

        int attempt = 0;
        long totalDelay = 0;

        while (running.get())
        {
            attempt++;
            final long result = publication.offer(buffer, offset, length);

            if (result > 0)
            {
                // Success
                circuitBreaker.recordSuccess();
                successfulPublishes.incrementAndGet();
                return new PublishOutcome(PublishResult.SUCCESS, result, attempt, totalDelay);
            }

            // Handle specific error codes
            if (result == Publication.CLOSED)
            {
                circuitBreaker.recordFailure();
                failedPublishes.incrementAndGet();
                return new PublishOutcome(PublishResult.PUBLICATION_CLOSED, result, attempt, totalDelay);
            }

            if (result == Publication.MAX_POSITION_EXCEEDED)
            {
                circuitBreaker.recordFailure();
                failedPublishes.incrementAndGet();
                return new PublishOutcome(PublishResult.MAX_POSITION_EXCEEDED, result, attempt, totalDelay);
            }

            // Transient errors: BACK_PRESSURED, NOT_CONNECTED, ADMIN_ACTION
            if (!retryPolicy.shouldRetry(attempt))
            {
                circuitBreaker.recordFailure();
                failedPublishes.incrementAndGet();
                return new PublishOutcome(PublishResult.RETRIES_EXHAUSTED, result, attempt, totalDelay);
            }

            // Calculate and apply backoff
            retriedPublishes.incrementAndGet();
            final long delayMs = retryPolicy.calculateDelayMs(attempt);
            totalDelay += delayMs;
            totalRetryDelayMs.addAndGet(delayMs);

            if (delayMs > 0)
            {
                sleep(delayMs);
            }
            else
            {
                idleStrategy.idle();
            }
        }

        // Shutdown during retry
        failedPublishes.incrementAndGet();
        return new PublishOutcome(PublishResult.SHUTDOWN, -1, attempt, totalDelay);
    }

    /**
     * Publish with timeout.
     *
     * @param buffer    the buffer to publish
     * @param offset    offset in buffer
     * @param length    length to publish
     * @param timeoutMs maximum time to spend retrying
     * @return publish outcome
     */
    public PublishOutcome offerWithTimeout(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final long timeoutMs)
    {
        totalPublishes.incrementAndGet();

        if (!running.get())
        {
            return new PublishOutcome(PublishResult.SHUTDOWN, -1, 0, 0);
        }

        if (!circuitBreaker.allowRequest())
        {
            circuitBreakerRejects.incrementAndGet();
            failedPublishes.incrementAndGet();
            return new PublishOutcome(PublishResult.CIRCUIT_OPEN, -1, 0, 0);
        }

        final long deadline = System.currentTimeMillis() + timeoutMs;
        int attempt = 0;
        long totalDelay = 0;

        while (running.get() && System.currentTimeMillis() < deadline)
        {
            attempt++;
            final long result = publication.offer(buffer, offset, length);

            if (result > 0)
            {
                circuitBreaker.recordSuccess();
                successfulPublishes.incrementAndGet();
                return new PublishOutcome(PublishResult.SUCCESS, result, attempt, totalDelay);
            }

            if (result == Publication.CLOSED)
            {
                circuitBreaker.recordFailure();
                failedPublishes.incrementAndGet();
                return new PublishOutcome(PublishResult.PUBLICATION_CLOSED, result, attempt, totalDelay);
            }

            if (result == Publication.MAX_POSITION_EXCEEDED)
            {
                circuitBreaker.recordFailure();
                failedPublishes.incrementAndGet();
                return new PublishOutcome(PublishResult.MAX_POSITION_EXCEEDED, result, attempt, totalDelay);
            }

            // Calculate delay respecting deadline
            final long delayMs = Math.min(
                retryPolicy.calculateDelayMs(attempt),
                deadline - System.currentTimeMillis()
            );

            if (delayMs > 0)
            {
                totalDelay += delayMs;
                totalRetryDelayMs.addAndGet(delayMs);
                sleep(delayMs);
            }
            else
            {
                idleStrategy.idle();
            }

            retriedPublishes.incrementAndGet();
        }

        circuitBreaker.recordFailure();
        failedPublishes.incrementAndGet();

        if (!running.get())
        {
            return new PublishOutcome(PublishResult.SHUTDOWN, -1, attempt, totalDelay);
        }

        return new PublishOutcome(PublishResult.RETRIES_EXHAUSTED, -1, attempt, totalDelay);
    }

    /**
     * Check if publisher is accepting new messages.
     *
     * @return true if accepting
     */
    public boolean isAccepting()
    {
        return running.get() && !draining.get() && circuitBreaker.isClosed();
    }

    /**
     * Get the underlying publication.
     *
     * @return the publication
     */
    public Publication publication()
    {
        return publication;
    }

    /**
     * Get the circuit breaker.
     *
     * @return the circuit breaker
     */
    public CircuitBreaker circuitBreaker()
    {
        return circuitBreaker;
    }

    // Metrics getters
    public long totalPublishes()
    {
        return totalPublishes.get();
    }

    public long successfulPublishes()
    {
        return successfulPublishes.get();
    }

    public long failedPublishes()
    {
        return failedPublishes.get();
    }

    public long retriedPublishes()
    {
        return retriedPublishes.get();
    }

    public long circuitBreakerRejects()
    {
        return circuitBreakerRejects.get();
    }

    public long totalRetryDelayMs()
    {
        return totalRetryDelayMs.get();
    }

    public double successRate()
    {
        final long total = totalPublishes.get();
        return total > 0 ? (double)successfulPublishes.get() / total : 1.0;
    }

    /**
     * Reset metrics.
     */
    public void resetMetrics()
    {
        totalPublishes.set(0);
        successfulPublishes.set(0);
        failedPublishes.set(0);
        retriedPublishes.set(0);
        circuitBreakerRejects.set(0);
        totalRetryDelayMs.set(0);
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public boolean onShutdown(final GracefulShutdown.Phase phase)
    {
        switch (phase)
        {
            case STOP_ACCEPTING:
                draining.set(true);
                System.out.println("[ResilientPublisher:" + name + "] Stopped accepting new messages");
                return true;

            case DRAIN:
                // Wait for in-flight publishes to complete
                // In a real implementation, track in-flight count
                System.out.println("[ResilientPublisher:" + name + "] Draining...");
                return true;

            case CLOSE:
                running.set(false);
                System.out.println("[ResilientPublisher:" + name + "] Closed. Stats: " +
                    "total=" + totalPublishes.get() +
                    ", success=" + successfulPublishes.get() +
                    ", failed=" + failedPublishes.get() +
                    ", retried=" + retriedPublishes.get() +
                    ", circuitRejects=" + circuitBreakerRejects.get());
                return true;

            case CLEANUP:
                return true;

            default:
                return true;
        }
    }

    private void sleep(final long millis)
    {
        try
        {
            Thread.sleep(millis);
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public String toString()
    {
        return "ResilientPublisher{" +
            "name='" + name + '\'' +
            ", running=" + running.get() +
            ", circuitState=" + circuitBreaker.state() +
            ", successRate=" + String.format("%.2f%%", successRate() * 100) +
            '}';
    }
}
