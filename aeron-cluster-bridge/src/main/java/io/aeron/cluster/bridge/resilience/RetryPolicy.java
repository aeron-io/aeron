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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Configurable retry policy with multiple backoff strategies.
 * <p>
 * Supports:
 * <ul>
 *   <li>Fixed delay retry</li>
 *   <li>Exponential backoff</li>
 *   <li>Exponential backoff with jitter</li>
 *   <li>Linear backoff</li>
 * </ul>
 */
public final class RetryPolicy
{
    /**
     * Backoff strategy types.
     */
    public enum BackoffStrategy
    {
        /** Fixed delay between retries */
        FIXED,
        /** Exponential increase: delay * 2^attempt */
        EXPONENTIAL,
        /** Exponential with random jitter to prevent thundering herd */
        EXPONENTIAL_WITH_JITTER,
        /** Linear increase: delay * attempt */
        LINEAR
    }

    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;
    private final BackoffStrategy strategy;
    private final double jitterFactor;

    private RetryPolicy(
        final int maxAttempts,
        final long initialDelayMs,
        final long maxDelayMs,
        final BackoffStrategy strategy,
        final double jitterFactor)
    {
        this.maxAttempts = maxAttempts;
        this.initialDelayMs = initialDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.strategy = strategy;
        this.jitterFactor = jitterFactor;
    }

    /**
     * Create a fixed delay retry policy.
     *
     * @param maxAttempts maximum retry attempts
     * @param delayMs     delay between retries in milliseconds
     * @return the retry policy
     */
    public static RetryPolicy fixed(final int maxAttempts, final long delayMs)
    {
        return new RetryPolicy(maxAttempts, delayMs, delayMs, BackoffStrategy.FIXED, 0.0);
    }

    /**
     * Create an exponential backoff retry policy.
     *
     * @param maxAttempts    maximum retry attempts
     * @param initialDelayMs initial delay in milliseconds
     * @param maxDelayMs     maximum delay cap in milliseconds
     * @return the retry policy
     */
    public static RetryPolicy exponential(final int maxAttempts, final long initialDelayMs, final long maxDelayMs)
    {
        return new RetryPolicy(maxAttempts, initialDelayMs, maxDelayMs, BackoffStrategy.EXPONENTIAL, 0.0);
    }

    /**
     * Create an exponential backoff with jitter retry policy.
     * Jitter helps prevent thundering herd problems.
     *
     * @param maxAttempts    maximum retry attempts
     * @param initialDelayMs initial delay in milliseconds
     * @param maxDelayMs     maximum delay cap in milliseconds
     * @param jitterFactor   jitter factor (0.0 to 1.0), e.g., 0.5 = ±50% randomization
     * @return the retry policy
     */
    public static RetryPolicy exponentialWithJitter(
        final int maxAttempts,
        final long initialDelayMs,
        final long maxDelayMs,
        final double jitterFactor)
    {
        return new RetryPolicy(maxAttempts, initialDelayMs, maxDelayMs, BackoffStrategy.EXPONENTIAL_WITH_JITTER, jitterFactor);
    }

    /**
     * Create a linear backoff retry policy.
     *
     * @param maxAttempts    maximum retry attempts
     * @param initialDelayMs initial delay in milliseconds
     * @param maxDelayMs     maximum delay cap in milliseconds
     * @return the retry policy
     */
    public static RetryPolicy linear(final int maxAttempts, final long initialDelayMs, final long maxDelayMs)
    {
        return new RetryPolicy(maxAttempts, initialDelayMs, maxDelayMs, BackoffStrategy.LINEAR, 0.0);
    }

    /**
     * Calculate the delay for the given attempt number.
     *
     * @param attempt the attempt number (1-based)
     * @return delay in milliseconds
     */
    public long calculateDelayMs(final int attempt)
    {
        if (attempt <= 0)
        {
            return 0;
        }

        long delay;

        switch (strategy)
        {
            case FIXED:
                delay = initialDelayMs;
                break;

            case EXPONENTIAL:
                delay = initialDelayMs * (1L << Math.min(attempt - 1, 30));
                break;

            case EXPONENTIAL_WITH_JITTER:
                delay = initialDelayMs * (1L << Math.min(attempt - 1, 30));
                final double jitter = 1.0 + (ThreadLocalRandom.current().nextDouble() * 2 - 1) * jitterFactor;
                delay = (long)(delay * jitter);
                break;

            case LINEAR:
                delay = initialDelayMs * attempt;
                break;

            default:
                delay = initialDelayMs;
        }

        return Math.min(delay, maxDelayMs);
    }

    /**
     * Check if retry should be attempted.
     *
     * @param attempt current attempt number (1-based)
     * @return true if more retries allowed
     */
    public boolean shouldRetry(final int attempt)
    {
        return attempt < maxAttempts;
    }

    public int maxAttempts()
    {
        return maxAttempts;
    }

    public long initialDelayMs()
    {
        return initialDelayMs;
    }

    public long maxDelayMs()
    {
        return maxDelayMs;
    }

    public BackoffStrategy strategy()
    {
        return strategy;
    }

    @Override
    public String toString()
    {
        return "RetryPolicy{" +
            "maxAttempts=" + maxAttempts +
            ", initialDelayMs=" + initialDelayMs +
            ", maxDelayMs=" + maxDelayMs +
            ", strategy=" + strategy +
            ", jitterFactor=" + jitterFactor +
            '}';
    }
}
