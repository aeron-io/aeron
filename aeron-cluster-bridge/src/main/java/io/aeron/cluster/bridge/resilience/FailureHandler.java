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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Centralized failure handler for Aeron publication errors.
 * <p>
 * Maps Aeron error codes to appropriate recovery strategies.
 */
public final class FailureHandler
{
    /**
     * Failure type classification.
     */
    public enum FailureType
    {
        /** Transient, retry immediately */
        TRANSIENT,
        /** Temporary overload, retry with backoff */
        OVERLOAD,
        /** Requires reconnection */
        CONNECTION_LOST,
        /** Fatal, cannot recover */
        FATAL,
        /** Unknown error */
        UNKNOWN
    }

    /**
     * Recovery action to take.
     */
    public enum RecoveryAction
    {
        /** Retry immediately */
        RETRY_IMMEDIATE,
        /** Retry with backoff delay */
        RETRY_WITH_BACKOFF,
        /** Reconnect and retry */
        RECONNECT,
        /** Fail fast, don't retry */
        FAIL_FAST,
        /** Buffer for later retry */
        BUFFER_AND_RETRY
    }

    /**
     * Failure analysis result.
     */
    public static final class FailureAnalysis
    {
        public final FailureType type;
        public final RecoveryAction action;
        public final String description;
        public final boolean shouldLog;

        FailureAnalysis(
            final FailureType type,
            final RecoveryAction action,
            final String description,
            final boolean shouldLog)
        {
            this.type = type;
            this.action = action;
            this.description = description;
            this.shouldLog = shouldLog;
        }
    }

    // Error counters
    private final AtomicLong backPressureCount = new AtomicLong(0);
    private final AtomicLong notConnectedCount = new AtomicLong(0);
    private final AtomicLong adminActionCount = new AtomicLong(0);
    private final AtomicLong closedCount = new AtomicLong(0);
    private final AtomicLong maxPositionCount = new AtomicLong(0);

    /**
     * Analyze a publication error code and determine recovery strategy.
     *
     * @param errorCode the Aeron publication error code
     * @return failure analysis with recommended action
     */
    public FailureAnalysis analyze(final long errorCode)
    {
        if (errorCode == Publication.BACK_PRESSURED)
        {
            backPressureCount.incrementAndGet();
            return new FailureAnalysis(
                FailureType.OVERLOAD,
                RecoveryAction.RETRY_WITH_BACKOFF,
                "Subscriber cannot keep up - back pressure applied",
                backPressureCount.get() % 1000 == 1  // Log every 1000th
            );
        }

        if (errorCode == Publication.NOT_CONNECTED)
        {
            notConnectedCount.incrementAndGet();
            return new FailureAnalysis(
                FailureType.CONNECTION_LOST,
                RecoveryAction.RETRY_WITH_BACKOFF,
                "No subscribers connected",
                notConnectedCount.get() % 100 == 1  // Log every 100th
            );
        }

        if (errorCode == Publication.ADMIN_ACTION)
        {
            adminActionCount.incrementAndGet();
            return new FailureAnalysis(
                FailureType.TRANSIENT,
                RecoveryAction.RETRY_IMMEDIATE,
                "Internal admin action in progress",
                false  // Don't log, very transient
            );
        }

        if (errorCode == Publication.CLOSED)
        {
            closedCount.incrementAndGet();
            return new FailureAnalysis(
                FailureType.FATAL,
                RecoveryAction.RECONNECT,
                "Publication has been closed",
                true
            );
        }

        if (errorCode == Publication.MAX_POSITION_EXCEEDED)
        {
            maxPositionCount.incrementAndGet();
            return new FailureAnalysis(
                FailureType.FATAL,
                RecoveryAction.FAIL_FAST,
                "Publication term exhausted - max position exceeded",
                true
            );
        }

        return new FailureAnalysis(
            FailureType.UNKNOWN,
            RecoveryAction.RETRY_WITH_BACKOFF,
            "Unknown error code: " + errorCode,
            true
        );
    }

    /**
     * Get recommended delay for retry based on failure type.
     *
     * @param type    failure type
     * @param attempt current attempt number
     * @return recommended delay in nanoseconds
     */
    public long getRetryDelayNanos(final FailureType type, final int attempt)
    {
        switch (type)
        {
            case TRANSIENT:
                return 0;  // Immediate retry

            case OVERLOAD:
                // Exponential backoff: 1µs, 2µs, 4µs, 8µs, ... up to 1ms
                return Math.min(1000L * (1L << Math.min(attempt, 10)), 1_000_000);

            case CONNECTION_LOST:
                // Longer backoff: 1ms, 2ms, 4ms, ... up to 100ms
                return Math.min(1_000_000L * (1L << Math.min(attempt, 7)), 100_000_000);

            case FATAL:
            case UNKNOWN:
            default:
                return 10_000_000;  // 10ms
        }
    }

    // Metrics getters
    public long backPressureCount()
    {
        return backPressureCount.get();
    }

    public long notConnectedCount()
    {
        return notConnectedCount.get();
    }

    public long adminActionCount()
    {
        return adminActionCount.get();
    }

    public long closedCount()
    {
        return closedCount.get();
    }

    public long maxPositionCount()
    {
        return maxPositionCount.get();
    }

    /**
     * Reset all counters.
     */
    public void resetCounters()
    {
        backPressureCount.set(0);
        notConnectedCount.set(0);
        adminActionCount.set(0);
        closedCount.set(0);
        maxPositionCount.set(0);
    }

    @Override
    public String toString()
    {
        return "FailureHandler{" +
            "backPressure=" + backPressureCount.get() +
            ", notConnected=" + notConnectedCount.get() +
            ", adminAction=" + adminActionCount.get() +
            ", closed=" + closedCount.get() +
            ", maxPosition=" + maxPositionCount.get() +
            '}';
    }
}
