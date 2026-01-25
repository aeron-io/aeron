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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Circuit Breaker pattern implementation for Aeron Bridge.
 * <p>
 * States:
 * <ul>
 *   <li>CLOSED: Normal operation, requests flow through</li>
 *   <li>OPEN: Circuit tripped, requests fail fast</li>
 *   <li>HALF_OPEN: Testing if service recovered</li>
 * </ul>
 * <p>
 * Transitions:
 * <pre>
 *   CLOSED --[failure threshold exceeded]--> OPEN
 *   OPEN --[timeout expired]--> HALF_OPEN
 *   HALF_OPEN --[success]--> CLOSED
 *   HALF_OPEN --[failure]--> OPEN
 * </pre>
 */
public final class CircuitBreaker
{
    /**
     * Circuit breaker states.
     */
    public enum State
    {
        /** Normal operation */
        CLOSED,
        /** Circuit tripped, failing fast */
        OPEN,
        /** Testing recovery */
        HALF_OPEN
    }

    /**
     * Listener for circuit breaker state changes.
     */
    @FunctionalInterface
    public interface StateChangeListener
    {
        void onStateChange(State oldState, State newState, String reason);
    }

    private final String name;
    private final int failureThreshold;
    private final long openDurationMs;
    private final int halfOpenMaxCalls;

    private final AtomicReference<State> state = new AtomicReference<>(State.CLOSED);
    private final AtomicInteger failureCount = new AtomicInteger(0);
    private final AtomicInteger successCount = new AtomicInteger(0);
    private final AtomicLong lastFailureTimeMs = new AtomicLong(0);
    private final AtomicLong openedAtMs = new AtomicLong(0);
    private final AtomicInteger halfOpenCallCount = new AtomicInteger(0);

    private volatile StateChangeListener stateChangeListener;

    /**
     * Create a new circuit breaker.
     *
     * @param name             identifier for logging
     * @param failureThreshold number of failures before opening circuit
     * @param openDurationMs   how long to stay open before trying half-open
     * @param halfOpenMaxCalls max calls to test in half-open state
     */
    public CircuitBreaker(
        final String name,
        final int failureThreshold,
        final long openDurationMs,
        final int halfOpenMaxCalls)
    {
        this.name = name;
        this.failureThreshold = failureThreshold;
        this.openDurationMs = openDurationMs;
        this.halfOpenMaxCalls = halfOpenMaxCalls;
    }

    /**
     * Create with default settings.
     *
     * @param name identifier for logging
     * @return circuit breaker with defaults (5 failures, 30s open, 3 half-open calls)
     */
    public static CircuitBreaker withDefaults(final String name)
    {
        return new CircuitBreaker(name, 5, 30_000, 3);
    }

    /**
     * Set state change listener.
     *
     * @param listener the listener
     * @return this for chaining
     */
    public CircuitBreaker onStateChange(final StateChangeListener listener)
    {
        this.stateChangeListener = listener;
        return this;
    }

    /**
     * Check if call is allowed.
     *
     * @return true if call should proceed
     */
    public boolean allowRequest()
    {
        final State currentState = state.get();

        switch (currentState)
        {
            case CLOSED:
                return true;

            case OPEN:
                // Check if we should transition to half-open
                if (System.currentTimeMillis() - openedAtMs.get() >= openDurationMs)
                {
                    if (transitionTo(State.OPEN, State.HALF_OPEN, "timeout expired"))
                    {
                        halfOpenCallCount.set(0);
                        return true;
                    }
                }
                return false;

            case HALF_OPEN:
                // Allow limited calls in half-open
                return halfOpenCallCount.incrementAndGet() <= halfOpenMaxCalls;

            default:
                return false;
        }
    }

    /**
     * Record a successful call.
     */
    public void recordSuccess()
    {
        final State currentState = state.get();

        switch (currentState)
        {
            case CLOSED:
                failureCount.set(0);
                successCount.incrementAndGet();
                break;

            case HALF_OPEN:
                successCount.incrementAndGet();
                // After successful half-open calls, close the circuit
                if (successCount.get() >= halfOpenMaxCalls)
                {
                    transitionTo(State.HALF_OPEN, State.CLOSED, "half-open success threshold reached");
                    reset();
                }
                break;

            case OPEN:
                // Shouldn't happen, but ignore
                break;
        }
    }

    /**
     * Record a failed call.
     */
    public void recordFailure()
    {
        final State currentState = state.get();
        lastFailureTimeMs.set(System.currentTimeMillis());

        switch (currentState)
        {
            case CLOSED:
                final int failures = failureCount.incrementAndGet();
                if (failures >= failureThreshold)
                {
                    openCircuit("failure threshold exceeded: " + failures + " >= " + failureThreshold);
                }
                break;

            case HALF_OPEN:
                // Single failure in half-open reopens circuit
                openCircuit("failure in half-open state");
                break;

            case OPEN:
                // Already open, just record
                failureCount.incrementAndGet();
                break;
        }
    }

    /**
     * Execute a supplier with circuit breaker protection.
     *
     * @param supplier the operation to execute
     * @param fallback fallback if circuit is open
     * @param <T>      return type
     * @return result from supplier or fallback
     */
    public <T> T execute(final Supplier<T> supplier, final Supplier<T> fallback)
    {
        if (!allowRequest())
        {
            return fallback.get();
        }

        try
        {
            final T result = supplier.get();
            recordSuccess();
            return result;
        }
        catch (final Exception e)
        {
            recordFailure();
            throw e;
        }
    }

    /**
     * Execute a runnable with circuit breaker protection.
     *
     * @param runnable the operation to execute
     * @param fallback fallback if circuit is open
     */
    public void execute(final Runnable runnable, final Runnable fallback)
    {
        if (!allowRequest())
        {
            fallback.run();
            return;
        }

        try
        {
            runnable.run();
            recordSuccess();
        }
        catch (final Exception e)
        {
            recordFailure();
            throw e;
        }
    }

    /**
     * Manually open the circuit.
     *
     * @param reason reason for opening
     */
    public void openCircuit(final String reason)
    {
        final State oldState = state.get();
        if (state.compareAndSet(oldState, State.OPEN))
        {
            openedAtMs.set(System.currentTimeMillis());
            notifyStateChange(oldState, State.OPEN, reason);
        }
    }

    /**
     * Manually close the circuit.
     */
    public void closeCircuit()
    {
        final State oldState = state.get();
        if (transitionTo(oldState, State.CLOSED, "manual close"))
        {
            reset();
        }
    }

    /**
     * Reset all counters.
     */
    public void reset()
    {
        failureCount.set(0);
        successCount.set(0);
        halfOpenCallCount.set(0);
    }

    /**
     * Get current state.
     *
     * @return the current state
     */
    public State state()
    {
        return state.get();
    }

    /**
     * Check if circuit is closed (normal operation).
     *
     * @return true if closed
     */
    public boolean isClosed()
    {
        return state.get() == State.CLOSED;
    }

    /**
     * Check if circuit is open (failing fast).
     *
     * @return true if open
     */
    public boolean isOpen()
    {
        return state.get() == State.OPEN;
    }

    /**
     * Get failure count.
     *
     * @return current failure count
     */
    public int failureCount()
    {
        return failureCount.get();
    }

    /**
     * Get success count.
     *
     * @return current success count
     */
    public int successCount()
    {
        return successCount.get();
    }

    public String name()
    {
        return name;
    }

    private boolean transitionTo(final State from, final State to, final String reason)
    {
        if (state.compareAndSet(from, to))
        {
            notifyStateChange(from, to, reason);
            return true;
        }
        return false;
    }

    private void notifyStateChange(final State oldState, final State newState, final String reason)
    {
        System.out.println("[CircuitBreaker:" + name + "] " + oldState + " -> " + newState + " (" + reason + ")");
        final StateChangeListener listener = stateChangeListener;
        if (listener != null)
        {
            try
            {
                listener.onStateChange(oldState, newState, reason);
            }
            catch (final Exception e)
            {
                System.err.println("[CircuitBreaker:" + name + "] State change listener error: " + e.getMessage());
            }
        }
    }

    @Override
    public String toString()
    {
        return "CircuitBreaker{" +
            "name='" + name + '\'' +
            ", state=" + state.get() +
            ", failureCount=" + failureCount.get() +
            ", successCount=" + successCount.get() +
            ", failureThreshold=" + failureThreshold +
            ", openDurationMs=" + openDurationMs +
            '}';
    }
}
