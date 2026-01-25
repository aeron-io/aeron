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
package io.aeron.cluster.bridge.monitoring;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Health check framework for Bridge components.
 * <p>
 * Provides liveness and readiness probes for Kubernetes/container deployments.
 */
public final class HealthCheck
{
    /**
     * Health status levels.
     */
    public enum Status
    {
        /** Component is healthy */
        HEALTHY,
        /** Component is degraded but functional */
        DEGRADED,
        /** Component is unhealthy */
        UNHEALTHY,
        /** Status unknown */
        UNKNOWN
    }

    /**
     * Individual health check result.
     */
    public static final class CheckResult
    {
        public final String name;
        public final Status status;
        public final String message;
        public final Instant timestamp;

        public CheckResult(final String name, final Status status, final String message)
        {
            this.name = name;
            this.status = status;
            this.message = message;
            this.timestamp = Instant.now();
        }

        public boolean isHealthy()
        {
            return status == Status.HEALTHY;
        }
    }

    /**
     * Health check interface.
     */
    @FunctionalInterface
    public interface Check
    {
        CheckResult execute();
    }

    private final List<Check> livenessChecks = new CopyOnWriteArrayList<>();
    private final List<Check> readinessChecks = new CopyOnWriteArrayList<>();

    /**
     * Register a liveness check.
     * Liveness checks determine if the application should be restarted.
     *
     * @param check the check to register
     * @return this for chaining
     */
    public HealthCheck registerLiveness(final Check check)
    {
        livenessChecks.add(check);
        return this;
    }

    /**
     * Register a readiness check.
     * Readiness checks determine if the application can accept traffic.
     *
     * @param check the check to register
     * @return this for chaining
     */
    public HealthCheck registerReadiness(final Check check)
    {
        readinessChecks.add(check);
        return this;
    }

    /**
     * Run all liveness checks.
     *
     * @return aggregated result
     */
    public AggregateResult checkLiveness()
    {
        return runChecks(livenessChecks);
    }

    /**
     * Run all readiness checks.
     *
     * @return aggregated result
     */
    public AggregateResult checkReadiness()
    {
        return runChecks(readinessChecks);
    }

    private AggregateResult runChecks(final List<Check> checks)
    {
        final List<CheckResult> results = new ArrayList<>();
        Status overall = Status.HEALTHY;

        for (final Check check : checks)
        {
            try
            {
                final CheckResult result = check.execute();
                results.add(result);

                if (result.status == Status.UNHEALTHY)
                {
                    overall = Status.UNHEALTHY;
                }
                else if (result.status == Status.DEGRADED && overall == Status.HEALTHY)
                {
                    overall = Status.DEGRADED;
                }
            }
            catch (final Exception e)
            {
                results.add(new CheckResult("exception", Status.UNHEALTHY, e.getMessage()));
                overall = Status.UNHEALTHY;
            }
        }

        return new AggregateResult(overall, results);
    }

    /**
     * Aggregated health check result.
     */
    public static final class AggregateResult
    {
        public final Status status;
        public final List<CheckResult> details;
        public final Instant timestamp;

        AggregateResult(final Status status, final List<CheckResult> details)
        {
            this.status = status;
            this.details = details;
            this.timestamp = Instant.now();
        }

        public boolean isHealthy()
        {
            return status == Status.HEALTHY || status == Status.DEGRADED;
        }

        public String toJson()
        {
            final StringBuilder sb = new StringBuilder();
            sb.append("{\"status\":\"").append(status).append("\",");
            sb.append("\"timestamp\":\"").append(timestamp).append("\",");
            sb.append("\"checks\":[");

            for (int i = 0; i < details.size(); i++)
            {
                final CheckResult r = details.get(i);
                if (i > 0)
                {
                    sb.append(",");
                }
                sb.append("{\"name\":\"").append(r.name).append("\",");
                sb.append("\"status\":\"").append(r.status).append("\",");
                sb.append("\"message\":\"").append(r.message.replace("\"", "\\\"")).append("\"}");
            }

            sb.append("]}");
            return sb.toString();
        }
    }

    // =========================================================================
    // Pre-built common checks
    // =========================================================================

    /**
     * Create a publication connectivity check.
     */
    public static Check publicationConnected(final String name, final java.util.function.BooleanSupplier isConnected)
    {
        return () ->
        {
            final boolean connected = isConnected.getAsBoolean();
            return new CheckResult(
                name,
                connected ? Status.HEALTHY : Status.DEGRADED,
                connected ? "Connected" : "Not connected"
            );
        };
    }

    /**
     * Create a sequence progress check.
     */
    public static Check sequenceProgress(
        final String name,
        final java.util.function.LongSupplier sequenceSupplier,
        final long staleThresholdMs)
    {
        final long[] lastCheck = {0, System.currentTimeMillis()};

        return () ->
        {
            final long currentSeq = sequenceSupplier.getAsLong();
            final long now = System.currentTimeMillis();

            if (currentSeq > lastCheck[0])
            {
                lastCheck[0] = currentSeq;
                lastCheck[1] = now;
                return new CheckResult(name, Status.HEALTHY, "Sequence progressing: " + currentSeq);
            }

            final long staleDuration = now - lastCheck[1];
            if (staleDuration > staleThresholdMs)
            {
                return new CheckResult(
                    name,
                    Status.DEGRADED,
                    "Sequence stale for " + staleDuration + "ms at " + currentSeq
                );
            }

            return new CheckResult(name, Status.HEALTHY, "Sequence: " + currentSeq);
        };
    }

    /**
     * Create a memory check.
     */
    public static Check memoryUsage(final double warningThreshold, final double criticalThreshold)
    {
        return () ->
        {
            final Runtime runtime = Runtime.getRuntime();
            final long used = runtime.totalMemory() - runtime.freeMemory();
            final long max = runtime.maxMemory();
            final double usage = (double)used / max;

            if (usage > criticalThreshold)
            {
                return new CheckResult(
                    "memory",
                    Status.UNHEALTHY,
                    String.format("Memory critical: %.1f%% used", usage * 100)
                );
            }
            else if (usage > warningThreshold)
            {
                return new CheckResult(
                    "memory",
                    Status.DEGRADED,
                    String.format("Memory warning: %.1f%% used", usage * 100)
                );
            }

            return new CheckResult(
                "memory",
                Status.HEALTHY,
                String.format("Memory OK: %.1f%% used", usage * 100)
            );
        };
    }
}
