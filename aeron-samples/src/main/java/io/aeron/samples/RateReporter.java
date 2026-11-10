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
package io.aeron.samples;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.LockSupport;

class RateReporterLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

class RateReporterValues extends RateReporterLhsPadding
{
    static final AtomicLongFieldUpdater<RateReporterValues> TOTAL_BYTES_UPDATER =
        AtomicLongFieldUpdater.newUpdater(RateReporterValues.class, "totalBytes");
    static final AtomicLongFieldUpdater<RateReporterValues> TOTAL_MESSAGES_UPDATER =
        AtomicLongFieldUpdater.newUpdater(RateReporterValues.class, "totalMessages");

    volatile long totalBytes;
    volatile long totalMessages;
}

class RateReporterRhsPadding extends RateReporterValues
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;
}

/**
 * Tracker and reporter of throughput rates.
 */
public final class RateReporter extends RateReporterRhsPadding implements Runnable
{
    /**
     * Interface for reporting of rate information.
     */
    @FunctionalInterface
    public interface Reporter
    {
        /**
         * Called for a rate report.
         *
         * @param messagesPerSec since last report.
         * @param bytesPerSec    since last report.
         * @param totalMessages  since beginning of reporting.
         * @param totalBytes     since beginning of reporting.
         */
        void onReport(double messagesPerSec, double bytesPerSec, long totalMessages, long totalBytes);
    }

    private final long reportIntervalNs;
    private final long parkNs;
    private long lastTotalBytes;
    private long lastTotalMessages;
    private long lastTimestamp;
    private volatile boolean running = true;
    private final Reporter reportingFunc;

    /**
     * Create a rate reporter with the given report interval in nanoseconds and the reporting function.
     *
     * @param reportInterval in nanoseconds.
     * @param reportingFunc  to call for reporting rates.
     */
    public RateReporter(final long reportInterval, final Reporter reportingFunc)
    {
        this.reportIntervalNs = reportInterval;
        this.parkNs = reportInterval;
        this.reportingFunc = reportingFunc;
        lastTimestamp = System.nanoTime();
    }

    /**
     * Run loop for the rate reporter.
     */
    @Override
    public void run()
    {
        do
        {
            LockSupport.parkNanos(parkNs);

            final long currentTotalMessages = totalMessages;
            final long currentTotalBytes = totalBytes;
            final long currentTimestamp = System.nanoTime();

            final long timeSpanNs = currentTimestamp - lastTimestamp;
            final double messagesPerSec =
                ((currentTotalMessages - lastTotalMessages) * (double)reportIntervalNs) / (double)timeSpanNs;
            final double bytesPerSec =
                ((currentTotalBytes - lastTotalBytes) * (double)reportIntervalNs) / (double)timeSpanNs;

            reportingFunc.onReport(messagesPerSec, bytesPerSec, currentTotalMessages, currentTotalBytes);

            lastTotalBytes = currentTotalBytes;
            lastTotalMessages = currentTotalMessages;
            lastTimestamp = currentTimestamp;
        }
        while (running);
    }

    /**
     * Signal the run loop to exit. Does not block.
     */
    public void halt()
    {
        running = false;
    }

    /**
     * Notify rate reporter of number of messages and length received, sent, etc.
     *
     * @param length received, sent, etc.
     */
    public void onMessage(final long length)
    {
        TOTAL_BYTES_UPDATER.lazySet(this, totalBytes + length);
        TOTAL_MESSAGES_UPDATER.lazySet(this, totalMessages + 1);
    }
}
