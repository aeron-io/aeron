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

abstract class DutyCycleTrackerLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

abstract class DutyCycleTrackerFields extends DutyCycleTrackerLhsPadding
{
    long timeOfLastUpdateNs;
}

/**
 * Tracker to handle tracking the duration of a duty cycle.
 */
public class DutyCycleTracker extends DutyCycleTrackerFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;

    /**
     * Default constructor.
     */
    public DutyCycleTracker()
    {
    }

    /**
     * Update the last known clock time.
     *
     * @param nowNs to update with.
     */
    public final void update(final long nowNs)
    {
        timeOfLastUpdateNs = nowNs;
    }

    /**
     * Pass measurement to tracker and report updating last known clock time with time.
     *
     * @param nowNs of the measurement.
     */
    public final void measureAndUpdate(final long nowNs)
    {
        final long cycleTimeNs = nowNs - timeOfLastUpdateNs;

        timeOfLastUpdateNs = nowNs;
        reportMeasurement(cycleTimeNs);
    }

    /**
     * Callback called to report duration of cycle.
     *
     * @param durationNs of the duty cycle.
     */
    public void reportMeasurement(final long durationNs)
    {
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "DutyCycleTracker{" +
            "timeOfLastUpdateNs=" + timeOfLastUpdateNs +
            '}';
    }
}