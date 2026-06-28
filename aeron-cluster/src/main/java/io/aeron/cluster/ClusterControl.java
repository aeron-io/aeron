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
package io.aeron.cluster;

import io.aeron.*;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.service.ClusterCounters;
import io.aeron.cluster.service.ClusteredServiceContainer;
import org.agrona.*;
import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.nio.charset.StandardCharsets;

import static io.aeron.CncFileDescriptor.*;

/**
 * Toggle control {@link ToggleState}s for a cluster node such as {@link ToggleState#SUSPEND} or
 * {@link ToggleState#RESUME}. This can only be applied to the {@link io.aeron.cluster.service.Cluster.Role#LEADER}.
 */
public final class ClusterControl
{
    /**
     * Toggle states for controlling the cluster node once it has entered the active state after initialising.
     * The toggle can only we switched into a new state from {@link #NEUTRAL} and will be reset by the
     * {@link io.aeron.cluster.ConsensusModule} once the triggered action is complete.
     */
    public enum ToggleState
    {
        /**
         * Inactive state, not accepting new actions.
         */
        INACTIVE(0),

        /**
         * Neutral state ready to accept a new action.
         */
        NEUTRAL(1),

        /**
         * Suspend processing of ingress and timers.
         */
        SUSPEND(2),

        /**
         * Resume processing of ingress and timers.
         */
        RESUME(3),

        /**
         * Take a snapshot of cluster state.
         */
        SNAPSHOT(4),

        /**
         * Shut down the cluster in an orderly fashion by taking a snapshot first then terminating.
         */
        SHUTDOWN(5),

        /**
         * Abort processing and terminate the cluster without taking a snapshot.
         */
        ABORT(6),

        /**
         * Trigger a snapshot that will only occur on a cluster standby.
         */
        STANDBY_SNAPSHOT(7);

        private final int code;

        private static final ToggleState[] STATES = values();

        ToggleState(final int code)
        {
            if (code != ordinal())
            {
                throw new IllegalArgumentException(name() + " - code must equal ordinal value: code=" + code);
            }

            this.code = code;
        }

        /**
         * Code to be used as the indicator in the control toggle counter.
         *
         * @return code to be used as the indicator in the control toggle counter.
         */
        public final int code()
        {
            return code;
        }

        /**
         * Toggle the control counter to trigger the requested {@link ToggleState}.
         * <p>
         * This action is thread safe and will succeed if the toggle is in the {@link ToggleState#NEUTRAL} state,
         * or if toggle is {@link ToggleState#SUSPEND} and requested state is {@link ToggleState#RESUME}.
         *
         * @param controlToggle to change to the trigger state.
         * @return true if the counter toggles or false if it is in a state other than {@link ToggleState#NEUTRAL}.
         */
        public final boolean toggle(final AtomicCounter controlToggle)
        {
            if (code() == RESUME.code() && controlToggle.get() == SUSPEND.code())
            {
                return controlToggle.compareAndSet(SUSPEND.code(), RESUME.code());
            }

            return controlToggle.compareAndSet(NEUTRAL.code(), code());
        }

        /**
         * Reset the toggle to the {@link #NEUTRAL} state.
         *
         * @param controlToggle to be reset.
         */
        public static void reset(final AtomicCounter controlToggle)
        {
            controlToggle.set(NEUTRAL.code());
        }

        /**
         * Activate the toggle by setting it to the {@link #NEUTRAL} state.
         *
         * @param controlToggle to be activated.
         */
        public static void activate(final AtomicCounter controlToggle)
        {
            controlToggle.set(NEUTRAL.code());
        }

        /**
         * Activate the toggle by setting it to the {@link #INACTIVE} state.
         *
         * @param controlToggle to be deactivated.
         */
        public static void deactivate(final AtomicCounter controlToggle)
        {
            controlToggle.set(INACTIVE.code());
        }

        /**
         * Get the {@link ToggleState} for a given control toggle.
         *
         * @param controlToggle to get the current state for.
         * @return the state for the current control toggle.
         * @throws ClusterException if the counter is not one of the valid values.
         */
        public static ToggleState get(final AtomicCounter controlToggle)
        {
            if (controlToggle.isClosed())
            {
                throw new ClusterException("counter is closed");
            }

            final long toggleValue = controlToggle.get();
            if (toggleValue < 0 || toggleValue > (STATES.length - 1))
            {
                throw new ClusterException("invalid toggle value: " + toggleValue);
            }

            return STATES[(int)toggleValue];
        }
    }

    private ClusterControl()
    {
    }

    /**
     * Counter type id for the control toggle.
     */
    public static final int CONTROL_TOGGLE_TYPE_ID = AeronCounters.CLUSTER_CONTROL_TOGGLE_TYPE_ID;

    /**
     * Map a {@link CountersReader} over the provided {@link File} for the CnC file.
     *
     * @param cncFile for the counters.
     * @return a {@link CountersReader} over the provided CnC file.
     */
    public static CountersReader mapCounters(final File cncFile)
    {
        final MappedByteBuffer cncByteBuffer = IoUtil.mapExistingFile(cncFile, "cnc");
        final DirectBuffer cncMetaData = createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaData.getInt(cncVersionOffset(0));

        CncFileDescriptor.checkVersion(cncVersion);

        return new CountersReader(
            createCountersMetaDataBuffer(cncByteBuffer, cncMetaData),
            createCountersValuesBuffer(cncByteBuffer, cncMetaData),
            StandardCharsets.US_ASCII);
    }

    /**
     * Find the control toggle counter or return null if not found.
     *
     * @param counters  to search within.
     * @param clusterId to which the allocated counter belongs.
     * @return the control toggle counter or return null if not found.
     */
    public static AtomicCounter findControlToggle(final CountersReader counters, final int clusterId)
    {
        final int counterId = ClusterCounters.find(counters, CONTROL_TOGGLE_TYPE_ID, clusterId);
        if (Aeron.NULL_VALUE != counterId)
        {
            return new AtomicCounter(counters.valuesBuffer(), counterId, null);
        }

        return null;
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        checkUsage(args);

        final ToggleState toggleState = ToggleState.valueOf(args[0].toUpperCase());

        final File cncFile = CommonContext.newDefaultCncFile();
        System.out.println("Command `n Control file " + cncFile);

        final CountersReader countersReader = mapCounters(cncFile);
        final int clusterId = ClusteredServiceContainer.Configuration.clusterId();
        final AtomicCounter controlToggle = findControlToggle(countersReader, clusterId);

        if (null == controlToggle)
        {
            System.out.println("Failed to find control toggle");
            System.exit(0);
        }

        if (toggleState.toggle(controlToggle))
        {
            System.out.println(toggleState + " toggled successfully");
        }
        else
        {
            System.out.println(toggleState + " did NOT toggle: current state=" + ToggleState.get(controlToggle));
        }
    }

    private static void checkUsage(final String[] args)
    {
        if (1 != args.length)
        {
            System.out.format("Usage: [-Daeron.dir=<directory containing CnC file> -Daeron.cluster.id=<id>] " +
                ClusterControl.class.getName() + " <action>%n");

            System.exit(0);
        }
    }
}
