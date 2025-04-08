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

import org.agrona.concurrent.status.AtomicCounter;
import org.agrona.concurrent.status.CountersReader;

interface ToggleApplication<T extends Enum<T>>
{
    T get(AtomicCounter counter);

    boolean apply(AtomicCounter counter, T targetState);

    AtomicCounter find(CountersReader countersReader, int clusterId);

    @SuppressWarnings("BooleanMethodIsAlwaysInverted")
    boolean isNeutral(T toggleState);

    /**
     * Cluster control toggle.
     */
    ToggleApplication<ClusterControl.ToggleState> CLUSTER_CONTROL = new ToggleApplication<ClusterControl.ToggleState>()
    {
        public ClusterControl.ToggleState get(final AtomicCounter counter)
        {
            return ClusterControl.ToggleState.get(counter);
        }

        public boolean apply(final AtomicCounter counter, final ClusterControl.ToggleState targetState)
        {
            return targetState.toggle(counter);
        }

        public AtomicCounter find(final CountersReader countersReader, final int clusterId)
        {
            return ClusterControl.findControlToggle(countersReader, clusterId);
        }

        public boolean isNeutral(final ClusterControl.ToggleState toggleState)
        {
            return ClusterControl.ToggleState.NEUTRAL == toggleState;
        }
    };

    /**
     * Node state file control toggle.
     */
    ToggleApplication<NodeControl.ToggleState> NODE_CONTROL = new ToggleApplication<NodeControl.ToggleState>()
    {
        public NodeControl.ToggleState get(final AtomicCounter counter)
        {
            return NodeControl.ToggleState.get(counter);
        }

        public boolean apply(final AtomicCounter counter, final NodeControl.ToggleState targetState)
        {
            return targetState.toggle(counter);
        }

        public AtomicCounter find(final CountersReader countersReader, final int clusterId)
        {
            return NodeControl.findControlToggle(countersReader, clusterId);
        }

        public boolean isNeutral(final NodeControl.ToggleState toggleState)
        {
            return NodeControl.ToggleState.NEUTRAL == toggleState;
        }
    };
}
