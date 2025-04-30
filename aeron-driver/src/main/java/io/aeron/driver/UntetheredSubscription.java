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

import org.agrona.concurrent.status.ReadablePosition;

class UntetheredSubscription
{
    @SuppressWarnings("JavadocVariable")
    enum State
    {
        ACTIVE,
        LINGER,
        RESTING
    }

    State state = State.ACTIVE;
    long timeOfLastUpdateNs;
    final SubscriptionLink subscriptionLink;
    final ReadablePosition position;

    UntetheredSubscription(final SubscriptionLink subscriptionLink, final ReadablePosition position, final long timeNs)
    {
        this.subscriptionLink = subscriptionLink;
        this.position = position;
        this.timeOfLastUpdateNs = timeNs;
    }

    void state(final State newState, final long nowNs, final int streamId, final int sessionId)
    {
        logStateChange(state, newState, subscriptionLink.registrationId, streamId, sessionId, nowNs);
        state = newState;
        timeOfLastUpdateNs = nowNs;
    }

    private void logStateChange(
        final State oldState,
        final State newState,
        final long subscriptionId,
        final int streamId,
        final int sessionId,
        final long nowNs)
    {
//        System.out.println(nowNs + ": subscriptionId=" + subscriptionId + ", streamId=" + streamId +
//            ", sessionId=" + sessionId + ", " + oldState + " -> " + newState);
    }
}
