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

import io.aeron.CommonContext;
import io.aeron.driver.media.ReceiveChannelEndpoint;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.status.ReadablePosition;

import java.util.IdentityHashMap;
import java.util.Map;

/**
 * Subscription registration from a client used for liveness tracking.
 */
public abstract class SubscriptionLink implements DriverManagedResource
{
    final long registrationId;
    final int streamId;
    final boolean isSparse;
    final boolean isTether;
    final boolean isResponse;
    final boolean isRejoin;
    final CommonContext.InferableBoolean group;
    final String channel;
    final AeronClient aeronClient;
    final IdentityHashMap<Subscribable, ReadablePosition> positionBySubscribableMap;
    private final IntHashSet noRejoinSessionIds;
    int sessionId;
    boolean hasSessionId;
    boolean reachedEndOfLife = false;

    SubscriptionLink(
        final long registrationId,
        final int streamId,
        final String channel,
        final AeronClient aeronClient,
        final SubscriptionParams params)
    {
        this.registrationId = registrationId;
        this.streamId = streamId;
        this.channel = channel;
        this.aeronClient = aeronClient;
        this.hasSessionId = params.hasSessionId;
        this.sessionId = params.sessionId;
        this.isSparse = params.isSparse;
        this.isTether = params.isTether;
        this.group = params.group;
        this.isResponse = params.isResponse;
        this.isRejoin = params.isRejoin;

        positionBySubscribableMap = new IdentityHashMap<>(hasSessionId ? 1 : 8);
        noRejoinSessionIds = params.isRejoin ? null : new IntHashSet();
    }

    /**
     * Channel URI the subscription is on.
     *
     * @return channel URI the subscription is on.
     */
    public final String channel()
    {
        return channel;
    }

    /**
     * Stream id the subscription is on.
     *
     * @return stream id the subscription is on.
     */
    public final int streamId()
    {
        return streamId;
    }

    /**
     * Registration id of the subscription.
     *
     * @return registration id of the subscription.
     */
    public final long registrationId()
    {
        return registrationId;
    }

    AeronClient aeronClient()
    {
        return aeronClient;
    }

    final int sessionId()
    {
        return sessionId;
    }

    void sessionId(final int sessionId)
    {
        this.hasSessionId = true;
        this.sessionId = sessionId;
    }

    boolean isResponse()
    {
        return isResponse;
    }

    ReceiveChannelEndpoint channelEndpoint()
    {
        return null;
    }

    boolean isReliable()
    {
        return true;
    }

    boolean isRejoin()
    {
        return isRejoin;
    }

    boolean isTether()
    {
        return isTether;
    }

    boolean isSparse()
    {
        return isSparse;
    }

    CommonContext.InferableBoolean group()
    {
        return group;
    }

    boolean hasSessionId()
    {
        return hasSessionId;
    }

    boolean matches(final NetworkPublication publication)
    {
        return false;
    }

    boolean matches(final PublicationImage image)
    {
        return false;
    }

    boolean matches(final IpcPublication publication)
    {
        return false;
    }

    boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final SubscriptionParams params)
    {
        return false;
    }

    boolean matches(final ReceiveChannelEndpoint channelEndpoint, final int streamId, final int sessionId)
    {
        return false;
    }

    boolean isLinked(final Subscribable subscribable)
    {
        return positionBySubscribableMap.containsKey(subscribable);
    }

    void link(final Subscribable subscribable, final ReadablePosition position)
    {
        positionBySubscribableMap.put(subscribable, position);
    }

    void unlink(final Subscribable subscribable)
    {
        positionBySubscribableMap.remove(subscribable);
    }

    /**
     * Record a session id that this subscription should not rejoin. Only used when
     * {@code rejoin=false}. Called from {@code transitionToLinger} when the image for
     * this session becomes unavailable.
     *
     * @param sessionId the session id to suppress.
     */
    void addNoRejoinSessionId(final int sessionId)
    {
        if (null != noRejoinSessionIds)
        {
            noRejoinSessionIds.add(sessionId);
        }
    }

    /**
     * Check if this subscription should suppress image creation for the given session id.
     * Returns true if this subscription has {@code rejoin=false} and has previously been
     * linked to an image for this session.
     *
     * @param sessionId to check.
     * @return true if the session should be rejected.
     */
    boolean hasNoRejoinForSession(final int sessionId)
    {
        return null != noRejoinSessionIds && noRejoinSessionIds.contains(sessionId);
    }

    boolean isWildcardOrSessionIdMatch(final int sessionId)
    {
        return (!hasSessionId && !isResponse()) || this.sessionId == sessionId;
    }

    boolean supportsMds()
    {
        return false;
    }

    void notifyUnavailableImages(final DriverConductor conductor)
    {
        for (final Map.Entry<Subscribable, ReadablePosition> entry : positionBySubscribableMap.entrySet())
        {
            final Subscribable subscribable = entry.getKey();
            conductor.notifyUnavailableImageLink(subscribable.subscribableRegistrationId(), this);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        for (final Map.Entry<Subscribable, ReadablePosition> entry : positionBySubscribableMap.entrySet())
        {
            final Subscribable subscribable = entry.getKey();
            final ReadablePosition position = entry.getValue();
            subscribable.removeSubscriber(this, position);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void onTimeEvent(final long timeNs, final long timeMs, final DriverConductor conductor)
    {
        if (aeronClient.hasTimedOut())
        {
            reachedEndOfLife = true;
            conductor.cleanupSubscriptionLink(this);
        }
    }

    /**
     * {@inheritDoc}
     */
    public boolean hasReachedEndOfLife()
    {
        return reachedEndOfLife;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return this.getClass().getName() + "{" +
            "registrationId=" + registrationId +
            ", streamId=" + streamId +
            ", sessionId=" + sessionId +
            ", hasSessionId=" + hasSessionId +
            ", isReliable=" + isReliable() +
            ", isSparse=" + isSparse() +
            ", isTether=" + isTether() +
            ", isRejoin=" + isRejoin() +
            ", reachedEndOfLife=" + reachedEndOfLife +
            ", group=" + group +
            ", channel='" + channel + '\'' +
            ", aeronClient=" + aeronClient +
            ", positionBySubscribableMap=" + positionBySubscribableMap +
            '}';
    }
}

