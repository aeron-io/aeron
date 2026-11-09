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

import io.aeron.driver.media.UdpChannel;

class SpySubscriptionLink extends SubscriptionLink
{
    private final UdpChannel udpChannel;

    SpySubscriptionLink(
        final long registrationId,
        final UdpChannel spiedChannel,
        final int streamId,
        final AeronClient aeronClient,
        final SubscriptionParams params)
    {
        super(registrationId, streamId, spiedChannel.originalUriString(), aeronClient, params);

        this.udpChannel = spiedChannel;
    }

    @Override
    boolean matches(final NetworkPublication publication)
    {
        final UdpChannel publicationChannel = publication.channelEndpoint().udpChannel();
        final boolean isSameChannelTag = udpChannel.hasTag() && udpChannel.tag() == publicationChannel.tag();

        return publication.streamId() == streamId &&
            (isSameChannelTag ||
            isWildcardOrSessionIdMatch(publication.sessionId()) &&
            udpChannel.canonicalForm().equals(publicationChannel.canonicalForm()));
    }
}
