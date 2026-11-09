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

class IpcSubscriptionLink extends SubscriptionLink
{
    IpcSubscriptionLink(
        final long registrationId,
        final int streamId,
        final String channelUri,
        final AeronClient aeronClient,
        final SubscriptionParams params)
    {
        super(registrationId, streamId, channelUri, aeronClient, params);
    }

    @Override
    boolean matches(final IpcPublication publication)
    {
        return publication.streamId() == streamId && isWildcardOrSessionIdMatch(publication.sessionId());
    }
}
