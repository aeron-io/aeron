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

import io.aeron.Aeron;
import io.aeron.ChannelUri;
import io.aeron.Publication;
import org.agrona.CloseHelper;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;

class PublicationGroup<P extends Publication> implements AutoCloseable
{
    private final String[] endpoints;
    private final String channelTemplate;
    private final int streamId;
    private final PublicationFactory<P> publicationFactory;
    int cursor = 0;
    private int excludedPublicationCursorValue = -1;
    private P current;

    PublicationGroup(
        final String[] endpoints,
        final String channelTemplate,
        final int streamId,
        final PublicationFactory<P> publicationFactory)
    {

        this.endpoints = endpoints;
        this.channelTemplate = channelTemplate;
        this.streamId = streamId;
        this.publicationFactory = publicationFactory;
    }


    P next(final Aeron aeron)
    {
        final int cursor = nextCursor();
        final String endpoint = endpoints[cursor];

        final ChannelUri channelUri = ChannelUri.parse(channelTemplate);
        channelUri.put(ENDPOINT_PARAM_NAME, endpoint);
        final String channel = channelUri.toString();

        CloseHelper.quietClose(current);
        current = publicationFactory.addPublication(aeron, channel, streamId);
        return current;
    }

    private int nextCursor()
    {
        do
        {
            ++cursor;
            if (endpoints.length <= cursor)
            {
                cursor = 0;
            }
        }
        while (cursor == excludedPublicationCursorValue);

        return cursor;
    }

    P current()
    {
        return current;
    }

    void clearExclusion()
    {
        excludedPublicationCursorValue = -1;
    }

    void closeAndExcludeCurrent()
    {
        excludedPublicationCursorValue = cursor;
        CloseHelper.quietClose(current);
    }

    void shuffle()
    {
        close();
        clearExclusion();
        final Random random = ThreadLocalRandom.current();
        for (int i = endpoints.length; --i > -1;)
        {
            final int j = random.nextInt(i + 1);
            final String tmp = endpoints[i];
            endpoints[i] = endpoints[j];
            endpoints[j] = tmp;
        }
    }

    @Override
    public void close()
    {
        CloseHelper.quietClose(current);
    }

    public boolean isConnected()
    {
        return null != current && current.isConnected();
    }

    interface PublicationFactory<P>
    {
        P addPublication(Aeron aeron, String channel, int streamId);
    }

    @Override
    public String toString()
    {
        return "PublicationGroup{" +
            "endpoints=" + Arrays.toString(endpoints) +
            ", channelTemplate='" + channelTemplate + '\'' +
            ", streamId=" + streamId +
            ", publicationFactory=" + publicationFactory +
            ", cursor=" + cursor +
            ", excludedPublicationCursorValue=" + excludedPublicationCursorValue +
            ", current=" + current +
            '}';
    }
}
