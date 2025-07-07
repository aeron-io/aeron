/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron.response;

import io.aeron.Aeron;
import io.aeron.ChannelUriStringBuilder;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;

import java.util.function.Function;

/**
 * Sample client to be used with the response server.
 */
public class ResponseClient implements AutoCloseable, Agent
{
    private final Aeron aeron;
    private final FragmentHandler handler;
    private final String requestEndpoint;
    private final int requestStreamId;
    private final String responseControl;
    private final int responseStreamId;
    private final ChannelUriStringBuilder requestUriBuilder;
    private final ChannelUriStringBuilder responseUriBuilder;
    private ExclusivePublication publication;
    private Subscription subscription;

    /**
     * Construct a response client with the associated request endpoint. The response endpoint is not required as
     * the server will manage sending this to the client
     *
     * @param aeron            client to use to connect to the server.
     * @param handler          callback to handle response messages.
     * @param requestEndpoint  request publication's endpoint.
     * @param requestStreamId  request publication streamId
     * @param responseControl  control address for the response subscription.
     * @param responseStreamId response response streamId
     * @param requestChannel   channel fragment to allow for configuration of parameters on the request publication.
     *                         May be null. The 'endpoint' parameter is not required and will be replaced by the
     *                         <code>requestEndpoint</code> if specified.
     * @param responseChannel  channel fragment to allow for configuration parameters on the response subscription.
     *                         May be null. The 'control' parameter is not required and will be removed if specified.
     */
    public ResponseClient(
        final Aeron aeron,
        final FragmentHandler handler,
        final String requestEndpoint,
        final int requestStreamId,
        final String responseControl,
        final int responseStreamId,
        final String requestChannel,
        final String responseChannel)
    {
        this.aeron = aeron;
        this.handler = handler;
        this.requestEndpoint = requestEndpoint;
        this.requestStreamId = requestStreamId;
        this.responseControl = responseControl;
        this.responseStreamId = responseStreamId;

        requestUriBuilder = null != requestChannel ?
            new ChannelUriStringBuilder(requestChannel) : new ChannelUriStringBuilder();
        requestUriBuilder
            .media("udp")
            .endpoint(requestEndpoint);
        responseUriBuilder = null != responseChannel ?
            new ChannelUriStringBuilder(responseChannel) : new ChannelUriStringBuilder();
        responseUriBuilder
            .media("udp")
            .controlMode("response")
            .controlEndpoint(responseControl);
    }

    /**
     * Overload for {@link ResponseServer#ResponseServer(Aeron, Function, String, int, String, int, String, String)}
     * that defaults the channels to null.
     *
     * @param aeron            client to use to connect to the server.
     * @param handler          callback to handle response messages.
     * @param requestEndpoint  request publication's endpoint.
     * @param requestStreamId  request publication streamId
     * @param responseControl  control address for the response subscription.
     * @param responseStreamId response response streamId
     */
    public ResponseClient(
        final Aeron aeron,
        final FragmentHandler handler,
        final String requestEndpoint,
        final int requestStreamId,
        final String responseControl,
        final int responseStreamId)
    {
        this(aeron, handler, requestEndpoint, requestStreamId, responseControl, responseStreamId, null, null);
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        int workCount = 0;

        if (null == subscription)
        {
            subscription = aeron.addSubscription(responseUriBuilder.build(), responseStreamId);
        }

        if (null == publication && null != subscription)
        {
            publication = aeron.addExclusivePublication(
                requestUriBuilder.responseCorrelationId(subscription.registrationId()).build(),
                requestStreamId);

            workCount++;
        }

        if (null != subscription)
        {
            workCount += subscription.poll(handler, 10);
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public String roleName()
    {
        return "ResponseClient";
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        publication.revokeOnClose();
        CloseHelper.quietCloseAll(publication, subscription);
    }

    /**
     * Check that the client is connected.
     *
     * @return true if the client is connected.
     */
    public boolean isConnected()
    {
        return null != subscription && subscription.isConnected() && null != publication && publication.isConnected();
    }

    /**
     * Offer a message on the request channel.
     *
     * @param message to be sent
     * @return result code from the publication.
     */
    public long offer(final DirectBuffer message)
    {
        return publication.offer(message);
    }

    /**
     * Get the response control for the server.
     *
     * @return response control for the server.
     */
    public String responseControl()
    {
        return responseControl;
    }

    /**
     * Get the request endpoint for the server.
     *
     * @return request endpoint for the server.
     */
    public String requestEndpoint()
    {
        return requestEndpoint;
    }

    /**
     * Get the response subscription for the client.
     *
     * @return response subscription for the client.
     */
    public Subscription subscription()
    {
        return subscription;
    }

    /**
     * Get the request publication for the client.
     *
     * @return request publication for the client.
     */
    public ExclusivePublication publication()
    {
        return publication;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ResponseClient{" +
            "publication.isConnected=" + publication.isConnected() +
            ", subscription.isConnected=" + subscription.isConnected() +
            '}';
    }
}
