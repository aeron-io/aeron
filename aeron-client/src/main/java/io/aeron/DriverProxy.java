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
package io.aeron;

import io.aeron.command.*;
import io.aeron.exceptions.AeronException;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import static io.aeron.command.ControlProtocolEvents.*;

/**
 * Separates the concern of communicating with the client conductor away from the rest of the client.
 * <p>
 * For writing commands into the client conductor buffer.
 * <p>
 * <b>Note:</b> this class is not thread safe and is expecting to be called within {@link Aeron.Context#clientLock()}.
 */
public final class DriverProxy
{
    private final long clientId;
    private final PublicationMessageFlyweight publicationMessage = new PublicationMessageFlyweight();
    private final SubscriptionMessageFlyweight subscriptionMessage = new SubscriptionMessageFlyweight();
    private final RemoveMessageFlyweight removeMessage = new RemoveMessageFlyweight();
    private final DestinationMessageFlyweight destinationMessage = new DestinationMessageFlyweight();
    private final DestinationByIdMessageFlyweight destinationByIdMessage = new DestinationByIdMessageFlyweight();
    private final CounterMessageFlyweight counterMessage = new CounterMessageFlyweight();
    private final StaticCounterMessageFlyweight staticCounterMessageFlyweight = new StaticCounterMessageFlyweight();
    private final RejectImageFlyweight rejectImage = new RejectImageFlyweight();
    private final RingBuffer toDriverCommandBuffer;

    /**
     * Create a proxy to a media driver which sends commands via a {@link RingBuffer}.
     *
     * @param toDriverCommandBuffer to send commands via.
     * @param clientId              to represent the client.
     */
    public DriverProxy(final RingBuffer toDriverCommandBuffer, final long clientId)
    {
        this.toDriverCommandBuffer = toDriverCommandBuffer;
        this.clientId = clientId;
    }

    /**
     * Time of the last heartbeat to indicate the driver is alive.
     *
     * @return time of the last heartbeat to indicate the driver is alive.
     */
    public long timeOfLastDriverKeepaliveMs()
    {
        return toDriverCommandBuffer.consumerHeartbeatTime();
    }

    /**
     * Instruct the driver to add a concurrent publication.
     *
     * @param channel  uri in string format.
     * @param streamId within the channel.
     * @return the correlation id for the command.
     */
    public long addPublication(final String channel, final int streamId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = PublicationMessageFlyweight.computeLength(channel.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_PUBLICATION, length);
        if (index < 0)
        {
            throw new AeronException("could not write add publication command");
        }

        publicationMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .streamId(streamId)
            .channel(channel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Instruct the driver to add a non-concurrent, i.e. exclusive, publication.
     *
     * @param channel  uri in string format.
     * @param streamId within the channel.
     * @return the correlation id for the command.
     */
    public long addExclusivePublication(final String channel, final int streamId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = PublicationMessageFlyweight.computeLength(channel.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_EXCLUSIVE_PUBLICATION, length);
        if (index < 0)
        {
            throw new AeronException("could not write add exclusive publication command");
        }

        publicationMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .streamId(streamId)
            .channel(channel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Instruct the driver to remove a publication by its registration id.
     *
     * @param registrationId for the publication to be removed.
     * @return the correlation id for the command.
     */
    public long removePublication(final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int index = toDriverCommandBuffer.tryClaim(REMOVE_PUBLICATION, RemoveMessageFlyweight.length());
        if (index < 0)
        {
            throw new AeronException("could not write remove publication command");
        }

        removeMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationId(registrationId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Instruct the driver to add a subscription.
     *
     * @param channel  uri in string format.
     * @param streamId within the channel.
     * @return the correlation id for the command.
     */
    public long addSubscription(final String channel, final int streamId)
    {
        final long registrationId = Aeron.NULL_VALUE;
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = SubscriptionMessageFlyweight.computeLength(channel.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_SUBSCRIPTION, length);
        if (index < 0)
        {
            throw new AeronException("could not write add subscription command");
        }

        subscriptionMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationCorrelationId(registrationId)
            .streamId(streamId)
            .channel(channel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Instruct the driver to remove a subscription by its registration id.
     *
     * @param registrationId for the subscription to be removed.
     * @return the correlation id for the command.
     */
    public long removeSubscription(final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int index = toDriverCommandBuffer.tryClaim(REMOVE_SUBSCRIPTION, RemoveMessageFlyweight.length());
        if (index < 0)
        {
            throw new AeronException("could not write remove subscription command");
        }

        removeMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationId(registrationId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Add a destination to the send channel of an existing MDC Publication.
     *
     * @param registrationId  of the Publication.
     * @param endpointChannel for the destination.
     * @return the correlation id for the command.
     */
    public long addDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = DestinationMessageFlyweight.computeLength(endpointChannel.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_DESTINATION, length);
        if (index < 0)
        {
            throw new AeronException("could not write add destination command");
        }

        destinationMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Remove a destination from the send channel of an existing MDC Publication.
     *
     * @param registrationId  of the Publication.
     * @param endpointChannel used for the {@link #addDestination(long, String)} command.
     * @return the correlation id for the command.
     */
    public long removeDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = DestinationMessageFlyweight.computeLength(endpointChannel.length());
        final int index = toDriverCommandBuffer.tryClaim(REMOVE_DESTINATION, length);
        if (index < 0)
        {
            throw new AeronException("could not write remove destination command");
        }

        destinationMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Remove a destination from the send channel of an existing MDC Publication.
     *
     * @param publicationRegistrationId  of the Publication.
     * @param destinationRegistrationId used for the {@link #addDestination(long, String)} command.
     * @return the correlation id for the command.
     */
    public long removeDestination(final long publicationRegistrationId, final long destinationRegistrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int index = toDriverCommandBuffer.tryClaim(
            REMOVE_DESTINATION_BY_ID, DestinationByIdMessageFlyweight.MESSAGE_LENGTH);
        if (index < 0)
        {
            throw new AeronException("could not write remove destination command");
        }

        destinationByIdMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .resourceRegistrationId(publicationRegistrationId)
            .destinationRegistrationId(destinationRegistrationId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Add a destination to the receive channel endpoint of an existing MDS Subscription.
     *
     * @param registrationId  of the Subscription.
     * @param endpointChannel for the destination.
     * @return the correlation id for the command.
     */
    public long addRcvDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = DestinationMessageFlyweight.computeLength(endpointChannel.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_RCV_DESTINATION, length);
        if (index < 0)
        {
            throw new AeronException("could not write add rcv destination command");
        }

        destinationMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Remove a destination from the receive channel endpoint of an existing MDS Subscription.
     *
     * @param registrationId  of the Subscription.
     * @param endpointChannel used for the {@link #addRcvDestination(long, String)} command.
     * @return the correlation id for the command.
     */
    public long removeRcvDestination(final long registrationId, final String endpointChannel)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = DestinationMessageFlyweight.computeLength(endpointChannel.length());
        final int index = toDriverCommandBuffer.tryClaim(REMOVE_RCV_DESTINATION, length);
        if (index < 0)
        {
            throw new AeronException("could not write remove rcv destination command");
        }

        destinationMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationCorrelationId(registrationId)
            .channel(endpointChannel)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Add a new counter with a type id plus the label and key are provided in buffers.
     *
     * @param typeId      for associating with the counter.
     * @param keyBuffer   containing the metadata key.
     * @param keyOffset   offset at which the key begins.
     * @param keyLength   length in bytes for the key.
     * @param labelBuffer containing the label.
     * @param labelOffset offset at which the label begins.
     * @param labelLength length in bytes for the label.
     * @return the correlation id for the command.
     */
    public long addCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = CounterMessageFlyweight.computeLength(keyLength, labelLength);
        final int index = toDriverCommandBuffer.tryClaim(ADD_COUNTER, length);
        if (index < 0)
        {
            throw new AeronException("could not write add counter command");
        }

        counterMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .keyBuffer(keyBuffer, keyOffset, keyLength)
            .labelBuffer(labelBuffer, labelOffset, labelLength)
            .typeId(typeId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Add a new counter with a type id and label, the key will be blank.
     *
     * @param typeId for associating with the counter.
     * @param label  that is human-readable for the counter.
     * @return the correlation id for the command.
     */
    public long addCounter(final int typeId, final String label)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = CounterMessageFlyweight.computeLength(0, label.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_COUNTER, length);
        if (index < 0)
        {
            throw new AeronException("could not write add counter command");
        }

        counterMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .keyBuffer(null, 0, 0)
            .label(label)
            .typeId(typeId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Instruct the media driver to remove an existing counter by its registration id.
     *
     * @param registrationId of counter to remove.
     * @return the correlation id for the command.
     */
    public long removeCounter(final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int index = toDriverCommandBuffer.tryClaim(REMOVE_COUNTER, RemoveMessageFlyweight.length());
        if (index < 0)
        {
            throw new AeronException("could not write remove counter command");
        }

        removeMessage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .registrationId(registrationId)
            .clientId(clientId)
            .correlationId(correlationId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    /**
     * Notify the media driver that this client is closing.
     */
    public void clientClose()
    {
        final int index = toDriverCommandBuffer.tryClaim(CLIENT_CLOSE, CorrelatedMessageFlyweight.LENGTH);
        if (index > 0)
        {
            new CorrelatedMessageFlyweight()
                .wrap(toDriverCommandBuffer.buffer(), index)
                .clientId(clientId)
                .correlationId(Aeron.NULL_VALUE);

            toDriverCommandBuffer.commit(index);
        }
    }

    /**
     * Instruct the media driver to terminate.
     *
     * @param tokenBuffer containing the authentication token.
     * @param tokenOffset at which the token begins.
     * @param tokenLength in bytes.
     * @return true is successfully sent.
     */
    public boolean terminateDriver(final DirectBuffer tokenBuffer, final int tokenOffset, final int tokenLength)
    {
        final int length = TerminateDriverFlyweight.computeLength(tokenLength);
        final int index = toDriverCommandBuffer.tryClaim(TERMINATE_DRIVER, length);
        if (index > 0)
        {
            new TerminateDriverFlyweight()
                .wrap(toDriverCommandBuffer.buffer(), index)
                .tokenBuffer(tokenBuffer, tokenOffset, tokenLength)
                .clientId(clientId)
                .correlationId(Aeron.NULL_VALUE);

            toDriverCommandBuffer.commit(index);
            return true;
        }

        return false;
    }

    /**
     * Reject a specific image.
     *
     * @param imageCorrelationId of the image to be invalidated
     * @param position      of the image when invalidation occurred
     * @param reason        user supplied reason for invalidation, reported back to publication
     * @return              the correlationId of the request for invalidation.
     */
    public long rejectImage(
        final long imageCorrelationId,
        final long position,
        final String reason)
    {
        final int length = RejectImageFlyweight.computeLength(reason);
        final int index = toDriverCommandBuffer.tryClaim(REJECT_IMAGE, length);

        if (index < 0)
        {
            throw new AeronException("could not write reject image command");
        }

        final long correlationId = toDriverCommandBuffer.nextCorrelationId();

        rejectImage
            .wrap(toDriverCommandBuffer.buffer(), index)
            .clientId(clientId)
            .correlationId(correlationId)
            .imageCorrelationId(imageCorrelationId)
            .position(position)
            .reason(reason);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }


    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "DriverProxy{" +
            "clientId=" + clientId +
            '}';
    }

    long addStaticCounter(
        final int typeId,
        final DirectBuffer keyBuffer,
        final int keyOffset,
        final int keyLength,
        final DirectBuffer labelBuffer,
        final int labelOffset,
        final int labelLength,
        final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = StaticCounterMessageFlyweight.computeLength(keyLength, labelLength);
        final int index = toDriverCommandBuffer.tryClaim(ADD_STATIC_COUNTER, length);
        if (index < 0)
        {
            throw new AeronException("could not write add counter command");
        }

        staticCounterMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .keyBuffer(keyBuffer, keyOffset, keyLength)
            .labelBuffer(labelBuffer, labelOffset, labelLength)
            .typeId(typeId)
            .registrationId(registrationId)
            .correlationId(correlationId)
            .clientId(clientId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }

    long addStaticCounter(final int typeId, final String label, final long registrationId)
    {
        final long correlationId = toDriverCommandBuffer.nextCorrelationId();
        final int length = StaticCounterMessageFlyweight.computeLength(0, label.length());
        final int index = toDriverCommandBuffer.tryClaim(ADD_STATIC_COUNTER, length);
        if (index < 0)
        {
            throw new AeronException("could not write add counter command");
        }

        staticCounterMessageFlyweight
            .wrap(toDriverCommandBuffer.buffer(), index)
            .keyBuffer(null, 0, 0)
            .label(label)
            .typeId(typeId)
            .registrationId(registrationId)
            .correlationId(correlationId)
            .clientId(clientId);

        toDriverCommandBuffer.commit(index);

        return correlationId;
    }
}
