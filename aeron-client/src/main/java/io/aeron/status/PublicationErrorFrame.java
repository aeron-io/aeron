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
package io.aeron.status;

import io.aeron.command.PublicationErrorFrameFlyweight;

import java.net.InetSocketAddress;

/**
 * Encapsulates the data received when a publication receives an error frame.
 */
public class PublicationErrorFrame implements Cloneable
{
    private long registrationId;
    private int sessionId;
    private int streamId;
    private long receiverId;
    private long destinationRegistrationId;
    private long groupTag;
    private int errorCode;
    private String errorMessage;
    private InetSocketAddress sourceAddress;

    /**
     * Registration id of the publication that received the error frame.
     *
     * @return registration id of the publication.
     */
    public long registrationId()
    {
        return registrationId;
    }

    /**
     * Session id of the publication that received the error frame.
     *
     * @return session id of the publication.
     */
    public int sessionId()
    {
        return sessionId;
    }

    /**
     * Stream id of the publication that received the error frame.
     *
     * @return stream id of the publication.
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * Receiver id of the source that send the error frame.
     *
     * @return receiver id of the source that send the error frame.
     */
    public long receiverId()
    {
        return receiverId;
    }

    /**
     * Group tag of the source that sent the error frame.
     *
     * @return group tag of the source that sent the error frame or {@link io.aeron.Aeron#NULL_VALUE} if the source did not have a group
     * tag set.
     */
    public long groupTag()
    {
        return groupTag;
    }

    /**
     * The error code of the error frame received.
     *
     * @return the error code.
     */
    public int errorCode()
    {
        return errorCode;
    }

    /**
     * The error message of the error frame received.
     *
     * @return the error message.
     */
    public String errorMessage()
    {
        return errorMessage;
    }

    /**
     * The address of the remote source that sent the error frame.
     *
     * @return address of the remote source.
     */
    public InetSocketAddress sourceAddress()
    {
        return sourceAddress;
    }

    /**
     * The registrationId of the destination. Only used with manual MDC publications. Will be
     * {@link io.aeron.Aeron#NULL_VALUE} otherwise.
     *
     * @return registrationId of the destination or {@link io.aeron.Aeron#NULL_VALUE}.
     */
    public long destinationRegistrationId()
    {
        return destinationRegistrationId;
    }

    /**
     * Set the fields of the publication error frame from the flyweight.
     *
     * @param frameFlyweight that was received from the client message buffer.
     * @return this for fluent API.
     */
    public PublicationErrorFrame set(final PublicationErrorFrameFlyweight frameFlyweight)
    {
        registrationId = frameFlyweight.registrationId();
        sessionId = frameFlyweight.sessionId();
        streamId = frameFlyweight.streamId();
        receiverId = frameFlyweight.receiverId();
        groupTag = frameFlyweight.groupTag();
        sourceAddress = frameFlyweight.sourceAddress();
        errorCode = frameFlyweight.errorCode().value();
        errorMessage = frameFlyweight.errorMessage();
        destinationRegistrationId = frameFlyweight.destinationRegistrationId();

        return this;
    }

    /**
     * Return a copy of this message. Useful if a callback is reusing an instance of this class to avoid unnecessary
     * allocation.
     *
     * @return a copy of this instance's data.
     */
    public PublicationErrorFrame clone()
    {
        try
        {
            return (PublicationErrorFrame)super.clone();
        }
        catch (final CloneNotSupportedException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Build a String representation of the error frame.
     *
     * @return a String representation of the error frame.
     */
    public String toString()
    {
        return "CounterMessageFlyweight{" +
            "registrationId=" + registrationId() +
            ", sessionId=" + sessionId() +
            ", streamId=" + streamId() +
            ", receiverId=" + receiverId() +
            ", destinationRegistrationId=" + destinationRegistrationId() +
            ", groupTag=" + groupTag() +
            ", errorCode=" + errorCode() +
            ", errorMessage=" + errorMessage() +
            ", sourceAddress=" + sourceAddress() +
            "}";
    }
}
