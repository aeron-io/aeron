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

import io.aeron.exceptions.AeronException;
import io.aeron.logbuffer.BlockHandler;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.RawBlockHandler;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.LocalSocketAddressStatus;
import org.agrona.collections.ArrayUtil;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.ENDPOINT_PARAM_NAME;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_MANUAL;
import static io.aeron.CommonContext.MDC_CONTROL_MODE_PARAM_NAME;
import static io.aeron.CommonContext.SPY_PREFIX;

abstract class SubscriptionLhsPadding
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;
}

abstract class SubscriptionFields extends SubscriptionLhsPadding
{
    static final Image[] EMPTY_IMAGES = new Image[0];

    final long registrationId;
    final int streamId;
    int roundRobinIndex = 0;
    volatile boolean isClosed = false;
    volatile Image[] images = EMPTY_IMAGES;
    final ClientConductor conductor;
    final String channel;
    final AvailableImageHandler availableImageHandler;
    final UnavailableImageHandler unavailableImageHandler;
    ChannelUri channelUri;
    String resolvedChannel;
    String resolvedEndpoint;
    int channelStatusId = ChannelEndpointStatus.NO_ID_ALLOCATED;

    SubscriptionFields(
        final long registrationId,
        final int streamId,
        final ClientConductor clientConductor,
        final String channel,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        this.registrationId = registrationId;
        this.streamId = streamId;
        this.conductor = clientConductor;
        this.channel = channel;
        this.availableImageHandler = availableImageHandler;
        this.unavailableImageHandler = unavailableImageHandler;
    }
}

/**
 * Aeron Subscriber API for receiving a reconstructed {@link Image} for a stream of messages from publishers on
 * a given channel and streamId pair, i.e. a {@link Publication}. {@link Image}s are aggregated under a
 * {@link Subscription}.
 * <p>
 * {@link Subscription}s are created via an {@link Aeron} object, and received messages are delivered
 * to the {@link FragmentHandler}.
 * <p>
 * By default, fragmented messages are not reassembled before delivery. If an application must
 * receive whole messages, even if they were fragmented, then the Subscriber
 * should be created with a {@link FragmentAssembler} or a custom implementation.
 * <p>
 * It is an application's responsibility to {@link #poll} the {@link Subscription} for new messages.
 * <p>
 * <b>Note:</b>Subscriptions are not threadsafe and should not be shared between subscribers.
 *
 * @see FragmentAssembler
 * @see ControlledFragmentHandler
 * @see Aeron#addSubscription(String, int)
 * @see Aeron#addSubscription(String, int, AvailableImageHandler, UnavailableImageHandler)
 */
public final class Subscription extends SubscriptionFields implements AutoCloseable
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;

    Subscription(
        final ClientConductor conductor,
        final String channel,
        final int streamId,
        final long registrationId,
        final AvailableImageHandler availableImageHandler,
        final UnavailableImageHandler unavailableImageHandler)
    {
        super(
            registrationId,
            streamId,
            conductor,
            channel,
            availableImageHandler,
            unavailableImageHandler);
    }

    /**
     * Media address for delivery to the channel.
     *
     * @return Media address for delivery to the channel.
     */
    public String channel()
    {
        return channel;
    }

    /**
     * Stream identity for scoping within the channel media address.
     *
     * @return Stream identity for scoping within the channel media address.
     */
    public int streamId()
    {
        return streamId;
    }

    /**
     * Return the registration id used to register this Subscription with the media driver.
     *
     * @return registration id
     */
    public long registrationId()
    {
        return registrationId;
    }

    /**
     * Callback used to indicate when an {@link Image} becomes available under this {@link Subscription}.
     *
     * @return callback used to indicate when an {@link Image} becomes available under this {@link Subscription}.
     */
    public AvailableImageHandler availableImageHandler()
    {
        return availableImageHandler;
    }

    /**
     * Callback used to indicate when an {@link Image} goes unavailable under this {@link Subscription}.
     *
     * @return Callback used to indicate when an {@link Image} goes unavailable under this {@link Subscription}.
     */
    public UnavailableImageHandler unavailableImageHandler()
    {
        return unavailableImageHandler;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered within a session.
     * <p>
     * To assemble messages that span multiple fragments then use {@link FragmentAssembler}.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit when polling across multiple {@link Image}s.
     * @return the number of fragments received.
     */
    public int poll(final FragmentHandler fragmentHandler, final int fragmentLimit)
    {
        final Image[] images = this.images;
        final int length = images.length;
        int fragmentsRead = 0;

        int startingIndex = roundRobinIndex++;
        if (startingIndex >= length)
        {
            roundRobinIndex = startingIndex = 0;
        }

        for (int i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].poll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        for (int i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].poll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        return fragmentsRead;
    }

    /**
     * Poll in a controlled manner the {@link Image}s under the subscription for available message fragments.
     * Control is applied to message fragments in the stream. If more fragments can be read on another stream
     * they will even if BREAK or ABORT is returned from the fragment handler.
     * <p>
     * Each fragment read will be a whole message if it is under MTU length. If larger than MTU then it will come
     * as a series of fragments ordered within a session.
     * <p>
     * To assemble messages that span multiple fragments then use {@link ControlledFragmentAssembler}.
     *
     * @param fragmentHandler callback for handling each message fragment as it is read.
     * @param fragmentLimit   number of message fragments to limit when polling across multiple {@link Image}s.
     * @return the number of fragments received.
     * @see ControlledFragmentHandler
     */
    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        final Image[] images = this.images;
        final int length = images.length;
        int fragmentsRead = 0;

        int startingIndex = roundRobinIndex++;
        if (startingIndex >= length)
        {
            roundRobinIndex = startingIndex = 0;
        }

        for (int i = startingIndex; i < length && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].controlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        for (int i = 0; i < startingIndex && fragmentsRead < fragmentLimit; i++)
        {
            fragmentsRead += images[i].controlledPoll(fragmentHandler, fragmentLimit - fragmentsRead);
        }

        return fragmentsRead;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments in blocks.
     * <p>
     * This method is useful for operations like bulk archiving and messaging indexing.
     *
     * @param blockHandler     to receive a block of fragments from each {@link Image}.
     * @param blockLengthLimit for each {@link Image} polled.
     * @return the number of bytes consumed.
     */
    public long blockPoll(final BlockHandler blockHandler, final int blockLengthLimit)
    {
        long bytesConsumed = 0;
        for (final Image image : images)
        {
            bytesConsumed += image.blockPoll(blockHandler, blockLengthLimit);
        }

        return bytesConsumed;
    }

    /**
     * Poll the {@link Image}s under the subscription for available message fragments in blocks.
     * <p>
     * This method is useful for operations like bulk archiving a stream to file.
     *
     * @param rawBlockHandler  to receive a block of fragments from each {@link Image}.
     * @param blockLengthLimit for each {@link Image} polled.
     * @return the number of bytes consumed.
     */
    public long rawPoll(final RawBlockHandler rawBlockHandler, final int blockLengthLimit)
    {
        long bytesConsumed = 0;
        for (final Image image : images)
        {
            bytesConsumed += image.rawPoll(rawBlockHandler, blockLengthLimit);
        }

        return bytesConsumed;
    }

    /**
     * Is this subscription connected by having at least one open publication {@link Image}.
     *
     * @return true if this subscription connected by having at least one open publication {@link Image}.
     */
    public boolean isConnected()
    {
        for (final Image image : images)
        {
            if (!image.isClosed())
            {
                return true;
            }
        }

        return false;
    }

    /**
     * Has this subscription currently no {@link Image}s?
     *
     * @return has subscription currently no {@link Image}s?
     */
    public boolean hasNoImages()
    {
        return images.length == 0;
    }

    /**
     * Count of {@link Image}s associated to this subscription.
     *
     * @return count of {@link Image}s associated to this subscription.
     */
    public int imageCount()
    {
        return images.length;
    }

    /**
     * Return the {@link Image} associated with the given sessionId.
     *
     * @param sessionId associated with the {@link Image}.
     * @return Image associated with the given sessionId or null if no Image exist.
     */
    public Image imageBySessionId(final int sessionId)
    {
        Image result = null;

        for (final Image image : images)
        {
            if (sessionId == image.sessionId())
            {
                result = image;
                break;
            }
        }

        return result;
    }

    /**
     * Get the {@link Image} at the given index from the images array.
     *
     * @param index in the array.
     * @return image at given index.
     * @throws ArrayIndexOutOfBoundsException if the index is not valid.
     */
    public Image imageAtIndex(final int index)
    {
        return images[index];
    }

    /**
     * Get a {@link List} of active {@link Image}s that match this subscription.
     *
     * @return an unmodifiable {@link List} of active {@link Image}s that match this subscription.
     */
    public List<Image> images()
    {
        return Collections.unmodifiableList(Arrays.asList(images));
    }

    /**
     * Iterate over the {@link Image}s for this subscription.
     *
     * @param consumer to handle each {@link Image}.
     */
    public void forEachImage(final Consumer<Image> consumer)
    {
        for (final Image image : images)
        {
            consumer.accept(image);
        }
    }

    /**
     * Close the Subscription so that associated {@link Image}s can be released.
     * <p>
     * This method is idempotent.
     */
    public void close()
    {
        if (!isClosed)
        {
            conductor.removeSubscription(this);
        }
    }

    /**
     * Has this object been closed and should no longer be used?
     *
     * @return true if it has been closed otherwise false.
     */
    public boolean isClosed()
    {
        return isClosed;
    }

    /**
     * Get the status of the media channel for this Subscription.
     * <p>
     * The status will be {@link io.aeron.status.ChannelEndpointStatus#ERRORED} if a socket exception occurs on setup
     * and {@link io.aeron.status.ChannelEndpointStatus#ACTIVE} if all is well.
     *
     * @return status for the channel as one of the constants from {@link ChannelEndpointStatus} with it being
     * {@link ChannelEndpointStatus#NO_ID_ALLOCATED} if the subscription is closed.
     * @see io.aeron.status.ChannelEndpointStatus
     */
    public long channelStatus()
    {
        if (isClosed)
        {
            return ChannelEndpointStatus.NO_ID_ALLOCATED;
        }

        return conductor.channelStatus(channelStatusId);
    }

    /**
     * Get the counter used to represent the channel status for this Subscription.
     *
     * @return the counter used to represent the channel status for this Subscription.
     */
    public int channelStatusId()
    {
        return channelStatusId;
    }

    /**
     * Fetches the local socket addresses for this subscription. If the channel is not
     * {@link io.aeron.status.ChannelEndpointStatus#ACTIVE}, then this will return an empty list.
     * <p>
     * The format is as follows:
     * <br>
     * <br>
     * IPv4: <code>ip address:port</code>
     * <br>
     * IPv6: <code>[ip6 address]:port</code>
     * <br>
     * <br>
     * This is to match the formatting used in the Aeron URI.
     *
     * @return {@link List} of local socket addresses for this subscription.
     * @see #channelStatus()
     */
    public List<String> localSocketAddresses()
    {
        return LocalSocketAddressStatus.findAddresses(conductor.countersReader(), channelStatus(), channelStatusId);
    }

    /**
     * Add a destination manually to a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to add.
     */
    public void addDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Subscription is closed");
        }

        conductor.addRcvDestination(registrationId, endpointChannel);
    }

    /**
     * Remove a previously added destination from a multi-destination Subscription.
     *
     * @param endpointChannel for the destination to remove.
     */
    public void removeDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Subscription is closed");
        }

        conductor.removeRcvDestination(registrationId, endpointChannel);
    }

    /**
     * Asynchronously add a destination manually to a multi-destination Subscription.
     * <p>
     * Errors will be delivered asynchronously to the {@link Aeron.Context#errorHandler()}. Completion can be
     * tracked by passing the returned correlation id to {@link Aeron#isCommandActive(long)}.
     *
     * @param endpointChannel for the destination to add.
     * @return the correlationId for the command.
     */
    public long asyncAddDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Subscription is closed");
        }

        return conductor.asyncAddRcvDestination(registrationId, endpointChannel);
    }

    /**
     * Asynchronously remove a previously added destination from a multi-destination Subscription.
     * <p>
     * Errors will be delivered asynchronously to the {@link Aeron.Context#errorHandler()}. Completion can be
     * tracked by passing the returned correlation id to {@link Aeron#isCommandActive(long)}.
     *
     * @param endpointChannel for the destination to remove.
     * @return the correlationId for the command.
     */
    public long asyncRemoveDestination(final String endpointChannel)
    {
        if (isClosed)
        {
            throw new AeronException("Subscription is closed");
        }

        return conductor.asyncRemoveRcvDestination(registrationId, endpointChannel);
    }

    /**
     * Resolve channel endpoint and replace it with the port from the ephemeral range when 0 was provided.
     *
     * @return original channel URI string if it does not have an endpoint (IPC, response channel, MDS) or if endpoint
     * is not using an ephemeral port, channel URI where ephemeral port is resolved to the actual bind port or
     * {@code null} if the channel is not {@link io.aeron.status.ChannelEndpointStatus#ACTIVE}.
     * @see #channelStatus()
     * @see #localSocketAddresses()
     */
    public String tryResolveChannelEndpointPort()
    {
        if (null == resolvedChannel)
        {
            if (null == channelUri)
            {
                final ChannelUri uri = ChannelUri.parse(channel);
                final String endpoint = uri.get(ENDPOINT_PARAM_NAME);
                if (null == endpoint ||
                    !endpoint.endsWith(":0") ||
                    MDC_CONTROL_MODE_MANUAL.equals(uri.get(MDC_CONTROL_MODE_PARAM_NAME)))
                {
                    resolvedChannel = channel;
                    return channel;
                }
                channelUri = uri;
            }

            final String endpoint = resolvedEndpoint();
            if (null != endpoint)
            {
                channelUri.replaceEndpointWildcardPort(endpoint);
                resolvedChannel = channelUri.toString();
            }
        }
        return resolvedChannel;
    }

    /**
     * Find the resolved endpoint for the channel. This may be {@code null} if MDS is used and no destination is yet
     * added. The result will similar to taking the first element returned from {@link #localSocketAddresses()}. If more
     * than one destination is added then the first found is returned.
     *
     * @return resolved endpoint or {@code null} if not found.
     * @see #channelStatus()
     * @see #localSocketAddresses()
     */
    public String resolvedEndpoint()
    {
        if (null == resolvedEndpoint)
        {
            resolvedEndpoint =
                LocalSocketAddressStatus.findAddress(conductor.countersReader(), channelStatus(), channelStatusId);
        }
        return resolvedEndpoint;
    }

    void channelStatusId(final int id)
    {
        channelStatusId = id;
    }

    void internalClose(final long lingerDurationNs)
    {
        final Image[] images = this.images;
        this.images = EMPTY_IMAGES;
        isClosed = true;
        conductor.closeImages(images, unavailableImageHandler, lingerDurationNs);
    }

    void addImage(final Image image)
    {
        images = ArrayUtil.add(images, image);
    }

    Image removeImage(final long correlationId)
    {
        final Image[] oldArray = images;
        Image removedImage = null;

        int i = 0;
        for (final Image image : oldArray)
        {
            if (image.correlationId() == correlationId)
            {
                removedImage = image;
                break;
            }

            i++;
        }

        if (null != removedImage)
        {
            removedImage.close();
            images = oldArray.length == 1 ? EMPTY_IMAGES : ArrayUtil.remove(oldArray, i);
            conductor.releaseLogBuffers(removedImage.logBuffers(), correlationId, NULL_VALUE);
        }

        return removedImage;
    }

    void rejectImage(final long correlationId, final long position, final String reason)
    {
        if (channel().startsWith(SPY_PREFIX))
        {
            throw new AeronException("spies can not reject images");
        }

        conductor.rejectImage(correlationId, position, reason);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "Subscription{" +
            "registrationId=" + registrationId +
            ", isClosed=" + isClosed +
            ", isConnected=" + isConnected() +
            ", streamId=" + streamId +
            ", channel='" + channel + '\'' +
            ", localSocketAddresses=" + localSocketAddresses() +
            ", imageCount=" + imageCount() +
            '}';
    }
}
