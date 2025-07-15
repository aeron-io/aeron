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
package io.aeron.driver.media;

import io.aeron.driver.DriverConductorProxy;
import io.aeron.driver.MediaDriver;
import io.aeron.status.ChannelEndpointStatus;
import io.aeron.status.LocalSocketAddressStatus;
import org.agrona.CloseHelper;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;

abstract class ReceiveDestinationTransportLhsPadding extends UdpChannelTransport
{
    byte p000, p001, p002, p003, p004, p005, p006, p007, p008, p009, p010, p011, p012, p013, p014, p015;
    byte p016, p017, p018, p019, p020, p021, p022, p023, p024, p025, p026, p027, p028, p029, p030, p031;
    byte p032, p033, p034, p035, p036, p037, p038, p039, p040, p041, p042, p043, p044, p045, p046, p047;
    byte p048, p049, p050, p051, p052, p053, p054, p055, p056, p057, p058, p059, p060, p061, p062, p063;

    ReceiveDestinationTransportLhsPadding(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final MediaDriver.Context context,
        final int socketRcvbufLength,
        final int socketSndbufLength)
    {
        super(
            udpChannel, endPointAddress, bindAddress, connectAddress, context.receiverPortManager(),
            context, socketRcvbufLength, socketSndbufLength);
    }
}

abstract class ReceiveDestinationTransportHotFields extends ReceiveDestinationTransportLhsPadding
{
    long timeOfLastActivityNs;

    ReceiveDestinationTransportHotFields(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final MediaDriver.Context context,
        final int socketRcvbufLength,
        final int socketSndbufLength)
    {
        super(
            udpChannel, endPointAddress, bindAddress, connectAddress, context, socketRcvbufLength, socketSndbufLength);
    }
}

abstract class ReceiveDestinationTransportRhsPadding extends ReceiveDestinationTransportHotFields
{
    byte p064, p065, p066, p067, p068, p069, p070, p071, p072, p073, p074, p075, p076, p077, p078, p079;
    byte p080, p081, p082, p083, p084, p085, p086, p087, p088, p089, p090, p091, p092, p093, p094, p095;
    byte p096, p097, p098, p099, p100, p101, p102, p103, p104, p105, p106, p107, p108, p109, p110, p111;
    byte p112, p113, p114, p115, p116, p117, p118, p119, p120, p121, p122, p123, p124, p125, p126, p127;

    ReceiveDestinationTransportRhsPadding(
        final UdpChannel udpChannel,
        final InetSocketAddress endPointAddress,
        final InetSocketAddress bindAddress,
        final InetSocketAddress connectAddress,
        final MediaDriver.Context context,
        final int socketRcvbufLength,
        final int socketSndbufLength)
    {
        super(
            udpChannel, endPointAddress, bindAddress, connectAddress, context, socketRcvbufLength, socketSndbufLength);
    }
}

/**
 * Destination endpoint representation for reception into an image from a UDP transport.
 */
public final class ReceiveDestinationTransport extends ReceiveDestinationTransportRhsPadding
{
    private InetSocketAddress currentControlAddress;
    private final AtomicCounter localSocketAddressIndicator;

    /**
     * Construct transport for a receiving destination.
     *
     * @param udpChannel                  for the destination.
     * @param context                     for configuration.
     * @param localSocketAddressIndicator to indicate status of the transport.
     * @param receiveChannelEndpoint      to which this destination belongs.
     */
    public ReceiveDestinationTransport(
        final UdpChannel udpChannel,
        final MediaDriver.Context context,
        final AtomicCounter localSocketAddressIndicator,
        final ReceiveChannelEndpoint receiveChannelEndpoint)
    {
        super(
            udpChannel,
            udpChannel.remoteData(),
            udpChannel.remoteData(),
            null,
            context,
            receiveChannelEndpoint.socketRcvbufLength(),
            receiveChannelEndpoint.socketSndbufLength());

        this.timeOfLastActivityNs = context.receiverCachedNanoClock().nanoTime();
        this.currentControlAddress = udpChannel.hasExplicitControl() ? udpChannel.localControl() : null;
        this.localSocketAddressIndicator = localSocketAddressIndicator;
    }

    /**
     * Open the channel by the receiver.
     *
     * @param conductorProxy  for sending instructions by to the driver conductor.
     * @param statusIndicator for the channel.
     */
    public void openChannel(final DriverConductorProxy conductorProxy, final AtomicCounter statusIndicator)
    {
        openDatagramChannel(statusIndicator);

        LocalSocketAddressStatus.updateBindAddress(
            localSocketAddressIndicator, bindAddressAndPort(), context.countersMetaDataBuffer());
        localSocketAddressIndicator.setRelease(ChannelEndpointStatus.ACTIVE);
    }

    /**
     * Close the networking elements of the ReceiveChannelEndpoint, but leave other components (e.g. counters) in place.
     */
    public void closeTransport()
    {
        super.close();
    }

    /**
     * Close indicator counters associated with the transport.
     */
    public void closeIndicators()
    {
        CloseHelper.close(localSocketAddressIndicator);
    }

    /**
     * Has the channel explicit control address.
     *
     * @return true if the channel has explicit control address.
     */
    public boolean hasExplicitControl()
    {
        return udpChannel.hasExplicitControl();
    }

    /**
     * Get the explicit control address for the channel.
     *
     * @return the explicit control address for the channel if set, otherwise null.
     */
    public InetSocketAddress explicitControlAddress()
    {
        return udpChannel.hasExplicitControl() ? currentControlAddress : null;
    }

    /**
     * Store the {@link SelectionKey} for the registered transport.
     *
     * @param key the {@link SelectionKey} for the registered transport.
     */
    public void selectionKey(final SelectionKey key)
    {
        selectionKey = key;
    }

    /**
     * Store the time of last activity for the destination.
     *
     * @param nowNs the time of last activity for the destination.
     */
    public void timeOfLastActivityNs(final long nowNs)
    {
        this.timeOfLastActivityNs = nowNs;
    }

    /**
     * Get the time of last activity for the destination.
     *
     * @return the time of last activity for the destination.
     */
    public long timeOfLastActivityNs()
    {
        return timeOfLastActivityNs;
    }

    /**
     * {@link UdpChannel} associated with the destination.
     *
     * @return the {@link UdpChannel} associated with the destination.
     */
    public UdpChannel udpChannel()
    {
        return udpChannel;
    }

    /**
     * Store the time current control address for the destination.
     *
     * @param newAddress control address for the destination.
     */
    public void currentControlAddress(final InetSocketAddress newAddress)
    {
        this.currentControlAddress = newAddress;
    }

    /**
     * The current control address for the destination.
     *
     * @return the time current control address for the destination.
     */
    public InetSocketAddress currentControlAddress()
    {
        return currentControlAddress;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ReceiveDestinationTransport{" +
            "currentControlAddress=" + currentControlAddress +
            ", localSocketAddressIndicator=" + localSocketAddressIndicator +
            '}';
    }
}
