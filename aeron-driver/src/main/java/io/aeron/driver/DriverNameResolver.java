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

import io.aeron.AeronCounters;
import io.aeron.driver.media.NetworkUtil;
import io.aeron.driver.media.UdpChannel;
import io.aeron.driver.media.UdpNameResolutionTransport;
import io.aeron.driver.status.SystemCounterDescriptor;
import io.aeron.protocol.HeaderFlyweight;
import io.aeron.protocol.ResolutionEntryFlyweight;
import org.agrona.BufferUtil;
import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import org.agrona.collections.ArrayListUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static io.aeron.driver.DriverNameResolverCache.fullLengthMatch;
import static io.aeron.driver.media.NetworkUtil.formatAddressAndPort;
import static io.aeron.protocol.ResolutionEntryFlyweight.*;
import static org.agrona.BitUtil.CACHE_LINE_LENGTH;

/**
 * Default {@link NameResolver} for the {@link MediaDriver}.
 */
final class DriverNameResolver implements AutoCloseable, UdpNameResolutionTransport.UdpFrameHandler, NameResolver
{
    static NameResolver bootstrapNameResolver = DefaultNameResolver.INSTANCE;
    private static final String RESOLVER_NEIGHBORS_COUNTER_LABEL = "Resolver neighbors";

    // TODO: make these configurable
    private static final long SELF_RESOLUTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);
    static final long NEIGHBOR_RESOLUTION_INTERVAL_MS = TimeUnit.SECONDS.toMillis(2);
    private static final long TIMEOUT_MS = TimeUnit.SECONDS.toMillis(10);
    private static final long WORK_INTERVAL_MS = 10;

    private final ByteBuffer byteBuffer = BufferUtil.allocateDirectAligned(
        Configuration.MAX_UDP_PAYLOAD_LENGTH, CACHE_LINE_LENGTH);
    private final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(byteBuffer);
    private final HeaderFlyweight headerFlyweight = new HeaderFlyweight(unsafeBuffer);
    private final ResolutionEntryFlyweight resolutionEntryFlyweight = new ResolutionEntryFlyweight();
    private final ArrayList<Neighbor> neighborList = new ArrayList<>();

    private final UdpNameResolutionTransport transport;
    private final DriverNameResolverCache cache;
    private final NameResolver delegateResolver;
    private final AtomicCounter invalidPackets;
    private final AtomicCounter shortSends;
    private final AtomicCounter neighborsCounter;
    private final AtomicCounter cacheEntriesCounter;
    private final byte[] nameTempBuffer = new byte[ResolutionEntryFlyweight.MAX_NAME_LENGTH];
    private final byte[] addressTempBuffer = new byte[ResolutionEntryFlyweight.ADDRESS_LENGTH_IP6];

    private final String localDriverName;
    private InetSocketAddress localSocketAddress;
    private final byte[] localName;
    private byte[] localAddress;

    private final String[] bootstrapNeighbors;
    private InetSocketAddress bootstrapNeighborAddress;
    private long bootstrapNeighborResolveDeadlineMs;

    private final long neighborTimeoutMs = TIMEOUT_MS;
    private final long selfResolutionIntervalMs = SELF_RESOLUTION_INTERVAL_MS;
    private final long neighborResolutionIntervalMs = NEIGHBOR_RESOLUTION_INTERVAL_MS;
    private final int mtuLength;
    private final boolean preferIPv6 = false;

    private long workDeadlineMs = 0;
    private long selfResolutionDeadlineMs;
    private long neighborResolutionDeadlineMs;

    DriverNameResolver(final MediaDriver.Context ctx)
    {
        mtuLength = ctx.mtuLength();
        invalidPackets = ctx.systemCounters().get(SystemCounterDescriptor.INVALID_PACKETS);
        shortSends = ctx.systemCounters().get(SystemCounterDescriptor.SHORT_SENDS);
        delegateResolver = ctx.nameResolver();

        localDriverName = ctx.resolverName();
        localName = localDriverName.getBytes(StandardCharsets.US_ASCII);
        localSocketAddress = UdpNameResolutionTransport.getInterfaceAddress(ctx.resolverInterface());
        localAddress = localSocketAddress.getAddress().getAddress();

        bootstrapNeighbors = null != ctx.resolverBootstrapNeighbor() ?
            ctx.resolverBootstrapNeighbor().split(",") : null;
        if (null != bootstrapNeighbors)
        {
            final long nowNs = ctx.nanoClock().nanoTime();
            final DutyCycleTracker nameResolverTimeTracker = ctx.nameResolverTimeTracker();
            nameResolverTimeTracker.update(nowNs);

            bootstrapNeighborAddress = resolveBootstrapNeighbor();

            final long endNs = ctx.nanoClock().nanoTime();
            nameResolverTimeTracker.measureAndUpdate(endNs);
        }
        else
        {
            bootstrapNeighborAddress = null;
        }

        final long nowMs = ctx.epochClock().time();
        bootstrapNeighborResolveDeadlineMs = nowMs + TIMEOUT_MS;

        selfResolutionDeadlineMs = 0;
        neighborResolutionDeadlineMs = nowMs + neighborResolutionIntervalMs;

        cache = new DriverNameResolverCache(TIMEOUT_MS);

        final UdpChannel resolverChannel =
            UdpChannel.parse("aeron:udp?endpoint=" +
                NetworkUtil.formatAddressAndPort(localSocketAddress.getAddress(), localSocketAddress.getPort()),
                delegateResolver);
        transport = new UdpNameResolutionTransport(resolverChannel, localSocketAddress, unsafeBuffer, ctx);

        neighborsCounter = ctx.countersManager().newCounter(
            RESOLVER_NEIGHBORS_COUNTER_LABEL, AeronCounters.NAME_RESOLVER_NEIGHBORS_COUNTER_TYPE_ID);
        cacheEntriesCounter = ctx.countersManager().newCounter(
            "Resolver cache entries: name=" + localDriverName,
            AeronCounters.NAME_RESOLVER_CACHE_ENTRIES_COUNTER_TYPE_ID);

        openDatagramChannel();
    }

    /**
     * {@inheritDoc}
     */
    public void close()
    {
        CloseHelper.close(transport);
    }

    /**
     * {@inheritDoc}
     */
    public int doWork(final long nowMs)
    {
        int workCount = 0;

        if (workDeadlineMs - nowMs < 0)
        {
            workDeadlineMs = nowMs + WORK_INTERVAL_MS;
            workCount += transport.poll(this, nowMs);
            workCount += cache.timeoutOldEntries(nowMs, cacheEntriesCounter);
            workCount += timeoutNeighbors(nowMs);

            if (nowMs > selfResolutionDeadlineMs)
            {
                sendSelfResolutions(nowMs);
            }

            if (nowMs > neighborResolutionDeadlineMs)
            {
                sendNeighborResolutions(nowMs);
            }
        }

        return workCount;
    }

    /**
     * {@inheritDoc}
     */
    public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
    {
        DriverNameResolverCache.CacheEntry entry;

        if (preferIPv6)
        {
            entry = cache.lookup(name, RES_TYPE_NAME_TO_IP6_MD);
            if (null == entry)
            {
                entry = cache.lookup(name, RES_TYPE_NAME_TO_IP4_MD);
            }
        }
        else
        {
            entry = cache.lookup(name, RES_TYPE_NAME_TO_IP4_MD);
        }

        InetAddress resolvedAddress = null;
        try
        {
            if (null == entry)
            {
                if (name.equals(localDriverName))
                {
                    return localSocketAddress.getAddress();
                }

                return delegateResolver.resolve(name, uriParamName, isReResolution);
            }

            resolvedAddress = InetAddress.getByAddress(entry.address);
        }
        catch (final UnknownHostException ignore)
        {
        }

        return resolvedAddress;
    }

    /**
     * {@inheritDoc}
     */
    public String lookup(final String name, final String uriParamName, final boolean isReLookup)
    {
        // here we would look up advertised endpoints/control IP:port pairs by name. Currently, we just return delegate.
        return delegateResolver.lookup(name, uriParamName, isReLookup);
    }

    /**
     * {@inheritDoc}
     */
    public int onFrame(
        final UnsafeBuffer unsafeBuffer, final int length, final InetSocketAddress srcAddress, final long nowMs)
    {
        if (headerFlyweight.headerType() == HDR_TYPE_RES)
        {
            int offset = MIN_HEADER_LENGTH;

            while (length > offset)
            {
                resolutionEntryFlyweight.wrap(unsafeBuffer, offset, length - offset);

                if ((length - offset) < resolutionEntryFlyweight.entryLength())
                {
                    invalidPackets.increment();
                    return 0;
                }

                onResolutionEntry(resolutionEntryFlyweight, srcAddress, nowMs);

                offset += resolutionEntryFlyweight.entryLength();
            }

            return length;
        }

        return 0;
    }

    private void openDatagramChannel()
    {
        transport.openDatagramChannel(null);

        final InetSocketAddress boundAddress = transport.boundAddress();
        if (null != boundAddress)
        {
            localSocketAddress = boundAddress;
            localAddress = boundAddress.getAddress().getAddress();

            neighborsCounter.updateLabel(buildNeighborsCounterLabel());
        }
    }

    private String buildNeighborsCounterLabel()
    {
        final StringBuilder builder = new StringBuilder(RESOLVER_NEIGHBORS_COUNTER_LABEL);
        builder.append(": bound ").append(transport.bindAddressAndPort());

        if (null != bootstrapNeighborAddress)
        {
            builder.append(" bootstrap ").append(
                formatAddressAndPort(bootstrapNeighborAddress.getAddress(), bootstrapNeighborAddress.getPort()));
        }

        return builder.toString();
    }

    private int timeoutNeighbors(final long nowMs)
    {
        int workCount = 0;

        final ArrayList<Neighbor> neighborList = this.neighborList;
        for (int lastIndex = neighborList.size() - 1, i = lastIndex; i >= 0; i--)
        {
            final Neighbor neighbor = neighborList.get(i);

            if (nowMs > (neighbor.timeOfLastActivityMs + neighborTimeoutMs))
            {
                Neighbor.neighborRemoved(nowMs, neighbor.socketAddress);
                ArrayListUtil.fastUnorderedRemove(neighborList, i, lastIndex--);
                workCount++;
            }
        }

        final int neighborCount = neighborList.size();
        if (neighborsCounter.getWeak() != neighborCount)
        {
            neighborsCounter.setRelease(neighborCount);
        }

        return workCount;
    }

    private void sendSelfResolutions(final long nowMs)
    {
        byteBuffer.clear();

        final int currentOffset = HeaderFlyweight.MIN_HEADER_LENGTH;
        final byte resType = preferIPv6 ? RES_TYPE_NAME_TO_IP6_MD : RES_TYPE_NAME_TO_IP4_MD;

        headerFlyweight
            .headerType(HeaderFlyweight.HDR_TYPE_RES)
            .flags((short)0)
            .version(HeaderFlyweight.CURRENT_VERSION);

        resolutionEntryFlyweight.wrap(unsafeBuffer, currentOffset, unsafeBuffer.capacity() - currentOffset);
        resolutionEntryFlyweight
            .resType(resType)
            .flags(SELF_FLAG)
            .udpPort((short)localSocketAddress.getPort())
            .ageInMs(0)
            .putAddress(localAddress)
            .putName(localName);

        final int length = resolutionEntryFlyweight.entryLength() + MIN_HEADER_LENGTH;
        headerFlyweight.frameLength(length);

        byteBuffer.limit(length);

        boolean sendToBootstrap = null != bootstrapNeighborAddress;
        for (final Neighbor neighbor : neighborList)
        {
            sendResolutionFrameTo(byteBuffer, neighbor.socketAddress);

            if (sendToBootstrap && neighbor.socketAddress.equals(bootstrapNeighborAddress))
            {
                sendToBootstrap = false;
            }
        }

        if (sendToBootstrap)
        {
            if (nowMs > bootstrapNeighborResolveDeadlineMs)
            {
                final InetSocketAddress oldAddress = this.bootstrapNeighborAddress;
                bootstrapNeighborAddress = resolveBootstrapNeighbor();
                bootstrapNeighborResolveDeadlineMs = nowMs + TIMEOUT_MS;

                if (!oldAddress.equals(bootstrapNeighborAddress))
                {
                    neighborsCounter.updateLabel(buildNeighborsCounterLabel());

                    // avoid sending resolution frame if new bootstrap is in the neighbors list
                    for (final Neighbor neighbor : neighborList)
                    {
                        if (neighbor.socketAddress.equals(bootstrapNeighborAddress))
                        {
                            sendToBootstrap = false;
                            break;
                        }
                    }
                }
            }

            if (sendToBootstrap)
            {
                sendResolutionFrameTo(byteBuffer, bootstrapNeighborAddress);
            }
        }

        selfResolutionDeadlineMs = nowMs + selfResolutionIntervalMs;
    }

    private void sendResolutionFrameTo(final ByteBuffer buffer, final InetSocketAddress remoteAddress)
    {
        buffer.position(0);

        final int bytesRemaining = buffer.remaining();
        final int bytesSent = transport.sendTo(buffer, remoteAddress);

        if (0 <= bytesSent && bytesSent < bytesRemaining)
        {
            shortSends.increment();
        }
    }

    private void onResolutionEntry(
        final ResolutionEntryFlyweight resolutionEntry, final InetSocketAddress srcAddress, final long nowMs)
    {
        final byte resType = resolutionEntry.resType();
        final boolean isSelf = SELF_FLAG == resolutionEntryFlyweight.flags();
        byte[] addr = addressTempBuffer;

        final int addressLength = resolutionEntryFlyweight.getAddress(addressTempBuffer);
        if (isSelf && ResolutionEntryFlyweight.isAnyLocalAddress(addressTempBuffer, addressLength))
        {
            addr = srcAddress.getAddress().getAddress();
        }

        final int nameLength = resolutionEntryFlyweight.getName(nameTempBuffer);
        final long timeOfLastActivity = nowMs - resolutionEntryFlyweight.ageInMs();
        final int port = resolutionEntryFlyweight.udpPort();

        // use name and port to indicate it is from this resolver instead of searching interfaces
        if (port == localSocketAddress.getPort() && fullLengthMatch(localName, nameTempBuffer, nameLength))
        {
            return;
        }

        cache.addOrUpdateEntry(
            nameTempBuffer, nameLength, timeOfLastActivity, resType, addr, port, cacheEntriesCounter);

        Neighbor neighbor = findNeighborByAddress(addr, addressLength, port);
        if (null == neighbor)
        {
            try
            {
                final byte[] neighborAddress = Arrays.copyOf(addr, addressLength);
                neighbor = new Neighbor(
                    new InetSocketAddress(InetAddress.getByAddress(neighborAddress), port),
                    neighborAddress,
                    timeOfLastActivity);
                Neighbor.neighborAdded(nowMs, neighbor.socketAddress);
                neighborList.add(neighbor);
                neighborsCounter.setRelease(neighborList.size());
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
        else if (isSelf)
        {
            neighbor.timeOfLastActivityMs = timeOfLastActivity;
        }
    }

    private Neighbor findNeighborByAddress(final byte[] address, final int addressLength, final int port)
    {
        for (final Neighbor neighbor : neighborList)
        {
            if (port == neighbor.socketAddress.getPort() &&
                fullLengthMatch(neighbor.neighborAddress, address, addressLength))
            {
                return neighbor;
            }
        }

        return null;
    }

    private void sendNeighborResolutions(final long nowMs)
    {
        for (final DriverNameResolverCache.Iterator iter = cache.resetIterator(); iter.hasNext(); )
        {
            byteBuffer.clear();
            headerFlyweight
                .headerType(HeaderFlyweight.HDR_TYPE_RES)
                .flags((short)0)
                .version(HeaderFlyweight.CURRENT_VERSION);

            int currentOffset = HeaderFlyweight.MIN_HEADER_LENGTH;

            while (iter.hasNext())
            {
                final DriverNameResolverCache.CacheEntry entry = iter.next();

                if (currentOffset + entryLengthRequired(entry.type, entry.name.length) > mtuLength)
                {
                    iter.rewindNext();
                    break;
                }

                resolutionEntryFlyweight.wrap(unsafeBuffer, currentOffset, unsafeBuffer.capacity() - currentOffset);
                resolutionEntryFlyweight
                    .resType(entry.type)
                    .flags((short)0)
                    .udpPort((short)entry.port)
                    .ageInMs((int)(nowMs - entry.timeOfLastActivityMs))
                    .putAddress(entry.address)
                    .putName(entry.name);

                final int length = resolutionEntryFlyweight.entryLength();
                currentOffset += length;
            }

            headerFlyweight.frameLength(currentOffset);
            byteBuffer.limit(currentOffset);

            for (final Neighbor neighbor : neighborList)
            {
                sendResolutionFrameTo(byteBuffer, neighbor.socketAddress);
            }
        }

        neighborResolutionDeadlineMs = nowMs + neighborResolutionIntervalMs;
    }

    private InetSocketAddress resolveBootstrapNeighbor()
    {
        Exception t = null;
        for (final String neighbor : bootstrapNeighbors)
        {
            try
            {
                return UdpNameResolutionTransport.getInetSocketAddress(neighbor, bootstrapNameResolver);
            }
            catch (final Exception ex)
            {
                if (null == t)
                {
                    t = ex;
                }
                else
                {
                    t.addSuppressed(ex);
                }
            }
        }

        if (null != t)
        {
            LangUtil.rethrowUnchecked(t);
        }

        return null;
    }

    static class Neighbor
    {
        final InetSocketAddress socketAddress;
        final byte[] neighborAddress;
        long timeOfLastActivityMs;

        Neighbor(final InetSocketAddress socketAddress, final byte[] neighborAddress, final long nowMs)
        {
            this.socketAddress = socketAddress;
            this.neighborAddress = neighborAddress;
            this.timeOfLastActivityMs = nowMs;
        }

        static void neighborAdded(final long nowMs, final InetSocketAddress address)
        {
//            System.out.println(nowMs + " neighbor added: " + address);
        }

        static void neighborRemoved(final long nowMs, final InetSocketAddress address)
        {
//            System.out.println(nowMs + " neighbor removed: " + address);
        }
    }
}
