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
package io.aeron.samples.cluster.tutorial;

import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.cluster.codecs.EventCode;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.BackoffIdleStrategy;
import org.agrona.concurrent.IdleStrategy;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.samples.cluster.tutorial.BasicAuctionClusteredService.*;
import static io.aeron.samples.cluster.tutorial.BasicAuctionClusteredServiceNode.calculatePort;

/**
 * Client for communicating with {@link BasicAuctionClusteredService}.
 */
// tag::client[]
public class BasicAuctionClusterClient implements EgressListener
// end::client[]
{
    private final MutableDirectBuffer actionBidBuffer = new ExpandableArrayBuffer();
    private final IdleStrategy idleStrategy = new BackoffIdleStrategy();
    private final long customerId;
    private final int numOfBids;
    private final int bidIntervalMs;

    private long nextCorrelationId = 0;
    private long lastBidSeen = 100;

    static final String LOCAL_IP = System.getProperty(
        "aeron.cluster.tutorial.localIP", "127.0.0.1");

    /**
     * Construct a new cluster client for the auction.
     *
     * @param customerId    for the client.
     * @param numOfBids     to make as a client.
     * @param bidIntervalMs between the bids.
     */
    public BasicAuctionClusterClient(final long customerId, final int numOfBids, final int bidIntervalMs)
    {
        this.customerId = customerId;
        this.numOfBids = numOfBids;
        this.bidIntervalMs = bidIntervalMs;
    }

    /**
     * {@inheritDoc}
     */
    // tag::response[]
    public void onMessage(
        final long clusterSessionId,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        final long correlationId = buffer.getLong(offset + CORRELATION_ID_OFFSET);
        final long customerId = buffer.getLong(offset + CUSTOMER_ID_OFFSET);
        final long currentPrice = buffer.getLong(offset + PRICE_OFFSET);
        final boolean bidSucceed = 0 != buffer.getByte(offset + BID_SUCCEEDED_OFFSET);

        lastBidSeen = currentPrice;

        printOutput(
            "SessionMessage(" + clusterSessionId + ", " + correlationId + "," +
            customerId + ", " + currentPrice + ", " + bidSucceed + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onSessionEvent(
        final long correlationId,
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final EventCode code,
        final String detail)
    {
        printOutput(
            "SessionEvent(" + correlationId + ", " + leadershipTermId + ", " +
            leaderMemberId + ", " + code + ", " + detail + ")");
    }

    /**
     * {@inheritDoc}
     */
    public void onNewLeader(
        final long clusterSessionId,
        final long leadershipTermId,
        final int leaderMemberId,
        final String ingressEndpoints)
    {
        printOutput("NewLeader(" + clusterSessionId + ", " + leadershipTermId + ", " + leaderMemberId + ")");
    }
    // end::response[]

    private void bidInAuction(final AeronCluster aeronCluster)
    {
        long keepAliveDeadlineMs = 0;
        long nextBidDeadlineMs = System.currentTimeMillis() + ThreadLocalRandom.current().nextInt(1000);
        int bidsLeftToSend = numOfBids;

        while (!Thread.currentThread().isInterrupted())
        {
            final long currentTimeMs = System.currentTimeMillis();

            if (nextBidDeadlineMs <= currentTimeMs && bidsLeftToSend > 0)
            {
                final long price = lastBidSeen + ThreadLocalRandom.current().nextInt(10);
                final long correlationId = sendBid(aeronCluster, price);

                nextBidDeadlineMs = currentTimeMs + ThreadLocalRandom.current().nextInt(bidIntervalMs);
                keepAliveDeadlineMs = currentTimeMs + 1_000;       // <1>
                --bidsLeftToSend;

                printOutput(
                    "Sent(" + (correlationId) + ", " + customerId + ", " + price + ") bidsRemaining=" +
                    bidsLeftToSend);
            }
            else if (keepAliveDeadlineMs <= currentTimeMs)         // <2>
            {
                if (bidsLeftToSend > 0)
                {
                    aeronCluster.sendKeepAlive();
                    keepAliveDeadlineMs = currentTimeMs + 1_000;   // <3>
                }
                else
                {
                    break;
                }
            }

            idleStrategy.idle(aeronCluster.pollEgress());
        }
    }

    // tag::publish[]
    private long sendBid(final AeronCluster aeronCluster, final long price)
    {
        final long correlationId = nextCorrelationId++;
        actionBidBuffer.putLong(CORRELATION_ID_OFFSET, correlationId);            // <1>
        actionBidBuffer.putLong(CUSTOMER_ID_OFFSET, customerId);
        actionBidBuffer.putLong(PRICE_OFFSET, price);

        idleStrategy.reset();
        while (aeronCluster.offer(actionBidBuffer, 0, BID_MESSAGE_LENGTH) < 0)    // <2>
        {
            idleStrategy.idle(aeronCluster.pollEgress());                         // <3>
        }

        return correlationId;
    }
    // end::publish[]

    /**
     * Ingress endpoints generated from a list of hostnames.
     *
     * @param hostnames for the cluster members.
     * @return a formatted string of ingress endpoints for connecting to a cluster.
     */
    public static String ingressEndpoints(final List<String> hostnames)
    {
        final StringBuilder sb = new StringBuilder();
        for (int i = 0; i < hostnames.size(); i++)
        {
            sb.append(i).append('=');
            sb.append(hostnames.get(i)).append(':').append(
                calculatePort(i, BasicAuctionClusteredServiceNode.CLIENT_FACING_PORT_OFFSET));
            sb.append(',');
        }

        sb.setLength(sb.length() - 1);

        return sb.toString();
    }

    private void printOutput(final String message)
    {
        System.out.println(message);
    }

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        final int customerId = Integer.parseInt(System.getProperty("aeron.cluster.tutorial.customerId"));       // <1>
        final int numOfBids = Integer.parseInt(System.getProperty("aeron.cluster.tutorial.numOfBids"));         // <2>
        final int bidIntervalMs = Integer.parseInt(System.getProperty("aeron.cluster.tutorial.bidIntervalMs")); // <3>

        final String[] hostnames = System.getProperty(
            "aeron.cluster.tutorial.hostnames", "localhost,localhost,localhost").split(",");
        final String ingressEndpoints = ingressEndpoints(Arrays.asList(hostnames));

        final BasicAuctionClusterClient client = new BasicAuctionClusterClient(customerId, numOfBids, bidIntervalMs);

        // tag::connect[]
        try (
            MediaDriver mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()                      // <1>
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true));
            AeronCluster aeronCluster = AeronCluster.connect(
                new AeronCluster.Context()
                .egressListener(client)                                                                         // <2>
                .egressChannel("aeron:udp?endpoint=" + LOCAL_IP + ":0")                                         // <3>
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .ingressChannel("aeron:udp")                                                                    // <4>
                .ingressEndpoints(ingressEndpoints)))                                                           // <5>
        {
        // end::connect[]
            client.bidInAuction(aeronCluster);
        }
    }
}
