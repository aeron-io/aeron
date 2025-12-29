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
package io.aeron.cluster.service;

import io.aeron.DirectBufferVector;
import io.aeron.Publication;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.DirectBuffer;

/**
 * Session representing a connected external client to the cluster.
 */
public interface ClientSession
{
    /**
     * Return value to indicate egress to a session is mocked out by the cluster when in follower mode.
     */
    long MOCKED_OFFER = 1;

    /**
     * Cluster session identity uniquely allocated when the session was opened.
     *
     * @return the cluster session identity uniquely allocated when the session was opened.
     */
    long id();

    /**
     * The response channel stream id for responding to the client.
     *
     * @return response channel stream id for responding to the client.
     */
    int responseStreamId();

    /**
     * The response channel for responding to the client.
     *
     * @return response channel for responding to the client.
     */
    String responseChannel();

    /**
     * Cluster session encoded principal from when the session was authenticated.
     *
     * @return The encoded Principal passed. May be 0 length to indicate none present.
     */
    byte[] encodedPrincipal();

    /**
     * Close of this {@link ContainerClientSession} by sending the request to the consensus module.
     * <p>
     * This method is idempotent.
     */
    void close();

    /**
     * Indicates that a request to close this session has been made.
     *
     * @return whether a request to close this session has been made.
     */
    boolean isClosing();

    /**
     * Non-blocking publish of a partial buffer containing a message to a cluster.
     *
     * @param buffer containing message.
     * @param offset offset in the buffer at which the encoded message begins.
     * @param length in bytes of the encoded message.
     * @return the same as {@link Publication#offer(DirectBuffer, int, int)} when in {@link Cluster.Role#LEADER},
     * otherwise {@link #MOCKED_OFFER} when a follower.
     */
    long offer(DirectBuffer buffer, int offset, int length);

    /**
     * Non-blocking publish by gathering buffer vectors into a message. The first vector will be replaced by the cluster
     * egress header so must be left unused.
     *
     * @param vectors which make up the message.
     * @return the same as {@link Publication#offer(DirectBufferVector[])}.
     * @see Publication#offer(DirectBufferVector[]) when in {@link Cluster.Role#LEADER},
     * otherwise {@link #MOCKED_OFFER} when a follower.
     */
    long offer(DirectBufferVector[] vectors);

    /**
     * Try to claim a range in the publication log into which a message can be written with zero copy semantics.
     * Once the message has been written then {@link BufferClaim#commit()} should be called thus making it available.
     * <p>
     * On successful claim, the Cluster egress header will be written to the start of the claimed buffer section.
     * Clients <b>MUST</b> write into the claimed buffer region at offset + {@link AeronCluster#SESSION_HEADER_LENGTH}.
     * <pre>{@code
     *     final DirectBuffer srcBuffer = acquireMessage();
     *
     *     if (session.tryClaim(length, bufferClaim) > 0L)
     *     {
     *         try
     *         {
     *              final MutableDirectBuffer buffer = bufferClaim.buffer();
     *              final int offset = bufferClaim.offset();
     *              // ensure that data is written at the correct offset
     *              buffer.putBytes(offset + AeronCluster.SESSION_HEADER_LENGTH, srcBuffer, 0, length);
     *         }
     *         finally
     *         {
     *             bufferClaim.commit();
     *         }
     *     }
     * }</pre>
     *
     * @param length      of the range to claim in bytes. The additional bytes for the session header will be added.
     * @param bufferClaim to be populated if the claim succeeds.
     * @return The new stream position, otherwise a negative error value as specified in
     *         {@link io.aeron.Publication#tryClaim(int, BufferClaim)} when in {@link Cluster.Role#LEADER},
     *         otherwise {@link #MOCKED_OFFER} when a follower.
     * @throws IllegalArgumentException if the length is greater than {@link io.aeron.Publication#maxPayloadLength()}.
     * @see Publication#tryClaim(int, BufferClaim)
     * @see BufferClaim#commit()
     * @see BufferClaim#abort()
     */
    long tryClaim(int length, BufferClaim bufferClaim);

    /**
     * Get the maximum payload length that can be sent in a single message via {@link #tryClaim(int, BufferClaim)}.
     * <p>
     * This allows service implementations to determine the maximum message size enabling them to choose between
     * {@link #tryClaim(int, BufferClaim)} for small messages (zero-copy) and {@link #offer(DirectBuffer, int, int)}
     * for larger messages (automatic fragmentation).
     *
     * @return the maximum payload length in bytes.
     */
    int maxPayloadLength();

    /**
     * Get the maximum message length that can be sent in a single message via {@link #offer(DirectBuffer, int, int)}.
     *
     * @return the maximum message length in bytes.
     */
    int maxMessageLength();
}
