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
package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.security.NullCredentialsSupplier;

import java.util.Objects;

/**
 * Contains the optional parameters that can be passed to a Replication Request. Controls the behaviour of the
 * replication including tagging, stop position, extending destination recordings, live merging, and setting the
 * maximum length of the file I/O operations.
 */
public class ReplicationParams
{
    private long stopPosition;
    private long dstRecordingId;
    private String liveDestination;
    private String replicationChannel;
    private long channelTagId;
    private long subscriptionTagId;
    private int fileIoMaxLength;
    private int replicationSessionId;
    private byte[] encodedCredentials;
    private String srcResponseChannel;
    private long maxReplayBytesPerSecond;

    /**
     * Initialise all parameters to defaults.
     */
    @SuppressWarnings("this-escape")
    public ReplicationParams()
    {
        reset();
    }

    /**
     * Reset the state of the parameters to the default for reuse.
     *
     * @return this for a fluent API.
     */
    public ReplicationParams reset()
    {
        stopPosition = AeronArchive.NULL_POSITION;
        dstRecordingId = Aeron.NULL_VALUE;
        liveDestination = null;
        replicationChannel = null;
        channelTagId = Aeron.NULL_VALUE;
        subscriptionTagId = Aeron.NULL_VALUE;
        fileIoMaxLength = Aeron.NULL_VALUE;
        replicationSessionId = Aeron.NULL_VALUE;
        encodedCredentials = NullCredentialsSupplier.NULL_CREDENTIAL;
        srcResponseChannel = null;
        maxReplayBytesPerSecond = 0;

        return this;
    }

    /**
     * Set the stop position for replication, default is {@link AeronArchive#NULL_POSITION}, which will continuously
     * replicate.
     *
     * @param stopPosition position to stop the replication at.
     * @return this for a fluent API
     */
    public ReplicationParams stopPosition(final long stopPosition)
    {
        this.stopPosition = stopPosition;
        return this;
    }

    /**
     * The stop position for this replication request.
     * @return stop position
     */
    public long stopPosition()
    {
        return stopPosition;
    }

    /**
     * The recording in the local archive to extend. Default is {@link Aeron#NULL_VALUE} which will trigger the creation
     * of a new recording in the destination archive.
     *
     * @param dstRecordingId destination recording to extend.
     * @return this for a fluent API.
     */
    public ReplicationParams dstRecordingId(final long dstRecordingId)
    {
        this.dstRecordingId = dstRecordingId;
        return this;
    }

    /**
     * Destination recording id to extend.
     *
     * @return destination recording id.
     */
    public long dstRecordingId()
    {
        return dstRecordingId;
    }

    /**
     * Destination for the live stream if merge is required. Default is null for no merge.
     *
     * @param liveChannel for the live stream merge
     * @return this for a fluent API.
     */
    public ReplicationParams liveDestination(final String liveChannel)
    {
        this.liveDestination = liveChannel;
        return this;
    }

    /**
     * Gets the destination for the live stream merge.
     *
     * @return destination for live stream merge.
     */
    public String liveDestination()
    {
        return liveDestination;
    }

    /**
     * Channel to use for replicating the recording, empty string will mean that the default channel is used.
     * @return channel to replicate the recording.
     */
    public String replicationChannel()
    {
        return replicationChannel;
    }

    /**
     * Channel use to replicate the recording. Default is null which will use the context's default replication
     * channel
     *
     * @param replicationChannel to use for replicating the recording.
     * @return this for a fluent API.
     */
    public ReplicationParams replicationChannel(final String replicationChannel)
    {
        this.replicationChannel = replicationChannel;
        return this;
    }

    /**
     * The channel used by the archive's subscription for replication will have the supplied channel tag applied to it.
     * The default value for channelTagId is {@link Aeron#NULL_VALUE}
     *
     * @param channelTagId tag to apply to the archive's subscription.
     * @return this for a fluent API
     */
    public ReplicationParams channelTagId(final long channelTagId)
    {
        this.channelTagId = channelTagId;
        return this;
    }

    /**
     * Gets channel tag id for the archive subscription.
     *
     * @return channel tag id.
     */
    public long channelTagId()
    {
        return channelTagId;
    }

    /**
     * The channel used by the archive's subscription for replication will have the supplied subscription tag applied to
     * it. The default value for subscriptionTagId is {@link Aeron#NULL_VALUE}
     *
     * @param subscriptionTagId tag to apply to the archive's subscription.
     * @return this for a fluent API
     */
    public ReplicationParams subscriptionTagId(final long subscriptionTagId)
    {
        this.subscriptionTagId = subscriptionTagId;
        return this;
    }

    /**
     * Gets subscription tag id for the archive subscription.
     *
     * @return subscription tag id.
     */
    public long subscriptionTagId()
    {
        return subscriptionTagId;
    }

    /**
     * The maximum size of a file operation when reading from the archive to execute the replication. Will use the value
     * defined in the context otherwise. This can be used reduce the size of file IO operations to lower the
     * priority of some replays. Setting it to a value larger than the context value will have no affect.
     *
     * @param fileIoMaxLength maximum length of a file I/O operation.
     * @return this for a fluent API
     */
    public ReplicationParams fileIoMaxLength(final int fileIoMaxLength)
    {
        this.fileIoMaxLength = fileIoMaxLength;
        return this;
    }

    /**
     * Gets the maximum length for file IO operations in the replay. Defaults to {@link Aeron#NULL_VALUE} if not
     * set, which will trigger the use of the Archive.Context default.
     *
     * @return maximum length of a file I/O operation.
     */
    public int fileIoMaxLength()
    {
        return this.fileIoMaxLength;
    }

    /**
     * Sets the session-id to be used for the replicated file instead of the session id from the source archive. This
     * is useful in cases where we are replicating the same recording in multiple stages.
     *
     * @param replicationSessionId the session-id to be set for the received recording.
     * @return this for fluent API
     */
    public ReplicationParams replicationSessionId(final int replicationSessionId)
    {
        this.replicationSessionId = replicationSessionId;
        return this;
    }

    /**
     * The session-id to be used for the replicated recording.
     *
     * @return session-id to be useful for the replicated recording.
     */
    public int replicationSessionId()
    {
        return this.replicationSessionId;
    }

    /**
     * Sets the encoded credentials that will be passed to the source archive for authentication. Currently only simple
     * authentication (i.e. not challenge/response) is supported for replication.
     *
     * @param encodedCredentials credentials to be passed to the source archive.
     * @return this for a fluent API.
     */
    public ReplicationParams encodedCredentials(final byte[] encodedCredentials)
    {
        this.encodedCredentials = encodedCredentials;
        return this;
    }

    /**
     * Gets the encoded credentials that will be used to authenticate against the source archive.
     *
     * @return encoded credentials used for authentication.
     */
    public byte[] encodedCredentials()
    {
        return encodedCredentials;
    }

    /**
     * Control address of the source archive to use response channels during replication.
     *
     * @param responseChannel control address of the response publication for the source archive.
     * @return this for a fluent API.
     */
    public ReplicationParams srcResponseChannel(final String responseChannel)
    {
        this.srcResponseChannel = responseChannel;
        return this;
    }

    /**
     * Control address of the source archive to use response channels during replication.
     *
     * @return control address of the response publication for the source archive.
     */
    public String srcResponseChannel()
    {
        return srcResponseChannel;
    }

    /**
     * Limit the throughput of this replication to at most the given number of bytes per second, so a large or
     * catch-up replication (e.g. a standby snapshot pulled onto a cluster node) is trickled in without saturating
     * the archive I/O or media driver and disturbing latency-sensitive work on the host. Enforced by the source
     * archive's replay. A value of {@code 0} (the default) or negative means unlimited.
     *
     * @param maxReplayBytesPerSecond maximum replication throughput in bytes per second, or {@code 0} for unlimited.
     * @return this for a fluent API.
     */
    public ReplicationParams maxReplayBytesPerSecond(final long maxReplayBytesPerSecond)
    {
        this.maxReplayBytesPerSecond = maxReplayBytesPerSecond;
        return this;
    }

    /**
     * The maximum replication throughput in bytes per second, or {@code 0} for unlimited (the default).
     *
     * @return maximum replication throughput in bytes per second.
     * @see #maxReplayBytesPerSecond(long)
     */
    public long maxReplayBytesPerSecond()
    {
        return maxReplayBytesPerSecond;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }
        final ReplicationParams that = (ReplicationParams)o;
        return stopPosition == that.stopPosition && dstRecordingId == that.dstRecordingId &&
            channelTagId == that.channelTagId && subscriptionTagId == that.subscriptionTagId &&
            fileIoMaxLength == that.fileIoMaxLength && replicationSessionId == that.replicationSessionId &&
            maxReplayBytesPerSecond == that.maxReplayBytesPerSecond &&
            Objects.equals(liveDestination, that.liveDestination) &&
            Objects.equals(replicationChannel, that.replicationChannel);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        return Objects.hash(
            stopPosition, dstRecordingId, liveDestination, replicationChannel, channelTagId, subscriptionTagId,
            fileIoMaxLength, replicationSessionId, maxReplayBytesPerSecond);
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "ReplicationParams{" +
            "stopPosition=" + stopPosition +
            ", dstRecordingId=" + dstRecordingId +
            ", liveDestination='" + liveDestination + '\'' +
            ", replicationChannel='" + replicationChannel + '\'' +
            ", channelTagId=" + channelTagId +
            ", subscriptionTagId=" + subscriptionTagId +
            ", fileIoMaxLength=" + fileIoMaxLength +
            ", replicationSessionId=" + replicationSessionId +
            ", maxReplayBytesPerSecond=" + maxReplayBytesPerSecond +
            '}';
    }
}
