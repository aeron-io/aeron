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

import io.aeron.*;
import io.aeron.cluster.client.ClusterException;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.RegistrationException;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.*;

import java.util.Arrays;

final class ContainerClientSession implements ClientSession
{
    private final long id;
    private final int responseStreamId;
    private final String responseChannel;
    private final byte[] encodedPrincipal;

    private final ClusteredServiceAgent clusteredServiceAgent;
    private Publication responsePublication;
    private boolean isClosing;

    ContainerClientSession(
        final long sessionId,
        final int responseStreamId,
        final String responseChannel,
        final byte[] encodedPrincipal,
        final ClusteredServiceAgent clusteredServiceAgent)
    {
        this.id = sessionId;
        this.responseStreamId = responseStreamId;
        this.responseChannel = responseChannel;
        this.encodedPrincipal = encodedPrincipal;
        this.clusteredServiceAgent = clusteredServiceAgent;
    }


    @Override
    public long id()
    {
        return id;
    }

    @Override
    public int responseStreamId()
    {
        return responseStreamId;
    }

    @Override
    public String responseChannel()
    {
        return responseChannel;
    }

    @Override
    public byte[] encodedPrincipal()
    {
        return encodedPrincipal;
    }

    @Override
    public void close()
    {
        if (null != clusteredServiceAgent.getClientSession(id))
        {
            clusteredServiceAgent.closeClientSession(id);
        }
    }

    @Override
    public boolean isClosing()
    {
        return isClosing;
    }

    @Override
    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        return clusteredServiceAgent.offer(id, responsePublication, buffer, offset, length);
    }

    @Override
    public long offer(final DirectBufferVector[] vectors)
    {
        return clusteredServiceAgent.offer(id, responsePublication, vectors);
    }

    @Override
    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        return clusteredServiceAgent.tryClaim(id, responsePublication, length, bufferClaim);
    }

    void connect(final Aeron aeron)
    {
        try
        {
            if (null == responsePublication)
            {
                responsePublication = aeron.addPublication(responseChannel, responseStreamId);
            }
        }
        catch (final RegistrationException ex)
        {
            clusteredServiceAgent.handleError(new ClusterException(
                "failed to connect session response publication: " + ex.getMessage(), AeronException.Category.WARN));
        }
    }

    void markClosing()
    {
        this.isClosing = true;
    }

    void resetClosing()
    {
        isClosing = false;
    }

    void disconnect(final ErrorHandler errorHandler)
    {
        CloseHelper.close(errorHandler, responsePublication);
        responsePublication = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "ClientSession{" +
            "id=" + id +
            ", responseStreamId=" + responseStreamId +
            ", responseChannel='" + responseChannel + '\'' +
            ", encodedPrincipal=" + Arrays.toString(encodedPrincipal) +
            ", responsePublication=" + responsePublication +
            ", isClosing=" + isClosing +
            '}';
    }
}
