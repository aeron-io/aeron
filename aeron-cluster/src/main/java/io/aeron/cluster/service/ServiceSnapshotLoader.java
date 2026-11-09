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

import io.aeron.Image;
import io.aeron.ImageControlledFragmentAssembler;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.*;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

import java.util.concurrent.TimeUnit;

import static io.aeron.cluster.service.ClusteredServiceContainer.Configuration.SNAPSHOT_TYPE_ID;

final class ServiceSnapshotLoader implements ControlledFragmentHandler
{
    private static final int FRAGMENT_LIMIT = 10;

    private boolean inSnapshot = false;
    private boolean isDone = false;
    private int appVersion;
    private TimeUnit timeUnit;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final SnapshotMarkerDecoder snapshotMarkerDecoder = new SnapshotMarkerDecoder();
    private final ClientSessionDecoder clientSessionDecoder = new ClientSessionDecoder();
    private final ImageControlledFragmentAssembler fragmentAssembler = new ImageControlledFragmentAssembler(this);
    private final Image image;
    private final ClusteredServiceAgent agent;

    ServiceSnapshotLoader(final Image image, final ClusteredServiceAgent agent)
    {
        this.image = image;
        this.agent = agent;
    }

    boolean isDone()
    {
        return isDone;
    }

    int appVersion()
    {
        return appVersion;
    }

    TimeUnit timeUnit()
    {
        return timeUnit;
    }

    int poll()
    {
        return image.controlledPoll(fragmentAssembler, FRAGMENT_LIMIT);
    }

    @Override
    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        messageHeaderDecoder.wrap(buffer, offset);

        final int schemaId = messageHeaderDecoder.schemaId();
        if (MessageHeaderDecoder.SCHEMA_ID != schemaId)
        {
            throw new ClusterException("expected schemaId=" + MessageHeaderDecoder.SCHEMA_ID + ", actual=" + schemaId);
        }

        switch (messageHeaderDecoder.templateId())
        {
            case SnapshotMarkerDecoder.TEMPLATE_ID:
                snapshotMarkerDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final long typeId = snapshotMarkerDecoder.typeId();
                if (SNAPSHOT_TYPE_ID != typeId)
                {
                    throw new ClusterException("unexpected snapshot type: " + typeId);
                }

                switch (snapshotMarkerDecoder.mark())
                {
                    case BEGIN:
                        if (inSnapshot)
                        {
                            throw new ClusterException("already in snapshot");
                        }
                        inSnapshot = true;
                        appVersion = snapshotMarkerDecoder.appVersion();
                        timeUnit = ClusterClock.map(snapshotMarkerDecoder.timeUnit());
                        return Action.CONTINUE;

                    case END:
                        if (!inSnapshot)
                        {
                            throw new ClusterException("missing begin snapshot");
                        }
                        isDone = true;
                        return Action.BREAK;

                    case SECTION:
                    case NULL_VAL:
                        break;
                }
                break;

            case ClientSessionDecoder.TEMPLATE_ID:
                clientSessionDecoder.wrap(
                    buffer,
                    offset + MessageHeaderDecoder.ENCODED_LENGTH,
                    messageHeaderDecoder.blockLength(),
                    messageHeaderDecoder.version());

                final String responseChannel = clientSessionDecoder.responseChannel();
                final byte[] encodedPrincipal = new byte[clientSessionDecoder.encodedPrincipalLength()];
                clientSessionDecoder.getEncodedPrincipal(encodedPrincipal, 0, encodedPrincipal.length);

                agent.addSession(
                    clientSessionDecoder.clusterSessionId(),
                    clientSessionDecoder.responseStreamId(),
                    responseChannel,
                    encodedPrincipal);
                break;
        }

        return Action.CONTINUE;
    }
}
