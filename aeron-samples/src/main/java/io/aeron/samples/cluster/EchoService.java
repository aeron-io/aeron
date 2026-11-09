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
package io.aeron.samples.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.ClientSession;
import io.aeron.cluster.service.Cluster;
import io.aeron.cluster.service.ClusteredService;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.IdleStrategy;

class EchoService implements ClusteredService
{
    private static final int SEND_ATTEMPTS = 3;
    protected Cluster cluster;
    protected IdleStrategy idleStrategy;

    @Override
    public void onStart(final Cluster cluster, final Image snapshotImage)
    {
        this.cluster = cluster;
        this.idleStrategy = cluster.idleStrategy();
    }

    @Override
    public void onSessionOpen(final ClientSession session, final long timestamp)
    {
    }

    @Override
    public void onSessionClose(final ClientSession session, final long timestamp, final CloseReason closeReason)
    {
    }

    @Override
    public void onSessionMessage(
        final ClientSession session,
        final long timestamp,
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        idleStrategy.reset();
        int attempts = SEND_ATTEMPTS;
        do
        {
            final long result = session.offer(buffer, offset, length);
            if (result > 0)
            {
                return;
            }
            idleStrategy.idle();
        }
        while (--attempts > 0);
    }

    @Override
    public void onTimerEvent(final long correlationId, final long timestamp)
    {
    }

    @Override
    public void onTakeSnapshot(final ExclusivePublication snapshotPublication)
    {
    }

    @Override
    public void onRoleChange(final Cluster.Role newRole)
    {
    }

    @Override
    public void onTerminate(final Cluster cluster)
    {
    }
}
