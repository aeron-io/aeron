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

import io.aeron.Aeron;
import io.aeron.AeronCounters;
import io.aeron.Counter;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.CountersReader;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.concurrent.status.CountersReader.KEY_OFFSET;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static org.agrona.concurrent.status.CountersReader.RECORD_ALLOCATED;
import static org.agrona.concurrent.status.CountersReader.RECORD_UNUSED;

/**
 * For allocating and finding cluster associated counters identified by
 * {@link ClusteredServiceContainer.Context#clusterId()}.
 */
public final class ClusterCounters
{
    /**
     * Suffix for `clusterId` in the counter label.
     */
    public static final String CLUSTER_ID_LABEL_SUFFIX = " - clusterId=";
    static final String SERVICE_ID_SUFFIX = " serviceId=";

    private ClusterCounters()
    {
    }

    /**
     * Allocate a counter to represent component state within a cluster.
     *
     * @param aeron      to allocate the counter.
     * @param tempBuffer temporary storage to create label and metadata.
     * @param name       of the counter for the label.
     * @param typeId     for the counter.
     * @param clusterId  to which the allocated counter belongs.
     * @return the new {@link Counter}.
     */
    public static Counter allocate(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final int clusterId)
    {
        int index = 0;
        tempBuffer.putInt(index, clusterId);
        index += SIZE_OF_INT;
        final int keyLength = index;

        index += tempBuffer.putStringWithoutLengthAscii(index, name);
        index += tempBuffer.putStringWithoutLengthAscii(index, CLUSTER_ID_LABEL_SUFFIX);
        index += tempBuffer.putIntAscii(index, clusterId);

        return aeron.addCounter(typeId, tempBuffer, 0, keyLength, tempBuffer, keyLength, index - keyLength);
    }

    /**
     * Allocate a counter to represent component state within a cluster and append a version info to its label.
     *
     * @param aeron          to allocate the counter.
     * @param tempBuffer     temporary storage to create label and metadata.
     * @param name           of the counter for the label.
     * @param typeId         for the counter.
     * @param clusterId      to which the allocated counter belongs.
     * @param version        of the component.
     * @param commitHashCode Git commit SHA of the component.
     * @return the new {@link Counter}.
     */
    public static Counter allocateVersioned(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final int clusterId,
        final String version,
        final String commitHashCode)
    {
        int index = 0;
        tempBuffer.putInt(index, clusterId);
        index += SIZE_OF_INT;
        final int keyLength = index;

        index += tempBuffer.putStringWithoutLengthAscii(index, name);
        index += tempBuffer.putStringWithoutLengthAscii(index, CLUSTER_ID_LABEL_SUFFIX);
        index += tempBuffer.putIntAscii(index, clusterId);
        index += AeronCounters.appendVersionInfo(tempBuffer, index, version, commitHashCode);

        return aeron.addCounter(typeId, tempBuffer, 0, keyLength, tempBuffer, keyLength, index - keyLength);
    }

    /**
     * Find the counter id for a type of counter in a cluster.
     *
     * @param counters  to search within.
     * @param typeId    of the counter.
     * @param clusterId to which the allocated counter belongs.
     * @return the matching counter id or {@link Aeron#NULL_VALUE} if not found.
     */
    public static int find(final CountersReader counters, final int typeId, final int clusterId)
    {
        final AtomicBuffer buffer = counters.metaDataBuffer();

        for (int i = 0, maxCounterId = counters.maxCounterId(); i <= maxCounterId; i++)
        {
            final int counterState = counters.getCounterState(i);

            if (counterState == RECORD_ALLOCATED)
            {
                if (counters.getCounterTypeId(i) == typeId &&
                    buffer.getInt(CountersReader.metaDataOffset(i) + KEY_OFFSET) == clusterId)
                {
                    return i;
                }
            }
            else if (RECORD_UNUSED == counterState)
            {
                break;
            }
        }

        return NULL_COUNTER_ID;
    }

    /**
     * Allocate a counter to represent component state within a cluster.
     *
     * @param aeron      to allocate the counter.
     * @param tempBuffer temporary storage to create label and metadata.
     * @param name       of the counter for the label.
     * @param typeId     for the counter.
     * @param clusterId  to which the allocated counter belongs.
     * @param serviceId  to which the allocated counter belongs.
     * @return the {@link Counter} for the commit position.
     */
    public static Counter allocateServiceCounter(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final String name,
        final int typeId,
        final int clusterId,
        final int serviceId)
    {
        int index = 0;
        tempBuffer.putInt(index, clusterId);
        index += SIZE_OF_INT;
        tempBuffer.putInt(index, serviceId);
        index += SIZE_OF_INT;
        final int keyLength = index;

        index += tempBuffer.putStringWithoutLengthAscii(index, name);
        index += tempBuffer.putStringWithoutLengthAscii(index, CLUSTER_ID_LABEL_SUFFIX);
        index += tempBuffer.putIntAscii(index, clusterId);
        index += tempBuffer.putStringWithoutLengthAscii(index, SERVICE_ID_SUFFIX);
        index += tempBuffer.putIntAscii(index, serviceId);

        return aeron.addCounter(typeId, tempBuffer, 0, keyLength, tempBuffer, keyLength, index - keyLength);
    }

    static Counter allocateServiceErrorCounter(
        final Aeron aeron,
        final MutableDirectBuffer tempBuffer,
        final int clusterId,
        final int serviceId)
    {
        int index = 0;
        tempBuffer.putInt(index, clusterId);
        index += SIZE_OF_INT;
        tempBuffer.putInt(index, serviceId);
        index += SIZE_OF_INT;
        final int keyLength = index;

        index += tempBuffer.putStringWithoutLengthAscii(index, "Cluster Container Errors");
        index += tempBuffer.putStringWithoutLengthAscii(index, CLUSTER_ID_LABEL_SUFFIX);
        index += tempBuffer.putIntAscii(index, clusterId);
        index += tempBuffer.putStringWithoutLengthAscii(index, SERVICE_ID_SUFFIX);
        index += tempBuffer.putIntAscii(index, serviceId);
        index += AeronCounters.appendVersionInfo(
            tempBuffer, index, ClusteredServiceContainerVersion.VERSION, ClusteredServiceContainerVersion.GIT_SHA);

        return aeron.addCounter(
            AeronCounters.CLUSTER_CLUSTERED_SERVICE_ERROR_COUNT_TYPE_ID,
            tempBuffer,
            0,
            keyLength,
            tempBuffer,
            keyLength,
            index - keyLength);
    }
}
