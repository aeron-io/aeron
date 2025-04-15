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
package io.aeron.cluster;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Publication;
import io.aeron.cluster.codecs.CloseReason;
import io.aeron.cluster.service.Cluster;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AgentTerminationException;

/**
 * Extension for handling messages from external schemas unknown to core Aeron Cluster code
 * thus providing an extension to the core ingress consensus module behaviour.
 */
public interface ConsensusModuleExtension extends AutoCloseable
{
    /**
     * Schema supported by this extension.
     *
     * @return schema id supported.
     */
    int supportedSchemaId();

    /**
     * Start event where the extension can perform any initialisation required and load snapshot state.
     * The snapshot image can be null if no previous snapshot exists.
     * <p>
     * <b>Note:</b> As this is a potentially long-running operation the implementation should use
     * {@link Cluster#idleStrategy()} and then occasionally call {@link org.agrona.concurrent.IdleStrategy#idle()} or
     * {@link org.agrona.concurrent.IdleStrategy#idle(int)}, especially when polling the {@link Image} returns 0.
     *
     * @param consensusModuleControl with which the extension can interact.
     * @param snapshotImage          from which the extension can load its state which can be null when no snapshot.
     */
    void onStart(ConsensusModuleControl consensusModuleControl, Image snapshotImage);

    /**
     * An extension should implement this method to do its work. Long-running operations should be decomposed.
     * <p>
     * The return value is used for implementing an idle strategy that can be employed when no work is
     * currently available for the extension to process.
     * <p>
     * If the extension wished to terminate and close then a {@link AgentTerminationException} can be thrown.
     *
     * @param nowNs is cluster time in nanoseconds.
     * @return 0 to indicate no work was currently available, a positive value otherwise.
     */
    int doWork(long nowNs);

    /**
     * Cluster election is complete and new publication is added for the leadership term. If the node is a follower
     * then the publication will be null.
     *
     * @param consensusControlState state to allow extension to control the consensus module.
     */
    void onElectionComplete(ConsensusControlState consensusControlState);

    /**
     * New Leadership term and the consensus control state has changed.
     *
     * @param consensusControlState state to allow extension to control the consensus module.
     */
    void onNewLeadershipTerm(ConsensusControlState consensusControlState);

    /**
     * Callback for handling messages received as ingress to a cluster.
     * <p>
     * Within this callback reentrant calls to the {@link io.aeron.Aeron} client are not permitted and
     * will result in undefined behaviour.
     *
     * @param actingBlockLength acting block length.
     * @param templateId        the message template id (already parsed from header).
     * @param schemaId          the schema id.
     * @param actingVersion     acting version from header
     * @param buffer            containing the data.
     * @param offset            at which the data begins.
     * @param length            of the data in bytes.
     * @param header            representing the metadata for the data.
     * @return The action to be taken with regard to the stream position after the callback.
     */
    ControlledFragmentHandler.Action onIngressExtensionMessage(
        int actingBlockLength,
        int templateId,
        int schemaId,
        int actingVersion,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);

    /**
     * Callback for handling committed log messages (for follower or recovery).
     * <p>
     * Within this callback reentrant calls to the {@link io.aeron.Aeron} client are not permitted and
     * will result in undefined behaviour.
     *
     * @param actingBlockLength acting block length.
     * @param templateId        the message template id (already parsed from header).
     * @param schemaId          the schema id.
     * @param actingVersion     acting version from header
     * @param buffer            containing the data.
     * @param offset            at which the data begins.
     * @param length            of the data in bytes.
     * @param header            representing the metadata for the data.
     * @return The action to be taken with regard to the stream position after the callback.
     */
    ControlledFragmentHandler.Action onLogExtensionMessage(
        int actingBlockLength,
        int templateId,
        int schemaId,
        int actingVersion,
        DirectBuffer buffer,
        int offset,
        int length,
        Header header);

    /**
     * {@inheritDoc}
     */
    void close();

    /**
     * Callback indicating a cluster session has opened.
     *
     * @param clusterSessionId of the opened session which is unique and not reused.
     */
    void onSessionOpened(long clusterSessionId);

    /**
     * Callback indicating a cluster session has closed.
     *
     * @param clusterSessionId  of the opened session which is unique and not reused.
     * @param closeReason       reason to closing session
     */
    void onSessionClosed(long clusterSessionId, CloseReason closeReason);

    /**
     * Callback when preparing for a new Raft leadership term - before election.
     */
    void onPrepareForNewLeadership();

    /**
     * The extension should take a snapshot and store its state to the provided archive {@link ExclusivePublication}.
     * <p>
     * <b>Note:</b> As this is a potentially long-running operation the implementation should use
     * {@link Cluster#idleStrategy()} and then occasionally call {@link org.agrona.concurrent.IdleStrategy#idle()} or
     * {@link org.agrona.concurrent.IdleStrategy#idle(int)},
     * especially when the snapshot {@link ExclusivePublication} returns {@link Publication#BACK_PRESSURED}.
     *
     * @param snapshotPublication to which the state should be recorded.
     */
    void onTakeSnapshot(ExclusivePublication snapshotPublication);
}
