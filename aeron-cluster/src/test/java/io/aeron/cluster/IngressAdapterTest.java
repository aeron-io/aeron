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

import static org.mockito.Mockito.*;

import java.util.function.Consumer;
import java.util.stream.Stream;

import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import io.aeron.cluster.codecs.AdminRequestDecoder;
import io.aeron.cluster.codecs.AdminRequestEncoder;
import io.aeron.cluster.codecs.AdminRequestType;
import io.aeron.cluster.codecs.ChallengeResponseDecoder;
import io.aeron.cluster.codecs.ChallengeResponseEncoder;
import io.aeron.cluster.codecs.MessageHeaderDecoder;
import io.aeron.cluster.codecs.MessageHeaderEncoder;
import io.aeron.cluster.codecs.SessionCloseRequestDecoder;
import io.aeron.cluster.codecs.SessionCloseRequestEncoder;
import io.aeron.cluster.codecs.SessionConnectRequestDecoder;
import io.aeron.cluster.codecs.SessionConnectRequestEncoder;
import io.aeron.cluster.codecs.SessionKeepAliveDecoder;
import io.aeron.cluster.codecs.SessionKeepAliveEncoder;
import io.aeron.cluster.codecs.SessionMessageHeaderDecoder;
import io.aeron.cluster.codecs.SessionMessageHeaderEncoder;

class IngressAdapterTest
{
    private final ConsensusModuleAgent consensusModuleAgent = mock(ConsensusModuleAgent.class);
    private final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
    private final Header header = new Header(0, 0);
    private final IngressAdapter adapter = new IngressAdapter(0, consensusModuleAgent);

    @SuppressWarnings("unused") // name used for test display name
    @ParameterizedTest(name = "{index} {0}")
    @MethodSource
    void shouldDelegateToConsensusModuleAgent(
        final String name,
        final Consumer<MutableDirectBuffer> encoder,
        final int blockLength,
        final ConsensusModuleAgentExpectation expectation)
    {
        encoder.accept(buffer);

        final int length = MessageHeaderDecoder.ENCODED_LENGTH + blockLength;

        adapter.onMessage(buffer, 0, length, header);

        expectation.expect(verify(consensusModuleAgent), buffer, header);
    }

    static Stream<Arguments> shouldDelegateToConsensusModuleAgent()
    {
        return Stream.of(
        Arguments.of(
            "UnknownSchema",
            (Consumer<MutableDirectBuffer>)(buffer) ->
            {
                new MessageHeaderEncoder().wrap(buffer, 0)
                    .blockLength(0)
                    .schemaId(17)
                    .templateId(19)
                    .version(0);
            }, 0,  // no wrapping
            (ConsensusModuleAgentExpectation)(a, buffer, header) ->
                a.onExtensionMessage(
                    0, 19, 17, 0, buffer, 0,
                    MessageHeaderDecoder.ENCODED_LENGTH, header)),
        Arguments.of(
            "SessionMessageHeaderDecoder",
            (Consumer<MutableDirectBuffer>)(buffer) ->
            {
                new SessionMessageHeaderEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                    .leadershipTermId(17)
                    .clusterSessionId(19)
                    .timestamp(23);
            }, SessionMessageHeaderDecoder.BLOCK_LENGTH,
            (ConsensusModuleAgentExpectation)(a, buffer, header) ->
                a.onIngressMessage(17, 19,
                    buffer, MessageHeaderDecoder.ENCODED_LENGTH + SessionMessageHeaderDecoder.BLOCK_LENGTH, 0)),
        Arguments.of(
            "SessionConnectRequestDecoder",
            (Consumer<MutableDirectBuffer>)(buffer) ->
            {
                new SessionConnectRequestEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                    .correlationId(17)
                    .responseStreamId(19)
                    .version(23)
                    .responseChannel("x");
            }, SessionConnectRequestDecoder.BLOCK_LENGTH,
            (ConsensusModuleAgentExpectation)(a, buffer, header) ->
                a.onSessionConnect(17, 19, 23, "x", new byte[0], header)),
        Arguments.of(
            "SessionCloseRequestDecoder",
            (Consumer<MutableDirectBuffer>)(buffer) ->
            {
                new SessionCloseRequestEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                    .leadershipTermId(17)
                    .clusterSessionId(19);
            }, SessionCloseRequestDecoder.BLOCK_LENGTH,
            (ConsensusModuleAgentExpectation)(a, buffer, header) -> a.onSessionClose(17, 19)),
        Arguments.of(
            "SessionKeepAliveDecoder",
            (Consumer<MutableDirectBuffer>)(buffer) ->
            {
                new SessionKeepAliveEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                    .leadershipTermId(17)
                    .clusterSessionId(19);
            }, SessionKeepAliveDecoder.BLOCK_LENGTH,
            (ConsensusModuleAgentExpectation)(a, buffer, header) -> a.onSessionKeepAlive(17, 19, header)),
        Arguments.of(
            "ChallengeResponseDecoder",
            (Consumer<MutableDirectBuffer>)(buffer) ->
            {
                final byte[] challenge = "foo".getBytes();
                new ChallengeResponseEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                    .correlationId(17)
                    .clusterSessionId(19)
                    .putEncodedCredentials(challenge, 0, challenge.length);
            }, ChallengeResponseDecoder.BLOCK_LENGTH,
            (ConsensusModuleAgentExpectation)(a, buffer, header) ->
                a.onIngressChallengeResponse(17, 19, "foo".getBytes())),
        Arguments.of(
            "AdminRequestDecoder",
            (Consumer<MutableDirectBuffer>)(buffer) ->
            {
                final byte[] payload = "foo".getBytes();
                new AdminRequestEncoder().wrapAndApplyHeader(buffer, 0, new MessageHeaderEncoder())
                    .leadershipTermId(17)
                    .clusterSessionId(19)
                    .correlationId(23)
                    .putPayload(payload, 0, payload.length);
            }, AdminRequestDecoder.BLOCK_LENGTH,
            (ConsensusModuleAgentExpectation)(a, buffer, header) ->
                a.onAdminRequest(17, 19, 23,
                    AdminRequestType.SNAPSHOT, buffer,
                    AdminRequestDecoder.BLOCK_LENGTH +
                            MessageHeaderDecoder.ENCODED_LENGTH +
                            AdminRequestDecoder.payloadHeaderLength(),
                    "foo".getBytes().length)));
    }

    private interface ConsensusModuleAgentExpectation
    {
        void expect(ConsensusModuleAgent agent, DirectBuffer buffer, Header header);
    }
}
