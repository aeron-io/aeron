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

import io.aeron.ChannelUri;
import io.aeron.CommonContext;
import io.aeron.driver.exceptions.InvalidChannelException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.aeron.CommonContext.SESSION_ID_PARAM_NAME;
import static io.aeron.CommonContext.STREAM_ID_PARAM_NAME;
import static java.util.concurrent.TimeUnit.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class PublicationParamsTest
{
    private final MediaDriver.Context ctx = new MediaDriver.Context();
    private final DriverConductor conductor = mock(DriverConductor.class);

    @Test
    void basicParse()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:1010");
        final PublicationParams params = PublicationParams.getPublicationParams(uri, ctx, conductor, 42, "UDP");
        assertEquals(ctx.maxResend(), params.maxResend);
    }

    @Test
    void hasMaxRetransmits()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:1010|" +
            CommonContext.MAX_RESEND_PARAM_NAME + "=100");
        final PublicationParams params = PublicationParams.getPublicationParams(uri, ctx, conductor, 8, "UDP");
        assertEquals(100, params.maxResend);
    }

    @Test
    void hasNegativeMaxRetransmits()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:1010|" +
            CommonContext.MAX_RESEND_PARAM_NAME + "=-1234");
        final InvalidChannelException exception = assertThrows(
            InvalidChannelException.class,
            () -> PublicationParams.getPublicationParams(uri, ctx, conductor, -19, "UDP"));

        assertTrue(exception.getMessage().contains("must be > 0"));
    }

    @Test
    void hasTooBigMaxRetransmits()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:1010|" +
            CommonContext.MAX_RESEND_PARAM_NAME + "=" + Configuration.MAX_RESEND_MAX * 2);
        final InvalidChannelException exception = assertThrows(
            InvalidChannelException.class,
            () -> PublicationParams.getPublicationParams(uri, ctx, conductor, 22, "UDP"));

        assertTrue(exception.getMessage().contains("and <= " + Configuration.MAX_RESEND_MAX));
    }

    @Test
    void hasInvalidMaxRetransmits()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:1010|" +
            CommonContext.MAX_RESEND_PARAM_NAME + "=notanumber");
        final InvalidChannelException exception = assertThrows(
            InvalidChannelException.class,
            () -> PublicationParams.getPublicationParams(uri, ctx, conductor, 8, "UDP"));

        assertTrue(exception.getMessage().contains("must be a number"));
    }

    @Test
    void shouldSetStreamId()
    {
        final int streamId = 42;
        final ChannelUri uri = ChannelUri.parse("aeron:ipc?alias=x|stream-id=" + streamId);
        final PublicationParams params = PublicationParams.getPublicationParams(uri, ctx, conductor, 42, "IPC");

        assertEquals(streamId, params.streamId);
    }

    @Test
    void shouldRejectStreamIdThatIsNotANumber()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:8080|stream-id=abc");
        final InvalidChannelException exception = assertThrowsExactly(InvalidChannelException.class,
            () -> PublicationParams.getPublicationParams(uri, ctx, conductor, 42, "UDP"));
        assertEquals("ERROR - invalid " + STREAM_ID_PARAM_NAME + ", must be a number", exception.getMessage());
    }

    @Test
    void shouldRejectStreamIdTIfItDoesNotMatch()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:8080|stream-id=-19");
        final InvalidChannelException exception = assertThrowsExactly(InvalidChannelException.class,
            () -> PublicationParams.getPublicationParams(uri, ctx, conductor, 2112, "UDP"));
        assertEquals("ERROR - stream-id=-19 does not match provided streamId=2112", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { -10, 0, 1407 })
    void shouldRejectPublicationWindowLessThanMtu(final int pubWindow)
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:8080|mtu=1408|pub-wnd=" + pubWindow);
        final InvalidChannelException exception = assertThrowsExactly(InvalidChannelException.class,
            () -> PublicationParams.getPublicationParams(uri, ctx, conductor, 2112, "UDP"));
        assertEquals("ERROR - pub-wnd=" + pubWindow + " cannot be less than the mtu=1408", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { 100_000, 32 * 1024 + 1 })
    void shouldRejectPublicationWindowLargerThanHalfATermLength(final int pubWindow)
    {
        final ChannelUri uri =
            ChannelUri.parse("aeron:udp?endpoint=localhost:8080|term-length=64k|pub-wnd=" + pubWindow);
        final InvalidChannelException exception = assertThrowsExactly(InvalidChannelException.class,
            () -> PublicationParams.getPublicationParams(uri, ctx, conductor, 2112, "UDP"));
        assertEquals(
            "ERROR - pub-wnd=" + pubWindow + " must not exceed half the term-length=65536",
            exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(ints = { 2048, 32 * 1024 })
    void shouldSetPublicationWindowFromUriParameter(final int pubWindow)
    {
        final ChannelUri uri =
            ChannelUri.parse("aeron:udp?endpoint=localhost:8080|term-length=64k|pub-wnd=" + pubWindow);
        final PublicationParams params = PublicationParams.getPublicationParams(uri, ctx, conductor, 2112, "UDP");

        assertEquals(pubWindow, params.publicationWindowLength);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc?term-length=1m", "aeron:udp?endpoint=localhost:5555|term-length=128k" })
    void shouldSetPublicationWindowToHalfATermIfNotSet(final String uri)
    {
        ctx.ipcPublicationTermWindowLength(0).publicationTermWindowLength(0);

        final PublicationParams params =
            PublicationParams.getPublicationParams(ChannelUri.parse(uri), ctx, conductor, 100, "test");

        assertEquals(params.termLength / 2, params.publicationWindowLength);
    }

    @Test
    void shouldSetPublicationWindowFromConfigIpc()
    {
        ctx.ipcPublicationTermWindowLength(128 * 1024).publicationTermWindowLength(0);

        final ChannelUri uri = ChannelUri.parse("aeron:ipc?term-length=4m");
        final PublicationParams params =
            PublicationParams.getPublicationParams(uri, ctx, conductor, 100, "IPC");

        assertEquals(ctx.ipcPublicationTermWindowLength(), params.publicationWindowLength);
    }

    @Test
    void shouldSetPublicationWindowFromConfigUdp()
    {
        ctx.ipcPublicationTermWindowLength(0).publicationTermWindowLength(4096);

        final ChannelUri uri = ChannelUri.parse("aeron:udp?term-length=16m|endpoint=localhost:7777");
        final PublicationParams params =
            PublicationParams.getPublicationParams(uri, ctx, conductor, 100, "UDP");

        assertEquals(ctx.publicationTermWindowLength(), params.publicationWindowLength);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc?term-length=1m", "aeron:udp?endpoint=localhost:5555|term-length=128k" })
    void shouldGenerateSessionIdWhenNotSet(final String uri)
    {
        final int streamId = -98;
        final int sessionId = 134238032;
        when(conductor.nextAvailableSessionId(anyInt(), anyString())).thenReturn(sessionId);

        final ChannelUri channelUri = ChannelUri.parse(uri);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, streamId, channelUri.media());

        assertEquals(sessionId, params.sessionId);
        verify(conductor).nextAvailableSessionId(streamId, channelUri.media());
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc?mtu=1408", "aeron:udp?endpoint=localhost:5555" })
    void shouldSetSessionIdFromTheUriParameter(final String uri)
    {
        final int streamId = ThreadLocalRandom.current().nextInt();
        final int sessionId = ThreadLocalRandom.current().nextInt();

        final ChannelUri channelUri = ChannelUri.parse(uri + "|" + SESSION_ID_PARAM_NAME + "=" + sessionId);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, streamId, channelUri.media());

        assertEquals(sessionId, params.sessionId);
        verifyNoInteractions(conductor);
    }

    @Test
    void shouldRejectTaggedSessionIfTagIsInvalid()
    {
        final ChannelUri channelUri = ChannelUri.parse("aeron:udp?endpoint=localhost:5555|session-id=tag:abc");
        final InvalidChannelException exception = assertThrowsExactly(InvalidChannelException.class,
            () -> PublicationParams.getPublicationParams(channelUri, ctx, conductor, 42, channelUri.media()));
        assertEquals(
            "ERROR - session-id=tag:abc has an invalid tag: " +
            "channel=aeron:udp?session-id=tag:abc|endpoint=localhost:5555",
            exception.getMessage());
    }

    @Test
    void shouldRejectTaggedSessionIfUnknownPublicationReferenceUdp()
    {
        final long tag = ThreadLocalRandom.current().nextLong();

        final ChannelUri channelUri = ChannelUri.parse("aeron:udp?endpoint=localhost:5555|session-id=tag:" + tag);
        final InvalidChannelException exception = assertThrowsExactly(InvalidChannelException.class,
            () -> PublicationParams.getPublicationParams(channelUri, ctx, conductor, 42, channelUri.media()));
        assertEquals(
            "ERROR - session-id=tag:" + tag + " must reference a network publication: channel=" + channelUri,
            exception.getMessage());

        verify(conductor).findNetworkPublicationByTag(tag);
    }

    @Test
    void shouldRejectTaggedSessionIfUnknownPublicationReferenceIpc()
    {
        final long tag = ThreadLocalRandom.current().nextLong();

        final ChannelUri channelUri = ChannelUri.parse("aeron:ipc?session-id=tag:" + tag);
        final InvalidChannelException exception = assertThrowsExactly(InvalidChannelException.class,
            () -> PublicationParams.getPublicationParams(channelUri, ctx, conductor, 42, channelUri.media()));
        assertEquals(
            "ERROR - session-id=tag:" + tag + " must reference an IPC publication: channel=" + channelUri,
            exception.getMessage());

        verify(conductor).findIpcPublicationByTag(tag);
    }

    @Test
    void shouldInitializeSessionIdTermBufferLengthAndMtuFromReferencedUdpPublicationIfTagged()
    {
        final long tag = 42;
        final int mtu = 1408;
        final int termLength = 1024 * 1024;
        final int sessionId = -4658437;
        final NetworkPublication pub = mock(NetworkPublication.class);
        when(pub.sessionId()).thenReturn(sessionId);
        when(pub.mtuLength()).thenReturn(mtu);
        when(pub.termBufferLength()).thenReturn(termLength);
        when(conductor.findNetworkPublicationByTag(tag)).thenReturn(pub);

        final ChannelUri channelUri = ChannelUri.parse("aeron:udp?endpoint=localhost:5555|session-id=tag:" + tag);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 42, channelUri.media());

        assertEquals(sessionId, params.sessionId);
        assertEquals(mtu, params.mtuLength);
        assertEquals(termLength, params.termLength);
        verify(conductor).findNetworkPublicationByTag(tag);
    }

    @Test
    void shouldInitializeSessionIdTermBufferLengthAndMtuFromReferencedIpcPublicationIfTagged()
    {
        final long tag = -5;
        final int mtu = 2048;
        final int termLength = 64 * 1024;
        final int sessionId = 100;
        final IpcPublication pub = mock(IpcPublication.class);
        when(pub.sessionId()).thenReturn(sessionId);
        when(pub.mtuLength()).thenReturn(mtu);
        when(pub.termBufferLength()).thenReturn(termLength);
        when(conductor.findIpcPublicationByTag(tag)).thenReturn(pub);

        final ChannelUri channelUri = ChannelUri.parse("aeron:ipc?session-id=tag:" + tag);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, -87, channelUri.media());

        assertEquals(sessionId, params.sessionId);
        assertEquals(mtu, params.mtuLength);
        assertEquals(termLength, params.termLength);
        verify(conductor).findIpcPublicationByTag(tag);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc", "aeron:udp?endpoint=localhost:5555" })
    void shouldReturnTrueForEosIfNotDefined(final String uri)
    {
        final ChannelUri channelUri = ChannelUri.parse(uri);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertTrue(params.signalEos);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldSetEosFromUri(final boolean eos)
    {
        final ChannelUri channelUri = ChannelUri.parse("aeron:ipc?eos=" + eos);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(eos, params.signalEos);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc", "aeron:udp?endpoint=localhost:5555" })
    void shouldReturnConfigValueForSparse(final String uri)
    {
        ctx.termBufferSparseFile(ThreadLocalRandom.current().nextBoolean());

        final ChannelUri channelUri = ChannelUri.parse(uri);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(ctx.termBufferSparseFile(), params.isSparse);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldSetSparseFromUri(final boolean sprase)
    {
        final ChannelUri channelUri = ChannelUri.parse("aeron:ipc?sparse=" + sprase);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(sprase, params.isSparse);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc", "aeron:udp?endpoint=localhost:5555" })
    void shouldReturnConfigValueForSpiesSimulateConnectionIfNotDefined(final String uri)
    {
        ctx.spiesSimulateConnection(ThreadLocalRandom.current().nextBoolean());

        final ChannelUri channelUri = ChannelUri.parse(uri);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(ctx.spiesSimulateConnection(), params.spiesSimulateConnection);
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldSetSpiesSimulateConnectionFromUri(final boolean sprase)
    {
        final ChannelUri channelUri = ChannelUri.parse("aeron:ipc?ssc=" + sprase);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(sprase, params.spiesSimulateConnection);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc", "aeron:udp?endpoint=localhost:5555" })
    void shouldReturnLingerTimeoutFromConfigIfNotSet(final String uri)
    {
        ctx.publicationLingerTimeoutNs(SECONDS.toNanos(7));

        final ChannelUri channelUri = ChannelUri.parse(uri);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(ctx.publicationLingerTimeoutNs(), params.lingerTimeoutNs);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc?mtu=4k", "aeron:udp?endpoint=localhost:5555" })
    void shouldSetLingerTimeout(final String uri)
    {
        final long lingerTimeoutMs = 321;

        final ChannelUri channelUri = ChannelUri.parse(uri + "|linger=" + lingerTimeoutMs + "ms");
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(MILLISECONDS.toNanos(lingerTimeoutMs), params.lingerTimeoutNs);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc", "aeron:udp?endpoint=localhost:5555" })
    void shouldReturnUntetheredWindowLimitTimeoutFromConfigIfNotSet(final String uri)
    {
        ctx.untetheredWindowLimitTimeoutNs(SECONDS.toNanos(7));

        final ChannelUri channelUri = ChannelUri.parse(uri);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(ctx.untetheredWindowLimitTimeoutNs(), params.untetheredWindowLimitTimeoutNs);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc?mtu=4k", "aeron:udp?endpoint=localhost:5555" })
    void shouldSetUntetheredWindowLimitTimeout(final String uri)
    {
        final long timeoutUs = 456;

        final ChannelUri channelUri = ChannelUri.parse(uri + "|untethered-window-limit-timeout=" + timeoutUs + "us");
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(MICROSECONDS.toNanos(timeoutUs), params.untetheredWindowLimitTimeoutNs);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc", "aeron:udp?endpoint=localhost:5555" })
    void shouldReturnUntetheredRestingTimeoutFromConfigIfNotSet(final String uri)
    {
        ctx.untetheredRestingTimeoutNs(SECONDS.toNanos(7));

        final ChannelUri channelUri = ChannelUri.parse(uri);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(ctx.untetheredRestingTimeoutNs(), params.untetheredRestingTimeoutNs);
    }

    @ParameterizedTest
    @ValueSource(strings = { "aeron:ipc?mtu=4k", "aeron:udp?endpoint=localhost:5555" })
    void shouldSetUntetheredRestingTimeout(final String uri)
    {
        final long timeoutNs = 456;

        final ChannelUri channelUri = ChannelUri.parse(uri + "|untethered-resting-timeout=" + timeoutNs);
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(timeoutNs, params.untetheredRestingTimeoutNs);
    }

    @Test
    void shouldHonorPrecedenceForUntetheredLingerTimeoutWithDefaultsInContext()
    {

        // context
        final ChannelUri channelUri = ChannelUri.parse(
            "aeron:udp?endpoint=localhost:5555|untethered-window-limit-timeout=444ms");
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(444), params.untetheredWindowLimitTimeoutNs);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(444), params.untetheredLingerTimeoutNs);

    }

    @Test
    void shouldHonorPrecedenceForUntetheredLingerTimeoutWithOverriddenContext()
    {
        ctx.untetheredLingerTimeoutNs(TimeUnit.MILLISECONDS.toNanos(555));

        // context
        final ChannelUri channelUri = ChannelUri.parse(
            "aeron:udp?endpoint=localhost:5555|untethered-window-limit-timeout=444ms");
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(444), params.untetheredWindowLimitTimeoutNs);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(555), params.untetheredLingerTimeoutNs);
    }

    @Test
    void shouldHonorChannelOverrideForUntetheredLingerTimeout()
    {
        ctx.untetheredLingerTimeoutNs(TimeUnit.MILLISECONDS.toNanos(555));

        // context
        final ChannelUri channelUri = ChannelUri.parse(
            "aeron:udp?endpoint=localhost:5555|untethered-window-limit-timeout=444ms|untethered-linger-timeout=333ms");
        final PublicationParams params =
            PublicationParams.getPublicationParams(channelUri, ctx, conductor, 1, channelUri.media());
        assertEquals(TimeUnit.MILLISECONDS.toNanos(444), params.untetheredWindowLimitTimeoutNs);
        assertEquals(TimeUnit.MILLISECONDS.toNanos(333), params.untetheredLingerTimeoutNs);
    }

}
