/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.AvailableImageHandler;
import io.aeron.ChannelUri;
import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.UnavailableImageHandler;
import io.aeron.archive.client.AeronArchive.Context;
import io.aeron.exceptions.ConfigurationException;
import org.agrona.BitUtil;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.NoOpIdleStrategy;
import org.agrona.concurrent.NoOpLock;
import org.agrona.concurrent.SystemNanoClock;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.InOrder;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.MTU_LENGTH_PARAM_NAME;
import static io.aeron.CommonContext.SESSION_ID_PARAM_NAME;
import static io.aeron.CommonContext.SPARSE_PARAM_NAME;
import static io.aeron.CommonContext.TERM_LENGTH_PARAM_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.nullable;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class AeronArchiveTest
{
    private final Aeron aeron = mock(Aeron.class);
    private final ControlResponsePoller controlResponsePoller = mock(ControlResponsePoller.class);
    private final ArchiveProxy archiveProxy = mock(ArchiveProxy.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);

    @Test
    void asyncConnectedShouldConcludeContext()
    {
        final Context ctx = mock(Context.class);
        final IllegalStateException expectedException = new IllegalStateException("test");
        doThrow(expectedException).when(ctx).conclude();

        final IllegalStateException actualException =
            assertThrowsExactly(IllegalStateException.class, () -> AeronArchive.asyncConnect(ctx));
        assertSame(expectedException, actualException);

        verify(ctx).conclude();
        verifyNoMoreInteractions(ctx);
    }

    @Test
    void asyncConnectedShouldCloseContext()
    {
        final String responseChannel = "aeron:udp?endpoint=localhost:1234";
        final int responseStreamId = 49;
        final Context ctx = mock(Context.class);
        when(ctx.aeron()).thenReturn(aeron);
        when(ctx.controlResponseChannel()).thenReturn(responseChannel);
        when(ctx.controlResponseStreamId()).thenReturn(responseStreamId);
        final RuntimeException error = new RuntimeException("subscription");
        when(aeron.asyncAddSubscription(
            eq(responseChannel),
            eq(responseStreamId),
            nullable(AvailableImageHandler.class),
            any(UnavailableImageHandler.class))).thenThrow(error);

        final RuntimeException actualException =
            assertThrowsExactly(RuntimeException.class, () -> AeronArchive.asyncConnect(ctx));
        assertSame(error, actualException);

        final InOrder inOrder = inOrder(ctx, aeron);
        inOrder.verify(ctx).conclude();
        inOrder.verify(ctx).aeron();
        inOrder.verify(ctx).controlResponseChannel();
        inOrder.verify(ctx).controlResponseStreamId();
        inOrder.verify(aeron).asyncAddSubscription(
            eq(responseChannel),
            eq(responseStreamId),
            nullable(AvailableImageHandler.class),
            any(UnavailableImageHandler.class));
        inOrder.verify(ctx).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void asyncConnectedShouldCloseResourceInCaseOfExceptionUponStartup()
    {
        final String responseChannel = "aeron:udp?endpoint=localhost:0";
        final int responseStreamId = 49;
        final String requestChannel = "aeron:udp?endpoint=localhost:1234";
        final int requestStreamId = -15;
        final long subscriptionId = -3275938475934759L;

        final Context ctx = mock(Context.class);
        when(ctx.aeron()).thenReturn(aeron);
        when(ctx.controlResponseChannel()).thenReturn(responseChannel);
        when(ctx.controlResponseStreamId()).thenReturn(responseStreamId);
        when(ctx.controlRequestChannel()).thenReturn(requestChannel);
        when(ctx.controlRequestStreamId()).thenReturn(requestStreamId);
        when(aeron.asyncAddSubscription(
            eq(responseChannel),
            eq(responseStreamId),
            nullable(AvailableImageHandler.class),
            any(UnavailableImageHandler.class))).thenReturn(subscriptionId);
        final IndexOutOfBoundsException error = new IndexOutOfBoundsException("exception");
        when(aeron.context()).thenThrow(error);

        final IndexOutOfBoundsException actualException =
            assertThrowsExactly(IndexOutOfBoundsException.class, () -> AeronArchive.asyncConnect(ctx));
        assertSame(error, actualException);

        final InOrder inOrder = inOrder(ctx, aeron);
        inOrder.verify(ctx).conclude();
        inOrder.verify(ctx).aeron();
        inOrder.verify(ctx).controlResponseChannel();
        inOrder.verify(ctx).controlResponseStreamId();
        inOrder.verify(aeron).asyncAddSubscription(
            eq(responseChannel),
            eq(responseStreamId),
            nullable(AvailableImageHandler.class),
            any(UnavailableImageHandler.class));
        inOrder.verify(aeron).asyncRemoveSubscription(subscriptionId);
        inOrder.verify(ctx).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void closeNotOwningAeronClient()
    {
        final long controlSessionId = 42;
        final long archiveId = -190;

        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.nanoClock()).thenReturn(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final IllegalMonitorStateException aeronException = new IllegalMonitorStateException("aeron closed");
        doThrow(aeronException).when(aeron).close();

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(true);
        final IllegalStateException publicationException = new IllegalStateException("publication is closed");
        doThrow(publicationException).when(publication).close();

        final Subscription subscription = mock(Subscription.class);
        when(controlResponsePoller.subscription()).thenReturn(subscription);
        final IndexOutOfBoundsException subscriptionException = new IndexOutOfBoundsException("subscription");
        doThrow(subscriptionException).when(subscription).close();

        when(archiveProxy.publication()).thenReturn(publication);
        final IndexOutOfBoundsException closeSessionException = new IndexOutOfBoundsException();
        when(archiveProxy.closeSession(controlSessionId)).thenThrow(closeSessionException);

        final Context context = new Context()
            .aeron(aeron)
            .idleStrategy(NoOpIdleStrategy.INSTANCE)
            .messageTimeoutNs(100)
            .lock(NoOpLock.INSTANCE)
            .errorHandler(errorHandler)
            .ownsAeronClient(false);
        final AeronArchive aeronArchive =
            new AeronArchive(context, controlResponsePoller, archiveProxy, controlSessionId, archiveId);

        aeronArchive.close();

        final InOrder inOrder = inOrder(errorHandler);
        inOrder.verify(errorHandler).onError(argThat(
            ex ->
            {
                final Throwable[] suppressed = ex.getSuppressed();
                return closeSessionException == ex &&
                    publicationException == suppressed[0] &&
                    subscriptionException == suppressed[1];
            }));
        inOrder.verifyNoMoreInteractions();
        verify(publication).close();
        verify(subscription).close();
    }

    @Test
    void closeOwningAeronClient()
    {
        final long controlSessionId = 42;
        final long archiveId = 555;

        final Aeron.Context aeronContext = mock(Aeron.Context.class);
        when(aeronContext.nanoClock()).thenReturn(SystemNanoClock.INSTANCE);
        when(aeron.context()).thenReturn(aeronContext);
        final IllegalMonitorStateException aeronException = new IllegalMonitorStateException("aeron closed");
        doThrow(aeronException).when(aeron).close();

        final Publication publication = mock(Publication.class);
        when(publication.isConnected()).thenReturn(true);
        doThrow(new IllegalStateException("publication is closed")).when(publication).close();

        final Subscription subscription = mock(Subscription.class);
        when(controlResponsePoller.subscription()).thenReturn(subscription);
        doThrow(new IndexOutOfBoundsException("subscription")).when(subscription).close();

        when(archiveProxy.publication()).thenReturn(publication);
        final IndexOutOfBoundsException closeSessionException = new IndexOutOfBoundsException();
        when(archiveProxy.closeSession(controlSessionId)).thenThrow(closeSessionException);

        final Context context = new Context()
            .aeron(aeron)
            .idleStrategy(NoOpIdleStrategy.INSTANCE)
            .messageTimeoutNs(100)
            .lock(NoOpLock.INSTANCE)
            .errorHandler(errorHandler)
            .ownsAeronClient(true);
        final AeronArchive aeronArchive =
            new AeronArchive(context, controlResponsePoller, archiveProxy, controlSessionId, archiveId);

        final IndexOutOfBoundsException ex = assertThrows(IndexOutOfBoundsException.class, aeronArchive::close);

        assertSame(closeSessionException, ex);
        final InOrder inOrder = inOrder(errorHandler);
        inOrder.verify(errorHandler).onError(closeSessionException);
        inOrder.verifyNoMoreInteractions();

        assertEquals(aeronException, ex.getSuppressed()[0]);
    }

    @Test
    void shouldClose() throws Exception
    {
        final Exception previousException = new Exception();
        final Exception thrownException = new Exception();

        final AutoCloseable throwingCloseable = mock(AutoCloseable.class);
        final AutoCloseable nonThrowingCloseable = mock(AutoCloseable.class);
        doThrow(thrownException).when(throwingCloseable).close();

        assertNull(AeronArchive.quietClose(null, nonThrowingCloseable));
        assertEquals(previousException, AeronArchive.quietClose(previousException, nonThrowingCloseable));
        final Exception ex = AeronArchive.quietClose(previousException, throwingCloseable);
        assertEquals(previousException, ex);
        assertEquals(thrownException, ex.getSuppressed()[0]);
        assertEquals(thrownException, AeronArchive.quietClose(null, throwingCloseable));
    }

    @ParameterizedTest
    @ValueSource(longs = { NULL_VALUE, Long.MAX_VALUE, Long.MIN_VALUE, 0, 4468236482L })
    void shouldReturnAssignedArchiveId(final long archiveId)
    {
        final long controlSessionId = -3924293;
        when(aeron.context()).thenReturn(new Aeron.Context());
        final Context context = new Context()
            .aeron(aeron)
            .idleStrategy(NoOpIdleStrategy.INSTANCE)
            .messageTimeoutNs(100)
            .lock(NoOpLock.INSTANCE)
            .errorHandler(errorHandler)
            .ownsAeronClient(true);

        final AeronArchive aeronArchive =
            new AeronArchive(context, controlResponsePoller, archiveProxy, controlSessionId, archiveId);

        assertEquals(archiveId, aeronArchive.archiveId());
    }

    @ParameterizedTest
    @CsvSource({
        "aeron:udp?endpoint=localhost:3388|mtu=2048, " +
            "aeron:udp?session-id=5|endpoint=localhost:0|sparse=true|mtu=1024",
        "aeron:udp?endpoint=localhost:3388, " +
            "aeron:udp?control=localhost:10000|control-mode=dynamic",
        "aeron:udp?endpoint=localhost:3388, aeron:udp?control-mode=manual",
        "aeron:ipc?alias=request|ssc=false|linger=0|session-id=42|sparse=false, " +
            "aeron:ipc?term-length=64k|alias=response",
    })
    void shouldAddAUniqueSessionIdParameterToBothRequestAndResponseChannels(
        final String requestChannel, final String responseChannel)
    {
        final int requestStreamId = 42;
        final int responseStreamId = -19;
        final int sessionId = BitUtil.generateRandomisedId();
        when(aeron.nextSessionId(requestStreamId)).thenReturn(sessionId);

        final Context context = new Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .errorHandler(errorHandler)
            .controlRequestChannel(requestChannel)
            .controlRequestStreamId(requestStreamId)
            .controlResponseChannel(responseChannel)
            .controlResponseStreamId(responseStreamId)
            .controlTermBufferSparse(false)
            .controlTermBufferLength(128 * 1024)
            .controlMtuLength(4096);

        assertEquals(requestChannel, context.controlRequestChannel());
        assertEquals(requestStreamId, context.controlRequestStreamId());
        assertEquals(responseChannel, context.controlResponseChannel());
        assertEquals(responseStreamId, context.controlResponseStreamId());

        context.conclude();

        verify(aeron).nextSessionId(requestStreamId);
        assertEquals(requestStreamId, context.controlRequestStreamId());
        assertEquals(responseStreamId, context.controlResponseStreamId());

        final ChannelUri actualRequestChannel = ChannelUri.parse(context.controlRequestChannel());
        final ChannelUri actualResponseChannel = ChannelUri.parse(context.controlResponseChannel());
        assertTrue(actualRequestChannel.containsKey(SESSION_ID_PARAM_NAME), "session-id was not added");
        assertEquals(Integer.toString(sessionId), actualRequestChannel.get(SESSION_ID_PARAM_NAME));
        assertEquals(Integer.toString(sessionId), actualResponseChannel.get(SESSION_ID_PARAM_NAME));

        ChannelUri.parse(requestChannel).forEachParameter((key, value) ->
        {
            if (!SESSION_ID_PARAM_NAME.equals(key))
            {
                assertEquals(value, actualRequestChannel.get(key));
            }
        });

        ChannelUri.parse(responseChannel).forEachParameter((key, value) ->
        {
            if (!SESSION_ID_PARAM_NAME.equals(key))
            {
                assertEquals(value, actualResponseChannel.get(key));
            }
        });
    }

    @Test
    void shouldNotAddASessionIdIfControlModeResponseIsSpecifiedOnTheResponseChannel()
    {
        final int requestStreamId = 100;
        final int responseStreamId = 200;
        final String requestChannel = "aeron:udp?endpoint=localhost:8080";
        final String responseChannel = "aeron:udp?control-mode=response|control=localhost:10002";
        final Context context = new Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .errorHandler(errorHandler)
            .controlRequestChannel(requestChannel)
            .controlRequestStreamId(requestStreamId)
            .controlResponseChannel(responseChannel)
            .controlResponseStreamId(responseStreamId);

        assertEquals(requestChannel, context.controlRequestChannel());
        assertEquals(requestStreamId, context.controlRequestStreamId());
        assertEquals(responseChannel, context.controlResponseChannel());
        assertEquals(responseStreamId, context.controlResponseStreamId());

        context.conclude();

        assertEquals(requestStreamId, context.controlRequestStreamId());
        assertEquals(responseStreamId, context.controlResponseStreamId());

        final ChannelUri actualRequestChannel = ChannelUri.parse(context.controlRequestChannel());
        final ChannelUri actualResponseChannel = ChannelUri.parse(context.controlResponseChannel());
        assertNull(actualRequestChannel.get(SESSION_ID_PARAM_NAME), "unexpected session-id on request channel");
        assertNull(actualResponseChannel.get(SESSION_ID_PARAM_NAME), "unexpected session-id on response channel");

        ChannelUri.parse(requestChannel)
            .forEachParameter((key, value) -> assertEquals(value, actualRequestChannel.get(key)));

        ChannelUri.parse(responseChannel)
            .forEachParameter((key, value) -> assertEquals(value, actualResponseChannel.get(key)));
    }

    @Test
    void shouldAddDefaultUriParametersIfNotSpecified()
    {
        final int requestStreamId = 10;
        final int responseStreamId = 20;
        final String requestChannel = "aeron:udp?endpoint=localhost:8080";
        final String responseChannel = "aeron:udp?endpoint=localhost:0";
        final Context context = new Context()
            .aeron(aeron)
            .ownsAeronClient(false)
            .errorHandler(errorHandler)
            .controlRequestChannel(requestChannel)
            .controlRequestStreamId(requestStreamId)
            .controlResponseChannel(responseChannel)
            .controlResponseStreamId(responseStreamId)
            .controlMtuLength(2048)
            .controlTermBufferLength(256 * 1024)
            .controlTermBufferSparse(true);

        assertEquals(requestChannel, context.controlRequestChannel());
        assertEquals(requestStreamId, context.controlRequestStreamId());
        assertEquals(responseChannel, context.controlResponseChannel());
        assertEquals(responseStreamId, context.controlResponseStreamId());

        context.conclude();

        assertEquals(requestStreamId, context.controlRequestStreamId());
        assertEquals(responseStreamId, context.controlResponseStreamId());

        final ChannelUri actualRequestChannel = ChannelUri.parse(context.controlRequestChannel());
        final ChannelUri actualResponseChannel = ChannelUri.parse(context.controlResponseChannel());
        assertEquals(String.valueOf(context.controlMtuLength()), actualRequestChannel.get(MTU_LENGTH_PARAM_NAME));
        assertEquals(String.valueOf(context.controlMtuLength()), actualResponseChannel.get(MTU_LENGTH_PARAM_NAME));
        assertEquals(
            String.valueOf(context.controlTermBufferLength()), actualRequestChannel.get(TERM_LENGTH_PARAM_NAME));
        assertEquals(
            String.valueOf(context.controlTermBufferLength()), actualResponseChannel.get(TERM_LENGTH_PARAM_NAME));
        assertEquals(String.valueOf(context.controlTermBufferSparse()), actualRequestChannel.get(SPARSE_PARAM_NAME));
        assertEquals(String.valueOf(context.controlTermBufferSparse()), actualResponseChannel.get(SPARSE_PARAM_NAME));
    }

    @ParameterizedTest
    @ValueSource(ints = { Integer.MIN_VALUE, -1, 0 })
    void shouldRejectInvalidRetryAttempts(final int retryAttempts)
    {
        final Context context = new Context()
            .aeron(aeron)
            .controlRequestChannel("aeron:udp")
            .controlResponseChannel("aeron:udp")
            .messageRetryAttempts(retryAttempts);
        assertEquals(retryAttempts, context.messageRetryAttempts());

        final ConfigurationException exception = assertThrowsExactly(ConfigurationException.class, context::conclude);
        assertEquals(
            "ERROR - AeronArchive.Context.messageRetryAttempts must be > 0, got: " + retryAttempts,
            exception.getMessage());
    }

    @Test
    void maxRetryAttemptsDefaultValue()
    {
        final Context context = new Context();
        assertEquals(AeronArchive.Configuration.MESSAGE_RETRY_ATTEMPTS_DEFAULT, context.messageRetryAttempts());
    }

    @Test
    void maxRetryAttemptsSystemProperty()
    {
        System.setProperty(AeronArchive.Configuration.MESSAGE_RETRY_ATTEMPTS_PROP_NAME, "111");
        try
        {
            final Context context = new Context();
            assertEquals(111, context.messageRetryAttempts());
        }
        finally
        {
            System.clearProperty(AeronArchive.Configuration.MESSAGE_RETRY_ATTEMPTS_PROP_NAME);
        }
    }
}
