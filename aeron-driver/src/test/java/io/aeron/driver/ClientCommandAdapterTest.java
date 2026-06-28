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

import io.aeron.ErrorCode;
import io.aeron.command.ControlProtocolEvents;
import io.aeron.command.PublicationMessageFlyweight;
import io.aeron.exceptions.AeronException;
import io.aeron.exceptions.ControlProtocolException;
import io.aeron.exceptions.StorageSpaceException;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

class ClientCommandAdapterTest
{
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final RingBuffer toDriverCommands = mock(RingBuffer.class);
    private final ClientProxy clientProxy = mock(ClientProxy.class);
    private final DriverConductor driverConductor = mock(DriverConductor.class);
    private final ClientCommandAdapter clientCommandAdapter = new ClientCommandAdapter(
        errorHandler, toDriverCommands, clientProxy, driverConductor);

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
    private final PublicationMessageFlyweight publicationMsgFlyweight = new PublicationMessageFlyweight();

    @Test
    void shouldThrowControlProtocolExceptionIfUnknownCommand()
    {
        final int msgTypeId = 932467234;
        final ErrorCode expectedErrorCode = ErrorCode.UNKNOWN_COMMAND_TYPE_ID;
        final String expectedErrorMessage = "ERROR - command typeId=" + msgTypeId;

        clientCommandAdapter.onMessage(msgTypeId, buffer, 0, 5);

        final ArgumentCaptor<ControlProtocolException> exceptionArgumentCaptor =
            ArgumentCaptor.forClass(ControlProtocolException.class);
        final InOrder inOrder = inOrder(errorHandler, clientProxy);
        inOrder.verify(errorHandler).onError(exceptionArgumentCaptor.capture());
        inOrder.verify(clientProxy).onError(0, expectedErrorCode, expectedErrorMessage);
        inOrder.verifyNoMoreInteractions();

        final ControlProtocolException exception = exceptionArgumentCaptor.getValue();
        assertEquals(expectedErrorCode, exception.errorCode());
        assertEquals(AeronException.Category.ERROR, exception.category());
        assertEquals(expectedErrorMessage, exception.getMessage());
    }

    @Test
    void shouldHandleGenericException()
    {
        final long clientId = Long.MIN_VALUE + 63847263784238L;
        final long correlationId = 4213;
        final int streamId = -19;
        final String channel = "aeron:udp?alias=test|endpoint=localhost:8080";
        final IllegalArgumentException exception = new IllegalArgumentException(new IOException("dummy error"));
        final ErrorCode expectedErrorCode = ErrorCode.GENERIC_ERROR;
        final String expectedErrorMessage = exception.getClass().getName() + " : " + exception.getMessage();
        doThrow(exception)
            .when(driverConductor)
            .onAddNetworkPublication(channel, streamId, correlationId, clientId, false);
        publicationMsgFlyweight.wrap(buffer, 0)
            .clientId(clientId)
            .correlationId(correlationId);
        publicationMsgFlyweight
            .streamId(streamId)
            .channel(channel);

        clientCommandAdapter.onMessage(
            ControlProtocolEvents.ADD_PUBLICATION, buffer, 0, publicationMsgFlyweight.length());

        final InOrder inOrder = inOrder(driverConductor, errorHandler, clientProxy);
        inOrder.verify(driverConductor).onAddNetworkPublication(channel, streamId, correlationId, clientId, false);
        inOrder.verify(errorHandler).onError(exception);
        inOrder.verify(clientProxy).onError(correlationId, expectedErrorCode, expectedErrorMessage);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldHandleStorageSpaceException()
    {
        final long clientId = 109;
        final long correlationId = 42;
        final int streamId = 100;
        final String channel = "aeron:ipc";
        final StorageSpaceException exception = new StorageSpaceException("storage error");
        final ErrorCode expectedErrorCode = ErrorCode.STORAGE_SPACE;
        final String expectedErrorMessage = exception.getMessage();
        doThrow(exception)
            .when(driverConductor)
            .onAddIpcPublication(channel, streamId, correlationId, clientId, false);
        publicationMsgFlyweight.wrap(buffer, 0)
            .clientId(clientId)
            .correlationId(correlationId);
        publicationMsgFlyweight
            .streamId(streamId)
            .channel(channel);

        clientCommandAdapter.onMessage(
            ControlProtocolEvents.ADD_PUBLICATION, buffer, 0, publicationMsgFlyweight.length());

        final InOrder inOrder = inOrder(driverConductor, errorHandler, clientProxy);
        inOrder.verify(driverConductor).onAddIpcPublication(channel, streamId, correlationId, clientId, false);
        inOrder.verify(errorHandler).onError(exception);
        inOrder.verify(clientProxy).onError(correlationId, expectedErrorCode, expectedErrorMessage);
        inOrder.verifyNoMoreInteractions();
    }

    @ParameterizedTest
    @MethodSource("noSpaceLeftExceptions")
    void shouldHandleNoSpaceLeftOnTheDeviceIOException(final Exception exception)
    {
        final long clientId = 109;
        final long correlationId = 42;
        final int streamId = 100;
        final String channel = "aeron:ipc";
        final ErrorCode expectedErrorCode = ErrorCode.STORAGE_SPACE;
        final String expectedErrorMessage = exception.getMessage();
        doAnswer(invocation ->
        {
            LangUtil.rethrowUnchecked(exception);
            return null;
        }).when(driverConductor)
            .onAddIpcPublication(channel, streamId, correlationId, clientId, false);
        publicationMsgFlyweight.wrap(buffer, 0)
            .clientId(clientId)
            .correlationId(correlationId);
        publicationMsgFlyweight
            .streamId(streamId)
            .channel(channel);

        clientCommandAdapter.onMessage(
            ControlProtocolEvents.ADD_PUBLICATION, buffer, 0, publicationMsgFlyweight.length());

        final InOrder inOrder = inOrder(driverConductor, errorHandler, clientProxy);
        inOrder.verify(driverConductor).onAddIpcPublication(channel, streamId, correlationId, clientId, false);
        inOrder.verify(errorHandler).onError(exception);
        inOrder.verify(clientProxy).onError(correlationId, expectedErrorCode, expectedErrorMessage);
        inOrder.verifyNoMoreInteractions();
    }

    private static List<Throwable> noSpaceLeftExceptions()
    {
        return Arrays.asList(
            new IOException("No space left on device"),
            new UncheckedIOException(new IOException("No space left on device")),
            new RuntimeException(new IOException("No space left on device")));
    }
}
