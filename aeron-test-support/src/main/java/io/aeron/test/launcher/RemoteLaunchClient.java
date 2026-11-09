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
package io.aeron.test.launcher;

import io.aeron.driver.Configuration;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;

public final class RemoteLaunchClient implements AutoCloseable
{
    private final String host;
    private final int port;
    private SocketChannel clientChannel;

    private RemoteLaunchClient(final String host, final int port)
    {
        this.host = host;
        this.port = port;
    }

    public static RemoteLaunchClient connect(final String host, final int port) throws IOException
    {
        final RemoteLaunchClient remoteLaunchClient = new RemoteLaunchClient(host, port);
        remoteLaunchClient.init();

        return remoteLaunchClient;
    }

    private void init() throws IOException
    {
        clientChannel = SocketChannel.open();
        clientChannel.socket().connect(new InetSocketAddress(host, port), 5_000);
    }

    public ReadableByteChannel execute(final String... command) throws IOException
    {
        return execute(true, command);
    }

    public SocketChannel execute(final boolean usingBlockingIo, final String... command) throws IOException
    {
        final ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        try (ObjectOutputStream out = new ObjectOutputStream(byteArrayOutputStream))
        {
            out.writeObject(command);
        }

        final ByteBuffer buffer = ByteBuffer.allocateDirect(byteArrayOutputStream.size());
        buffer.put(byteArrayOutputStream.toByteArray());
        buffer.flip();
        clientChannel.write(buffer);

        clientChannel.configureBlocking(usingBlockingIo);

        return clientChannel;
    }

    public void executeBlocking(final OutputStream out, final String... command) throws IOException
    {
        try (ReadableByteChannel commandResponse = execute(command))
        {
            final ByteBuffer buffer = ByteBuffer.allocate(Configuration.filePageSize());

            while (commandResponse.isOpen())
            {
                final int read = commandResponse.read(buffer);
                if (read < 0)
                {
                    break;
                }

                buffer.flip();
                out.write(buffer.array(), buffer.position(), buffer.limit());
                buffer.clear();
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        clientChannel.close();
    }
}
