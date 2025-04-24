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
package io.aeron.test.cluster;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.agrona.ErrorHandler;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;


import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.service.ClusterTerminationException;
import io.aeron.exceptions.AeronException;

public class ClusterTests
{
    public static final String HELLO_WORLD_MSG = "Hello World!";
    public static final String NO_OP_MSG = "No op!           ";
    public static final String REGISTER_TIMER_MSG = "Register a timer!";
    public static final String ECHO_SERVICE_IPC_INGRESS_MSG = "Echo as Service IPC ingress";
    public static final String ECHO_SERVICE_IPC_INGRESS_MSG_SKIP_FOLLOWER =
        "Echo as Service IPC ingress (skip follower)";
    public static final String UNEXPECTED_MSG =
        "Should never get this message because it is not going to be committed!";
    public static final String ERROR_MSG = "This message will cause an error";
    public static final String LARGE_MSG;
    public static final String TERMINATE_MSG = "Please terminate the clustered service";
    public static final String PAUSE = "Please pause when processing message";

    static
    {
        final byte[] bs = new byte[1024];
        Arrays.fill(bs, (byte)'a');
        LARGE_MSG = new String(bs, StandardCharsets.US_ASCII);
    }

    private static final AtomicReference<Throwable> ERROR = new AtomicReference<>();
    private static final AtomicReference<Throwable> WARNING = new AtomicReference<>();

    public static final Runnable NOOP_TERMINATION_HOOK = () -> {};

    public static Runnable terminationHook(final AtomicBoolean isTerminationExpected, final AtomicBoolean hasTerminated)
    {
        return
            () ->
            {
                if (null != isTerminationExpected && isTerminationExpected.get())
                {
                    if (null != hasTerminated)
                    {
                        hasTerminated.set(true);
                    }
                }
            };
    }

    public static ErrorHandler errorHandler(final int memberId)
    {
        return
            (ex) ->
            {
                if (ex instanceof AeronException &&
                    ((AeronException)ex).category() == AeronException.Category.WARN || shouldDownScaleToWarning(ex))
                {
                    addWarning(ex);
                    return;
                }

                if (ex instanceof ClusterTerminationException)
                {
                    return;
                }

                addError(ex);
                printMessageAndStackTrace("\n*** Error in member " + memberId + " ***\n\n", ex);
                printWarning();
            };
    }

    private static void printMessageAndStackTrace(final String message, final Throwable ex)
    {
        final StringWriter out = new StringWriter();
        final PrintWriter writer = new PrintWriter(out);
        writer.println(message);
        ex.printStackTrace(writer);
        System.err.println(out);
    }

    public static void addError(final Throwable ex)
    {
        final Throwable error = ERROR.get();
        if (null == error)
        {
            ERROR.set(ex);
        }
        else if (error != ex)
        {
            error.addSuppressed(ex);
        }
    }

    public static void addWarning(final Throwable ex)
    {
        final Throwable warning = WARNING.get();
        if (null == warning)
        {
            WARNING.set(ex);
        }
        else if (warning != ex)
        {
            warning.addSuppressed(ex);
        }
    }

    public static void printWarning()
    {
        final Throwable warning = WARNING.get();
        if (null != warning)
        {
            printMessageAndStackTrace("\n*** Warning captured ***", warning);
            warning.printStackTrace();
        }
    }

    public static void failOnClusterError()
    {
        Throwable error = ERROR.getAndSet(null);
        Throwable warning = WARNING.getAndSet(null);

        if (null != error && shouldDownScaleToWarning(error))
        {
            warning = error;
            error = null;
        }

        if (null != error)
        {
            if (null != warning)
            {
                System.err.println("\n*** Warning captured with error ***");
                warning.printStackTrace(System.err);
            }

            throw new RuntimeException("Cluster node received error", error);
        }

        if (Thread.currentThread().isInterrupted() && null != warning)
        {
            System.err.println("\n*** Warning captured with interrupt ***");
            warning.printStackTrace(System.err);
        }
    }

    public static void failOnClusterWarning()
    {
        final Throwable warning = WARNING.getAndSet(null);

        if (warning != null)
        {
            System.err.println("\n*** Warning captured ***");
            warning.printStackTrace(System.err);

            throw new RuntimeException("Cluster node received warning", warning);
        }
    }

    public static void resetClusterError()
    {
        ERROR.set(null);
        WARNING.set(null);
    }

    private static boolean shouldDownScaleToWarning(final Throwable error)
    {
        int depthLimit = 10;
        Throwable maybeWarning = error;
        while (null != maybeWarning && 0 < --depthLimit)
        {
            if (maybeWarning instanceof UnknownHostException)
            {
                return true;
            }

            maybeWarning = maybeWarning.getCause();
        }

        return false;
    }

    public static Thread startPublisherThread(final TestCluster testCluster, final MutableInteger messageCounter)
    {
        final Thread thread = new Thread(
            () ->
            {
                final IdleStrategy idleStrategy = YieldingIdleStrategy.INSTANCE;
                final AeronCluster client = testCluster.client();
                final ExpandableArrayBuffer msgBuffer = testCluster.msgBuffer();
                final int messageLength = msgBuffer.putStringWithoutLengthAscii(0, HELLO_WORLD_MSG);

                while (!Thread.interrupted())
                {
                    final long result = client.offer(msgBuffer, 0, messageLength);
                    if (result > 0)
                    {
                        messageCounter.increment();
                    }
                    else
                    {
                        if (client.isClosed())
                        {
                            break;
                        }
                    }

                    try
                    {
                        Thread.sleep(1);
                    }
                    catch (final InterruptedException ignore)
                    {
                        break;
                    }

                    idleStrategy.idle(client.pollEgress());
                }
            });

        thread.setDaemon(true);
        thread.setName("message-thread");
        thread.start();

        return thread;
    }
}
