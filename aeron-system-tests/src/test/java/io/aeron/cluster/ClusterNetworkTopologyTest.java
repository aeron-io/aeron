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

import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;
import io.aeron.CommonContext;
import io.aeron.cluster.client.AeronCluster;
import io.aeron.cluster.client.EgressListener;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.samples.cluster.ClusterConfig;
import io.aeron.samples.cluster.EchoServiceNode;
import io.aeron.samples.cluster.tutorial.BasicAuctionClusterClient;
import io.aeron.test.EventLogExtension;
import io.aeron.test.InterruptAfter;
import io.aeron.test.InterruptingTestCallback;
import io.aeron.test.SystemTestWatcher;
import io.aeron.test.Tests;
import io.aeron.test.TopologyTest;
import io.aeron.test.driver.TestMediaDriver;
import io.aeron.test.launcher.FileResolveUtil;
import io.aeron.test.launcher.RemoteLaunchClient;
import org.agrona.IoUtil;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.MutableLong;
import org.agrona.collections.MutableReference;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.*;

@TopologyTest
@ExtendWith({ EventLogExtension.class, InterruptingTestCallback.class })
@EnabledOnOs(OS.LINUX)
class ClusterNetworkTopologyTest
{
    private static final int REMOTE_LAUNCH_PORT = 11112;
    private static final long STARTUP_CANVASS_TIMEOUT_S =
        NANOSECONDS.toSeconds(2 * ConsensusModule.Configuration.leaderHeartbeatTimeoutNs());
    private static final long LIVENESS_TIMEOUT_S = 30;
    private static final long PUBLICATION_UNBLOCK_TIMEOUT_S = 60;
    private static final long MESSAGE_TIMEOUT_S = 20;
    private static final List<String> HOSTNAMES = Arrays.asList("10.42.0.10", "10.42.0.11", "10.42.0.12");
    private static final List<String> INTERNAL_HOSTNAMES = Arrays.asList("10.42.1.10", "10.42.1.11", "10.42.1.12");

    @RegisterExtension
    final SystemTestWatcher systemTestWatcher = new SystemTestWatcher();

    private File baseDir;

    @BeforeEach
    void setUp()
    {
        Tests.await(
            () ->
            {
                final List<VirtualMachineDescriptor> list = VirtualMachine.list();
                final List<VirtualMachineDescriptor> echoServices = list.stream()
                    .filter((vm) -> EchoServiceNode.class.getName().equals(vm.displayName()))
                    .collect(Collectors.toList());

                if (!echoServices.isEmpty())
                {
                    System.out.println(echoServices);
                    Tests.sleep(200, () -> "Failed to shutdown EchoServiceNode");
                }

                return echoServices.isEmpty();
            },
            SECONDS.toNanos(10));

        baseDir = FileResolveUtil.resolveClusterScriptDir();
        IoUtil.delete(new File(baseDir, "node0"), true);
        IoUtil.delete(new File(baseDir, "node1"), true);
        IoUtil.delete(new File(baseDir, "node2"), true);
    }

    @Test
    void shouldGetNetworkInformationFromAgentNodes()
    {
        assertTimeoutPreemptively(
            Duration.ofMillis(10_000),
            () ->
            {
                RemoteLaunchClient.connect(HOSTNAMES.get(0), REMOTE_LAUNCH_PORT)
                    .executeBlocking(System.out, "/usr/sbin/ip", "a");
                RemoteLaunchClient.connect(HOSTNAMES.get(1), REMOTE_LAUNCH_PORT)
                    .executeBlocking(System.out, "/usr/sbin/ip", "a");
                RemoteLaunchClient.connect(HOSTNAMES.get(2), REMOTE_LAUNCH_PORT)
                    .executeBlocking(System.out, "/usr/sbin/ip", "a");
            });
    }

    private static Stream<Arguments> provideTopologyConfigurations()
    {
        return Stream.of(
            Arguments.of(HOSTNAMES, null, "aeron:udp", null),
            Arguments.of(HOSTNAMES, null, "aeron:udp?endpoint=239.20.90.11:9152|interface=10.42.0.0/24", null),
            Arguments.of(HOSTNAMES, null, "aeron:udp", "aeron:udp?endpoint=239.20.90.13:9152|interface=10.42.0.0/24"),
            Arguments.of(HOSTNAMES, null, "aeron:udp", "aeron:udp?endpoint=239.20.90.13:9152|interface=10.42.1.0/24"),
            Arguments.of(HOSTNAMES, INTERNAL_HOSTNAMES, "aeron:udp", null));
    }

    private static Stream<Arguments> singleTopologyConfigurations()
    {
        return Stream.of(Arguments.of(
            HOSTNAMES, null, "aeron:udp?endpoint=239.20.90.11:9152|interface=10.42.0.0/24", null));
    }

    @ParameterizedTest
    @MethodSource("provideTopologyConfigurations")
    @InterruptAfter(60)
    void shouldGetEchoFromCluster(
        final List<String> hostnames,
        final List<String> internalHostnames,
        final String ingressChannel,
        final String logChannel) throws Exception
    {
        assertNotNull(hostnames);
        assertEquals(3, hostnames.size());
        setupDataCollection(3);
        final String ingressEndpoints = ingressChannel.contains("endpoint") ?
            null : BasicAuctionClusterClient.ingressEndpoints(hostnames);

        try (
            RemoteLaunchClient remote0 = RemoteLaunchClient.connect(hostnames.get(0), REMOTE_LAUNCH_PORT);
            RemoteLaunchClient remote1 = RemoteLaunchClient.connect(hostnames.get(1), REMOTE_LAUNCH_PORT);
            RemoteLaunchClient remote2 = RemoteLaunchClient.connect(hostnames.get(2), REMOTE_LAUNCH_PORT))
        {
            final Selector selector = Selector.open();
            launchNode(hostnames, internalHostnames, ingressChannel, logChannel, remote0, selector, 0);
            launchNode(hostnames, internalHostnames, ingressChannel, logChannel, remote1, selector, 1);
            launchNode(hostnames, internalHostnames, ingressChannel, logChannel, remote2, selector, 2);

            connectAndSendMessages(ingressChannel, ingressEndpoints, selector, 1);
        }
    }

    @ParameterizedTest
    @MethodSource("singleTopologyConfigurations")
    @InterruptAfter(60)
    void shouldLogReplicate(
        final List<String> hostnames,
        final List<String> internalHostnames,
        final String ingressChannel,
        final String logChannel) throws Exception
    {
        assertNotNull(hostnames);
        assertEquals(3, hostnames.size());
        setupDataCollection(3);

        final String ingressEndpoints = ingressChannel.contains("endpoint") ?
            null : BasicAuctionClusterClient.ingressEndpoints(hostnames);

        try (
            RemoteLaunchClient remote0 = RemoteLaunchClient.connect(hostnames.get(0), REMOTE_LAUNCH_PORT);
            RemoteLaunchClient remote1 = RemoteLaunchClient.connect(hostnames.get(1), REMOTE_LAUNCH_PORT))
        {
            final Selector selector = Selector.open();
            launchNode(hostnames, internalHostnames, ingressChannel, logChannel, remote0, selector, 0);
            launchNode(hostnames, internalHostnames, ingressChannel, logChannel, remote1, selector, 1);

            connectAndSendMessages(ingressChannel, ingressEndpoints, selector, 10);
        }

        Thread.sleep(5_000);

        try (
            RemoteLaunchClient remote0 = RemoteLaunchClient.connect(hostnames.get(0), REMOTE_LAUNCH_PORT);
            RemoteLaunchClient remote2 = RemoteLaunchClient.connect(hostnames.get(2), REMOTE_LAUNCH_PORT))
        {
            final Selector selector = Selector.open();
            launchNode(hostnames, internalHostnames, ingressChannel, logChannel, remote0, selector, 0);
            launchNode(hostnames, internalHostnames, ingressChannel, logChannel, remote2, selector, 2);

            connectAndSendMessages(ingressChannel, ingressEndpoints, selector, 10);
        }
    }

    private void setupDataCollection(final int nodeCount)
    {
        for (int nodeId = 0; nodeId < nodeCount; nodeId++)
        {
            systemTestWatcher.dataCollector().add(
                new File(CommonContext.getAeronDirectoryName() + "-" + nodeId + "-driver"));
            systemTestWatcher.dataCollector().add(new File(clusterNodeDir(nodeId), ClusterConfig.ARCHIVE_SUB_DIR));
            systemTestWatcher.dataCollector().add(new File(clusterNodeDir(nodeId), ClusterConfig.CLUSTER_SUB_DIR));
            systemTestWatcher.dataCollector().add(new File(clusterNodeDir(nodeId), "event.log"));
            systemTestWatcher.dataCollector().add(new File(clusterNodeDir(nodeId), "command.out"));
        }
    }

    private File clusterNodeDir(final int nodeId)
    {
        return new File(baseDir, "aeron-cluster-" + nodeId);
    }

    private void launchNode(
        final List<String> hostnames,
        final List<String> internalHostnames,
        final String ingressChannel,
        final String logChannel,
        final RemoteLaunchClient remote0,
        final Selector selector,
        final int nodeId) throws IOException
    {
        final String[] command0 = deriveCommand(nodeId, hostnames, internalHostnames, ingressChannel, logChannel);
        final SocketChannel execute0 = remote0.execute(false, command0);
        final Node node = new Node();

        execute0.register(selector, SelectionKey.OP_READ, node);
        while (node.checkOutput("Started Cluster Node"))
        {
            pollSelector(selector);
        }
    }

    private void connectAndSendMessages(
        final String ingressChannel,
        final String ingressEndpoints,
        final Selector selector,
        final int messageCount)
    {
        final String message = "Hello World!";
        final MutableDirectBuffer messageBuffer = new UnsafeBuffer(ByteBuffer.allocate(128));
        final int length = messageBuffer.putStringAscii(0, message);
        final MutableReference<String> egressResponse = new MutableReference<>();

        final EgressListener egressListener =
            (clusterSessionId, timestamp, buffer, offset, length1, header) ->
            {
                final String stringAscii = buffer.getStringAscii(offset);
                egressResponse.set(stringAscii);
            };

        try (TestMediaDriver mediaDriver = TestMediaDriver.launch(new MediaDriver.Context()
                .threadingMode(ThreadingMode.SHARED)
                .dirDeleteOnStart(true)
                .dirDeleteOnShutdown(true), systemTestWatcher);
            AeronCluster.AsyncConnect asyncConnect = AeronCluster.asyncConnect(new AeronCluster.Context()
                .messageTimeoutNs(SECONDS.toNanos(STARTUP_CANVASS_TIMEOUT_S * 2))
                .egressListener(egressListener)
                .egressChannel("aeron:udp?endpoint=10.42.0.1:0")
                .aeronDirectoryName(mediaDriver.aeronDirectoryName())
                .ingressChannel(ingressChannel)
                .ingressEndpoints(ingressEndpoints));
            AeronCluster aeronCluster = pollUntilConnected(asyncConnect, selector))
        {
            for (int i = 0; i < messageCount; i++)
            {
                Tests.await(
                    () ->
                    {
                        final long position = aeronCluster.offer(messageBuffer, 0, length);
                        pollSelector(selector);
                        return 0 < position;
                    },
                    SECONDS.toNanos(MESSAGE_TIMEOUT_S));

                final MutableLong nextKeepAliveNs = new MutableLong(System.nanoTime() + MILLISECONDS.toNanos(100));
                Tests.await(
                    () ->
                    {
                        final long nowNs = System.nanoTime();
                        if (nowNs >= nextKeepAliveNs.get())
                        {
                            aeronCluster.sendKeepAlive();
                            nextKeepAliveNs.set(nowNs + MILLISECONDS.toNanos(100));
                        }
                        aeronCluster.pollEgress();
                        pollSelector(selector);
                        return message.equals(egressResponse.get());
                    },
                    SECONDS.toNanos(MESSAGE_TIMEOUT_S));
            }
        }
    }

    private AeronCluster pollUntilConnected(final AeronCluster.AsyncConnect asyncConnect, final Selector selector)
    {
        AeronCluster aeronCluster;
        while (null == (aeronCluster = asyncConnect.poll()))
        {
            pollSelector(selector);
            Tests.sleep(1);
        }

        return aeronCluster;
    }

    @Test
    void shouldMatchPatternSplitAcrossReads()
    {
        final Node n = new Node();
        final String s = "Some text first: \u1F600";
        final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);

        final ByteBuffer allocate = ByteBuffer.allocate(1024);
        allocate.put(bytes, 0, 19);
        n.applyResponseData(allocate);
        allocate.put(bytes, 19, bytes.length - 19);
        n.applyResponseData(allocate);

        assertTrue(n.checkOutput(s));
    }

    @Test
    void shouldMatchPatternSplitAcrossBufferBoundary()
    {
        final Node n = new Node();
        final String s = "Some text first: \u1F600";
        final byte[] toMatch = s.getBytes(StandardCharsets.UTF_8);
        final byte[] bytes = new byte[4096];
        Arrays.fill(bytes, (byte)'a');

        final ByteBuffer allocate = ByteBuffer.allocate(4096);

        allocate.put(bytes);
        n.applyResponseData(allocate);

        System.arraycopy(toMatch, 0, bytes, bytes.length - (toMatch.length / 2), toMatch.length / 2);
        allocate.put(bytes);
        n.applyResponseData(allocate);

        Arrays.fill(bytes, (byte)'a');
        System.arraycopy(toMatch, (toMatch.length / 2), bytes, 0, toMatch.length - (toMatch.length / 2));
        allocate.put(bytes);
        n.applyResponseData(allocate);

        assertTrue(n.checkOutput(s));
    }

    static final class Node
    {
        private final CharBuffer textOutput = CharBuffer.allocate(8192);
        private final ByteBuffer binaryOutput = ByteBuffer.allocateDirect(textOutput.capacity() / 2);
        private final CharBuffer textOutputDup = textOutput.duplicate();
        private final CharsetDecoder decoder = StandardCharsets.UTF_8.newDecoder();

        void readChannel(final ReadableByteChannel channel) throws IOException
        {
            final int read = channel.read(binaryOutput);
            if (read > 0)
            {
                int initialTextPosition = textOutput.position();
                applyResponseData(binaryOutput);
                final int resultTextPosition = textOutput.position();

                if (resultTextPosition < initialTextPosition)
                {
                    initialTextPosition -= textOutput.capacity() / 2;
                }

                if (initialTextPosition < resultTextPosition)
                {
                    textOutputDup.clear().position(initialTextPosition).limit(resultTextPosition);
                }
            }
        }

        boolean checkOutput(final String regexToMatch)
        {
            final Pattern pattern = Pattern.compile(regexToMatch);
            final CharBuffer duplicate = textOutput.duplicate();
            duplicate.flip();

            return pattern.matcher(duplicate).find();
        }

        private void applyResponseData(final ByteBuffer data)
        {
            data.flip();

            final CoderResult result = decoder.decode(data, textOutput, false);
            if (CoderResult.OVERFLOW == result)
            {
                textOutput.limit(textOutput.capacity());
                textOutput.position(textOutput.capacity() / 2);
                textOutput.compact();
                decoder.decode(data, textOutput, false);
            }

            data.compact();
        }
    }

    private void pollSelector(final Selector selector)
    {
        try
        {
            final int select = selector.selectNow();
            if (select < 1)
            {
                return;
            }

            final Set<SelectionKey> selectionKeys = selector.selectedKeys();
            for (final SelectionKey selectionKey : selectionKeys)
            {
                final Node node = (Node)selectionKey.attachment();
                final ReadableByteChannel toReadFrom = (ReadableByteChannel)selectionKey.channel();

                node.readChannel(toReadFrom);
            }
            selectionKeys.clear();
        }
        catch (final IOException ex)
        {
            throw new UncheckedIOException(ex);
        }
    }

    private String[] deriveCommand(
        final int nodeId,
        final List<String> hostnames,
        final List<String> internalHostnames,
        final String ingressChannel,
        final String logChannel)
    {
        final ArrayList<String> command = new ArrayList<>();
        command.add(FileResolveUtil.resolveJavaBinary().getAbsolutePath());

        // Clean up and create the cluster node's base directory
        final File clusterDir = clusterNodeDir(nodeId);
        IoUtil.delete(clusterDir, false);
        IoUtil.ensureDirectoryExists(clusterDir, "cluster base directory");

        command.add("--add-opens");
        command.add("java.base/jdk.internal.misc=ALL-UNNAMED");
        command.add("--add-opens");
        command.add("java.base/java.util.zip=ALL-UNNAMED");
        command.add("-Xmx32m");
        command.add("-cp");
        command.add(FileResolveUtil.resolveAeronAllJar().getAbsolutePath());
        command.add("-javaagent:" + FileResolveUtil.resolveAeronAgentJar().getAbsolutePath());
        command.add("-Djava.net.preferIPv4Stack=true");
        command.add("-Daeron.dir.delete.on.start=true");
        command.add("-Daeron.event.cluster.log=all");
        command.add("-Daeron.event.cluster.log.disable=CANVASS_POSITION,APPEND_POSITION,COMMIT_POSITION");
        command.add("-Daeron.event.log.filename=" + new File(clusterDir, "event.log").getAbsolutePath());
        command.add("-Daeron.driver.resolver.name=node" + nodeId);
        command.add("-Daeron.cluster.startup.canvass.timeout=" + STARTUP_CANVASS_TIMEOUT_S + "s");
        command.add("-Daeron.client.liveness.timeout=" + LIVENESS_TIMEOUT_S + "s");
        command.add("-Daeron.driver.timeout=" + LIVENESS_TIMEOUT_S + "s");
        command.add("-Daeron.publication.unblock.timeout=" + PUBLICATION_UNBLOCK_TIMEOUT_S + "s");

        if (null != ingressChannel)
        {
            command.add("-Daeron.cluster.ingress.channel=" + ingressChannel);
        }

        if (null != logChannel)
        {
            command.add("-Daeron.cluster.log.channel=" + logChannel);
        }

        command.add("-Daeron.cluster.tutorial.hostnames=" + String.join(",", hostnames));
        if (null != internalHostnames && internalHostnames.size() == hostnames.size())
        {
            command.add("-Daeron.cluster.tutorial.hostnames.internal=" + String.join(",", hostnames));
        }

        command.add("-Daeron.cluster.tutorial.nodeId=" + nodeId);
        command.add("-Daeron.cluster.tutorial.baseDir=" + clusterDir);
        command.add(EchoServiceNode.class.getName());

        return command.toArray(new String[0]);
    }
}
