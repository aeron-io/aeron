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
import io.aeron.ChannelUriStringBuilder;
import io.aeron.driver.exceptions.InvalidChannelException;
import io.aeron.driver.media.UdpChannel;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.LangUtil;
import org.agrona.Strings;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import static io.aeron.driver.media.ControlMode.DYNAMIC;
import static io.aeron.driver.media.ControlMode.MANUAL;
import static io.aeron.driver.media.ControlMode.RESPONSE;
import static java.net.InetAddress.getByName;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class UdpChannelTest
{
    @Test
    void shouldHandleExplicitLocalAddressAndPortFormat()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?interface=localhost:40123|endpoint=localhost:40124");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    void shouldHandleExplicitLocalControlAddressAndPortFormatIPv4()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?control=localhost:40124|control-mode=dynamic");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("0.0.0.0", 0)));
    }

    @Test
    void shouldHandleExplicitLocalControlAddressAndPortFormatIPv6()
    {
        assumeTrue(System.getProperty("java.net.preferIPv4Stack") == null);

        final UdpChannel udpChannel = UdpChannel.parse(
            "aeron:udp?control=[fe80::5246:5dff:fe73:df06]:40124|control-mode=dynamic");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("fe80::5246:5dff:fe73:df06", 40124)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("fe80::5246:5dff:fe73:df06", 40124)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("::", 0)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("::", 0)));
    }

    @Test
    void shouldNotAllowDynamicControlModeWithoutExplicitControl()
    {
        try
        {
            UdpChannel.parse("aeron:udp?control-mode=dynamic");
            fail("InvalidChannelException expected");
        }
        catch (final InvalidChannelException ex)
        {
            final Throwable cause = ex.getCause();
            assertNotNull(cause);
            assertThat(cause.getMessage(), containsString("explicit control expected with dynamic control mode"));
        }
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    void shouldHandleExplicitLocalAddressAndPortFormatWithAeronUri(
        final String endpointKey, final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(
            uri(endpointKey, "localhost:40124", interfaceKey, "localhost:40123"));

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 40123)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    void shouldHandleImpliedLocalAddressAndPortFormat()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=localhost:40124");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    void shouldHandleImpliedLocalAddressAndPortFormatWithAeronUri(final String endpointKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(uri(endpointKey, "localhost:40124"));

        assertThat(udpChannel.localData(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @Test
    void shouldThrowExceptionForIncorrectScheme()
    {
        assertThrows(InvalidChannelException.class, () -> UdpChannel.parse("unknownudp://localhost:40124"));
    }

    @Test
    void shouldThrowExceptionForMissingAddressWithAeronUri()
    {
        assertThrows(InvalidChannelException.class, () -> UdpChannel.parse("aeron:udp"));
    }

    @Test
    void shouldThrowExceptionOnEvenMulticastAddress()
    {
        assertThrows(InvalidChannelException.class, () -> UdpChannel.parse("aeron:udp?endpoint=224.10.9.8"));
    }

    @Test
    void shouldParseValidMulticastAddress() throws IOException
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?interface=localhost|endpoint=224.10.9.9:40124");

        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteControl(), isMulticastAddress("224.10.9.10", 40124));
        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteData(), isMulticastAddress("224.10.9.9", 40124));
        assertThat(udpChannel.localInterface(),
            is(NetworkInterface.getByInetAddress(getByName("localhost"))));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    void shouldParseValidMulticastAddressWithAeronUri(final String endpointKey, final String interfaceKey)
        throws IOException
    {
        final UdpChannel udpChannel = UdpChannel.parse(
            uri(endpointKey, "224.10.9.9:40124", interfaceKey, "localhost"));

        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteControl(), isMulticastAddress("224.10.9.10", 40124));
        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteData(), isMulticastAddress("224.10.9.9", 40124));
        assertThat(udpChannel.localInterface(),
            is(NetworkInterface.getByInetAddress(getByName("localhost"))));
    }

    private Matcher<InetSocketAddress> isMulticastAddress(final String addressName, final int port)
        throws UnknownHostException
    {
        final InetAddress inetAddress = getByName(addressName);
        return is(new InetSocketAddress(inetAddress, port));
    }

    @Test
    void shouldHandleImpliedLocalPortFormat()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?interface=localhost|endpoint=localhost:40124");

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    void shouldHandleImpliedLocalPortFormatWithAeronUri(final String endpointKey, final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(
            uri(endpointKey, "localhost:40124", interfaceKey, "localhost"));

        assertThat(udpChannel.localData(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("localhost", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    void shouldHandleIPv4AnyAddressAsInterfaceAddressForUnicast(
        final String endpointKey, final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(
            uri(endpointKey, "localhost:40124", interfaceKey, "0.0.0.0"));

        assertThat(udpChannel.localData(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("0.0.0.0", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("localhost", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("localhost", 40124)));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    void shouldHandleIPv6AnyAddressAsInterfaceAddressForUnicast(
        final String endpointKey, final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(uri(endpointKey, "[::1]:40124", interfaceKey, "[::]"));

        assertThat(udpChannel.localData(), is(new InetSocketAddress("::", 0)));
        assertThat(udpChannel.localControl(), is(new InetSocketAddress("::", 0)));
        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("::1", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("::1", 40124)));
    }

    @Test
    void shouldHandleLocalhostLookup()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=localhost:40124");

        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("127.0.0.1", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("127.0.0.1", 40124)));
    }

    @ParameterizedTest
    @ValueSource(strings = "endpoint")
    void shouldHandleLocalhostLookupWithAeronUri(final String endpointKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(uri(endpointKey, "localhost:40124"));

        assertThat(udpChannel.remoteData(), is(new InetSocketAddress("127.0.0.1", 40124)));
        assertThat(udpChannel.remoteControl(), is(new InetSocketAddress("127.0.0.1", 40124)));
    }

    @Test
    void shouldHandleBeingUsedAsMapKey()
    {
        final UdpChannel udpChannel1 = UdpChannel.parse("aeron:udp?endpoint=localhost:40124");
        final UdpChannel udpChannel2 = UdpChannel.parse("aeron:udp?endpoint=localhost:40124");

        final Map<UdpChannel, Integer> map = new HashMap<>();

        map.put(udpChannel1, 1);
        assertThat(map.get(udpChannel2), is(1));
    }

    @Test
    void shouldThrowExceptionWhenNoPortSpecified()
    {
        assertThrows(InvalidChannelException.class, () -> UdpChannel.parse("aeron:udp?endpoint=localhost"));
    }

    @Test
    void shouldHandleCanonicalFormForUnicastCorrectly()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=192.168.0.1:20000");
        final UdpChannel udpChannelLocal = UdpChannel.parse("aeron:udp?interface=127.0.0.1|endpoint=192.168.0.1:20000");
        final UdpChannel udpChannelLocalPort = UdpChannel.parse(
            "aeron:udp?interface=127.0.0.1:40455|endpoint=192.168.0.1:20000");
        final UdpChannel udpChannelLocalhost = UdpChannel.parse(
            "aeron:udp?interface=localhost|endpoint=localhost:20000");

        assertThat(udpChannel.canonicalForm(), is("UDP-0.0.0.0:0-192.168.0.1:20000"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-127.0.0.1:0-192.168.0.1:20000"));
        assertThat(udpChannelLocalPort.canonicalForm(), is("UDP-127.0.0.1:40455-192.168.0.1:20000"));
        assertThat(udpChannelLocalhost.canonicalForm(), is("UDP-127.0.0.1:0-localhost:20000"));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    void shouldHandleIpV6CanonicalFormForUnicastCorrectly(
        final String endpointKey, final String interfaceKey)
    {
        assumeTrue(System.getProperty("java.net.preferIPv4Stack") == null);

        final UdpChannel udpChannelLocal =
            UdpChannel.parse(uri(endpointKey, "192.168.0.1:20000", interfaceKey, "[::1]"));
        final UdpChannel udpChannelLocalPort =
            UdpChannel.parse(uri(endpointKey, "[fe80::5246:5dff:fe73:df06]:20000", interfaceKey, "127.0.0.1:40455"));

        assertThat(
            udpChannelLocal.canonicalForm(),
            is("UDP-" + udpChannelLocal.localData().getHostString() + ":0-192.168.0.1:20000"));
        assertThat(
            udpChannelLocalPort.canonicalForm(),
            is("UDP-127.0.0.1:40455-[fe80::5246:5dff:fe73:df06]:20000"));
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    void shouldHandleCanonicalFormForUnicastCorrectlyWithAeronUri(
        final String endpointKey, final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(uri(endpointKey, "192.168.0.1:20000"));
        final UdpChannel udpChannelLocal =
            UdpChannel.parse(uri(endpointKey, "192.168.0.1:20000", interfaceKey, "127.0.0.1"));
        final UdpChannel udpChannelLocalPort =
            UdpChannel.parse(uri(endpointKey, "192.168.0.1:20000", interfaceKey, "127.0.0.1:40455"));
        final UdpChannel udpChannelLocalhost =
            UdpChannel.parse(uri(endpointKey, "localhost:20000", interfaceKey, "localhost"));

        assertThat(udpChannel.canonicalForm(), is("UDP-0.0.0.0:0-192.168.0.1:20000"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-127.0.0.1:0-192.168.0.1:20000"));
        assertThat(udpChannelLocalPort.canonicalForm(), is("UDP-127.0.0.1:40455-192.168.0.1:20000"));
        assertThat(udpChannelLocalhost.canonicalForm(), is("UDP-127.0.0.1:0-localhost:20000"));
    }

    @Test
    void shouldGetProtocolFamilyForIpV4()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:12345|interface=127.0.0.1");
        assertThat(udpChannel.protocolFamily(), is(StandardProtocolFamily.INET));
    }

    @Test
    void shouldGetProtocolFamilyForIpV6()
    {
        assumeTrue(System.getProperty("java.net.preferIPv4Stack") == null);

        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=[::1]:12345|interface=[::1]");
        assertThat(udpChannel.protocolFamily(), is(StandardProtocolFamily.INET6));
    }

    @Test
    void shouldGetProtocolFamilyForIpV4WithoutLocalSpecified()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:12345");
        assertThat(udpChannel.protocolFamily(), is(StandardProtocolFamily.INET));
    }

    @Test
    void shouldGetProtocolFamilyForIpV6WithoutLocalSpecified()
    {
        assumeTrue(System.getProperty("java.net.preferIPv4Stack") == null);

        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?endpoint=[::1]:12345");
        assertThat(udpChannel.protocolFamily(), is(StandardProtocolFamily.INET6));
    }

    @Test
    void shouldHandleCanonicalFormWithNsLookup()
    {
        final UdpChannel udpChannelExampleCom = UdpChannel.parse("aeron:udp?endpoint=localhost:20000");
        assertThat(udpChannelExampleCom.canonicalForm(), is("UDP-0.0.0.0:0-localhost:20000"));
    }

    @Test
    void shouldHandleCanonicalFormForMulticastWithLocalPort()
    {
        final UdpChannel udpChannelLocalPort = UdpChannel.parse(
            "aeron:udp?interface=127.0.0.1:40455|endpoint=224.0.1.1:20000");
        assertThat(udpChannelLocalPort.canonicalForm(), is("UDP-127.0.0.1:40455-224.0.1.1:20000"));

        final UdpChannel udpChannelSubnetLocalPort =
            UdpChannel.parse("aeron:udp?interface=127.0.0.0:40455/29|endpoint=224.0.1.1:20000");
        assertThat(
            udpChannelSubnetLocalPort.canonicalForm(),
            matchesPattern("UDP-127\\.0\\.0\\.[1-7]:40455-224\\.0\\.1\\.1:20000"));
    }

    @Test
    void shouldHandleCanonicalFormForMulticastCorrectly()
    {
        final UdpChannel udpChannel = UdpChannel.parse("aeron:udp?interface=localhost|endpoint=224.0.1.1:20000");
        final UdpChannel udpChannelLocal = UdpChannel.parse("aeron:udp?interface=127.0.0.1|endpoint=224.0.1.1:20000");
        final UdpChannel udpChannelAllSystems = UdpChannel.parse(
            "aeron:udp?interface=localhost|endpoint=224.0.1.1:20000");
        final UdpChannel udpChannelDefault = UdpChannel.parse("aeron:udp?endpoint=224.0.1.1:20000");

        final UdpChannel udpChannelSubnet = UdpChannel.parse(
            "aeron:udp?interface=localhost/29|endpoint=224.0.1.1:20000");
        final UdpChannel udpChannelSubnetLocal = UdpChannel.parse(
            "aeron:udp?interface=127.0.0.0/29|endpoint=224.0.1.1:20000");

        assertThat(udpChannel.canonicalForm(), is("UDP-127.0.0.1:0-224.0.1.1:20000"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-127.0.0.1:0-224.0.1.1:20000"));

        final Pattern canonicalFormPattern = Pattern.compile("UDP-127\\.0\\.0\\.[1-7]:0-224\\.0\\.1\\.1:20000");
        assertThat(udpChannelAllSystems.canonicalForm(), matchesPattern(canonicalFormPattern));
        assertThat(udpChannelSubnet.canonicalForm(), matchesPattern(canonicalFormPattern));
        assertThat(udpChannelSubnetLocal.canonicalForm(), matchesPattern(canonicalFormPattern));

        assertThat(udpChannelDefault.localInterface(), supportsMulticastOrIsLoopback());
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    void shouldHandleCanonicalFormForMulticastCorrectlyWithAeronUri(
        final String endpointKey, final String interfaceKey)
    {
        final UdpChannel udpChannel = UdpChannel.parse(uri(endpointKey, "224.0.1.1:20000", interfaceKey, "localhost"));
        final UdpChannel udpChannelLocal =
            UdpChannel.parse(uri(endpointKey, "224.0.1.1:20000", interfaceKey, "127.0.0.1"));
        final UdpChannel udpChannelAllSystems =
            UdpChannel.parse(uri(endpointKey, "224.0.0.1:20000", interfaceKey, "127.0.0.1"));
        final UdpChannel udpChannelDefault = UdpChannel.parse(uri(endpointKey, "224.0.1.1:20000"));
        final UdpChannel udpChannelSubnet =
            UdpChannel.parse(uri(endpointKey, "224.0.1.1:20000", interfaceKey, "localhost/24"));
        final UdpChannel udpChannelSubnetLocal =
            UdpChannel.parse(uri(endpointKey, "224.0.1.1:20000", interfaceKey, "127.0.0.0/24"));

        assertThat(udpChannel.canonicalForm(), is("UDP-127.0.0.1:0-224.0.1.1:20000"));
        assertThat(udpChannelLocal.canonicalForm(), is("UDP-127.0.0.1:0-224.0.1.1:20000"));
        assertThat(udpChannelAllSystems.canonicalForm(), is("UDP-127.0.0.1:0-224.0.0.1:20000"));

        final Pattern canonicalFormPattern = Pattern.compile("UDP-127\\.0\\.0\\.[1-7]:0-224\\.0\\.1\\.1:20000");
        assertThat(udpChannelSubnet.canonicalForm(), matchesPattern(canonicalFormPattern));
        assertThat(udpChannelSubnetLocal.canonicalForm(), matchesPattern(canonicalFormPattern));
        assertThat(udpChannelDefault.localInterface(), supportsMulticastOrIsLoopback());
    }

    @ParameterizedTest
    @CsvSource("endpoint,interface")
    void shouldHandleIpV6CanonicalFormForMulticastCorrectly(final String endpointKey, final String interfaceKey)
    {
        assumeTrue(System.getProperty("java.net.preferIPv4Stack") == null);

        final UdpChannel udpChannel =
            UdpChannel.parse(uri(endpointKey, "[FF01::FD]:20000", interfaceKey, "localhost"));
        final UdpChannel udpChannelLocal =
            UdpChannel.parse(uri(endpointKey, "224.0.1.1:20000", interfaceKey, "[::1]:54321/64"));

        assertThat(
            udpChannel.canonicalForm(),
            is("UDP-127.0.0.1:0-" + udpChannel.remoteData().getHostString() + ":20000"));
        assertThat(
            udpChannelLocal.canonicalForm(),
            is("UDP-" + udpChannelLocal.localData().getHostString() + ":54321-224.0.1.1:20000"));
    }

    @Test
    void shouldUseTagsInCanonicalFormForMdsUris()
    {
        assertEquals(
            "UDP-0.0.0.0:0-0.0.0.0:0#1001",
            UdpChannel.parse("aeron:udp?control-mode=manual|tags=1001").canonicalForm());
    }

    @Test
    void shouldParseResponseControlMode()
    {
        final UdpChannel channel = UdpChannel.parse("aeron:udp?control-mode=response|control=127.0.0.1:10001");
        assertEquals("UDP-127.0.0.1:10001-0.0.0.0:0", channel.canonicalForm());
        assertTrue(channel.isResponseControlMode());
    }

    @Test
    void shouldUseTagsInCanonicalFormForWildcardPorts()
    {
        assertEquals(
            "UDP-127.0.0.1:0-127.0.0.1:9999#1001",
            UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:9999|control=127.0.0.1:0|tags=1001").canonicalForm());
        assertEquals(
            "UDP-0.0.0.0:0-127.0.0.1:0#1001",
            UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:0|tags=1001").canonicalForm());
    }

    @Test
    void shouldParseSocketRcvAndSndBufSizes()
    {
        final UdpChannel udpChannelWithBufferSizes = UdpChannel.parse(
            "aeron:udp?endpoint=127.0.0.1:9999|so-sndbuf=4096|so-rcvbuf=8192");
        assertEquals(4096, udpChannelWithBufferSizes.socketSndbufLength());
        assertEquals(8192, udpChannelWithBufferSizes.socketRcvbufLength());

        final UdpChannel udpChannelWithoutBufferSizes = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:9999");
        assertEquals(0, udpChannelWithoutBufferSizes.socketRcvbufLength());
        assertEquals(0, udpChannelWithoutBufferSizes.socketSndbufLength());
    }

    @Test
    void shouldParseReceiverWindow()
    {
        final UdpChannel udpChannelWithBufferSizes = UdpChannel.parse("aeron:udp?endpoint=127.0.0.1:9999|rcv-wnd=8192");
        assertEquals(8192, udpChannelWithBufferSizes.receiverWindowLength());
    }

    @Test
    void shouldParseChannelSendAndReceiveTimestampOffsets()
    {
        final UdpChannel channel = UdpChannel.parse(
            "aeron:udp?endpoint=localhost:0|channel-rcv-ts-offset=reserved|channel-snd-ts-offset=8");

        assertEquals(UdpChannel.RESERVED_VALUE_MESSAGE_OFFSET, channel.channelReceiveTimestampOffset());
        assertEquals(8, channel.channelSendTimestampOffset());
    }

    @ParameterizedTest
    @CsvSource({
        "NAME_ENDPOINT,192.168.1.1,,,UDP-127.0.0.1:0-NAME_ENDPOINT",
        "localhost:41024,127.0.0.1,,,UDP-127.0.0.1:0-localhost:41024",
        "[fe80::5246:5dff:fe73:df06]:20000,[fe80::5246:5dff:fe73:df06],,," +
            "UDP-127.0.0.1:0-[fe80::5246:5dff:fe73:df06]:20000",
        "NAME_ENDPOINT,224.0.1.1,,,UDP-127.0.0.1:0-224.0.1.1:40124",
        "NAME_ENDPOINT,192.168.1.1,NAME_CONTROL,192.168.1.2,UDP-NAME_CONTROL-NAME_ENDPOINT",
        "NAME_ENDPOINT,224.0.1.1,NAME_CONTROL,127.0.0.1,UDP-127.0.0.1:0-224.0.1.1:40124",
        "192.168.1.1:40124,192.168.1.1,NAME_CONTROL,192.168.1.2,UDP-NAME_CONTROL-192.168.1.1:40124",
        "192.168.1.1:40124,192.168.1.1,192.168.1.2:40192,192.168.1.2,UDP-192.168.1.2:40192-192.168.1.1:40124",
    })
    void shouldParseWithNameResolver(
        final String endpointName,
        final String endpointAddress,
        final String controlName,
        final String controlAddress,
        final String canonicalForm)
    {
        final int port = 40124;

        final NameResolver resolver = new NameResolver()
        {
            public InetAddress resolve(final String name, final String uriParamName, final boolean isReResolution)
            {
                return DefaultNameResolver.INSTANCE.resolve(name, uriParamName, isReResolution);
            }

            public String lookup(final String name, final String uriParamName, final boolean isReLookup)
            {
                if (endpointName.equals(name))
                {
                    return endpointAddress + ":" + port;
                }
                else if (controlName.equals(name))
                {
                    return controlAddress + ":" + port;
                }

                return name;
            }
        };

        final ChannelUriStringBuilder uriBuilder = new ChannelUriStringBuilder()
            .media("udp")
            .networkInterface("localhost");

        if (!Strings.isEmpty(endpointName))
        {
            uriBuilder.endpoint(endpointName);
        }

        if (!Strings.isEmpty(controlName))
        {
            uriBuilder.controlEndpoint(controlName);
        }

        final UdpChannel udpChannel = UdpChannel.parse(uriBuilder.build(), resolver);

        assertThat(udpChannel.canonicalForm(), is(canonicalForm));
    }

    @Test
    void reservedValueOffsetIsCorrect()
    {
        assertEquals(
            DataHeaderFlyweight.RESERVED_VALUE_OFFSET,
            DataHeaderFlyweight.HEADER_LENGTH + UdpChannel.RESERVED_VALUE_MESSAGE_OFFSET);
    }

    @Test
    void invalidGroupTagThrowsException()
    {
        assertThrows(
            InvalidChannelException.class, () -> UdpChannel.parse("aeron:udp?endpoint=localhost:8080|gtag=foo"));
    }

    @Test
    void validGroupTagAvailable()
    {
        final UdpChannel channel = UdpChannel.parse("aeron:udp?endpoint=localhost:8080|gtag=1234");
        assertEquals(1234L, channel.groupTag());
    }

    @Test
    void shouldSpecifyControlIfDynamicControlModeSpecified()
    {
        assertThrows(InvalidChannelException.class, () -> UdpChannel.parse("aeron:udp?control-mode=dynamic"));
    }

    @Test
    void shouldParseControlMode()
    {
        assertEquals(DYNAMIC, UdpChannel.parse("aeron:udp?control=localhost:8080|control-mode=dynamic").controlMode());
        assertEquals(MANUAL, UdpChannel.parse("aeron:udp?control=localhost:8080|control-mode=manual").controlMode());
        assertEquals(
            RESPONSE, UdpChannel.parse("aeron:udp?control=localhost:8080|control-mode=response").controlMode());
    }

    @Test
    void shouldParseNakDelay()
    {
        assertEquals(
            MILLISECONDS.toNanos(34),
            UdpChannel.parse("aeron:udp?endpoint=localhost:8080|nak-delay=34ms").nakDelayNs());
        assertEquals(
            MICROSECONDS.toNanos(27),
            UdpChannel.parse("aeron:udp?endpoint=localhost:8080|nak-delay=27us").nakDelayNs());
    }

    @ParameterizedTest
    @MethodSource("invalidMulticastAddresses")
    void isMulticastDestinationAddressThrowsInvalidChannelExceptionIfInvalid(final ChannelUri uri)
    {
        assertThrowsExactly(InvalidChannelException.class, () -> UdpChannel.isMulticastDestinationAddress(uri));
    }

    @ParameterizedTest
    @CsvSource({
        "aeron:udp?mtu=8192|endpoint=192.168.0.1:5656, false",
        "aeron:udp?mtu=8192|endpoint=224.0.0.1:5656, true",
        "aeron:udp?endpoint=239.255.255.250:1010, true",
        "aeron:udp?endpoint=[FF01:0:0:0:0:0:0:18C]:5555, true",
        "aeron:udp?endpoint=[ff02::1]:1234, true",
        "aeron:udp?endpoint=[fe80::ce81:b1c:bd2c:69e%utun3]:8080, false"
    })
    void shouldDetectMulticastAddress(final String uri, final boolean expected)
    {
        assertEquals(expected, UdpChannel.isMulticastDestinationAddress(ChannelUri.parse(uri)));
    }

    private static List<ChannelUri> invalidMulticastAddresses()
    {
        return Arrays.asList(
            null,
            ChannelUri.parse("aeron:ipc"),
            ChannelUri.parse("aeron:udp?mtu=4096"),
            ChannelUri.parse("aeron:udp?endpoint=test"),
            ChannelUri.parse("aeron:udp?endpoint=test:a"),
            ChannelUri.parse("aeron:udp?endpoint=[xx:xx:xx:xx]:5656"));
    }

    private static Matcher<NetworkInterface> supportsMulticastOrIsLoopback()
    {
        return new NetworkInterfaceTypeSafeMatcher();
    }

    private static String uri(
        final String endpointKey, final String endpointValue, final String interfaceKey, final String interfaceValue)
    {
        return "aeron:udp?" + endpointKey + "=" + endpointValue + "|" + interfaceKey + "=" + interfaceValue;
    }

    private static String uri(final String endpointKey, final String endpointValue)
    {
        return "aeron:udp?" + endpointKey + "=" + endpointValue;
    }

    static class NetworkInterfaceTypeSafeMatcher extends TypeSafeMatcher<NetworkInterface>
    {
        public void describeTo(final Description description)
        {
            description.appendText("Interface supports multicast or is loopack");
        }

        protected boolean matchesSafely(final NetworkInterface networkInterface)
        {
            boolean matchesSafely = false;
            try
            {
                matchesSafely = networkInterface.supportsMulticast() || networkInterface.isLoopback();
            }
            catch (final SocketException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }

            return matchesSafely;
        }
    }
}
