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
package io.aeron;

import io.aeron.test.Tests;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.aeron.ChannelUri.MAX_URI_LENGTH;
import static io.aeron.ChannelUri.transformAlias;
import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.*;

class ChannelUriTest
{
    @Test
    void shouldParseSimpleDefaultUri()
    {
        assertParseWithMedia("aeron:udp", "udp");
        assertParseWithMedia("aeron:ipc", "ipc");
        assertParseWithMediaAndPrefix("aeron-spy:aeron:ipc", "aeron-spy", "ipc");
    }

    @Test
    void shouldRejectUriWithoutAeronPrefix()
    {
        assertInvalid(":udp");
        assertInvalid("aeron");
        assertInvalid("aron:");
        assertInvalid("eeron:");
    }

    @Test
    void shouldRejectWithOutOfPlaceColon()
    {
        assertInvalid("aeron:udp:");
    }

    @Test
    void shouldRejectWithInvalidMedia()
    {
        assertInvalid("aeron:ipcsdfgfdhfgf");
    }

    @Test
    void shouldRejectWithMissingQuerySeparatorWhenFollowedWithParams()
    {
        assertInvalid("aeron:ipc|sparse=true");
    }

    @Test
    void shouldRejectWithInvalidParams()
    {
        assertInvalid("aeron:udp?endpoint=localhost:4652|-~@{]|=??#s!£$%====");
    }

    @ParameterizedTest
    @ValueSource(ints = { MAX_URI_LENGTH + 1, 10000 })
    void shouldRejectUriIfLengthExceedsMaxAllowed(final int length)
    {
        final String uri = Tests.generateStringWithSuffix("aeron:ipc?alias=", "x", length);
        final IllegalArgumentException exception =
            assertThrows(IllegalArgumentException.class, () -> ChannelUri.parse(uri));
        assertEquals("URI length (" + uri.length() + ") exceeds max supported length (" + MAX_URI_LENGTH +
            "): " + uri.substring(0, MAX_URI_LENGTH), exception.getMessage());
    }

    @Test
    void shouldParseWithMultipleParameters()
    {
        assertParseWithParams(
            "aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16",
            "endpoint", "224.10.9.8",
            "port", "4567",
            "interface", "192.168.0.3",
            "ttl", "16");
    }

    @Test
    void shouldAllowReturnDefaultIfParamNotSpecified()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=224.10.9.8");
        assertNull(uri.get("interface"));
        assertEquals("192.168.0.0", uri.get("interface", "192.168.0.0"));
    }

    @Test
    void shouldRoundTripToString()
    {
        final String uriString = "aeron:udp?endpoint=224.10.9.8:777";
        final ChannelUri uri = ChannelUri.parse(uriString);

        final String result = uri.toString();
        assertEquals(uriString, result);
    }

    @Test
    void shouldRoundTripToStringBuilder()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .media("udp")
            .endpoint("224.10.9.8:777");
        final String uriString = builder.build();
        final ChannelUri uri = ChannelUri.parse(uriString);

        assertEquals(uriString, uri.toString());
    }

    @Test
    void shouldRoundTripToStringBuilderWithPrefix()
    {
        final ChannelUriStringBuilder builder = new ChannelUriStringBuilder()
            .prefix(ChannelUri.SPY_QUALIFIER)
            .media("udp")
            .endpoint("224.10.9.8:777");
        final String uriString = builder.build();
        final ChannelUri uri = ChannelUri.parse(uriString);

        assertEquals(uriString, uri.toString());
    }

    @Test
    void equalsReturnsTrueWhenTheSameInstance()
    {
        final ChannelUri channelUri = ChannelUri.parse(
            "aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16");

        assertEquals(channelUri, channelUri);
    }

    @Test
    void equalsReturnsFalseIfComparedWithNull()
    {
        final ChannelUri channelUri = ChannelUri.parse(
            "aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16");

        assertNotEquals(channelUri, null);
    }

    @Test
    void equalsReturnsFalseIfComparedAnotherClass()
    {
        final ChannelUri channelUri = ChannelUri.parse(
            "aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16");

        //noinspection AssertBetweenInconvertibleTypes
        assertNotEquals(channelUri, 123);
    }

    @ParameterizedTest
    @MethodSource("equalityValues")
    void equality(final String uri1, final String uri2, final boolean expected)
    {
        final ChannelUri parsedUri1 = ChannelUri.parse(uri1);
        final ChannelUri parsedUri2 = ChannelUri.parse(uri2);

        assertEquals(expected, parsedUri1.equals(parsedUri2));
    }

    @ParameterizedTest
    @MethodSource("equalityValues")
    void hashCode(final String uri1, final String uri2, final boolean expected)
    {
        final ChannelUri parsedUri1 = ChannelUri.parse(uri1);
        final ChannelUri parsedUri2 = ChannelUri.parse(uri2);

        if (expected)
        {
            assertEquals(parsedUri1.hashCode(), parsedUri2.hashCode());
        }
        else
        {
            assertNotEquals(parsedUri1.hashCode(), parsedUri2.hashCode());
        }
    }

    @ParameterizedTest
    @CsvSource(value = {
        "aeron:udp?endpoint=poison|interface=iface|mtu=4444, dest1, aeron:udp?endpoint=dest1|interface=iface",
        "aeron:ipc, dest2, aeron:ipc?endpoint=dest2",
        "aeron:udp, localhost, aeron:udp?endpoint=localhost",
        "aeron:ipc?interface=here, there, aeron:ipc?endpoint=there|interface=here",
        "aeron-spy:aeron:udp?eol=true|interface=none, abc, aeron:udp?endpoint=abc|interface=none",
        "aeron:udp?interface=eth0|term-length=64k|ttl=0|endpoint=some, vm1, aeron:udp?endpoint=vm1|interface=eth0" })
    void createDestinationUriTest(final String channel, final String endpoint, final String expected)
    {
        final String destinationUri = ChannelUri.createDestinationUri(channel, endpoint);
        assertEquals(expected, destinationUri);
    }

    @Test
    void shouldSubstituteEndpoint()
    {
        assertSubstitution("aeron:udp?endpoint=localhost:12345", "aeron:udp?endpoint=localhost:0", "localhost:12345");
        assertSubstitution(
            "aeron:udp?endpoint=localhost:12345", "aeron:udp?endpoint=localhost:12345", "localhost:54321");
        assertSubstitution("aeron:udp?endpoint=localhost:12345", "aeron:udp?endpoint=localhost:0", "127.0.0.1:12345");
        assertSubstitution("aeron:udp?endpoint=127.0.0.1:12345", "aeron:udp", "127.0.0.1:12345");
    }

    @Test
    void shouldThrowIfResolvedEndpointInvalid()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:udp?endpoint=localhost:0");
        assertThrows(IllegalArgumentException.class, () -> uri.replaceEndpointWildcardPort("localhost:0"));
        assertThrows(IllegalArgumentException.class, () -> uri.replaceEndpointWildcardPort("localhost"));
        assertThrows(NullPointerException.class, () -> uri.replaceEndpointWildcardPort(null));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldIterateOverParameters()
    {
        final ChannelUri uri = ChannelUri.parse(
            "aeron:udp?endpoint=myhost:0|ttl=1|mtu=8k|term-length=64M|alias=text|x=42");
        final BiConsumer<String, String> parameterConsumer = mock(BiConsumer.class);

        uri.forEachParameter(parameterConsumer);

        verify(parameterConsumer).accept("endpoint", "myhost:0");
        verify(parameterConsumer).accept("ttl", "1");
        verify(parameterConsumer).accept("mtu", "8k");
        verify(parameterConsumer).accept("term-length", "64M");
        verify(parameterConsumer).accept("alias", "text");
        verify(parameterConsumer).accept("x", "42");
        verifyNoMoreInteractions(parameterConsumer);
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldNotCallConsumerIfNoParameters()
    {
        final ChannelUri uri = ChannelUri.parse("aeron:ipc");
        final BiConsumer<String, String> parameterConsumer = mock(BiConsumer.class);

        uri.forEachParameter(parameterConsumer);

        verifyNoInteractions(parameterConsumer);
    }

    @ParameterizedTest
    @NullAndEmptySource
    void shouldReturnOriginalUriWhenAliasIsEmpty(final String alias)
    {
        final String uri = "aeron:udp";
        assertEquals(uri, ChannelUri.addAliasIfAbsent(uri, alias));
    }

    @Test
    void shouldReturnOriginalUriWhenAliasIsAlreadyDefined()
    {
        final String uri = "aeron:udp?alias=xyz|term-length=64k";
        final String alias = "new alias";
        assertEquals(uri, ChannelUri.addAliasIfAbsent(uri, alias));
    }

    @ParameterizedTest
    @ValueSource(strings = {
        "aeron:udp?term-length=64k|ssc=false|mtu=8k",
        "aeron:ipc",
        "aeron:udp?custom=alias",
    })
    void shouldReturnNewUriWithAnAliasAdded(final String uri)
    {
        final String alias = "my alias";
        final ChannelUri channelUri = ChannelUri.parse(uri);
        channelUri.put(CommonContext.ALIAS_PARAM_NAME, alias);

        final String result = ChannelUri.addAliasIfAbsent(uri, alias);
        assertNotNull(result);
        assertEquals(channelUri, ChannelUri.parse(result));
    }

    @Test
    void shouldTransformAlias()
    {
        assertEquals("aeron:ipc", transformAlias("aeron:ipc", (alias) -> alias));
        assertEquals("aeron:ipc?alias=foo", transformAlias("aeron:ipc?alias=foo", (alias) -> alias));
        assertEquals("aeron:ipc", transformAlias("aeron:ipc?alias=foo", (alias) -> null));
        assertEquals("aeron:ipc", transformAlias("aeron:ipc?alias=foo", (alias) -> ""));
        final Function<String, String> fun = (alias) -> null == alias ? "default" : alias + "bar";
        assertEquals("aeron:ipc?alias=default", transformAlias("aeron:ipc", fun));
        assertEquals("aeron:ipc?alias=foobar", transformAlias("aeron:ipc?alias=foo", fun));
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = { "abc", "1.0", "+14x"})
    void validateSocketTosThrowsIllegalArgumentExceptionIfNotANumber(final String tos)
    {
        final IllegalArgumentException exception =
            assertThrowsExactly(IllegalArgumentException.class, () -> ChannelUri.validateSocketTos(tos));
        assertEquals("so-tos must be a number", exception.getMessage());
    }

    @ParameterizedTest
    @ValueSource(strings = { "-1", "256", "1000000"})
    void validateSocketTosThrowsIllegalArgumentExceptionIfOutOfRange(final String tos)
    {
        final IllegalArgumentException exception =
            assertThrowsExactly(IllegalArgumentException.class, () -> ChannelUri.validateSocketTos(tos));
        assertEquals("so-tos must be between 0 and 255 inclusive: " + tos, exception.getMessage());
    }

    private static void assertSubstitution(
        final String expected,
        final String originalChannel,
        final String resolvedEndpoint)
    {
        final ChannelUri uri = ChannelUri.parse(originalChannel);
        uri.replaceEndpointWildcardPort(resolvedEndpoint);
        assertEquals(expected, uri.toString());
    }

    private static List<Arguments> equalityValues()
    {
        return asList(
            arguments("aeron:udp?endpoint=224.10.9.8", "aeron:udp?endpoint=224.10.9.8", true),
            arguments(
                "aeron:udp?endpoint=224.10.9.8|port=4567|interface=192.168.0.3|ttl=16",
                "aeron:udp?port=4567|endpoint=224.10.9.8|ttl=16|interface=192.168.0.3",
                true),
            arguments(
                "aeron:udp?endpoint=224.10.9.8|tags=1,2",
                "aeron:udp?tags=1,2|endpoint=224.10.9.8",
                true),
            arguments("aeron:udp?endpoint=224.10.9.8", "aeron-spy:aeron:udp?endpoint=224.10.9.8", false),
            arguments("aeron:udp?endpoint=224.10.9.8", "aeron:ipc?endpoint=224.10.9.8", false),
            arguments("aeron:udp?endpoint=224.10.9.9", "aeron:udp?endpoint=224.10.8.8", false),
            arguments("aeron:udp?endpoint=224.10.9.8|ttl=16", "aeron:ipc?endpoint=224.10.9.8|port=4567", false),
            arguments("aeron:udp?endpoint=224.10.9.8|tags=2,1", "aeron:udp?endpoint=224.10.9.8|tags=1,2", false)
        );
    }

    private void assertParseWithParams(final String uriStr, final String... params)
    {
        if (params.length % 2 != 0)
        {
            throw new IllegalArgumentException();
        }

        final ChannelUri uri = ChannelUri.parse(uriStr);

        for (int i = 0; i < params.length; i += 2)
        {
            assertEquals(params[i + 1], uri.get(params[i]));
        }
    }

    private void assertParseWithMedia(final String uriStr, final String media)
    {
        assertParseWithMediaAndPrefix(uriStr, "", media);
    }

    private void assertParseWithMediaAndPrefix(final String uriStr, final String prefix, final String media)
    {
        final ChannelUri uri = ChannelUri.parse(uriStr);
        assertEquals("aeron", uri.scheme());
        assertEquals(prefix, uri.prefix());
        assertEquals(media, uri.media());
    }

    private static void assertInvalid(final String string)
    {
        try
        {
            ChannelUri.parse(string);
            fail(IllegalArgumentException.class.getName() + " not thrown");
        }
        catch (final IllegalArgumentException | IllegalStateException ignore)
        {
        }
    }
}
