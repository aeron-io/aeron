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

import io.aeron.logbuffer.LogBufferDescriptor;
import org.agrona.AsciiEncoding;
import org.agrona.Strings;
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.aeron.CommonContext.*;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static java.util.Objects.requireNonNull;

/**
 * Parser for Aeron channel URIs. The format is:
 * <pre>
 * aeron-uri = "aeron:" media [ "?" param *( "|" param ) ]
 * media     = *( "[^?:]" )
 * param     = key "=" value
 * key       = *( "[^=]" )
 * value     = *( "[^|]" )
 * </pre>
 * <p>
 * Multiple params with the same key are allowed, the last value specified takes precedence.
 *
 * @see ChannelUriStringBuilder
 */
public final class ChannelUri
{
    @SuppressWarnings("JavadocVariable")
    private enum State
    {
        MEDIA, PARAMS_KEY, PARAMS_VALUE
    }

    /**
     * URI Scheme for Aeron channels and destinations.
     */
    public static final String AERON_SCHEME = "aeron";

    /**
     * Qualifier for spy subscriptions which spy on outgoing network destined traffic efficiently.
     */
    public static final String SPY_QUALIFIER = "aeron-spy";

    /**
     * Invalid tag value returned when calling {@link #getTag(String)} and the channel is not tagged.
     */
    public static final long INVALID_TAG = Aeron.NULL_VALUE;

    /**
     * Max length in characters for the URI string.
     */
    public static final int MAX_URI_LENGTH = 4095;

    private static final int CHANNEL_TAG_INDEX = 0;
    private static final int ENTITY_TAG_INDEX = 1;

    private static final String AERON_PREFIX = AERON_SCHEME + ":";

    private String prefix;
    private String media;
    private final Object2ObjectHashMap<String, String> params;
    private final String[] tags;

    /**
     * Construct with the components provided to avoid parsing.
     *
     * @param prefix empty if no prefix is required otherwise expected to be 'aeron-spy'
     * @param media  for the channel which is typically "udp" or "ipc".
     * @param params for the query string as key value pairs.
     */
    private ChannelUri(final String prefix, final String media, final Object2ObjectHashMap<String, String> params)
    {
        this.prefix = prefix;
        this.media = media;
        this.params = params;
        this.tags = splitTags(params.get(TAGS_PARAM_NAME));
    }

    /**
     * The prefix for the channel.
     *
     * @return the prefix for the channel.
     */
    public String prefix()
    {
        return prefix;
    }

    /**
     * Change the prefix from what has been parsed.
     *
     * @param prefix to replace the existing prefix.
     * @return this for a fluent API.
     */
    public ChannelUri prefix(final String prefix)
    {
        this.prefix = prefix;
        return this;
    }

    /**
     * The media over which the channel operates.
     *
     * @return the media over which the channel operates.
     */
    public String media()
    {
        return media;
    }

    /**
     * Set the media over which the channel operates.
     *
     * @param media to replace the parsed value.
     * @return this for a fluent API.
     */
    public ChannelUri media(final String media)
    {
        validateMedia(media);
        this.media = media;
        return this;
    }

    /**
     * Is the channel {@link #media()} equal to {@link CommonContext#UDP_MEDIA}.
     *
     * @return true the channel {@link #media()} equals {@link CommonContext#UDP_MEDIA}.
     */
    public boolean isUdp()
    {
        return UDP_MEDIA.equals(media);
    }

    /**
     * Is the channel {@link #media()} equal to {@link CommonContext#IPC_MEDIA}.
     *
     * @return true the channel {@link #media()} equals {@link CommonContext#IPC_MEDIA}.
     */
    public boolean isIpc()
    {
        return IPC_MEDIA.equals(media);
    }

    /**
     * The scheme for the URI. Must be "aeron".
     *
     * @return the scheme for the URI.
     */
    public String scheme()
    {
        return AERON_SCHEME;
    }

    /**
     * Get a value for a given parameter key.
     *
     * @param key to lookup.
     * @return the value if set for the key otherwise null.
     */
    public String get(final String key)
    {
        return params.get(key);
    }

    /**
     * Get the value for a given parameter key or the default value provided if the key does not exist.
     *
     * @param key          to lookup.
     * @param defaultValue to be returned if no key match is found.
     * @return the value if set for the key otherwise the default value provided.
     */
    public String get(final String key, final String defaultValue)
    {
        final String value = params.get(key);
        if (null != value)
        {
            return value;
        }

        return defaultValue;
    }

    /**
     * Put a key and value pair in the map of params.
     *
     * @param key   of the param to be put.
     * @param value of the param to be put.
     * @return the existing value otherwise null.
     */
    public String put(final String key, final String value)
    {
        return params.put(key, value);
    }

    /**
     * Remove a key pair in the map of params.
     *
     * @param key of the param to be removed.
     * @return the previous value of the param or null.
     */
    public String remove(final String key)
    {
        return params.remove(key);
    }

    /**
     * Does the URI contain a value for the given key.
     *
     * @param key to be lookup.
     * @return true if the key has a value otherwise false.
     */
    public boolean containsKey(final String key)
    {
        return params.containsKey(key);
    }

    /**
     * Get the channel tag, if it exists, that refers to another channel.
     *
     * @return channel tag if it exists or null if not in this URI.
     * @see CommonContext#TAGS_PARAM_NAME
     * @see CommonContext#TAG_PREFIX
     */
    public String channelTag()
    {
        return tags.length > CHANNEL_TAG_INDEX ? tags[CHANNEL_TAG_INDEX] : null;
    }

    /**
     * Get the entity tag, if it exists, that refers to an entity such as subscription or publication.
     *
     * @return entity tag if it exists or null if not in this URI.
     * @see CommonContext#TAGS_PARAM_NAME
     * @see CommonContext#TAG_PREFIX
     */
    public String entityTag()
    {
        return tags.length > ENTITY_TAG_INDEX ? tags[ENTITY_TAG_INDEX] : null;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (!(o instanceof ChannelUri))
        {
            return false;
        }

        final ChannelUri that = (ChannelUri)o;

        return Objects.equals(prefix, that.prefix) &&
            Objects.equals(media, that.media) &&
            Objects.equals(params, that.params) &&
            Arrays.equals(tags, that.tags);
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode()
    {
        int result = 19;
        result = 31 * result + Objects.hashCode(prefix);
        result = 31 * result + Objects.hashCode(media);
        result = 31 * result + Objects.hashCode(params);
        result = 31 * result + Arrays.hashCode(tags);

        return result;
    }

    /**
     * Generate a String representation of the URI that is valid for an Aeron channel.
     *
     * @return a String representation of the URI that is valid for an Aeron channel.
     */
    public String toString()
    {
        final StringBuilder sb;
        if (prefix == null || prefix.isEmpty())
        {
            sb = new StringBuilder((params.size() * 20) + 10);
        }
        else
        {
            sb = new StringBuilder((params.size() * 20) + 20);
            sb.append(prefix);
            if (!prefix.endsWith(":"))
            {
                sb.append(':');
            }
        }

        sb.append(AERON_PREFIX).append(media);

        if (!params.isEmpty())
        {
            sb.append('?');

            for (final Map.Entry<String, String> entry : params.entrySet())
            {
                sb.append(entry.getKey()).append('=').append(entry.getValue()).append('|');
            }

            sb.setLength(sb.length() - 1);
        }

        return sb.toString();
    }

    /**
     * Initialise a channel for restarting a publication at a given position.
     *
     * @param position      at which the publication should be started.
     * @param initialTermId what which the stream would start.
     * @param termLength    for the stream.
     */
    public void initialPosition(final long position, final int initialTermId, final int termLength)
    {
        if (position < 0 || 0 != (position & (FRAME_ALIGNMENT - 1)))
        {
            throw new IllegalArgumentException("invalid position: " + position);
        }

        final int bitsToShift = LogBufferDescriptor.positionBitsToShift(termLength);
        final int termId = LogBufferDescriptor.computeTermIdFromPosition(position, bitsToShift, initialTermId);
        final int termOffset = (int)(position & (termLength - 1));

        put(INITIAL_TERM_ID_PARAM_NAME, Integer.toString(initialTermId));
        put(TERM_ID_PARAM_NAME, Integer.toString(termId));
        put(TERM_OFFSET_PARAM_NAME, Integer.toString(termOffset));
        put(TERM_LENGTH_PARAM_NAME, Integer.toString(termLength));
    }

    /**
     * Parse a {@link CharSequence} which contains an Aeron URI.
     *
     * @param uri to be parsed.
     * @return a new {@link ChannelUri} representing the URI string.
     */
    @SuppressWarnings("MethodLength")
    public static ChannelUri parse(final CharSequence uri)
    {
        final int length = uri.length();
        if (length > MAX_URI_LENGTH)
        {
            throw new IllegalArgumentException("URI length (" + length + ") exceeds max supported length (" +
                MAX_URI_LENGTH + "): " + uri.subSequence(0, MAX_URI_LENGTH));
        }

        int position = 0;
        final String prefix;
        if (startsWith(uri, 0, SPY_PREFIX))
        {
            prefix = SPY_QUALIFIER;
            position = SPY_PREFIX.length();
        }
        else
        {
            prefix = "";
        }

        if (!startsWith(uri, position, AERON_PREFIX))
        {
            throw new IllegalArgumentException("Aeron URIs must start with 'aeron:', found: " + uri);
        }
        else
        {
            position += AERON_PREFIX.length();
        }

        final StringBuilder builder = new StringBuilder();
        final Object2ObjectHashMap<String, String> params = new Object2ObjectHashMap<>();
        String media = null;
        String key = null;

        State state = State.MEDIA;
        for (int i = position; i < length; i++)
        {
            final char c = uri.charAt(i);
            switch (state)
            {
                case MEDIA:
                    switch (c)
                    {
                        case '?':
                            media = builder.toString();
                            builder.setLength(0);
                            state = State.PARAMS_KEY;
                            break;

                        case ':':
                        case '|':
                        case '=':
                            throw new IllegalArgumentException(
                                "encountered '" + c + "' within media definition at index " + i + " in " + uri);

                        default:
                            builder.append(c);
                    }
                    break;

                case PARAMS_KEY:
                    if (c == '=')
                    {
                        if (builder.isEmpty())
                        {
                            throw new IllegalStateException("empty key not allowed at index " + i + " in " + uri);
                        }
                        key = builder.toString();
                        builder.setLength(0);
                        state = State.PARAMS_VALUE;
                    }
                    else
                    {
                        if (c == '|')
                        {
                            throw new IllegalStateException("invalid end of key at index " + i + " in " + uri);
                        }
                        builder.append(c);
                    }
                    break;

                case PARAMS_VALUE:
                    if (c == '|')
                    {
                        params.put(key, builder.toString());
                        builder.setLength(0);
                        state = State.PARAMS_KEY;
                    }
                    else
                    {
                        builder.append(c);
                    }
                    break;

                default:
                    throw new IllegalStateException("unexpected state=" + state + " in " + uri);
            }
        }

        switch (state)
        {
            case MEDIA:
                media = builder.toString();
                validateMedia(media);
                break;

            case PARAMS_VALUE:
                params.put(key, builder.toString());
                break;

            default:
                throw new IllegalStateException("no more input found, state=" + state + " in " + uri);
        }

        return new ChannelUri(prefix, media, params);
    }

    /**
     * Add a sessionId to a given channel.
     *
     * @param channel   to add sessionId to.
     * @param sessionId to add to channel.
     * @return new string that represents channel with sessionId added.
     */
    public static String addSessionId(final String channel, final int sessionId)
    {
        final ChannelUri channelUri = ChannelUri.parse(channel);
        channelUri.put(CommonContext.SESSION_ID_PARAM_NAME, Integer.toString(sessionId));

        return channelUri.toString();
    }

    /**
     * Add alias to the uri if none exists.
     *
     * @param uri   to add alias to.
     * @param alias to add to the uri.
     * @return original uri if alias is empty or one is already defined, otherwise new uri with an alias.
     */
    public static String addAliasIfAbsent(final String uri, final String alias)
    {
        if (!Strings.isEmpty(alias))
        {
            final ChannelUri channelUri = ChannelUri.parse(uri);
            if (!channelUri.containsKey(CommonContext.ALIAS_PARAM_NAME))
            {
                channelUri.put(CommonContext.ALIAS_PARAM_NAME, alias);
                return channelUri.toString();
            }
        }
        return uri;
    }

    /**
     * Transforms an alias of the given uri using the given function. The function will be invoked with the original
     * alias or null if absent. The function's return value will be used as the alias in the returned uri. If the
     * function returns null or an empty string, the alias will be removed.
     *
     * @param uri      the uri to transform the alias of.
     * @param function the transformation function.
     * @return uri equivalent to the one passed in with its alias transformed.
     */
    public static String transformAlias(final String uri, final Function<String, String> function)
    {
        final ChannelUri channelUri = ChannelUri.parse(uri);
        final String originalAlias = channelUri.get(CommonContext.ALIAS_PARAM_NAME);
        final String transformedAlias = function.apply(originalAlias);
        if (Strings.isEmpty(transformedAlias))
        {
            channelUri.remove(CommonContext.ALIAS_PARAM_NAME);
        }
        else
        {
            channelUri.put(CommonContext.ALIAS_PARAM_NAME, transformedAlias);
        }
        return channelUri.toString();
    }

    /**
     * Is the param value tagged? (starts with the "tag:" prefix).
     *
     * @param paramValue to check if tagged.
     * @return true if tagged or false if not.
     * @see CommonContext#TAGS_PARAM_NAME
     * @see CommonContext#TAG_PREFIX
     */
    public static boolean isTagged(final String paramValue)
    {
        return startsWith(paramValue, 0, TAG_PREFIX);
    }

    /**
     * Get the value of the tag from a given parameter value.
     *
     * @param paramValue to extract the tag value from.
     * @return the value of the tag or {@link #INVALID_TAG} if not tagged.
     * @see CommonContext#TAGS_PARAM_NAME
     * @see CommonContext#TAG_PREFIX
     */
    public static long getTag(final String paramValue)
    {
        return isTagged(paramValue) ?
            AsciiEncoding.parseLongAscii(paramValue, 4, paramValue.length() - 4) : INVALID_TAG;
    }

    /**
     * Create a channel URI for a destination, i.e. a channel that uses {@code media} and {@code interface} parameters
     * of the original channel and adds specified {@code endpoint} to it. For example given the input channel is
     * {@code aeron:udp?mtu=1440|ttl=0|endpoint=localhost:8090|term-length=128k|interface=eth0} and the endpoint is
     * {@code 192.168.0.14} the output of this method will be {@code aeron:udp?endpoint=192.168.0.14|interface=eth0}.
     *
     * @param channel  for which the destination is being added.
     * @param endpoint for the target destination.
     * @return new channel URI for a destination.
     */
    public static String createDestinationUri(final String channel, final String endpoint)
    {
        final ChannelUri channelUri = ChannelUri.parse(channel);
        final String uri = AERON_PREFIX + channelUri.media() + "?" + ENDPOINT_PARAM_NAME + "=" + endpoint;
        final String networkInterface = channelUri.get(INTERFACE_PARAM_NAME);

        if (null != networkInterface)
        {
            return uri + "|" + INTERFACE_PARAM_NAME + "=" + networkInterface;
        }

        return uri;
    }

    /**
     * Uses the supplied endpoint to resolve any wildcard ports. If the existing endpoint has a value of "0" for then
     * the port of this endpoint will be used instead. If the endpoint is not specified in this uri, then the whole
     * supplied endpoint is used. If the endpoint exists and has a non-wildcard port, then the existing endpoint is
     * retained.
     *
     * @param resolvedEndpoint The endpoint to supply a resolved endpoint port.
     * @throws IllegalArgumentException if the supplied resolvedEndpoint does not have a port or the port is zero.
     * @throws NullPointerException     if the supplied resolvedEndpoint is null
     */
    public void replaceEndpointWildcardPort(final String resolvedEndpoint)
    {
        final int portSeparatorIndex = requireNonNull(resolvedEndpoint, "resolvedEndpoint is null").lastIndexOf(':');
        if (-1 == portSeparatorIndex)
        {
            throw new IllegalArgumentException("No port specified on resolvedEndpoint=" + resolvedEndpoint);
        }
        if (resolvedEndpoint.endsWith(":0"))
        {
            throw new IllegalArgumentException("Wildcard port specified on resolvedEndpoint=" + resolvedEndpoint);
        }

        final String existingEndpoint = get(ENDPOINT_PARAM_NAME);
        if (null == existingEndpoint)
        {
            put(ENDPOINT_PARAM_NAME, resolvedEndpoint);
        }
        else if (existingEndpoint.endsWith(":0"))
        {
            final String endpoint = existingEndpoint.substring(0, existingEndpoint.length() - 2) +
                resolvedEndpoint.substring(resolvedEndpoint.lastIndexOf(':'));
            put(ENDPOINT_PARAM_NAME, endpoint);
        }
    }

    /**
     * Call consumer for each parameter defined in the URI.
     *
     * @param consumer to be invoked for each parameter.
     */
    public void forEachParameter(final BiConsumer<String, String> consumer)
    {
        params.forEach(consumer);
    }

    /**
     * Determines if this channel has specified <code>control-mode=response</code>.
     *
     * @return true if this channel has specified <code>control-mode=response</code>.
     */
    public boolean hasControlModeResponse()
    {
        return CONTROL_MODE_RESPONSE.equals(get(MDC_CONTROL_MODE_PARAM_NAME));
    }

    /**
     * Take an endpoint with the format <code>host:port</code> and replace the port with a wilcard (<code>0</code>).
     *
     * @param endpoint to replace with a wildcard.
     * @return  the transformed value.
     */
    public static String replacePortWithWildcard(final String endpoint)
    {
        if (null == endpoint)
        {
            return null;
        }

        final int i = endpoint.lastIndexOf(':');
        if (-1 == i)
        {
            return null;
        }

        return endpoint.substring(0, i + 1) + "0";
    }

    /**
     * Determines if the supplied channel has specified <code>control-mode=response</code>.
     *
     * @param channelUri to check if the control mode is response
     * @return true if the supplied channel has specified <code>control-mode=response</code>.
     */
    public static boolean isControlModeResponse(final String channelUri)
    {
        return parse(channelUri).hasControlModeResponse();
    }

    private static void validateMedia(final String media)
    {
        if (IPC_MEDIA.equals(media) || UDP_MEDIA.equals(media))
        {
            return;
        }

        throw new IllegalArgumentException("unknown media: " + media);
    }

    private static boolean startsWith(final CharSequence input, final int position, final String prefix)
    {
        if ((input.length() - position) < prefix.length())
        {
            return false;
        }

        for (int i = 0; i < prefix.length(); i++)
        {
            if (input.charAt(position + i) != prefix.charAt(i))
            {
                return false;
            }
        }

        return true;
    }

    private static String[] splitTags(final String tagsValue)
    {
        String[] tags = ArrayUtil.EMPTY_STRING_ARRAY;

        if (null != tagsValue)
        {
            final int tagCount = countTags(tagsValue);
            if (tagCount == 1)
            {
                tags = new String[]{ tagsValue };
            }
            else
            {
                int tagStartPosition = 0;
                int tagIndex = 0;
                tags = new String[tagCount];

                for (int i = 0, length = tagsValue.length(); i < length; i++)
                {
                    if (tagsValue.charAt(i) == ',')
                    {
                        tags[tagIndex++] = tagsValue.substring(tagStartPosition, i);
                        tagStartPosition = i + 1;

                        if (tagIndex >= (tagCount - 1))
                        {
                            tags[tagIndex] = tagsValue.substring(tagStartPosition, length);
                        }
                    }
                }
            }
        }

        return tags;
    }

    private static int countTags(final String tags)
    {
        int count = 1;

        for (int i = 0, length = tags.length(); i < length; i++)
        {
            if (tags.charAt(i) == ',')
            {
                ++count;
            }
        }

        return count;
    }

    Map<String, String> diff(final ChannelUri that)
    {
        final HashMap<String, String> differingValues = new HashMap<>();

        if (!Objects.equals(prefix, that.prefix))
        {
            differingValues.put("prefix", prefix + " != " + that.prefix);
        }

        if (!Objects.equals(media, that.media))
        {
            differingValues.put("media", media + " != " + that.media);
        }

        if (!Objects.equals(params, that.params))
        {
            params.forEach(
                (key, value) ->
                {
                    final String thatValue = that.params.get(key);
                    if (!Objects.equals(value, thatValue))
                    {
                        differingValues.put(key, value + " != " + thatValue);
                    }
                });
        }

        if (!Arrays.equals(tags, that.tags))
        {
            differingValues.put(TAGS_PARAM_NAME, Arrays.toString(tags) + " != " + Arrays.toString(that.tags));
        }

        return differingValues;
    }
}
