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
package io.aeron.agent;

import org.agrona.AsciiEncoding;

/**
 * Utility methods for loggers.
 */
final class LogUtil
{
    private static final long NANOS_PER_SECOND = 1_000_000_000;

    /**
     * Render a nanosecond timestamp to the supplied {@link StringBuilder} in the following format.
     * <pre>
     *     [&lt;seconds&gt;.&lt;nanoseconds&gt;]
     * </pre>
     *
     * @param builder       to render the timestamp too.
     * @param timestampNs   the nanosecond timestamp.
     */
    public static void appendTimestamp(final StringBuilder builder, final long timestampNs)
    {
        final long seconds = timestampNs / NANOS_PER_SECOND;
        final long nanos = timestampNs - seconds * NANOS_PER_SECOND;
        final int numDigitsAfterDot = AsciiEncoding.digitCount(nanos);
        builder.append('[');
        builder.append(seconds);
        builder.append('.');
        for (int i = 0, size = 9 - numDigitsAfterDot; i < size; i++)
        {
            builder.append('0');
        }
        builder.append(nanos);
        builder.append(']').append(' ');
    }

    /**
     * Render a nanosecond timestamp as a string in the following format.
     * <pre>
     *     [&lt;seconds&gt;.&lt;nanoseconds&gt;]
     * </pre>
     * Note: this will allocate a string builder internally, for a low allocation option use
     * {@link LogUtil#appendTimestamp(StringBuilder, long)}.
     *
     * @param timestampNs   the nanosecond timestamp.
     * @return the string formatted nanosecond timestamp.
     * @see #appendTimestamp(StringBuilder, long)
     */
    public static String renderTimestamp(final long timestampNs)
    {
        final StringBuilder sb = new StringBuilder();
        appendTimestamp(sb, timestampNs);
        sb.setLength(sb.length() - 1);
        return sb.toString();
    }
}
