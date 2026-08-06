/*
 * Copyright 2014-2026 Real Logic Limited.
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
package io.aeron.test.logging;

import io.aeron.logging.LoggerEventCallback;
import org.agrona.DirectBuffer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A {@link LoggerEventCallback} that records a single decoded CBOR event message (header, field values, and
 * footer) so that a test can assert against it. Intended to be reused across many messages via {@link #reset()}.
 */
public class RecordingLoggerEventCallback implements LoggerEventCallback
{
    /**
     * The kind of value that was decoded for a given field.
     */
    public enum Kind
    {
        /**
         * The field was present but had a {@code null} value.
         */
        NULL,

        /**
         * The field held a string (or enum name) value.
         */
        STRING,

        /**
         * The field held a numeric value.
         */
        NUMBER,

        /**
         * The field held a boolean value.
         */
        BOOLEAN,

        /**
         * The field held a raw byte array value.
         */
        BYTES
    }

    /**
     * Decoded header of a message.
     *
     * @param eventType     of the message.
     * @param eventCode     of the message.
     * @param eventCodeName of the message.
     * @param timestamp     of the message.
     */
    public record Header(int eventType, int eventCode, String eventCodeName, long timestamp)
    {
    }

    /**
     * A single decoded field value, exposing whichever accessor matches {@link #kind}.
     */
    public static final class Value
    {
        private final long tag;
        private final Kind kind;
        private final String stringValue;
        private final long numberValue;
        private final boolean booleanValue;
        private final byte[] bytesValue;

        private Value(
            final long tag,
            final Kind kind,
            final String stringValue,
            final long numberValue,
            final boolean booleanValue,
            final byte[] bytesValue)
        {
            this.tag = tag;
            this.kind = kind;
            this.stringValue = stringValue;
            this.numberValue = numberValue;
            this.booleanValue = booleanValue;
            this.bytesValue = bytesValue;
        }

        static Value ofNull(final long tag)
        {
            return new Value(tag, Kind.NULL, null, 0L, false, null);
        }

        static Value ofString(final long tag, final String value)
        {
            return new Value(tag, Kind.STRING, value, 0L, false, null);
        }

        static Value ofNumber(final long tag, final long value)
        {
            return new Value(tag, Kind.NUMBER, null, value, false, null);
        }

        static Value ofBoolean(final long tag, final boolean value)
        {
            return new Value(tag, Kind.BOOLEAN, null, 0L, value, null);
        }

        static Value ofBytes(final long tag, final byte[] value)
        {
            return new Value(tag, Kind.BYTES, null, 0L, false, value);
        }

        /**
         * {@return the CBOR tag that was decoded alongside this value, or {@code NO_TAG} if none was present}
         */
        public long tag()
        {
            return tag;
        }

        /**
         * {@return the kind of value that was decoded}
         */
        public Kind kind()
        {
            return kind;
        }

        /**
         * {@return the string value, valid when {@link #kind()} is {@link Kind#STRING}}
         */
        public String stringValue()
        {
            return stringValue;
        }

        /**
         * {@return the numeric value, valid when {@link #kind()} is {@link Kind#NUMBER}}
         */
        public long numberValue()
        {
            return numberValue;
        }

        /**
         * {@return the boolean value, valid when {@link #kind()} is {@link Kind#BOOLEAN}}
         */
        public boolean booleanValue()
        {
            return booleanValue;
        }

        /**
         * {@return the byte array value, valid when {@link #kind()} is {@link Kind#BYTES}}
         */
        public byte[] bytesValue()
        {
            return bytesValue;
        }
    }

    private Header header;
    private boolean truncated;
    private final Map<String, Value> values = new LinkedHashMap<>();

    /**
     * Clears any previously decoded message so the instance can be reused for the next one.
     */
    public void reset()
    {
        header = null;
        truncated = false;
        values.clear();
    }

    /**
     * {@return the header of the most recently decoded message}
     */
    public Header header()
    {
        return header;
    }

    /**
     * {@return whether the most recently decoded message was truncated}
     */
    public boolean truncated()
    {
        return truncated;
    }

    /**
     * {@return the field values of the most recently decoded message, keyed by field name}
     */
    public Map<String, Value> values()
    {
        return values;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onHeader(
        final int eventType, final int eventCode, final CharSequence eventCodeName, final long timestamp)
    {
        header = new Header(eventType, eventCode, eventCodeName.toString(), timestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onValue(final CharSequence name, final long tag, final CharSequence value)
    {
        values.put(name.toString(), null == value ? Value.ofNull(tag) : Value.ofString(tag, value.toString()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onValue(final CharSequence name, final long tag, final long value)
    {
        values.put(name.toString(), Value.ofNumber(tag, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onValue(final CharSequence name, final long tag, final boolean value)
    {
        values.put(name.toString(), Value.ofBoolean(tag, value));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onValue(final CharSequence name, final long tag, final DirectBuffer value)
    {
        if (null == value)
        {
            values.put(name.toString(), Value.ofNull(tag));
            return;
        }

        final byte[] bytes = new byte[value.capacity()];
        value.getBytes(0, bytes);
        values.put(name.toString(), Value.ofBytes(tag, bytes));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onFooter(final boolean truncated)
    {
        this.truncated = truncated;
    }
}
