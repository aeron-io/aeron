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
package io.aeron.logging;

import org.agrona.AsciiSequenceView;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;

import java.util.List;

import static io.aeron.logging.CborUtils.ADDITIONAL_CONTENT_1_BYTE;
import static io.aeron.logging.CborUtils.ADDITIONAL_CONTENT_2_BYTE;
import static io.aeron.logging.CborUtils.ADDITIONAL_CONTENT_4_BYTE;
import static io.aeron.logging.CborUtils.ADDITIONAL_CONTENT_8_BYTE;
import static io.aeron.logging.CborUtils.ADDITIONAL_CONTENT_FALSE;
import static io.aeron.logging.CborUtils.ADDITIONAL_CONTENT_INDEFINITE;
import static io.aeron.logging.CborUtils.ADDITIONAL_CONTENT_NULL;
import static io.aeron.logging.CborUtils.ADDITIONAL_CONTENT_TRUE;
import static io.aeron.logging.CborUtils.ARRAY_MAJOR_TYPE;
import static io.aeron.logging.CborUtils.BREAK;
import static io.aeron.logging.CborUtils.ENTRIES_LENGTH;
import static io.aeron.logging.CborUtils.MAP_MAJOR_TYPE;
import static io.aeron.logging.CborUtils.NEGATIVE_INTEGER_MAJOR_TYPE;
import static io.aeron.logging.CborUtils.SIMPLE_VALUE_MAJOR_TYPE;
import static io.aeron.logging.CborUtils.TEXT_STRING_MAJOR_TYPE;
import static io.aeron.logging.CborUtils.UNSIGNED_INTEGER_MAJOR_TYPE;
import static java.nio.ByteOrder.BIG_ENDIAN;

/**
 * Top level handler for CBOR messages from the logger.
 */
public class CborDecode implements MessageHandler
{
    private final List<? extends LoggerEventCallback> loggers;
    private final AsciiSequenceView keyAsciiView = new AsciiSequenceView();
    private final AsciiSequenceView valueAsciiView = new AsciiSequenceView();
    private final DecodingState decodingState = new DecodingState();

    /**
     * Create with a list of loggers to delegate messages to.
     *
     * @param loggers to delegate messages to.
     */
    public CborDecode(final List<? extends LoggerEventCallback> loggers)
    {
        this.loggers = loggers;
    }

    /**
     * {@inheritDoc}
     */
    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        try
        {
            decodingState.wrap(buffer, index, length);

            parseMessage(decodingState);
        }
        catch (final InvalidMessage ex)
        {
            throw new RuntimeException(ex);
            // TODO: Put something here (Maybe onError callback?)
        }

    }

    private void parseMessage(final DecodingState state)
    {
        state.ensureRemaining(1);
        final int typeByte = (0xFF) & state.buffer().getByte(state.offset());
        state.incrementOffset(1);

        if ((ARRAY_MAJOR_TYPE | ENTRIES_LENGTH) == typeByte)
        {
            final long timestamp = parseLong(state);
            final int eventCode = (int)parseLong(state);

            for (int i = 0, n = loggers.size(); i < n; i++)
            {
                final LoggerEventCallback logger = loggers.get(i);
                logger.onHeader((0xFFFF_0000 & eventCode) >>> 16, (0xFFFF & eventCode), timestamp);
            }

            parseMap(decodingState);
        }
    }

    private long parseLong(final DecodingState state)
    {
        state.ensureRemaining(1);
        final int typeByte = (0xFF) & state.buffer().getByte(state.offset());
        state.incrementOffset(1);
        final int majorType = majorType(typeByte);
        final int additionalContent = additionalContent(typeByte);

        if (NEGATIVE_INTEGER_MAJOR_TYPE != majorType && UNSIGNED_INTEGER_MAJOR_TYPE != majorType)
        {
            throw new InvalidMessage("Expected timestamp");
        }

        return parseNumber(state, majorType, additionalContent);
    }

    private void parseSimpleValue(final DecodingState state, final int additionalContent)
    {
        switch (additionalContent)
        {
            case ADDITIONAL_CONTENT_FALSE:
            case ADDITIONAL_CONTENT_TRUE:
                for (int i = 0, n = loggers.size(); i < n; i++)
                {
                    final LoggerEventCallback logger = loggers.get(i);
                    logger.onValue(keyAsciiView, additionalContent == ADDITIONAL_CONTENT_TRUE);
                }
                break;
            case ADDITIONAL_CONTENT_NULL:
                for (int i = 0, n = loggers.size(); i < n; i++)
                {
                    final LoggerEventCallback logger = loggers.get(i);
                    logger.onValue(keyAsciiView, null);
                }
                break;
            default:
                throw new InvalidMessage("Invalid simple value");
        }
    }

    private void parseEntry(final DecodingState state)
    {
        if (state.isTerminated())
        {
            return;
        }

        final DirectBuffer buffer = state.buffer();
        state.ensureRemaining(1);
        final int keyTypeByte = (0xFF) & state.buffer().getByte(state.offset());
        final int keyMajorType = majorType(keyTypeByte);
        final int keyAdditionalContent = additionalContent(keyTypeByte);
        state.incrementOffset(1);
        // key handling
        if (TEXT_STRING_MAJOR_TYPE == keyMajorType)
        {
            parseString(state, keyAsciiView, keyAdditionalContent);
        }

        // value handling
        state.ensureRemaining(1);
        final int valueTypeByte = (0xFF) & buffer.getByte(state.offset());
        final int valueMajorType = majorType(valueTypeByte);
        final int valueAdditionalContent = additionalContent(valueTypeByte);
        state.incrementOffset(1);
        switch (valueMajorType)
        {
            case NEGATIVE_INTEGER_MAJOR_TYPE:
            case UNSIGNED_INTEGER_MAJOR_TYPE:
            {
                final long finalValue = parseNumber(state, valueMajorType, valueAdditionalContent);
                for (int i = 0, n = loggers.size(); i < n; i++)
                {
                    final LoggerEventCallback loggerEventCallback = loggers.get(i);
                    loggerEventCallback.onValue(keyAsciiView, finalValue);
                }

                break;
            }

            case TEXT_STRING_MAJOR_TYPE:
            {
                parseString(state, valueAsciiView, valueAdditionalContent);
                for (int i = 0, n = loggers.size(); i < n; i++)
                {
                    final LoggerEventCallback logger = loggers.get(i);
                    logger.onValue(keyAsciiView, valueAsciiView);
                }

                break;
            }

            case SIMPLE_VALUE_MAJOR_TYPE:
                parseSimpleValue(state, valueAdditionalContent);
                break;

            default:
                throw new InvalidMessage("Invalid value type");
        }
    }

    private static int additionalContent(final int keyTypeByte)
    {
        return (0b000_11111) & keyTypeByte;
    }

    private static int majorType(final int fullTypeByte)
    {
        return (0xFF) & (0b111_00000 & fullTypeByte);
    }

    private static long parseNumber(
        final DecodingState state,
        final int valueMajorType,
        final int valueAdditionalContent)
    {
        long value;
        if (valueAdditionalContent < ADDITIONAL_CONTENT_1_BYTE)
        {
            value = valueAdditionalContent;
        }
        else if (ADDITIONAL_CONTENT_1_BYTE == valueAdditionalContent)
        {
            state.ensureRemaining(1);
            value = 0xFF & state.buffer().getByte(state.offset());
            state.incrementOffset(1);
        }
        else if (ADDITIONAL_CONTENT_2_BYTE == valueAdditionalContent)
        {
            state.ensureRemaining(2);
            value = 0xFFFF & state.buffer().getShort(state.offset(), BIG_ENDIAN);
            state.incrementOffset(2);
        }
        else if (ADDITIONAL_CONTENT_4_BYTE == valueAdditionalContent)
        {
            state.ensureRemaining(4);
            value = 0xFFFFFFFFL & state.buffer().getInt(state.offset(), BIG_ENDIAN);
            state.incrementOffset(4);
        }
        else if (ADDITIONAL_CONTENT_8_BYTE == valueAdditionalContent)
        {
            state.ensureRemaining(8);
            value = state.buffer().getLong(state.offset(), BIG_ENDIAN);
            state.incrementOffset(8);
        }
        else
        {
            throw new InvalidMessage("Invalid value length");
        }

        if (NEGATIVE_INTEGER_MAJOR_TYPE == valueMajorType)
        {
            value = ~value;
        }

        return value;
    }

    private void parseString(
        final DecodingState state,
        final AsciiSequenceView targetView,
        final int keyAdditionalContent)
    {
        if (keyAdditionalContent < ADDITIONAL_CONTENT_1_BYTE)
        {
            targetView.wrap(state.buffer(), state.offset(), keyAdditionalContent);
            state.incrementOffset(keyAdditionalContent);
        }
        else if (ADDITIONAL_CONTENT_1_BYTE == keyAdditionalContent)
        {
            state.ensureRemaining(1);
            final int length = 0xFF & state.buffer().getByte(state.offset());
            targetView.wrap(state.buffer(), state.offset() + 1, length);
            state.incrementOffset(1 + length);
        }
        else if (ADDITIONAL_CONTENT_2_BYTE == keyAdditionalContent)
        {
            state.ensureRemaining(2);
            final int length = 0xFFFF & state.buffer().getShort(state.offset(), BIG_ENDIAN);
            targetView.wrap(state.buffer(), state.offset() + 2, length);
            state.incrementOffset(2 + length);
        }
        else if (ADDITIONAL_CONTENT_4_BYTE == keyAdditionalContent)
        {
            state.ensureRemaining(4);
            final int length = state.buffer().getInt(state.offset(), BIG_ENDIAN);
            targetView.wrap(state.buffer(), state.offset() + 4, length);
            state.incrementOffset(4 + length);
        }
        else
        {
            throw new InvalidMessage("Invalid key length");
        }
    }

    private void checkTermination(final DecodingState state)
    {
        state.ensureRemaining(1);
        final int currentByte = (0xFF) & (state.buffer().getByte(state.offset()));

        if (BREAK == currentByte)
        {
            state.terminate();
        }
    }

    private void parseMap(final DecodingState state)
    {
        state.ensureRemaining(1);
        final int typeByte = (0xFF) & state.buffer().getByte(state.offset());

        if ((MAP_MAJOR_TYPE | ADDITIONAL_CONTENT_INDEFINITE) == typeByte)
        {
            state.incrementOffset(1);
            while (!state.isTerminated())
            {
                // peek at current position
                checkTermination(state);
                parseEntry(state);
            }

            // TODO: Implement the truncation check
            for (int i = 0, n = loggers.size(); i < n; i++)
            {
                final LoggerEventCallback logger = loggers.get(i);
                logger.onFooter(false);
            }
        }
    }
}
