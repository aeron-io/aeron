package io.aeron.agent;

import io.aeron.exceptions.AeronEvent;
import org.agrona.AsciiSequenceView;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.MessageHandler;

import java.io.Serial;
import java.util.List;

import static io.aeron.agent.CborUtil.ADDITIONAL_CONTENT_1_BYTE;
import static io.aeron.agent.CborUtil.ADDITIONAL_CONTENT_2_BYTE;
import static io.aeron.agent.CborUtil.ADDITIONAL_CONTENT_4_BYTE;
import static io.aeron.agent.CborUtil.ADDITIONAL_CONTENT_8_BYTE;
import static io.aeron.agent.CborUtil.ADDITIONAL_CONTENT_INDEFINITE;
import static io.aeron.agent.CborUtil.BREAK;
import static io.aeron.agent.CborUtil.MAP_MAJOR_TYPE;
import static io.aeron.agent.CborUtil.NEGATIVE_INTEGER_MAJOR_TYPE;
import static io.aeron.agent.CborUtil.TEXT_STRING_MAJOR_TYPE;
import static io.aeron.agent.CborUtil.UNSIGNED_INTEGER_MAJOR_TYPE;
import static java.nio.ByteOrder.BIG_ENDIAN;

public class CborDecode implements MessageHandler
{
    private final List<LoggerEventCallback> loggers;
    private final AsciiSequenceView keyAsciiView = new AsciiSequenceView();
    private final AsciiSequenceView valueAsciiView = new AsciiSequenceView();

    static class InvalidMessage extends AeronEvent
    {
        @Serial
        private static final long serialVersionUID = 2520658673245952222L;
        InvalidMessage(final String message)
        {
            super(message);
        }
    }


    public CborDecode(final List<LoggerEventCallback> loggers)
    {
        this.loggers = loggers;
    }


    public void onMessage(final int msgTypeId, final MutableDirectBuffer buffer, final int index, final int length)
    {
        try
        {
            DecodingState state = new DecodingState(buffer, index, length);
            loggers.forEach(
                logger -> logger.onHeader(
                    EventCodeType.CLUSTER.getTypeCode(),
                    msgTypeId,
                    0L
                ));
            parseMap(state);
        }
        catch (final InvalidMessage e)
        {
            // TODO: Put something here
        }

    }

    private void parseEntry(final DecodingState state)
    {
        if (state.isTerminated())
        {
            return;
        }
        int index = state.position();
        DirectBuffer buffer = state.buffer();
        final int keyTypeByte = (0xFF) & state.buffer().getByte(index);
        final int keyMajorType = (0xFF) & (0b111_00000 & keyTypeByte);
        final int keyAdditionalContent = (0b000_11111) & keyTypeByte;
        state.incrementPosition(1);
        // key handling
        if (TEXT_STRING_MAJOR_TYPE == keyMajorType)
        {
            parseString(state, keyAsciiView, keyAdditionalContent);
        }


        // value handling
        final int valueTypeByte = (0xFF) & buffer.getByte(state.position());
        final int valueMajorType = (0xFF) & (0b111_00000 & valueTypeByte);
        final int valueAdditionalContent = (0b000_11111) & valueTypeByte;
        state.incrementPosition(1);
        switch (valueMajorType)
        {
            case NEGATIVE_INTEGER_MAJOR_TYPE:
            case UNSIGNED_INTEGER_MAJOR_TYPE:
                final long finalValue = parseNumber(
                    state,
                    valueAdditionalContent,
                    valueMajorType);
                loggers.forEach(logger -> logger.onValue(keyAsciiView, finalValue));
                break;
            case TEXT_STRING_MAJOR_TYPE:
                parseString(state, valueAsciiView, valueAdditionalContent);
                loggers.forEach(logger -> logger.onValue(keyAsciiView, valueAsciiView));
                break;
            default:
                throw new InvalidMessage("Invalid value type");
        }


    }

    private static long parseNumber(
        final DecodingState state,
        final int valueAdditionalContent,
        final int valueMajorType)
    {
        long value;
        if (valueAdditionalContent < ADDITIONAL_CONTENT_1_BYTE)
        {
            value = valueAdditionalContent;
        }
        else if (ADDITIONAL_CONTENT_1_BYTE == valueAdditionalContent)
        {
            value = 0xFF & state.buffer().getByte(state.position());
            state.incrementPosition(1);
        }
        else if (ADDITIONAL_CONTENT_2_BYTE == valueAdditionalContent)
        {
            value = 0xFFFF & state.buffer().getShort(state.position(), BIG_ENDIAN);
            state.incrementPosition(2);
        }
        else if (ADDITIONAL_CONTENT_4_BYTE == valueAdditionalContent)
        {
            value = state.buffer().getInt(state.position(), BIG_ENDIAN);
            state.incrementPosition(4);
        }
        else if (ADDITIONAL_CONTENT_8_BYTE == valueAdditionalContent)
        {
            value = state.buffer().getLong(state.position(), BIG_ENDIAN);
            state.incrementPosition(8);
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
            targetView.wrap(state.buffer(), state.position(), keyAdditionalContent);
            state.incrementPosition(keyAdditionalContent);
        }
        else if (ADDITIONAL_CONTENT_1_BYTE == keyAdditionalContent)
        {
            int length = 0xFF & state.buffer().getByte(state.position());
            targetView.wrap(state.buffer(), state.position() + 1, length);
            state.incrementPosition(1 + length);
        }
        else if (ADDITIONAL_CONTENT_2_BYTE == keyAdditionalContent)
        {
            int length = 0xFFFF & state.buffer().getShort(state.position(), BIG_ENDIAN);
            targetView.wrap(state.buffer(), state.position() + 2, length);
            state.incrementPosition(2 + length);
        }
        else if (ADDITIONAL_CONTENT_4_BYTE == keyAdditionalContent)
        {
            int length = state.buffer().getInt(state.position(), BIG_ENDIAN);
            targetView.wrap(state.buffer(), state.position() + 4, length);
            state.incrementPosition(4 + length);
        }
        else
        {
            throw new InvalidMessage("Invalid key length");
        }
    }

    private void checkTermination(final DecodingState state)
    {
        int currentByte = (0xFF) & (state.buffer().getByte(state.position()));
        if (currentByte == BREAK)
        {
            state.terminate();
        }
    }

    private void parseMap(final DecodingState state)
    {
        final int typeByte = (0xFF) & state.buffer().getByte(state.position());
        final int majorType = (0xFF) & (0b111_00000 & typeByte);
        final int additionalContent = (0b000_11111) & typeByte;


        if (MAP_MAJOR_TYPE == majorType)
        {
            if (ADDITIONAL_CONTENT_INDEFINITE == additionalContent)
            {
                state.incrementPosition(1);
                while (!state.isTerminated())
                {
                    // peek at current position
                    checkTermination(state);
                    parseEntry(state);
                }
                // TODO: Implement the truncation check
                loggers.forEach(logger -> logger.onFooter(false));
            }
        }
    }
}
