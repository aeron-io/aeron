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
package io.aeron.command;

import org.agrona.BitUtil;
import org.agrona.MutableDirectBuffer;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Message to denote that new buffers for a publication image are ready for a subscription.
 * <p>
 * <b>Note:</b> Layout should be SBE 2.0 compliant so that the source identity length is aligned.
 *
 * @see ControlProtocolEvents
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                       Correlation ID                          |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                         Session ID                            |
 *  +---------------------------------------------------------------+
 *  |                          Stream ID                            |
 *  +---------------------------------------------------------------+
 *  |                 Subscription Registration Id                  |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                    Subscriber Position Id                     |
 *  +---------------------------------------------------------------+
 *  |                       Log File Length                         |
 *  +---------------------------------------------------------------+
 *  |                     Log File Name (ASCII)                    ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 *  |                    Source identity Length                     |
 *  +---------------------------------------------------------------+
 *  |                    Source identity (ASCII)                   ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class ImageBuffersReadyFlyweight
{
    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int SESSION_ID_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int STREAM_ID_FIELD_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    private static final int SUBSCRIPTION_REGISTRATION_ID_OFFSET = STREAM_ID_FIELD_OFFSET + SIZE_OF_INT;
    private static final int SUBSCRIBER_POSITION_ID_OFFSET = SUBSCRIPTION_REGISTRATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int LOG_FILE_NAME_OFFSET = SUBSCRIBER_POSITION_ID_OFFSET + SIZE_OF_INT;

    private MutableDirectBuffer buffer;
    private int offset;

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public ImageBuffersReadyFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public final ImageBuffersReadyFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    /**
     * The correlation id field.
     *
     * @return correlation id field.
     */
    public long correlationId()
    {
        return buffer.getLong(offset + CORRELATION_ID_OFFSET);
    }

    /**
     * Set correlation id field.
     *
     * @param correlationId field value.
     * @return this for a fluent API.
     */
    public ImageBuffersReadyFlyweight correlationId(final long correlationId)
    {
        buffer.putLong(offset + CORRELATION_ID_OFFSET, correlationId);

        return this;
    }

    /**
     * Get the session id field.
     *
     * @return session id field.
     */
    public int sessionId()
    {
        return buffer.getInt(offset + SESSION_ID_OFFSET);
    }

    /**
     * Set session id field.
     *
     * @param sessionId field value.
     * @return this for a fluent API.
     */
    public ImageBuffersReadyFlyweight sessionId(final int sessionId)
    {
        buffer.putInt(offset + SESSION_ID_OFFSET, sessionId);

        return this;
    }

    /**
     * The stream id field.
     *
     * @return stream id field.
     */
    public int streamId()
    {
        return buffer.getInt(offset + STREAM_ID_FIELD_OFFSET);
    }

    /**
     * Set stream id field.
     *
     * @param streamId field value.
     * @return this for a fluent API.
     */
    public ImageBuffersReadyFlyweight streamId(final int streamId)
    {
        buffer.putInt(offset + STREAM_ID_FIELD_OFFSET, streamId);

        return this;
    }

    /**
     * Set the position counter Id for the subscriber.
     *
     * @param id for the subscriber position counter.
     * @return this for a fluent API.
     */
    public ImageBuffersReadyFlyweight subscriberPositionId(final int id)
    {
        buffer.putInt(offset + SUBSCRIBER_POSITION_ID_OFFSET, id);

        return this;
    }

    /**
     * The position counter Id for the subscriber.
     *
     * @return position counter Id for the subscriber.
     */
    public int subscriberPositionId()
    {
        return buffer.getInt(offset + SUBSCRIBER_POSITION_ID_OFFSET);
    }

    /**
     * Set the registration Id for the Subscription.
     *
     * @param id for the Subscription.
     * @return this for a fluent API.
     */
    public ImageBuffersReadyFlyweight subscriptionRegistrationId(final long id)
    {
        buffer.putLong(offset + SUBSCRIPTION_REGISTRATION_ID_OFFSET, id);

        return this;
    }

    /**
     * Return the registration Id for the Subscription.
     *
     * @return registration Id for the Subscription.
     */
    public long subscriptionRegistrationId()
    {
        return buffer.getLong(offset + SUBSCRIPTION_REGISTRATION_ID_OFFSET);
    }

    /**
     * The Log Filename in ASCII.
     *
     * @return log filename.
     */
    public String logFileName()
    {
        return buffer.getStringAscii(offset + LOG_FILE_NAME_OFFSET);
    }

    /**
     * Append the log file name to an {@link Appendable}.
     *
     * @param appendable to append log file name to.
     */
    public void appendLogFileName(final Appendable appendable)
    {
        buffer.getStringAscii(offset + LOG_FILE_NAME_OFFSET, appendable);
    }

    /**
     * Set the log filename in ASCII.
     *
     * @param logFileName for the image.
     * @return this for a fluent API.
     */
    public ImageBuffersReadyFlyweight logFileName(final String logFileName)
    {
        buffer.putStringAscii(offset + LOG_FILE_NAME_OFFSET, logFileName);
        return this;
    }

    /**
     * The source identity string in ASCII.
     *
     * @return source identity string.
     */
    public String sourceIdentity()
    {
        return buffer.getStringAscii(offset + sourceIdentityOffset());
    }

    /**
     * Append the source identity to an {@link Appendable}.
     *
     * @param appendable to append source identity to.
     */
    public void appendSourceIdentity(final Appendable appendable)
    {
        buffer.getStringAscii(offset + sourceIdentityOffset(), appendable);
    }

    /**
     * Set the source identity string in ASCII.
     * <p>Note: Can be called only after log file name was set!</p>
     *
     * @param value for the source identity.
     * @return this for a fluent API.
     * @see #logFileName(String)
     */
    public ImageBuffersReadyFlyweight sourceIdentity(final String value)
    {
        buffer.putStringAscii(offset + sourceIdentityOffset(), value);
        return this;
    }

    /**
     * Get the length of the current message.
     * <p>
     * NB: must be called after the data is written in order to be accurate.
     *
     * @return the length of the current message.
     */
    public int length()
    {
        final int sourceIdentityOffset = sourceIdentityOffset();
        return sourceIdentityOffset + buffer.getInt(offset + sourceIdentityOffset) + SIZE_OF_INT;
    }

    private int sourceIdentityOffset()
    {
        final int alignedLength = BitUtil.align(buffer.getInt(offset + LOG_FILE_NAME_OFFSET), SIZE_OF_INT);

        return LOG_FILE_NAME_OFFSET + SIZE_OF_INT + alignedLength;
    }
}
