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

import org.agrona.MutableDirectBuffer;

import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;

/**
 * Message to denote that new buffers have been created for a publication.
 *
 * @see ControlProtocolEvents
 * <pre>
 *   0                   1                   2                   3
 *   0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
 *  +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
 *  |                         Correlation ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                        Registration ID                        |
 *  |                                                               |
 *  +---------------------------------------------------------------+
 *  |                          Session ID                           |
 *  +---------------------------------------------------------------+
 *  |                           Stream ID                           |
 *  +---------------------------------------------------------------+
 *  |                  Publication Limit Counter ID                 |
 *  +---------------------------------------------------------------+
 *  |                  Channel Status Indicator ID                  |
 *  +---------------------------------------------------------------+
 *  |                        Log File Length                        |
 *  +---------------------------------------------------------------+
 *  |                     Log File Name (ASCII)                    ...
 * ...                                                              |
 *  +---------------------------------------------------------------+
 * </pre>
 */
public class PublicationBuffersReadyFlyweight
{
    private static final int CORRELATION_ID_OFFSET = 0;
    private static final int REGISTRATION_ID_OFFSET = CORRELATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int SESSION_ID_OFFSET = REGISTRATION_ID_OFFSET + SIZE_OF_LONG;
    private static final int STREAM_ID_FIELD_OFFSET = SESSION_ID_OFFSET + SIZE_OF_INT;
    private static final int PUBLICATION_LIMIT_COUNTER_ID_OFFSET = STREAM_ID_FIELD_OFFSET + SIZE_OF_INT;
    private static final int CHANNEL_STATUS_INDICATOR_ID_OFFSET = PUBLICATION_LIMIT_COUNTER_ID_OFFSET + SIZE_OF_INT;
    private static final int LOGFILE_FIELD_OFFSET = CHANNEL_STATUS_INDICATOR_ID_OFFSET + SIZE_OF_INT;

    private MutableDirectBuffer buffer;
    private int offset;

    /**
     * An empty flyweight, which should be wrapped over a buffer.
     *
     * @see #wrap(MutableDirectBuffer, int)
     */
    public PublicationBuffersReadyFlyweight()
    {
    }

    /**
     * Wrap the buffer at a given offset for updates.
     *
     * @param buffer to wrap.
     * @param offset at which the message begins.
     * @return this for a fluent API.
     */
    public final PublicationBuffersReadyFlyweight wrap(final MutableDirectBuffer buffer, final int offset)
    {
        this.buffer = buffer;
        this.offset = offset;

        return this;
    }

    /**
     * Get the correlation id field.
     *
     * @return correlation id field.
     */
    public long correlationId()
    {
        return buffer.getLong(offset + CORRELATION_ID_OFFSET);
    }

    /**
     * Set the correlation id field.
     *
     * @param correlationId field value.
     * @return this for a fluent API.
     */
    public PublicationBuffersReadyFlyweight correlationId(final long correlationId)
    {
        buffer.putLong(offset + CORRELATION_ID_OFFSET, correlationId);

        return this;
    }

    /**
     * Get the registration id field.
     *
     * @return registration id field.
     */
    public long registrationId()
    {
        return buffer.getLong(offset + REGISTRATION_ID_OFFSET);
    }

    /**
     * Set the correlation id field.
     *
     * @param registrationId field value.
     * @return this for a fluent API.
     */
    public PublicationBuffersReadyFlyweight registrationId(final long registrationId)
    {
        buffer.putLong(offset + REGISTRATION_ID_OFFSET, registrationId);

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
     * Set the session id field.
     *
     * @param sessionId field value.
     * @return this for a fluent API.
     */
    public PublicationBuffersReadyFlyweight sessionId(final int sessionId)
    {
        buffer.putInt(offset + SESSION_ID_OFFSET, sessionId);

        return this;
    }

    /**
     * Get the stream id field.
     *
     * @return stream id field.
     */
    public int streamId()
    {
        return buffer.getInt(offset + STREAM_ID_FIELD_OFFSET);
    }

    /**
     * Set the stream id field.
     *
     * @param streamId field value.
     * @return this for a fluent API.
     */
    public PublicationBuffersReadyFlyweight streamId(final int streamId)
    {
        buffer.putInt(offset + STREAM_ID_FIELD_OFFSET, streamId);

        return this;
    }

    /**
     * The publication limit counter id.
     *
     * @return publication limit counter id.
     */
    public int publicationLimitCounterId()
    {
        return buffer.getInt(offset + PUBLICATION_LIMIT_COUNTER_ID_OFFSET);
    }

    /**
     * set position counter id field.
     *
     * @param positionCounterId field value.
     * @return this for a fluent API.
     */
    public PublicationBuffersReadyFlyweight publicationLimitCounterId(final int positionCounterId)
    {
        buffer.putInt(offset + PUBLICATION_LIMIT_COUNTER_ID_OFFSET, positionCounterId);

        return this;
    }

    /**
     * The channel status counter id.
     *
     * @return channel status counter id.
     */
    public int channelStatusCounterId()
    {
        return buffer.getInt(offset + CHANNEL_STATUS_INDICATOR_ID_OFFSET);
    }

    /**
     * Set channel status counter id field.
     *
     * @param counterId field value.
     * @return this for a fluent API.
     */
    public PublicationBuffersReadyFlyweight channelStatusCounterId(final int counterId)
    {
        buffer.putInt(offset + CHANNEL_STATUS_INDICATOR_ID_OFFSET, counterId);

        return this;
    }

    /**
     * Get the log file name in ASCII.
     *
     * @return the log file name in ASCII.
     */
    public String logFileName()
    {
        return buffer.getStringAscii(offset + LOGFILE_FIELD_OFFSET);
    }

    /**
     * Append the log file name to an {@link Appendable}.
     *
     * @param appendable to append log file name to.
     */
    public void appendLogFileName(final Appendable appendable)
    {
        buffer.getStringAscii(offset + LOGFILE_FIELD_OFFSET, appendable);
    }

    /**
     * Set the log file name in ASCII.
     *
     * @param logFileName for the publication buffers.
     * @return the log file name in ASCII.
     */
    public PublicationBuffersReadyFlyweight logFileName(final String logFileName)
    {
        buffer.putStringAscii(offset + LOGFILE_FIELD_OFFSET, logFileName);
        return this;
    }

    /**
     * Get the length of the current message
     * <p>
     * NB: must be called after the data is written in order to be accurate.
     *
     * @return the length of the current message
     */
    public int length()
    {
        return buffer.getInt(offset + LOGFILE_FIELD_OFFSET) + LOGFILE_FIELD_OFFSET + SIZE_OF_INT;
    }
}
