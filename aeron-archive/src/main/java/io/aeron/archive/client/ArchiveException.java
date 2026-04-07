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
package io.aeron.archive.client;

import io.aeron.Aeron;
import io.aeron.exceptions.AeronException;

/**
 * Exception raised when communicating with the {@link AeronArchive}.
 */
public class ArchiveException extends AeronException
{
    /**
     * Generic archive error with detail likely in the message.
     */
    public static final int GENERIC = 0;

    /**
     * An active listing of recordings is currently in operation on the session.
     */
    public static final int ACTIVE_LISTING = 1;

    /**
     * The recording is currently active so the requested operation is not valid.
     */
    public static final int ACTIVE_RECORDING = 2;

    /**
     * A subscription is currently active for the requested channel and stream id which would clash.
     */
    public static final int ACTIVE_SUBSCRIPTION = 3;

    /**
     * The subscription for the requested operation is not known to the archive.
     */
    public static final int UNKNOWN_SUBSCRIPTION = 4;

    /**
     * The recording identity for the operation is not know to the archive.
     */
    public static final int UNKNOWN_RECORDING = 5;

    /**
     * The replay identity for the operation is not known to the archive.
     */
    public static final int UNKNOWN_REPLAY = 6;

    /**
     * The archive has reached its maximum concurrent replay sessions.
     */
    public static final int MAX_REPLAYS = 7;

    /**
     * The archive has reached its maximum concurrent recording sessions.
     */
    public static final int MAX_RECORDINGS = 8;

    /**
     * The extend-recording operation is not valid for the existing recording.
     */
    public static final int INVALID_EXTENSION = 9;

    /**
     * The archive is rejecting the session because of failed authentication.
     */
    public static final int AUTHENTICATION_REJECTED = 10;

    /**
     * The archive storage is at minimum threshold or exhausted.
     */
    public static final int STORAGE_SPACE = 11;

    /**
     * The replication identity for this operation is not known to the archive.
     */
    public static final int UNKNOWN_REPLICATION = 12;

    /**
     * The principle was not authorised to take the requested action.
     */
    public static final int UNAUTHORISED_ACTION = 13;

    /**
     * The replication session failed to connect to the source archive.
     */
    public static final int REPLICATION_CONNECTION_FAILURE = 14;

    /**
     * The recording specified for replay is empty.
     */
    public static final short EMPTY_RECORDING = 15;


    /**
     * The position specified for the operation is not valid, e.g. not frame-aligned, below start, or above stop.
     */
    public static final int INVALID_POSITION = 16;

    private static final long serialVersionUID = 386758252787901080L;

    /**
     * Error code.
     */
    private final int errorCode;
    /**
     * Command correlation id.
     */
    private final long correlationId;

    /**
     * Default ArchiveException exception as {@link Category#ERROR} and
     * {@link #errorCode()} = {@link #GENERIC}.
     */
    public ArchiveException()
    {
        super();
        errorCode = GENERIC;
        correlationId = Aeron.NULL_VALUE;
    }

    /**
     * ArchiveException exception as {@link Category#ERROR} and
     * {@link #errorCode()} = {@link #GENERIC}, plus detail.
     *
     * @param message providing detail.
     */
    public ArchiveException(final String message)
    {
        super(message);
        errorCode = GENERIC;
        correlationId = Aeron.NULL_VALUE;
    }

    /**
     * ArchiveException exception as {@link Category#ERROR}, plus detail and
     * error code.
     *
     * @param message   providing detail.
     * @param errorCode for type.
     */
    public ArchiveException(final String message, final int errorCode)
    {
        super(message);
        this.errorCode = errorCode;
        correlationId = Aeron.NULL_VALUE;
    }

    /**
     * ArchiveException exception as {@link Category#ERROR}, plus detail, cause,
     * and error code.
     *
     * @param message   providing detail.
     * @param cause     of the error.
     * @param errorCode for type.
     */
    public ArchiveException(final String message, final Throwable cause, final int errorCode)
    {
        super(message, cause);
        this.errorCode = errorCode;
        correlationId = Aeron.NULL_VALUE;
    }

    /**
     * ArchiveException exception as {@link Category#ERROR}, plus detail, error code,
     * and correlation if of the control request.
     *
     * @param message       providing detail.
     * @param errorCode     for type.
     * @param correlationId of the control request.
     */
    public ArchiveException(final String message, final int errorCode, final long correlationId)
    {
        super(message);
        this.errorCode = errorCode;
        this.correlationId = correlationId;
    }

    /**
     * ArchiveException exception {@link #errorCode()} = {@link #GENERIC}, plus detail and
     * {@link Category}.
     *
     * @param message  providing detail.
     * @param category for type.
     */
    public ArchiveException(final String message, final Category category)
    {
        super(message, category);
        this.errorCode = GENERIC;
        this.correlationId = Aeron.NULL_VALUE;
    }

    /**
     * ArchiveException exception {@link #errorCode()} = {@link #GENERIC}, plus detail, correlation id of control
     * request, and {@link Category}.
     *
     * @param message       providing detail.
     * @param correlationId of the control request.
     * @param category      for type.
     */
    public ArchiveException(final String message, final long correlationId, final Category category)
    {
        super(message, category);
        this.errorCode = GENERIC;
        this.correlationId = correlationId;
    }

    /**
     * ArchiveException exception, plus detail, error code, correlation id of control request,
     * and {@link Category}.
     *
     * @param message       providing detail.
     * @param errorCode     for type.
     * @param correlationId of the control request.
     * @param category      for type.
     */
    public ArchiveException(
        final String message, final int errorCode, final long correlationId, final Category category)
    {
        super(message, category);
        this.errorCode = errorCode;
        this.correlationId = correlationId;
    }

    /**
     * Builds an error message for a replay with a start position beyond the limit.
     *
     * @param recordingId the recording id
     * @param replayStartPosition the replay position
     * @param limitPosition the limit position
     * @return the created message
     */
    public static String buildReplayExceedsLimitErrorMsg(
        final long recordingId, final long replayStartPosition, final long limitPosition)
    {
        return "requested replay start position=" + replayStartPosition +
            " must be less than the limit position=" + limitPosition + " for recording " + recordingId;
    }

    /**
     * Builds an error message for a replay with a position before the start position.
     *
     * @param recordingId the recording id
     * @param replayStartPosition the replay start position
     * @param startPosition the start position of the recording
     * @return the created message
     */
    public static String buildReplayBeforeStartErrorMsg(
        final long recordingId, final long replayStartPosition, final long startPosition)
    {
        return "requested replay start position=" + replayStartPosition +
            " is less than recording start position=" + startPosition + " for recording " + recordingId;
    }

    /**
     * Builds an error message for an unknown recording.
     *
     * @param recordingId the recording id
     * @return the created message.
     */
    public static String buildUnknownRecordingErrorMsg(final long recordingId)
    {
        return "unknown recording id: " + recordingId;
    }

    /**
     * Error code providing more detail into what went wrong.
     *
     * @return code providing more detail into what went wrong.
     */
    public int errorCode()
    {
        return errorCode;
    }

    /**
     * Optional correlation-id associated with a control protocol request. Will be {@link Aeron#NULL_VALUE} if
     * not set.
     *
     * @return correlation-id associated with a control protocol request.
     */
    public long correlationId()
    {
        return correlationId;
    }

    /**
     * Return String representation for given error code.
     *
     * @param errorCode to convert.
     * @return error code string.
     */
    public static String errorCodeAsString(final int errorCode)
    {
        return switch (errorCode)
        {
            case GENERIC -> "GENERIC";
            case ACTIVE_LISTING -> "ACTIVE_LISTING";
            case ACTIVE_RECORDING -> "ACTIVE_RECORDING";
            case ACTIVE_SUBSCRIPTION -> "ACTIVE_SUBSCRIPTION";
            case UNKNOWN_SUBSCRIPTION -> "UNKNOWN_SUBSCRIPTION";
            case UNKNOWN_RECORDING -> "UNKNOWN_RECORDING";
            case UNKNOWN_REPLAY -> "UNKNOWN_REPLAY";
            case MAX_REPLAYS -> "MAX_REPLAYS";
            case MAX_RECORDINGS -> "MAX_RECORDINGS";
            case INVALID_EXTENSION -> "INVALID_EXTENSION";
            case AUTHENTICATION_REJECTED -> "AUTHENTICATION_REJECTED";
            case STORAGE_SPACE -> "STORAGE_SPACE";
            case UNKNOWN_REPLICATION -> "UNKNOWN_REPLICATION";
            case UNAUTHORISED_ACTION -> "UNAUTHORISED_ACTION";
            case REPLICATION_CONNECTION_FAILURE -> "REPLICATION_CONNECTION_FAILURE";
            case INVALID_POSITION -> "INVALID_POSITION";
            default -> "unknown error code: " + errorCode;
        };
    }
}
