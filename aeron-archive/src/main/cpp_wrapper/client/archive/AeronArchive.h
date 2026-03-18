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
#ifndef AERON_ARCHIVE_WRAPPER_H
#define AERON_ARCHIVE_WRAPPER_H

#include "client/aeron_archive.h"

#include "Aeron.h"
#include "client/util/ArchiveExceptions.h"

#include "ArchiveContext.h"
#include "ReplayParams.h"
#include "ReplicationParams.h"

namespace aeron { namespace archive { namespace client
{

constexpr const std::int64_t NULL_POSITION = aeron::NULL_VALUE;

constexpr const std::int64_t NULL_LENGTH = aeron::NULL_VALUE;

struct RecordingDescriptor
{
    RecordingDescriptor(
        std::int64_t controlSessionId,
        std::int64_t correlationId,
        std::int64_t recordingId,
        std::int64_t startTimestamp,
        std::int64_t stopTimestamp,
        std::int64_t startPosition,
        std::int64_t stopPosition,
        std::int32_t initialTermId,
        std::int32_t segmentFileLength,
        std::int32_t termBufferLength,
        std::int32_t mtuLength,
        std::int32_t sessionId,
        std::int32_t streamId,
        std::string strippedChannel,
        std::string originalChannel,
        std::string sourceIdentity) :
        m_controlSessionId(controlSessionId),
        m_correlationId(correlationId),
        m_recordingId(recordingId),
        m_startTimestamp(startTimestamp),
        m_stopTimestamp(stopTimestamp),
        m_startPosition(startPosition),
        m_stopPosition(stopPosition),
        m_initialTermId(initialTermId),
        m_segmentFileLength(segmentFileLength),
        m_termBufferLength(termBufferLength),
        m_mtuLength(mtuLength),
        m_sessionId(sessionId),
        m_streamId(streamId),
        m_strippedChannel(std::move(strippedChannel)),
        m_originalChannel(std::move(originalChannel)),
        m_sourceIdentity(std::move(sourceIdentity))
    {
    }

    std::int64_t m_controlSessionId;
    std::int64_t m_correlationId;
    std::int64_t m_recordingId;
    std::int64_t m_startTimestamp;
    std::int64_t m_stopTimestamp;
    std::int64_t m_startPosition;
    std::int64_t m_stopPosition;
    std::int32_t m_initialTermId;
    std::int32_t m_segmentFileLength;
    std::int32_t m_termBufferLength;
    std::int32_t m_mtuLength;
    std::int32_t m_sessionId;
    std::int32_t m_streamId;
    const std::string m_strippedChannel;
    const std::string m_originalChannel;
    const std::string m_sourceIdentity;
};

typedef std::function<void(RecordingDescriptor &recordingDescriptor)> recording_descriptor_consumer_t;

struct RecordingSubscriptionDescriptor
{
    RecordingSubscriptionDescriptor(
        std::int64_t controlSessionId,
        std::int64_t correlationId,
        std::int64_t subscriptionId,
        std::int32_t streamId,
        std::string strippedChannel) :
        m_controlSessionId(controlSessionId),
        m_correlationId(correlationId),
        m_subscriptionId(subscriptionId),
        m_streamId(streamId),
        m_strippedChannel(std::move(strippedChannel))
    {
    }

    std::int64_t m_controlSessionId;
    std::int64_t m_correlationId;
    std::int64_t m_subscriptionId;
    std::int32_t m_streamId;
    const std::string m_strippedChannel;
};

typedef std::function<void(RecordingSubscriptionDescriptor &recordingSubscriptionDescriptor)> recording_subscription_descriptor_consumer_t;

using namespace aeron::util;

/**
 * Client for interacting with a local or remote Aeron Archive.
 */
class AeronArchive
{

    friend class ReplayMerge;

public:
    using Context_t = aeron::archive::client::Context;

    /// Location of the source with respect to the archive.
    enum SourceLocation : int
    {
        /// Source is local to the archive and will be recorded using a spy Subscription.
        LOCAL = 0,

        /// Source is remote to the archive and will be recorded using a network Subscription.
        REMOTE = 1
    };

    /**
     * Allows for the async establishment of an Aeron Archive session.
     */
    class AsyncConnect
    {
        friend class AeronArchive;

    public:

        /**
         * Poll for a complete connection.
         *
         * @return a new AeronArchive when successfully connected, otherwise returns null
         *
         * @see aeron_archive_async_connect_poll
         */
        std::shared_ptr<AeronArchive> poll()
        {
            if (nullptr == m_async)
            {
                throw ArchiveException("AsyncConnect already complete", SOURCEINFO);
            }

            aeron_archive_t *aeron_archive = nullptr;

            if (aeron_archive_async_connect_poll(&aeron_archive, m_async) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }

            if (nullptr == aeron_archive)
            {
                return {};
            }

            m_async = nullptr; // _poll() just free'd this up

            return std::shared_ptr<AeronArchive>(
                new AeronArchive(
                    aeron_archive,
                    m_aeronW,
                    m_recordingSignalConsumer,
                    m_errorHandler,
                    m_delegatingInvoker,
                    m_maxErrorMessageLength));
        }

    private:
        explicit AsyncConnect(
            Context &ctx) :
            m_async(nullptr),
            m_aeronW(ctx.aeron()),
            m_recordingSignalConsumer(ctx.m_recordingSignalConsumer),
            m_errorHandler(ctx.m_errorHandler),
            m_delegatingInvoker(ctx.m_delegatingInvoker),
            m_maxErrorMessageLength(ctx.m_maxErrorMessageLength)
        {
            // async_connect makes a copy of the underlying aeron_archive_context_t
            if (aeron_archive_async_connect(&m_async, ctx.m_aeron_archive_ctx_t) < 0)
            {
                ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
            }
        }

        aeron_archive_async_connect_t *m_async;
        std::shared_ptr<Aeron> m_aeronW;

        const recording_signal_consumer_t m_recordingSignalConsumer;
        const exception_handler_t m_errorHandler;
        const delegating_invoker_t m_delegatingInvoker;

        const std::uint32_t m_maxErrorMessageLength;
    };

    /**
     * Initiate an asynchronous connection attempt to an Aeron Archive.
     *
     * @param ctx a reference to an Archive Context with relevant configuration
     * @return the AsyncConnect object that can be polled for completion
     *
     * @see aeron_archive_async_connect
     */
    static std::shared_ptr<AsyncConnect> asyncConnect(Context &ctx)
    {
        return std::shared_ptr<AsyncConnect>(new AsyncConnect(ctx));
    }

    /**
     *
     * @param ctx a reference to an Archive Context with relevant configuration
     * @return a new AeronArchive
     *
     * @see aeron_archive_connect
     */
    static std::shared_ptr<AeronArchive> connect(Context &ctx)
    {
        aeron_archive_t *aeron_archive = nullptr;

        if (aeron_archive_connect(&aeron_archive, ctx.m_aeron_archive_ctx_t) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return std::shared_ptr<AeronArchive>(
            new AeronArchive(
                aeron_archive,
                ctx.aeron(),
                ctx.m_recordingSignalConsumer,
                ctx.m_errorHandler,
                ctx.m_delegatingInvoker,
                ctx.m_maxErrorMessageLength));
    }

    /**
     * Close the connection to the Archive.
     *
     * @see aeron_archive_close
     */
    ~AeronArchive()
    {
        // make sure to clean things up in the correct order
        m_controlResponseSubscription = nullptr;

        aeron_archive_close(m_aeron_archive_t);
    }

    /**
     * Get the Context used to connect this archive client.
     *
     * @return the Context used to connect this archive client
     */
    const Context &context()
    {
        return m_archiveCtxW;
    }

    /**
     * The id of the archive.
     *
     * @return the id of the archive to which this client is connected
     *
     * @see aeron_archive_get_archive_id
     */
    std::int64_t archiveId()
    {
        return aeron_archive_get_archive_id(m_aeron_archive_t);
    }

    /**
     * Get a reference to the Subscription used to receive responses from the Archive.
     *
     * @return a reference to the Subscription
     *
     * @see aeron_archive_get_control_response_subscription
     */
    Subscription &controlResponseSubscription()
    {
        return *m_controlResponseSubscription;
    }

    // helpful for testing... not necessarily useful otherwise
    inline std::int64_t controlSessionId()
    {
        return aeron_archive_control_session_id(m_aeron_archive_t);
    }

    /**
     * Poll for recording signals, dispatching them to the configured handler.
     *
     * @return the number of RecordingSignals dispatched
     *
     * @see Context#recordingSignalConsumer
     * @see aeron_archive_poll_for_recording_signals
     */
    inline std::int32_t pollForRecordingSignals()
    {
        std::int32_t count;

        if (aeron_archive_poll_for_recording_signals(&count, m_aeron_archive_t) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * Poll the response stream once for an error.
     *
     * @return an error string, which will be empty if no error was found
     *
     * @see aeron_archive_poll_for_error_response
     */
    inline std::string pollForErrorResponse()
    {
        char *buffer = m_errorMessageBuffer.get();

        if (aeron_archive_poll_for_error_response(m_aeron_archive_t, buffer, m_archiveCtxW.maxErrorMessageLength()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return { buffer };
    }

    /**
     * Poll the response stream once for an error.
     * If an error handler is specified, the error (if found) will be dispatched to the handler.
     * If no error handler is specified, an exception will be thrown.
     *
     * @see aeron_archive_check_for_error_response
     */
    inline void checkForErrorResponse()
    {
        if (aeron_archive_check_for_error_response(m_aeron_archive_t) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Create a publication and set it up to be recorded.
     *
     * @param channel the channel for the publication
     * @param streamId the stream id for the publication
     * @return a Publication that's being recorded
     *
     * @see aeron_archive_add_recorded_publication
     */
    inline std::shared_ptr<Publication> addRecordedPublication(const std::string &channel, std::int32_t streamId)
    {
        aeron_publication_t *publication;

        if (aeron_archive_add_recorded_publication(
            &publication,
            m_aeron_archive_t,
            channel.c_str(),
            streamId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return std::make_shared<Publication>(m_archiveCtxW.aeron()->aeron(), publication);
    }

    /**
     * Create an exclusive publication and set it up to be recorded.
     *
     * @param channel the channel for the exclusive publication
     * @param streamId the stream id for the exclusive publication
     * @return an ExclusivePublication that's being recorded
     *
     * @see aeron_archive_add_recorded_exclusive_publication
     */
    inline std::shared_ptr<ExclusivePublication> addRecordedExclusivePublication(const std::string &channel, std::int32_t streamId)
    {
        aeron_exclusive_publication_t *exclusivePublication;

        if (aeron_archive_add_recorded_exclusive_publication(
            &exclusivePublication,
            m_aeron_archive_t,
            channel.c_str(),
            streamId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return std::make_shared<ExclusivePublication>(m_archiveCtxW.aeron()->aeron(), exclusivePublication);
    }

    /**
     * Start recording a channel/stream pairing.
     * <p>
     * Channels that include session id parameters are considered different than channels without session ids.
     * If a publication matches both a session id specific channel recording and a non session id specific recording,
     * it will be recorded twice.
     *
     * @param channel the channel of the publication to be recorded
     * @param streamId the stream id of the publication to be recorded
     * @param sourceLocation the SourceLocation of the publication to be recorded
     * @param autoStop should the recording be automatically stopped when complete
     * @return the recording's subscription id
     *
     * @see aeron_archive_start_recording
     */
    inline std::int64_t startRecording(
        const std::string &channel,
        std::int32_t streamId,
        SourceLocation sourceLocation,
        bool autoStop = false)
    {
        std::int64_t subscription_id;

        if (aeron_archive_start_recording(
            &subscription_id,
            m_aeron_archive_t,
            channel.c_str(),
            streamId,
            sourceLocation == SourceLocation::LOCAL ?
                AERON_ARCHIVE_SOURCE_LOCATION_LOCAL :
                AERON_ARCHIVE_SOURCE_LOCATION_REMOTE,
            autoStop) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return subscription_id;
    }

    /**
     * Fetch the position recorded for the specified recording.
     *
     * @param recordingId the archive recording id
     * @return the position of the specified recording
     *
     * @see aeron_archive_get_recording_position
     */
    inline std::int64_t getRecordingPosition(std::int64_t recordingId)
    {
        std::int64_t recording_position;

        if (aeron_archive_get_recording_position(
            &recording_position,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return recording_position;
    }

    /**
     * Fetch the start position for the specified recording.
     *
     * @param recordingId the archive recording id
     * @return the start position of the specified recording
     *
     * @see aeron_archive_get_start_position
     */
    inline std::int64_t getStartPosition(std::int64_t recordingId)
    {
        std::int64_t startPosition;

        if (aeron_archive_get_start_position(
            &startPosition,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return startPosition;
    }

    /**
     * Fetch the stop position for the specified recording.
     *
     * @param recordingId the active recording id
     * @return the stop position of the specified recording
     *
     * @see aeron_archive_get_stop_position
     */
    inline std::int64_t getStopPosition(std::int64_t recordingId)
    {
        std::int64_t stop_position;

        if (aeron_archive_get_stop_position(
            &stop_position,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stop_position;
    }

    /**
     * Fetch the stop or active position for the specified recording.
     *
     * @param recordingId the active recording id
     * @return the max recorded position of the specified recording
     *
     * @see aeron_archive_get_max_recorded_position
     */
    inline std::int64_t getMaxRecordedPosition(std::int64_t recordingId)
    {
        std::int64_t max_recorded_position;

        if (aeron_archive_get_max_recorded_position(
            &max_recorded_position,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return max_recorded_position;
    }

    /**
     * Stop recording the specified subscription id.
     * This is the subscription id returned from startRecording or extendRecording.
     *
     * @param subscriptionId the subscription id for the recording in the Aeron Archive
     *
     * @see aeron_archive_stop_recording_subscription
     */
    inline void stopRecording(std::int64_t subscriptionId)
    {
        if (aeron_archive_stop_recording_subscription(
            m_aeron_archive_t,
            subscriptionId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Try to stop a recording for the specified subscription id.
     * This is the subscription id returned from startRecording or extendRecording.
     *
     * @param subscriptionId the subscription id for the recording in the Aeron Archive
     * @return true if stopped, or false if the subscription is not currently active
     *
     * @see aeron_archive_try_stop_recording_subscription
     */
    inline bool tryStopRecording(std::int64_t subscriptionId)
    {
        bool stopped;

        if (aeron_archive_try_stop_recording_subscription(
            &stopped,
            m_aeron_archive_t,
            subscriptionId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stopped;
    }

    /**
     * Stop recording for the specified channel and stream.
     * <p>
     * Channels that include session id parameters are considered different than channels without session ids.
     * Stopping a recording on a channel without a session id parameter will not stop the recording of any
     * session id specific recordings that use the same channel and stream id.
     *
     * @param channel the channel of the recording to be stopped
     * @param streamId  the stream id of the recording to be stopped
     *
     * @see aeron_archive_stop_recording_channel_and_stream
     */
    inline void stopRecording(const std::string &channel, std::int32_t streamId)
    {
        if (aeron_archive_stop_recording_channel_and_stream(
            m_aeron_archive_t,
            channel.c_str(),
            streamId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Try to stop recording for the specified channel and stream.
     * <p>
     * Channels that include session id parameters are considered different than channels without session ids.
     * Stopping a recording on a channel without a session id parameter will not stop the recording of any
     * session id specific recordings that use the same channel and stream id.
     *
     * @param channel the channel of the recording to be stopped
     * @param streamId  the stream id of the recording to be stopped
     * @return true if stopped, or false if the subscription is not currently active
     *
     * @see aeron_archive_try_stop_recording_channel_and_stream
     */
    inline bool tryStopRecording(const std::string &channel, std::int32_t streamId)
    {
        bool stopped;

        if (aeron_archive_try_stop_recording_channel_and_stream(
            &stopped,
            m_aeron_archive_t,
            channel.c_str(),
            streamId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stopped;
    }

    /**
     * Stop recording for the specified recording id.
     *
     * @param recordingId the id of the recording to be stopped
     * @return true if stopped, or false if the subscription is not currently active
     *
     * @see aeron_archive_try_stop_recording_by_identity
     */
    inline bool tryStopRecordingByIdentity(std::int64_t recordingId)
    {
        bool stopped;

        if (aeron_archive_try_stop_recording_by_identity(
            &stopped,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stopped;
    }

    /**
     * Stop recording a session id specific recording that pertains to the given publication.
     *
     * @param publication the Publication to stop recording
     *
     * @see aeron_archive_stop_recording_publication
     */
    inline void stopRecording(const std::shared_ptr<Publication> &publication)
    {
        if (aeron_archive_stop_recording_publication(
            m_aeron_archive_t,
            publication->publication()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Stop recording a session id specific recording that pertains to the given ExclusivePublication.
     *
     * @param exclusivePublication the ExclusivePublication to stop recording
     *
     * @see aeron_archive_stop_recording_exclusive_publication
     */
    inline void stopRecording(const std::shared_ptr<ExclusivePublication> &exclusivePublication)
    {
        if (aeron_archive_stop_recording_exclusive_publication(
            m_aeron_archive_t,
            exclusivePublication->publication()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Find the last recording that matches the given criteria.
     *
     * @param minRecordingId the lowest recording id to search back to
     * @param channelFragment for a 'contains' match on the original channel stored with the Aeron Archive
     * @param streamId the stream id of the recording
     * @param sessionId the session id of the recording
     * @return the recording id that matches
     *
     * @see aeron_archive_find_last_matching_recording
     */
    inline std::int64_t findLastMatchingRecording(
        std::int64_t minRecordingId,
        const std::string &channelFragment,
        std::int32_t streamId,
        std::int32_t sessionId)
    {
        std::int64_t recording_id;

        if (aeron_archive_find_last_matching_recording(
            &recording_id,
            m_aeron_archive_t,
            minRecordingId,
            channelFragment.c_str(),
            streamId,
            sessionId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return recording_id;
    }

    /**
     * List a recording descriptor for a single recording id.
     *
     * @param recordingId the id of the recording
     * @param consumer to be called for each descriptor
     * @return the number of descriptors found
     *
     * @see aeron_archive_list_recording
     */
    inline std::int32_t listRecording(
        std::int64_t recordingId,
        const recording_descriptor_consumer_t &consumer)
    {
        std::int32_t count;

        if (aeron_archive_list_recording(
            &count,
            m_aeron_archive_t,
            recordingId,
            recording_descriptor_consumer_func,
            const_cast<void *>(reinterpret_cast<const void *>(&consumer))) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * List all recording descriptors starting at a particular recording id,
     * with a limit of total descriptors delivered.
     *
     * @param fromRecordingId the id at which to begin the listing
     * @param recordCount the limit of total descriptors to deliver
     * @param consumer to be called for each descriptor
     * @return the number of descriptors found
     *
     * @see aeron_archive_list_recordings
     */
    inline std::int32_t listRecordings(
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        const recording_descriptor_consumer_t &consumer)
    {
        std::int32_t count;

        if (aeron_archive_list_recordings(
            &count,
            m_aeron_archive_t,
            fromRecordingId,
            recordCount,
            recording_descriptor_consumer_func,
            const_cast<void *>(reinterpret_cast<const void *>(&consumer))) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * List all recording descriptors for a given channel fragment and stream id, starting at a particular recording id, with a limit of total descriptors delivered.
     *
     * @param fromRecordingId the id at which to begin the listing
     * @param recordCount the limit of total descriptors to deliver
     * @param channelFragment for a 'contains' match on the original channel stored with the Aeron Archive
     * @param streamId the stream id of the recording
     * @param consumer to be called for each descriptor
     * @return the number of descriptors found
     *
     * @see aeron_archive_list_recordings_for_uri
     */
    inline std::int32_t listRecordingsForUri(
        std::int64_t fromRecordingId,
        std::int32_t recordCount,
        const std::string &channelFragment,
        std::int32_t streamId,
        const recording_descriptor_consumer_t &consumer)
    {
        std::int32_t count;

        if (aeron_archive_list_recordings_for_uri(
            &count,
            m_aeron_archive_t,
            fromRecordingId,
            recordCount,
            channelFragment.c_str(),
            streamId,
            recording_descriptor_consumer_func,
            const_cast<void *>(reinterpret_cast<const void *>(&consumer))) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * Start a replay
     * <p>
     * The lower 32-bits of the replay session id contain the session id of the image of the received replay
     * and can be obtained by casting the replay session id to an int32_t.
     * All 64-bits are required to uniquely identify the replay when calling #stopReplay.
     *
     * @param recordingId the id of the recording
     * @param replayChannel the channel to which the replay should be sent
     * @param replayStreamId the stream id to which the replay should be sent
     * @param replayParams the ReplayParams that control the behavior of the replay
     * @return the replay session id
     *
     * @see aeron_archive_start_replay
     */
    inline std::int64_t startReplay(
        std::int64_t recordingId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        ReplayParams &replayParams)
    {
        std::int64_t replay_session_id;

        if (aeron_archive_start_replay(
            &replay_session_id,
            m_aeron_archive_t,
            recordingId,
            replayChannel.c_str(),
            replayStreamId,
            &replayParams.m_params) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return replay_session_id;
    }

    /**
     * Start a replay.
     *
     * @param recordingId the id of the recording
     * @param replayChannel the channel to which the replay should be sent
     * @param replayStreamId the stream id to which the replay should be sent
     * @param replayParams the ReplayParams that control the behavior of the replay
     * @return the Subscription created for consuming the replay
     *
     * @see aeron_archive_replay
     */
    inline std::shared_ptr<Subscription> replay(
        std::int64_t recordingId,
        const std::string &replayChannel,
        std::int32_t replayStreamId,
        ReplayParams &replayParams)
    {
        aeron_subscription_t *subscription;

        if (aeron_archive_replay(
            &subscription,
            m_aeron_archive_t,
            recordingId,
            replayChannel.c_str(),
            replayStreamId,
            &replayParams.m_params) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return std::make_shared<Subscription>(m_archiveCtxW.aeron()->aeron(), subscription, nullptr);
    }

    /**
     * Truncate a stopped recording to the specified position.
     * The position must be less than the stopped position.
     * The position must be on a fragment boundary.
     * Truncating a recording to the start position effectively deletes the recording.
     *
     * @param recordingId the id of the recording
     * @param position the position to which the recording will be truncated
     * @return the number of segments deleted
     *
     * @see aeron_archive_truncate_recording
     */
    inline std::int64_t truncateRecording(std::int64_t recordingId, std::int64_t position)
    {
        std::int64_t count;

        if (aeron_archive_truncate_recording(
            &count,
            m_aeron_archive_t,
            recordingId,
            position) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * Stop a replay session.
     *
     * @param replaySessionId the replay session id indicating the replay to stop
     *
     * @see aeron_archive_stop_replay
     */
    inline void stopReplay(std::int64_t replaySessionId)
    {
        if (aeron_archive_stop_replay(m_aeron_archive_t, replaySessionId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Stop all replays matching a recording id.
     * If recordingId is aeron::NULL_VALUE then match all replays.
     *
     * @param recordingId the id of the recording for which all replays will be stopped
     *
     * @see aeron_archive_stop_all_replays
     */
    inline void stopAllReplays(std::int64_t recordingId)
    {
        if (aeron_archive_stop_all_replays(
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * List active recording subscriptions in the Aeron Archive.
     * These are the result of calling aeron_archive_start_recording or aeron_archive_extend_recording.
     * The subscription id in the returned descriptor can be used when calling AeronArchive:stopRecording.
     *
     * @param pseudoIndex the index into the active list at which to begin listing
     * @param subscriptionCount the limit of the total descriptors to deliver
     * @param channelFragment for a 'contains' match on the original channel stored with the Aeron Archive
     * @param streamId the stream id of the recording
     * @param applyStreamId whether or not the stream id should be matched
     * @param consumer to be called for each recording subscription
     * @return the number of matched subscriptions
     *
     * @see aeron_archive_list_recording_subscriptions
     */
    inline std::int32_t listRecordingSubscriptions(
        std::int32_t pseudoIndex,
        std::int32_t subscriptionCount,
        const std::string &channelFragment,
        std::int32_t streamId,
        bool applyStreamId,
        const recording_subscription_descriptor_consumer_t &consumer)
    {
        std::int32_t count;

        if (aeron_archive_list_recording_subscriptions(
            &count,
            m_aeron_archive_t,
            pseudoIndex,
            subscriptionCount,
            channelFragment.c_str(),
            streamId,
            applyStreamId,
            recording_subscription_descriptor_consumer_func,
            const_cast<void *>(reinterpret_cast<const void *>(&consumer))) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * Purge a stopped recording.
     * i.e. Mark the recording as INVALID at the Archive and delete the corresponding segment files.
     * The space in the Catalog will be reclaimed upon compaction.
     *
     * @param recordingId the id of the stopped recording to be purged
     * @return the number of deleted segments
     *
     * @see aeron_archive_purge_recording
     */
    inline std::int64_t purgeRecording(std::int64_t recordingId)
    {
        std::int64_t deletedSegmentsCount;

        if (aeron_archive_purge_recording(
            &deletedSegmentsCount,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return deletedSegmentsCount;
    }

    /**
     * Extend an existing, non-active recording for a channel and stream pairing.
     * <p>
     * The channel must be configured with the initial position from which it will be extended.
     * This can be done with ChannelUriStringBuilder#initialPosition.
     * The details required to initialize can be found by calling #listRecording.
     *
     * @param recordingId the id of the existing recording
     * @param channel the channel of the publication to be recorded
     * @param streamId the stream id of the publication to be recorded
     * @param sourceLocation the source location of the publication to be recorded
     * @param autoStop should the recording be automatically stopped when complete
     * @return the subscription id of the recording
     *
     * @see aeron_archive_extend_recording
     */
    inline std::int64_t extendRecording(
        std::int64_t recordingId,
        const std::string &channel,
        std::int32_t streamId,
        SourceLocation sourceLocation,
        bool autoStop)
    {
        std::int64_t subscription_id;

        if (aeron_archive_extend_recording(
            &subscription_id,
            m_aeron_archive_t,
            recordingId,
            channel.c_str(),
            streamId,
            sourceLocation == SourceLocation::LOCAL ?
                AERON_ARCHIVE_SOURCE_LOCATION_LOCAL :
                AERON_ARCHIVE_SOURCE_LOCATION_REMOTE,
            autoStop) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return subscription_id;
    }

    /**
     * Replicate a recording from a source Archive to a destination.
     * This can be considered a backup for a primary Archive.
     * The source recording will be replayed via the provided replay channel and use the original stream id.
     * The behavior of the replication will be governed by the values specified in the ReplicationParams.
     * <p>
     * For a source recording that is still active, the replay can merge with the live stream and then follow it directly and no longer require the replay from the source.
     * This would require a multicast live destination.
     * <p>
     * Errors will be reported asynchronously and can be checked for with #checkForErrorResponse and #pollForErrorResponse
     *
     * @param srcRecordingId the recording id that must exist at the source archive
     * @param srcControlStreamId remote control channel for the source archive on which to instruct the replay
     * @param srcControlChannel remote control stream id for the source archive on which to instruct the replay
     * @param replicationParams optional parameters to configure the behavior of the replication
     * @return the replication id that can be used to stop the replication
     *
     * @see aeron_archive_replicate
     */
    inline std::int64_t replicate(
        std::int64_t srcRecordingId,
        std::int32_t srcControlStreamId,
        const std::string &srcControlChannel,
        ReplicationParams &replicationParams)
    {
        std::int64_t replicationId;

        if (aeron_archive_replicate(
            &replicationId,
            m_aeron_archive_t,
            srcRecordingId,
            srcControlChannel.c_str(),
            srcControlStreamId,
            &replicationParams.m_params) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return replicationId;
    }

    /**
     * Stop a replication by the replication id.
     *
     * @param replicationId the replication id retrieved when calling #replicate
     *
     * @see aeron_archive_stop_replication
     */
    inline void stopReplication(std::int64_t replicationId)
    {
        if (aeron_archive_stop_replication(m_aeron_archive_t, replicationId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Try to stop a replication by the replication id.
     *
     * @param replicationId the replication id retrieved when calling #replicate
     * @return true if stopped, or false if the recording is not currently active
     *
     * @see aeron_archive_try_stop_replication
     */
    inline bool tryStopReplication(std::int64_t replicationId)
    {
        bool stopped;

        if (aeron_archive_try_stop_replication(
            &stopped,
            m_aeron_archive_t,
            replicationId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return stopped;
    }

    /**
     * Detach segments from the beginning of a recording up to the provided new start position.
     * <p>
     * The new start position must be the first byte position of a segment after the existing start position.
     * <p>
     * It is not possible to detach segments which are active for recording or being replayed.
     *
     * @param recordingId the id of an existing recording
     * @param newStartPosition the new starting position for the recording after the segments are detached
     *
     * @see aeron_archive_detach_segments
     */
    inline void detachSegments(std::int64_t recordingId, std::int64_t newStartPosition)
    {
        if (aeron_archive_detach_segments(
            m_aeron_archive_t,
            recordingId,
            newStartPosition) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Delete segments which have been previously detached from a recording.
     *
     * @param recordingId the id of an existing recording
     * @return the number of segments deleted
     *
     * @see aeron_archive_delete_detached_segments
     */
    inline std::int64_t deleteDetachedSegments(std::int64_t recordingId)
    {
        std::int64_t count;

        if (aeron_archive_delete_detached_segments(
            &count,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * Purge (Detach and delete) segments from the beginning of a recording up to the provided new start position.
     * <p>
     * The new start position must be the first byte position of a segment after the existing start position.
     * <p>
     * It is not possible to detach segments which are active for recording or being replayed.
     *
     * @param recordingId the id of an existing recording
     * @param newStartPosition the new starting position for the recording after the segments are detached
     * @return the number of segments deleted
     *
     * @see aeron_archive_purge_segments
     */
    inline std::int64_t purgeSegments(std::int64_t recordingId, std::int64_t newStartPosition)
    {
        std::int64_t count;

        if (aeron_archive_purge_segments(
            &count,
            m_aeron_archive_t,
            recordingId,
            newStartPosition) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * Attach segments to the beginning of a recording to restore history that was previously detached.
     * <p>
     * Segment files must match the existing recording and join exactly to the start position of the recording they are being attached to.
     *
     * @param recordingId the id of an existing recording
     * @return the number of segments attached
     *
     * @see aeron_archive_attach_segments
     */
    inline std::int64_t attachSegments(std::int64_t recordingId)
    {
        std::int64_t count;

        if (aeron_archive_attach_segments(
            &count,
            m_aeron_archive_t,
            recordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * Migrate segments from a source recording and attach them to the beginning of a destination recording.
     * <p>
     * The source recording must match the destination recording for segment length, term length, mtu length,
     * stream id, plus the stop position and term id of the source must join with the start position of the destination
     * and be on a segment boundary.
     * <p>
     * The source recording will be effectively truncated back to its start position after the migration.
     *
     * @param srcRecordingId the id of an existing recording from which segments will be migrated
     * @param dstRecordingId the id of an exisintg recording to which segments will be migrated
     * @return the number of segments deleted
     */
    inline std::int64_t migrateSegments(std::int64_t srcRecordingId, std::int64_t dstRecordingId)
    {
        std::int64_t count;

        if (aeron_archive_migrate_segments(
            &count,
            m_aeron_archive_t,
            srcRecordingId,
            dstRecordingId) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        return count;
    }

    /**
     * Update the channel for a recording, i.e. replace original and stripped channel information in the catalog.
     *
     * @param recordingId the recording id to update.
     * @param newChannel  to use in the catalogue.
     */
    inline void updateChannel(std::int64_t recordingId, std::string newChannel)
    {
        if (aeron_archive_update_channel(
            m_aeron_archive_t,
            recordingId,
            newChannel.c_str()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    /**
     * Position of the recorded stream at the base of a segment file.
     * <p>
     * If a recording starts within a term then the base position can be before the recording started.
     *
     * @param startPosition start position of the stream
     * @param position the position in the stream to calculate the segment base position from
     * @param termBufferLength term buffer length of the stream
     * @param segmentFileLength segment file length, which is a multiple of term buffer length
     * @return the position of the recorded stream at the beginning of a segment file
     *
     * @see aeron_archive_segment_file_base_position
     */
    static std::int64_t segmentFileBasePosition(
        std::int64_t startPosition,
        std::int64_t position,
        std::int32_t termBufferLength,
        std::int32_t segmentFileLength)
    {
        return aeron_archive_segment_file_base_position(
            startPosition,
            position,
            termBufferLength,
            segmentFileLength);
    }

private:
    explicit AeronArchive(
        aeron_archive_t *aeron_archive,
        const std::shared_ptr<Aeron> &originalAeron,
        const recording_signal_consumer_t &recordingSignalConsumer,
        const exception_handler_t &errorHandler,
        const delegating_invoker_t &delegatingInvoker,
        const std::uint32_t maxErrorMessageLength) :
        m_aeron_archive_t(aeron_archive),
        m_archiveCtxW(aeron_archive_get_and_own_archive_context(m_aeron_archive_t))
    {
        // The following line divorces the aeron_t from the underlying aeron_archive
        aeron_archive_context_set_owns_aeron_client(m_archiveCtxW.m_aeron_archive_ctx_t, false);

        // Can't get the aeron_t via 'm_archiveCtxW.aeron()->aeron()' because m_archiveCtxW doesn't have an aeron set yet.
        // So use the C functions to acquire the underlying aeron_t.
        auto *aeron = aeron_archive_context_get_aeron(aeron_archive_get_archive_context(aeron_archive));

        m_archiveCtxW
            .aeron(nullptr == originalAeron ? std::make_shared<Aeron>(aeron) : originalAeron)
            .recordingSignalConsumer(recordingSignalConsumer)
            .delegatingInvoker(delegatingInvoker)
            .maxErrorMessageLength(maxErrorMessageLength);

        // If no previous errorHandler was set, then the underlying C code will use the default one, which just prints to stderr.
        // If there WAS an errorHandler set, then go ahead and copy it into the new archive context wrapper.
        if (nullptr != errorHandler)
        {
            m_archiveCtxW.errorHandler(errorHandler);
        }

        m_controlResponseSubscription = std::make_unique<Subscription>(
            aeron,
            aeron_archive_get_and_own_control_response_subscription(m_aeron_archive_t),
            nullptr);

        m_errorMessageBuffer = std::make_unique<char[]>(maxErrorMessageLength);
    }

    aeron_archive_t *m_aeron_archive_t = nullptr;
    Context m_archiveCtxW;
    std::unique_ptr<Subscription> m_controlResponseSubscription = nullptr;
    std::unique_ptr<char[]> m_errorMessageBuffer;

    static void recording_descriptor_consumer_func(
        aeron_archive_recording_descriptor_t *recording_descriptor,
        void *clientd)
    {
        auto consumer = *reinterpret_cast<recording_descriptor_consumer_t *>(clientd);

        RecordingDescriptor descriptor(
            recording_descriptor->control_session_id,
            recording_descriptor->correlation_id,
            recording_descriptor->recording_id,
            recording_descriptor->start_timestamp,
            recording_descriptor->stop_timestamp,
            recording_descriptor->start_position,
            recording_descriptor->stop_position,
            recording_descriptor->initial_term_id,
            recording_descriptor->segment_file_length,
            recording_descriptor->term_buffer_length,
            recording_descriptor->mtu_length,
            recording_descriptor->session_id,
            recording_descriptor->stream_id,
            recording_descriptor->stripped_channel,
            recording_descriptor->original_channel,
            recording_descriptor->source_identity);

        consumer(descriptor);
    }

    static void recording_subscription_descriptor_consumer_func(
        aeron_archive_recording_subscription_descriptor_t *recording_subscription_descriptor,
        void *clientd)
    {
        auto consumer = *reinterpret_cast<recording_subscription_descriptor_consumer_t *>(clientd);

        RecordingSubscriptionDescriptor descriptor(
            recording_subscription_descriptor->control_session_id,
            recording_subscription_descriptor->correlation_id,
            recording_subscription_descriptor->subscription_id,
            recording_subscription_descriptor->stream_id,
            recording_subscription_descriptor->stripped_channel);

        consumer(descriptor);
    }
};

}}}

#endif //AERON_ARCHIVE_WRAPPER_H
