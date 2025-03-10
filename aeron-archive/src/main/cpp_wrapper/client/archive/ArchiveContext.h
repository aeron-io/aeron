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
#ifndef AERON_ARCHIVE_WRAPPER_CONTEXT_H
#define AERON_ARCHIVE_WRAPPER_CONTEXT_H

extern "C"
{
#include "client/aeron_archive_context.h"
}

#include "AeronArchive.h"
#include "CredentialsSupplier.h"
#include "concurrent/YieldingIdleStrategy.h"

#include "Context.h"

namespace aeron { namespace archive { namespace client
{

class RecordingSignal
{
public:

    enum Value
    {
        START = INT32_C(0),
        STOP = INT32_C(1),
        EXTEND = INT32_C(2),
        REPLICATE = INT32_C(3),
        MERGE = INT32_C(4),
        SYNC = INT32_C(5),
        ABC_DELETE = INT32_C(6),
        REPLICATE_END = INT32_C(7),
        NULL_VALUE = INT32_MIN
    };

    RecordingSignal(
        std::int64_t controlSessionId,
        std::int64_t recordingId,
        std::int64_t subscriptionId,
        std::int64_t position,
        std::int32_t recordingSignalCode) :
        m_controlSessionId(controlSessionId),
        m_recordingId(recordingId),
        m_subscriptionId(subscriptionId),
        m_position(position),
        m_recordingSignalCode(recordingSignalCode)
    {
    }

    std::int64_t m_controlSessionId;
    std::int64_t m_recordingId;
    std::int64_t m_subscriptionId;
    std::int64_t m_position;
    std::int32_t m_recordingSignalCode;
};

typedef std::function<void(RecordingSignal &recordingSignal)> recording_signal_consumer_t;

typedef std::function<void()> delegating_invoker_t;

class Context
{
    friend class AeronArchive;

public:
    Context()
    {
        if (aeron_archive_context_init(&m_aeron_archive_ctx_t) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        setupContext();
    }

    Context(const Context &ctx)
    {
        aeron_archive_context_duplicate(&m_aeron_archive_ctx_t, ctx.m_aeron_archive_ctx_t);
        setupContext();
    }

    Context& operator=(const Context &ctx)
    {
        aeron_archive_context_close(m_aeron_archive_ctx_t);
        aeron_archive_context_duplicate(&m_aeron_archive_ctx_t, ctx.m_aeron_archive_ctx_t);
        setupContext();
        return *this;
    }

    ~Context()
    {
        aeron_archive_context_close(m_aeron_archive_ctx_t);
        m_aeron_archive_ctx_t = nullptr;
    }

    inline Context &aeron(std::shared_ptr<Aeron> aeron)
    {
        if (aeron_archive_context_set_aeron(m_aeron_archive_ctx_t,aeron->aeron()) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        m_aeronW = std::move(aeron);

        return *this;
    }

    inline std::shared_ptr<Aeron> aeron() const
    {
        return m_aeronW;
    }

    inline Context &aeronDirectoryName(const std::string &directoryName)
    {
        aeron_archive_context_set_aeron_directory_name(m_aeron_archive_ctx_t, directoryName.c_str());
        return *this;
    }

    inline std::string aeronDirectoryName() const
    {
        return { aeron_archive_context_get_aeron_directory_name(m_aeron_archive_ctx_t) };
    }

    inline Context &controlRequestChannel(const std::string &channel)
    {
        aeron_archive_context_set_control_request_channel(m_aeron_archive_ctx_t, channel.c_str());
        return *this;
    }

    inline std::string controlRequestChannel() const
    {
        return aeron_archive_context_get_control_request_channel(m_aeron_archive_ctx_t);
    }

    inline Context &controlRequestStreamId(const std::int32_t streamId)
    {
        aeron_archive_context_set_control_request_stream_id(m_aeron_archive_ctx_t, streamId);
        return *this;
    }

    inline std::int32_t controlRequestStreamId() const
    {
        return aeron_archive_context_get_control_request_stream_id(m_aeron_archive_ctx_t);
    }

    inline Context &controlResponseChannel(const std::string &channel)
    {
        aeron_archive_context_set_control_response_channel(m_aeron_archive_ctx_t, channel.c_str());
        return *this;
    }

    inline std::string controlResponseChannel() const
    {
        return aeron_archive_context_get_control_response_channel(m_aeron_archive_ctx_t);
    }

    inline Context &controlResponseStreamId(const std::int32_t streamId)
    {
        aeron_archive_context_set_control_response_stream_id(m_aeron_archive_ctx_t, streamId);
        return *this;
    }

    inline std::int32_t controlResponseStreamId() const
    {
        return aeron_archive_context_get_control_response_stream_id(m_aeron_archive_ctx_t);
    }

    inline Context &messageTimeoutNS(const std::int64_t messageTmoNS)
    {
        aeron_archive_context_set_message_timeout_ns(m_aeron_archive_ctx_t, messageTmoNS);
        return *this;
    }

    inline std::int64_t messageTimeoutNS() const
    {
        return aeron_archive_context_get_message_timeout_ns(m_aeron_archive_ctx_t);
    }

    template<typename IdleStrategy>
    inline Context &idleStrategy(IdleStrategy &idleStrategy)
    {
        m_idleFunc = [&idleStrategy](int work_count){ idleStrategy.idle(work_count); };
        return *this;
    }

    inline Context &credentialsSupplier(const CredentialsSupplier &credentialsSupplier)
    {
        m_credentialsSupplier.m_encodedCredentials = credentialsSupplier.m_encodedCredentials;
        m_credentialsSupplier.m_onChallenge = credentialsSupplier.m_onChallenge;
        m_credentialsSupplier.m_onFree = credentialsSupplier.m_onFree;
        return *this;
    }

    inline Context &recordingSignalConsumer(const recording_signal_consumer_t &consumer)
    {
        m_recordingSignalConsumer = consumer;
        return *this;
    }

    inline Context &errorHandler(const exception_handler_t &errorHandler)
    {
        if (aeron_archive_context_set_error_handler(
            m_aeron_archive_ctx_t,
            error_handler_func,
            (void *)this) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        m_errorHandler = errorHandler;
        return *this;
    }

    inline exception_handler_t errorHandler() const
    {
        return m_errorHandler;
    }

    inline Context &delegatingInvoker(const delegating_invoker_t &delegatingInvokerFunc)
    {
        m_delegatingInvoker = delegatingInvokerFunc;
        return *this;
    }

    inline delegating_invoker_t delegatingInvoker() const
    {
        return m_delegatingInvoker;
    }

    inline Context &maxErrorMessageLength(const std::uint32_t maxErrorMessageLength)
    {
        m_maxErrorMessageLength = maxErrorMessageLength;
        return *this;
    }

    inline std::uint32_t maxErrorMessageLength() const
    {
        return m_maxErrorMessageLength;
    }

private:
    aeron_archive_context_t *m_aeron_archive_ctx_t = nullptr; // backing C struct

    std::shared_ptr<Aeron> m_aeronW = nullptr;

    std::function<void(int work_count)> m_idleFunc;
    YieldingIdleStrategy m_defaultIdleStrategy;

    CredentialsSupplier m_credentialsSupplier;
    aeron_archive_encoded_credentials_t m_lastEncodedCredentials = {};

    recording_signal_consumer_t m_recordingSignalConsumer = nullptr;
    exception_handler_t m_errorHandler = nullptr;
    delegating_invoker_t m_delegatingInvoker = nullptr;

    std::uint32_t m_maxErrorMessageLength = 1000;

    // This Context is created after connect succeeds and wraps around a context_t created in the C layer
    explicit Context(
        aeron_archive_context_t *archive_context) :
        m_aeron_archive_ctx_t(archive_context)
    {
        setupContext();
    }

    void setupContext()
    {
        if (aeron_archive_context_set_idle_strategy(
            m_aeron_archive_ctx_t,
            idle,
            (void *)this) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        if (aeron_archive_context_set_credentials_supplier(
            m_aeron_archive_ctx_t,
            encodedCredentialsFunc,
            onChallengeFunc,
            onFreeFunc,
            (void *)this) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        if (aeron_archive_context_set_recording_signal_consumer(
            m_aeron_archive_ctx_t,
            recording_signal_consumer_func,
            (void *)this) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        if (aeron_archive_context_set_delegating_invoker(
            m_aeron_archive_ctx_t,
            delegating_invoker_func,
            (void *)this) < 0)
        {
            ARCHIVE_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        this->idleStrategy(m_defaultIdleStrategy);
    }

    static void idle(void *clientd, int work_count)
    {
        auto ctx = (Context *)clientd;

        ctx->m_idleFunc(work_count);
    }

    static aeron_archive_encoded_credentials_t *encodedCredentialsFunc(void *clientd)
    {
        auto ctx = (Context *)clientd;

        auto credentials = ctx->m_credentialsSupplier.m_encodedCredentials();

        ctx->m_lastEncodedCredentials = { credentials.first, credentials.second };

        return &ctx->m_lastEncodedCredentials;
    }

    static aeron_archive_encoded_credentials_t *onChallengeFunc(aeron_archive_encoded_credentials_t *encodedChallenge, void *clientd)
    {
        auto ctx = (Context *)clientd;

        auto credentials = ctx->m_credentialsSupplier.m_onChallenge({ encodedChallenge->data, encodedChallenge->length});

        ctx->m_lastEncodedCredentials = { credentials.first, credentials.second };

        return &ctx->m_lastEncodedCredentials;
    }

    static void onFreeFunc(aeron_archive_encoded_credentials_t *credentials, void *clientd)
    {
        auto ctx = (Context *)clientd;

        ctx->m_credentialsSupplier.m_onFree({ credentials->data, credentials->length});
    }

    static void recording_signal_consumer_func(
        aeron_archive_recording_signal_t *recording_signal,
        void *clientd)
    {
        auto ctx = (Context *)clientd;

        if (nullptr != ctx->m_recordingSignalConsumer)
        {
            RecordingSignal signal(
                recording_signal->control_session_id,
                recording_signal->recording_id,
                recording_signal->subscription_id,
                recording_signal->position,
                recording_signal->recording_signal_code);

            ctx->m_recordingSignalConsumer(signal);
        }
    }

    static void error_handler_func(void *clientd, int errcode, const char *message)
    {
        auto ctx = (Context *)clientd;

        if (nullptr != ctx->m_errorHandler)
        {
            try
            {
                ARCHIVE_MAP_TO_SOURCED_EXCEPTION_AND_THROW(errcode, message);
            }
            catch (SourcedException &exception)
            {
                ctx->m_errorHandler(exception);
            }
        }
    }

    static void delegating_invoker_func(void *clientd)
    {
        auto ctx = (Context *)clientd;

        if (nullptr != ctx->m_delegatingInvoker)
        {
            ctx->m_delegatingInvoker();
        }
    }

};

}}}

#endif //AERON_ARCHIVE_WRAPPER_CONTEXT_H
