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
#ifndef AERON_CHANNEL_URI_STRING_BUILDER_H
#define AERON_CHANNEL_URI_STRING_BUILDER_H

#include <memory>
#include <string>
#include <sstream>

#include "ChannelUri.h"
#include "concurrent/logbuffer/FrameDescriptor.h"
#include "concurrent/logbuffer/LogBufferDescriptor.h"

namespace aeron
{

using namespace aeron::util;

class ChannelUriStringBuilder
{
public:
    using this_t = ChannelUriStringBuilder;

    inline this_t &clear()
    {
        m_prefix.reset(nullptr);
        m_media.reset(nullptr);
        m_endpoint.reset(nullptr);
        m_networkInterface.reset(nullptr);
        m_controlEndpoint.reset(nullptr);
        m_controlMode.reset(nullptr);
        m_tags.reset(nullptr);
        m_alias.reset(nullptr);
        m_cc.reset(nullptr);
        m_fc.reset(nullptr);
        m_gtag.reset(nullptr);
        m_reliable.reset(nullptr);
        m_ttl.reset(nullptr);
        m_mtu.reset(nullptr);
        m_termLength.reset(nullptr);
        m_initialTermId.reset(nullptr);
        m_termId.reset(nullptr);
        m_termOffset.reset(nullptr);
        m_sessionId.reset(nullptr);
        m_linger.reset(nullptr);
        m_sparse.reset(nullptr);
        m_eos.reset(nullptr);
        m_tether.reset(nullptr);
        m_group.reset(nullptr);
        m_rejoin.reset(nullptr);
        m_ssc.reset(nullptr);
        m_socketRcvbufLength.reset(nullptr);
        m_socketSndbufLength.reset(nullptr);
        m_receiverWindowLength.reset(nullptr);
        m_isSessionIdTagged = false;
        m_mediaReceiveTimestampOffset.reset(nullptr);
        m_channelReceiveTimestampOffset.reset(nullptr);
        m_channelSendTimestampOffset.reset(nullptr);
        m_responseCorrelationId.reset(nullptr);
        m_nakDelay.reset(nullptr);
        m_untetheredWindowLimitTimeout.reset(nullptr);
        m_untetheredRestingTimeout.reset(nullptr);
        m_maxResend.reset(nullptr);

        return *this;
    }

    inline this_t &prefix(const std::string &prefix)
    {
        if (!prefix.empty() && prefix != SPY_QUALIFIER)
        {
            throw IllegalArgumentException("invalid prefix: " + prefix, SOURCEINFO, EINVAL);
        }

        m_prefix.reset(new std::string(prefix));
        return *this;
    }

    inline this_t &prefix(std::nullptr_t nullp)
    {
        m_prefix.reset(nullptr);
        return *this;
    }

    inline this_t &media(const std::string &media)
    {
        if (media != UDP_MEDIA && media != IPC_MEDIA)
        {
            throw IllegalArgumentException("invalid media: " + media, SOURCEINFO, EINVAL);
        }

        m_media.reset(new std::string(media));
        return *this;
    }

    inline this_t &endpoint(const std::string &endpoint)
    {
        m_endpoint.reset(new std::string(endpoint));
        return *this;
    }

    inline this_t &networkInterface(const std::string &networkInterface)
    {
        m_networkInterface.reset(new std::string(networkInterface));
        return *this;
    }

    inline this_t &controlEndpoint(const std::string &controlEndpoint)
    {
        m_controlEndpoint.reset(new std::string(controlEndpoint));
        return *this;
    }

    inline this_t &controlMode(const std::string &controlMode)
    {
        if (controlMode != MDC_CONTROL_MODE_MANUAL &&
            controlMode != MDC_CONTROL_MODE_DYNAMIC &&
            controlMode != CONTROL_MODE_RESPONSE)
        {
            throw IllegalArgumentException("invalid control mode: " + controlMode, SOURCEINFO, EINVAL);
        }

        m_controlMode.reset(new std::string(controlMode));
        return *this;
    }

    inline this_t &tags(const std::string &tags)
    {
        m_tags.reset(new std::string(tags));
        return *this;
    }

    inline this_t &alias(const std::string &alias)
    {
        m_alias.reset(new std::string(alias));
        return *this;
    }

    inline this_t &congestionControl(const std::string &congestionControl)
    {
        m_cc.reset(new std::string(congestionControl));
        return *this;
    }

    inline this_t &flowControl(const std::string &flowControl)
    {
        m_fc.reset(new std::string(flowControl));
        return *this;
    }

    inline this_t &groupTag(std::int64_t gtag)
    {
        m_gtag.reset(new Value(gtag));
        return *this;
    }

    inline this_t &reliable(bool reliable)
    {
        m_reliable.reset(new Value(reliable ? 1 : 0));
        return *this;
    }

    inline this_t &reliable(std::nullptr_t nullp)
    {
        m_reliable.reset(nullptr);
        return *this;
    }

    inline this_t &ttl(std::uint8_t ttl)
    {
        m_ttl.reset(new Value(ttl));
        return *this;
    }

    inline this_t &mtu(std::uint32_t mtu)
    {
        if (mtu < 32 || mtu > 65504)
        {
            throw IllegalArgumentException("MTU not in range 32-65504: " + std::to_string(mtu), SOURCEINFO, EINVAL);
        }

        if (0 != (mtu & static_cast<std::uint32_t>(concurrent::logbuffer::FrameDescriptor::FRAME_ALIGNMENT - 1)))
        {
            throw IllegalArgumentException(
                "MTU not a multiple of FRAME_ALIGNMENT: mtu=" + std::to_string(mtu), SOURCEINFO, EINVAL);
        }

        m_mtu.reset(new Value(mtu));
        return *this;
    }

    inline this_t &termLength(std::int32_t termLength)
    {
        concurrent::logbuffer::LogBufferDescriptor::checkTermLength(termLength);
        m_termLength.reset(new Value(termLength));
        return *this;
    }

    inline this_t &initialTermId(std::int32_t initialTermId)
    {
        m_initialTermId.reset(new Value(initialTermId));
        return *this;
    }

    inline this_t &termId(std::int32_t termId)
    {
        m_termId.reset(new Value(termId));
        return *this;
    }

    inline this_t &termOffset(std::uint32_t termOffset)
    {
        if (termOffset > concurrent::logbuffer::LogBufferDescriptor::TERM_MAX_LENGTH)
        {
            throw IllegalArgumentException("term offset not in range 0-1g: " + std::to_string(termOffset), SOURCEINFO, EINVAL);
        }

        if (0 != (termOffset & static_cast<std::uint32_t>(concurrent::logbuffer::FrameDescriptor::FRAME_ALIGNMENT - 1)))
        {
            throw IllegalArgumentException(
                "term offset not multiple of FRAME_ALIGNMENT: " + std::to_string(termOffset), SOURCEINFO, EINVAL);
        }

        m_termOffset.reset(new Value(termOffset));
        return *this;
    }

    inline this_t &sessionId(std::int32_t sessionId)
    {
        m_sessionId.reset(new Value(sessionId));
        return *this;
    }

    inline this_t &linger(std::int64_t lingerNs)
    {
        if (lingerNs < 0)
        {
            throw IllegalArgumentException("linger value cannot be negative: " + std::to_string(lingerNs), SOURCEINFO, EINVAL);
        }

        m_linger.reset(new Value(lingerNs));
        return *this;
    }

    inline this_t &sparse(bool sparse)
    {
        m_sparse.reset(new Value(sparse ? 1 : 0));
        return *this;
    }

    inline this_t &eos(bool eos)
    {
        m_eos.reset(new Value(eos ? 1 : 0));
        return *this;
    }

    inline this_t &tether(bool tether)
    {
        m_tether.reset(new Value(tether ? 1 : 0));
        return *this;
    }

    inline this_t &group(bool group)
    {
        m_group.reset(new Value(group ? 1 : 0));
        return *this;
    }

    inline this_t &rejoin(bool rejoin)
    {
        m_rejoin.reset(new Value(rejoin ? 1 : 0));
        return *this;
    }

    inline this_t &rejoin(std::nullptr_t nullp)
    {
        m_reliable.reset(nullptr);
        return *this;
    }

    inline this_t &spiesSimulateConnection(bool spiesSimulateConnection)
    {
        m_ssc.reset(new Value(spiesSimulateConnection ? 1 : 0));
        return *this;
    }

    inline this_t &spiesSimulateConnection(std::nullptr_t nullp)
    {
        m_ssc.reset(nullptr);
        return *this;
    }

    inline this_t &isSessionIdTagged(bool isSessionIdTagged)
    {
        m_isSessionIdTagged = isSessionIdTagged;
        return *this;
    }

    inline this_t &socketSndbufLength(std::uint32_t socketSndbufLength)
    {
        m_socketSndbufLength.reset(new Value(socketSndbufLength));
        return *this;
    }

    inline this_t &socketSndbufLength(std::nullptr_t socketSndbufLength)
    {
        m_socketSndbufLength.reset(nullptr);
        return *this;
    }

    inline this_t &socketRcvbufLength(std::uint32_t socketRcvbufLength)
    {
        m_socketRcvbufLength.reset(new Value(socketRcvbufLength));
        return *this;
    }

    inline this_t &socketRcvbufLength(std::nullptr_t)
    {
        m_socketRcvbufLength.reset(nullptr);
        return *this;
    }

    inline this_t &receiverWindowLength(std::uint32_t receiverWindowLength)
    {
        m_receiverWindowLength.reset(new Value(receiverWindowLength));
        return *this;
    }

    inline this_t &receiverWindowLength(std::nullptr_t)
    {
        m_receiverWindowLength.reset(nullptr);
        return *this;
    }

    inline this_t &initialPosition(std::int64_t position, std::int32_t initialTermId, std::int32_t termLength)
    {
        if (position < 0 || 0 != (position & (aeron::concurrent::logbuffer::FrameDescriptor::FRAME_ALIGNMENT - 1)))
        {
            throw IllegalArgumentException(
                "position not multiple of FRAME_ALIGNMENT: " + std::to_string(position), SOURCEINFO, EINVAL);
        }

        aeron::concurrent::logbuffer::LogBufferDescriptor::checkTermLength(termLength);
        int bitsToShift = BitUtil::numberOfTrailingZeroes(termLength);

        m_termLength.reset(new Value(termLength));
        m_initialTermId.reset(new Value(initialTermId));
        m_termId.reset(new Value((position >> bitsToShift) + initialTermId));
        m_termOffset.reset(new Value(position & (termLength - 1)));

        return *this;
    }

    inline this_t &mediaReceiveTimestampOffset(const std::string &mediaReceiveTimestampOffset)
    {
        m_mediaReceiveTimestampOffset.reset(new std::string(mediaReceiveTimestampOffset));
        return *this;
    }

    inline this_t &channelReceiveTimestampOffset(const std::string &receiveTimestampOffset)
    {
        m_channelReceiveTimestampOffset.reset(new std::string(receiveTimestampOffset));
        return *this;
    }

    inline this_t &channelSendTimestampOffset(const std::string &sendTimestampOffset)
    {
        m_channelSendTimestampOffset.reset(new std::string(sendTimestampOffset));
        return *this;
    }

    inline this_t &responseCorrelationId(std::int64_t responseCorrelationId)
    {
        m_responseCorrelationId.reset(new Value(responseCorrelationId));
        return *this;
    }

    inline this_t &nakDelay(std::int64_t nakDelay)
    {
        m_nakDelay.reset(new Value(nakDelay));
        return *this;
    }

    inline this_t &untetheredWindowLimitTimeout(std::int64_t timeout)
    {
        m_untetheredWindowLimitTimeout.reset(new Value(timeout));
        return *this;
    }

    inline this_t &untetheredRestingTimeout(std::int64_t timeout)
    {
        m_untetheredRestingTimeout.reset(new Value(timeout));
        return *this;
    }

    inline this_t &maxResend(std::int32_t maxResend)
    {
        m_maxResend.reset(new Value(maxResend));
        return *this;
    }

    std::string build()
    {
        std::ostringstream sb;

        if (m_prefix && !m_prefix->empty())
        {
            sb << *m_prefix << ':';
        }

        sb << AERON_SCHEME << ':' << *m_media << '?';

        append(sb, TAGS_PARAM_NAME, m_tags);
        append(sb, ENDPOINT_PARAM_NAME, m_endpoint);
        append(sb, INTERFACE_PARAM_NAME, m_networkInterface);
        append(sb, MDC_CONTROL_PARAM_NAME, m_controlEndpoint);
        append(sb, MDC_CONTROL_MODE_PARAM_NAME, m_controlMode);
        append(sb, MTU_LENGTH_PARAM_NAME, m_mtu);
        append(sb, TERM_LENGTH_PARAM_NAME, m_termLength);
        append(sb, INITIAL_TERM_ID_PARAM_NAME, m_initialTermId);
        append(sb, TERM_ID_PARAM_NAME, m_termId);
        append(sb, TERM_OFFSET_PARAM_NAME, m_termOffset);

        if (m_sessionId)
        {
            sb << SESSION_ID_PARAM_NAME << '=' << prefixTag(m_isSessionIdTagged, *m_sessionId) << '|';
        }

        append(sb, TTL_PARAM_NAME, m_ttl);

        if (m_reliable)
        {
            sb << RELIABLE_STREAM_PARAM_NAME << '=' << (m_reliable->value == 1 ? "true" : "false") << '|';
        }

        append(sb, LINGER_PARAM_NAME, m_linger);

        if (m_alias)
        {
            sb << ALIAS_PARAM_NAME << '=' << *m_alias << '|';
        }

        if (m_cc)
        {
            sb << CONGESTION_CONTROL_PARAM_NAME << '=' << *m_cc << '|';
        }

        if (m_fc)
        {
            sb << FLOW_CONTROL_PARAM_NAME << '=' << *m_fc << '|';
        }

        append(sb, GROUP_TAG_PARAM_NAME, m_gtag);

        if (m_sparse)
        {
            sb << SPARSE_PARAM_NAME << '=' << (m_sparse->value == 1 ? "true" : "false") << '|';
        }

        if (m_eos)
        {
            sb << EOS_PARAM_NAME << '=' << (m_eos->value == 1 ? "true" : "false") << '|';
        }

        if (m_tether)
        {
            sb << TETHER_PARAM_NAME << '=' << (m_tether->value == 1 ? "true" : "false") << '|';
        }

        if (m_group)
        {
            sb << GROUP_PARAM_NAME << '=' << (m_group->value == 1 ? "true" : "false") << '|';
        }

        if (m_rejoin)
        {
            sb << REJOIN_PARAM_NAME << '=' << (m_rejoin->value == 1 ? "true" : "false") << '|';
        }

        if (m_ssc)
        {
            sb << SPIES_SIMULATE_CONNECTION_PARAM_NAME << '=' << (m_ssc->value == 1 ? "true" : "false") << '|';
        }

        append(sb, SOCKET_SNDBUF_PARAM_NAME, m_socketSndbufLength);
        append(sb, SOCKET_RCVBUF_PARAM_NAME, m_socketRcvbufLength);
        append(sb, RECEIVER_WINDOW_LENGTH_PARAM_NAME, m_receiverWindowLength);
        append(sb, MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME, m_mediaReceiveTimestampOffset);
        append(sb, CHANNEL_RCV_TIMESTAMP_OFFSET_PARAM_NAME, m_channelReceiveTimestampOffset);
        append(sb, CHANNEL_SND_TIMESTAMP_OFFSET_PARAM_NAME, m_channelSendTimestampOffset);
        append(sb, RESPONSE_CORRELATION_ID_PARAM_NAME, m_responseCorrelationId);
        append(sb, NAK_DELAY_PARAM_NAME, m_nakDelay);
        append(sb, UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME, m_untetheredWindowLimitTimeout);
        append(sb, UNTETHERED_RESTING_TIMEOUT_PARAM_NAME, m_untetheredRestingTimeout);
        append(sb, MAX_RESEND_PARAM_NAME, m_maxResend);

        std::string result = sb.str();
        const char lastChar = result.back();

        if (lastChar == '|' || lastChar == '?')
        {
            result.pop_back();
        }

        return result;
    }

private:
    struct Value
    {
        std::int64_t value;

        explicit Value(std::int64_t v)
        {
            value = v;
        }
    };

    std::unique_ptr<std::string> m_prefix;
    std::unique_ptr<std::string> m_media;
    std::unique_ptr<std::string> m_endpoint;
    std::unique_ptr<std::string> m_networkInterface;
    std::unique_ptr<std::string> m_controlEndpoint;
    std::unique_ptr<std::string> m_controlMode;
    std::unique_ptr<std::string> m_tags;
    std::unique_ptr<std::string> m_alias;
    std::unique_ptr<std::string> m_cc;
    std::unique_ptr<std::string> m_fc;
    std::unique_ptr<Value> m_reliable;
    std::unique_ptr<Value> m_ttl;
    std::unique_ptr<Value> m_mtu;
    std::unique_ptr<Value> m_termLength;
    std::unique_ptr<Value> m_initialTermId;
    std::unique_ptr<Value> m_termId;
    std::unique_ptr<Value> m_termOffset;
    std::unique_ptr<Value> m_sessionId;
    std::unique_ptr<Value> m_gtag;
    std::unique_ptr<Value> m_linger;
    std::unique_ptr<Value> m_sparse;
    std::unique_ptr<Value> m_eos;
    std::unique_ptr<Value> m_tether;
    std::unique_ptr<Value> m_group;
    std::unique_ptr<Value> m_rejoin;
    std::unique_ptr<Value> m_ssc;
    std::unique_ptr<Value> m_socketSndbufLength;
    std::unique_ptr<Value> m_socketRcvbufLength;
    std::unique_ptr<Value> m_receiverWindowLength;
    std::unique_ptr<Value> m_responseCorrelationId;
    std::unique_ptr<Value> m_nakDelay;
    std::unique_ptr<Value> m_untetheredWindowLimitTimeout;
    std::unique_ptr<Value> m_untetheredRestingTimeout;
    std::unique_ptr<Value> m_maxResend;
    std::unique_ptr<std::string> m_mediaReceiveTimestampOffset;
    std::unique_ptr<std::string> m_channelReceiveTimestampOffset;
    std::unique_ptr<std::string> m_channelSendTimestampOffset;
    bool m_isSessionIdTagged = false;

    inline static std::string prefixTag(bool isTagged, Value &value)
    {
        return isTagged ? (std::string(TAG_PREFIX) + std::to_string(value.value)) : std::to_string(value.value);
    }

    inline static void append(std::ostringstream &sb, const char *name, std::unique_ptr<Value> &value)
    {
        if (value)
        {
            sb << name << '=' << value->value << '|';
        }
    }

    inline static void append(std::ostringstream &sb, const char *name, std::unique_ptr<std::string> &value)
    {
        if (value)
        {
            sb << name << '=' << *value << '|';
        }
    }
};

}
#endif //AERON_CHANNEL_URI_STRING_BUILDER_H
