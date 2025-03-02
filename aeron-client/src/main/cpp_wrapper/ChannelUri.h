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
#ifndef AERON_CHANNEL_URI_H
#define AERON_CHANNEL_URI_H

#include <memory>
#include <string>
#include <unordered_map>

#include "util/StringUtil.h"

namespace aeron
{

static constexpr const char SPY_QUALIFIER[] = "aeron-spy";
static constexpr const char AERON_SCHEME[] = "aeron";
static constexpr const char AERON_PREFIX[] = "aeron:";

static constexpr const char IPC_MEDIA[] = "ipc";
static constexpr const char UDP_MEDIA[] = "udp";
static constexpr const char IPC_CHANNEL[] = "aeron:ipc";
static constexpr const char SPY_PREFIX[] = "aeron-spy:";
static constexpr const char ENDPOINT_PARAM_NAME[] = "endpoint";
static constexpr const char INTERFACE_PARAM_NAME[] = "interface";
static constexpr const char INITIAL_TERM_ID_PARAM_NAME[] = "init-term-id";
static constexpr const char TERM_ID_PARAM_NAME[] = "term-id";
static constexpr const char TERM_OFFSET_PARAM_NAME[] = "term-offset";
static constexpr const char TERM_LENGTH_PARAM_NAME[] = "term-length";
static constexpr const char MTU_LENGTH_PARAM_NAME[] = "mtu";
static constexpr const char TTL_PARAM_NAME[] = "ttl";
static constexpr const char MDC_CONTROL_PARAM_NAME[] = "control";
static constexpr const char MDC_CONTROL_MODE_PARAM_NAME[] = "control-mode";
static constexpr const char MDC_CONTROL_MODE_MANUAL[] = "manual";
static constexpr const char MDC_CONTROL_MODE_DYNAMIC[] = "dynamic";
static constexpr const char CONTROL_MODE_RESPONSE[] = "response";
static constexpr const char SESSION_ID_PARAM_NAME[] = "session-id";
static constexpr const char LINGER_PARAM_NAME[] = "linger";
static constexpr const char RELIABLE_STREAM_PARAM_NAME[] = "reliable";
static constexpr const char TAGS_PARAM_NAME[] = "tags";
static constexpr const char TAG_PREFIX[] = "tag:";
static constexpr const char SPARSE_PARAM_NAME[] = "sparse";
static constexpr const char ALIAS_PARAM_NAME[] = "alias";
static constexpr const char EOS_PARAM_NAME[] = "eos";
static constexpr const char TETHER_PARAM_NAME[] = "tether";
static constexpr const char GROUP_PARAM_NAME[] = "group";
static constexpr const char REJOIN_PARAM_NAME[] = "rejoin";
static constexpr const char CONGESTION_CONTROL_PARAM_NAME[] = "cc";
static constexpr const char FLOW_CONTROL_PARAM_NAME[] = "fc";
static constexpr const char GROUP_TAG_PARAM_NAME[] = "gtag";
static constexpr const char SPIES_SIMULATE_CONNECTION_PARAM_NAME[] = "ssc";
static constexpr const char SOCKET_SNDBUF_PARAM_NAME[] = "so-sndbuf";
static constexpr const char SOCKET_RCVBUF_PARAM_NAME[] = "so-rcvbuf";
static constexpr const char RECEIVER_WINDOW_LENGTH_PARAM_NAME[] = "rcv-wnd";
static constexpr const char MEDIA_RCV_TIMESTAMP_OFFSET_PARAM_NAME[] = "media-rcv-ts-offset";
static constexpr const char CHANNEL_RCV_TIMESTAMP_OFFSET_PARAM_NAME[] = "channel-rcv-ts-offset";
static constexpr const char CHANNEL_SND_TIMESTAMP_OFFSET_PARAM_NAME[] = "channel-snd-ts-offset";
static constexpr const char RESPONSE_CORRELATION_ID_PARAM_NAME[] = "response-correlation-id";
static constexpr const char NAK_DELAY_PARAM_NAME[] = "nak-delay";
static constexpr const char UNTETHERED_WINDOW_LIMIT_TIMEOUT_PARAM_NAME[] = "untethered-window-limit-timeout";
static constexpr const char UNTETHERED_RESTING_TIMEOUT_PARAM_NAME[] = "untethered-resting-timeout";
static constexpr const char MAX_RESEND_PARAM_NAME[] = "max-resend";

using namespace aeron::util;

class ChannelUri
{
public:
    using this_t = ChannelUri;

    enum State : int
    {
        MEDIA,
        PARAMS_KEY,
        PARAMS_VALUE
    };

    ChannelUri(
        const std::string &prefix,
        const std::string &media,
        std::unique_ptr<std::unordered_map<std::string, std::string>> params) :
        m_prefix(prefix),
        m_media(media),
        m_params(std::move(params))
    {
    }

    inline std::string prefix()
    {
        return m_prefix;
    }

    inline this_t &prefix(const std::string &prefix)
    {
        m_prefix = prefix;
        return *this;
    }

    inline std::string media()
    {
        return m_media;
    }

    inline this_t &media(const std::string &media)
    {
        if (media != IPC_MEDIA && media != UDP_MEDIA)
        {
            throw IllegalArgumentException("unknown media: " + media, SOURCEINFO);
        }
        m_media = media;
        return *this;
    }

    inline std::string scheme()
    {
        return AERON_SCHEME;
    }

    inline std::string get(const std::string &key)
    {
        auto it = m_params->find(key);

        if (it != m_params->end())
        {
            return it->second;
        }

        return {};
    }

    inline std::string get(const std::string &key, const std::string &defaultValue)
    {
        auto it = m_params->find(key);

        if (it != m_params->end())
        {
            return it->second;
        }

        return defaultValue;
    }

    inline void put(const std::string &key, const std::string &value)
    {
        (*m_params)[key] = value;
    }

    inline std::string remove(const std::string &key)
    {
        std::string result;
        auto it = m_params->find(key);

        if (it != m_params->end())
        {
            result = it->second;
            m_params->erase(it);
        }

        return result;
    }

    inline bool containsKey(const std::string &key)
    {
        return m_params->find(key) != m_params->end();
    }
    
    inline bool hasControlModeResponse()
    {
        const std::string &controlMode = get(MDC_CONTROL_MODE_PARAM_NAME);
        return !controlMode.empty() && CONTROL_MODE_RESPONSE == controlMode;
    }

    std::string toString()
    {
        std::string sb;
        if (m_prefix.length() == 0)
        {
            sb.reserve((m_params->size() * 20) + 10);
        }
        else
        {
            sb.reserve((m_params->size() * 20) + 20);
            sb += m_prefix;
            if (':' != m_prefix.back())
            {
                sb += ':';
            }
        }

        sb += AERON_PREFIX;
        sb += m_media;

        if (!m_params->empty())
        {
            sb += '?';

            for (const auto &i : *m_params)
            {
                sb += i.first;
                sb += '=';
                sb += i.second;
                sb += '|';
            }

            sb.pop_back();
        }

        return sb;
    }

    static std::shared_ptr<ChannelUri> parse(const std::string &uri)
    {
        std::size_t position = 0;
        std::string prefix;
        if (startsWith(uri, 0, SPY_PREFIX))
        {
            prefix = SPY_QUALIFIER;
            position = sizeof(SPY_PREFIX) - 1;
        }
        else
        {
            prefix = "";
        }

        if (!startsWith(uri, position, AERON_PREFIX))
        {
            throw IllegalArgumentException("Aeron URIs must start with 'aeron:', found: " + uri, SOURCEINFO);
        }
        else
        {
            position += sizeof(AERON_PREFIX) - 1;
        }

        std::string builder;
        std::unique_ptr<std::unordered_map<std::string, std::string>> params(
            new std::unordered_map<std::string, std::string>());
        std::string media;
        std::string key;
        State state = State::MEDIA;

        for (std::size_t i = position; i < uri.length(); i++)
        {
            char c = uri[i];

            switch (state)
            {
                case State::MEDIA:
                    switch (c)
                    {
                        case '?':
                            media = builder;
                            builder.clear();
                            state = State::PARAMS_KEY;
                            break;

                        case ':':
                        case '|':
                        case '=':
                            throw IllegalStateException(
                                "encountered '" + std::to_string(c) + "' within media definition at index " +
                                    std::to_string(i) + " in " + uri, SOURCEINFO);

                        default:
                            builder += c;
                    }
                    break;

                case PARAMS_KEY:
                    if ('=' == c)
                    {
                        if (0 == builder.length())
                        {
                            throw IllegalStateException(
                                "empty key not allowed at index " + std::to_string(i) + " in " + uri, SOURCEINFO);

                        }
                        key = builder;
                        builder.clear();
                        state = State::PARAMS_VALUE;
                    }
                    else
                    {
                        if (c == '|')
                        {
                            throw IllegalStateException(
                                "invalid end of key at index " + std::to_string(i) + " in " + uri, SOURCEINFO);
                        }

                        builder += c;
                    }
                    break;

                case PARAMS_VALUE:
                    if ('|' == c)
                    {
                        params->emplace(key, builder);
                        builder.clear();
                        state = State::PARAMS_KEY;
                    }
                    else
                    {
                        builder += c;
                    }
                    break;
            }
        }

        switch (state)
        {
            case State::MEDIA:
                media = builder;
                if (media != IPC_MEDIA && media != UDP_MEDIA)
                {
                    throw IllegalArgumentException("unknown media: " + media, SOURCEINFO);
                }
                break;

            case PARAMS_VALUE:
                params->emplace(key, builder);
                break;

            default:
                throw IllegalArgumentException("no more input found, state=" + std::to_string(state), SOURCEINFO);
        }

        return std::make_shared<ChannelUri>(prefix, media, std::move(params));
    }

    inline static std::string addSessionId(const std::string &channel, std::int32_t sessionId)
    {
        std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(channel);

        channelUri->put(SESSION_ID_PARAM_NAME, std::to_string(sessionId));
        return channelUri->toString();
    }

    inline static std::string addAliasIfAbsent(const std::string& uri, const std::string& alias)
    {
        if (!alias.empty())
        {
            std::shared_ptr<ChannelUri> channelUri = ChannelUri::parse(uri);
            if (!channelUri->containsKey(ALIAS_PARAM_NAME))
            {
                channelUri->put(ALIAS_PARAM_NAME, alias);
                return channelUri->toString();
            }
        }
        return uri;
    }

private:
    std::string m_prefix;
    std::string m_media;
    std::unique_ptr<std::unordered_map<std::string, std::string>> m_params;
};

}
#endif //AERON_CHANNEL_URI_H
