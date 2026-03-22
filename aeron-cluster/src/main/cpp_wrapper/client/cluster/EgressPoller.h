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

#ifndef AERON_CLUSTER_EGRESS_POLLER_H
#define AERON_CLUSTER_EGRESS_POLLER_H

#include "client/aeron_cluster_egress_poller.h"
#include "Subscription.h"

namespace aeron { namespace cluster { namespace client
{

/**
 * Thin C++ wrapper around aeron_cluster_egress_poller_t.
 * Holds the poller by value (no heap allocation beyond what the C layer does internally).
 */
class EgressPoller
{
public:
    explicit EgressPoller(
        std::shared_ptr<Subscription> subscription,
        int fragmentLimit = AERON_CLUSTER_EGRESS_POLLER_FRAGMENT_LIMIT_DEFAULT) :
        m_subscription(std::move(subscription)),
        m_poller(nullptr)
    {
        if (aeron_cluster_egress_poller_create(
            &m_poller,
            m_subscription->subscription(),
            fragmentLimit) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
    }

    ~EgressPoller()
    {
        aeron_cluster_egress_poller_close(m_poller);
    }

    EgressPoller(const EgressPoller &) = delete;
    EgressPoller &operator=(const EgressPoller &) = delete;

    /**
     * Poll for egress messages.
     * @return the number of fragments consumed.
     */
    int poll()
    {
        int fragments = aeron_cluster_egress_poller_poll(m_poller);
        if (fragments < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        return fragments;
    }

    inline bool isPollComplete() const  { return m_poller->is_poll_complete; }
    inline bool wasChallenged()  const  { return m_poller->was_challenged;   }
    inline std::int32_t templateId()       const { return m_poller->template_id;       }
    inline std::int64_t clusterSessionId() const { return m_poller->cluster_session_id; }
    inline std::int64_t leadershipTermId() const { return m_poller->leadership_term_id; }
    inline std::int64_t correlationId()    const { return m_poller->correlation_id;     }
    inline std::int32_t leaderMemberId()   const { return m_poller->leader_member_id;   }
    inline std::int32_t eventCode()        const { return m_poller->event_code;         }

    inline std::string detail() const
    {
        if (m_poller->detail != nullptr && m_poller->detail_length > 0)
        {
            return {m_poller->detail, m_poller->detail_length};
        }
        return {};
    }

    aeron_cluster_egress_poller_t *poller() { return m_poller; }

private:
    std::shared_ptr<Subscription>      m_subscription;
    aeron_cluster_egress_poller_t     *m_poller;
};

}}}

#endif /* AERON_CLUSTER_EGRESS_POLLER_H */
