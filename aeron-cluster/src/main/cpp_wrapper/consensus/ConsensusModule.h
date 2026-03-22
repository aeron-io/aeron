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

#ifndef AERON_CLUSTER_CONSENSUS_MODULE_H
#define AERON_CLUSTER_CONSENSUS_MODULE_H

#include <atomic>
#include <thread>

#include "ConsensusModuleContext.h"
#include "consensus/aeron_consensus_module_agent.h"
#include "client/util/ClusterException.h"

namespace aeron { namespace cluster { namespace consensus
{

/**
 * C++ wrapper around aeron_consensus_module_agent_t.
 * Launches the consensus module in a background thread or supports invoker mode.
 */
class ConsensusModule
{
public:
    static std::unique_ptr<ConsensusModule> launch(ConsensusModuleContext &ctx)
    {
        return std::unique_ptr<ConsensusModule>(new ConsensusModule(ctx));
    }

    ~ConsensusModule()
    {
        stop();
        if (nullptr != m_agent) { aeron_consensus_module_agent_close(m_agent); }
    }

    ConsensusModule(const ConsensusModule &) = delete;
    ConsensusModule &operator=(const ConsensusModule &) = delete;

    void stop()
    {
        m_running = false;
        if (m_thread.joinable()) { m_thread.join(); }
    }

    /** Single work cycle for invoker mode. */
    int doWork()
    {
        return aeron_consensus_module_agent_do_work(m_agent, aeron_nano_clock());
    }

    bool isRunning() const { return m_running; }

private:
    explicit ConsensusModule(ConsensusModuleContext &ctx) :
        m_agent(nullptr), m_running(false)
    {
        aeron_cm_context_t *c = ctx.ctx();

        if (aeron_cm_context_conclude(c) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        if (aeron_consensus_module_agent_create(&m_agent, c) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }
        if (aeron_consensus_module_agent_on_start(m_agent) < 0)
        {
            CLUSTER_MAP_ERRNO_TO_SOURCED_EXCEPTION_AND_THROW;
        }

        m_running = true;
        m_thread = std::thread([this, c]() {
            while (m_running)
            {
                int work = aeron_consensus_module_agent_do_work(m_agent, aeron_nano_clock());
                if (0 == work)
                {
                    c->idle_strategy_func(c->idle_strategy_state, 0);
                }
            }
        });
    }

    aeron_consensus_module_agent_t *m_agent;
    std::atomic<bool>               m_running;
    std::thread                     m_thread;
};

}}}

#endif /* AERON_CLUSTER_CONSENSUS_MODULE_H */
