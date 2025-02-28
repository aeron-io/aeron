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
#ifndef AERON_AGENT_RUNNER_H
#define AERON_AGENT_RUNNER_H

#include <string>
#include <functional>
#include <thread>
#include <atomic>

#include "util/Exceptions.h"
#include "util/ScopeUtils.h"
#include "concurrent/logbuffer/TermReader.h"

#if !defined(AERON_COMPILER_MSVC)
#include <pthread.h>
#endif

namespace aeron {

namespace concurrent {

template <typename Agent, typename IdleStrategy>
class AgentRunner
{
public:
    AgentRunner(Agent &agent, IdleStrategy &idleStrategy, const util::exception_handler_t &exceptionHandler) :
        m_agent(agent),
        m_idleStrategy(idleStrategy),
        m_exceptionHandler(exceptionHandler),
        m_name("aeron-agent")
    {
    }

    AgentRunner(
        Agent &agent,
        IdleStrategy &idleStrategy,
        const util::exception_handler_t &exceptionHandler,
        const std::string &name) :
        m_agent(agent),
        m_idleStrategy(idleStrategy),
        m_exceptionHandler(exceptionHandler),
        m_name(name)
    {
    }

    /**
     * Name given to the thread running the agent.
     *
     * @return the name given to the thread running the agent.
     */
    inline const std::string &name() const
    {
        return m_name;
    }

    /**
     * Is the Agent started?
     *
     * @return is the Agent started?
     */
    inline bool isStarted() const
    {
        return m_isStarted.load(std::memory_order_acquire);
    }

    /**
     * Is the Agent running?
     *
     * @return is the Agent started successfully and not closed?
     */
    inline bool isRunning() const
    {
        return m_isRunning.load(std::memory_order_acquire);
    }

    /**
     * Has the Agent been closed?
     *
     * @return has the Agent been closed?
     */
    inline bool isClosed() const
    {
        return m_isClosed.load(std::memory_order_acquire);
    }

    /**
     * Start the Agent running. Start may be called only once and is invalid after close has been called.
     *
     * Will spawn a std::thread.
     */
    inline void start()
    {
        if (m_isClosed.load(std::memory_order_acquire))
        {
            throw util::IllegalStateException(std::string("AgentRunner closed"), SOURCEINFO);
        }

        bool expected = false;
        if (!m_isStarted.compare_exchange_strong(expected, true, std::memory_order_seq_cst))
        {
            throw util::IllegalStateException(std::string("AgentRunner already started"), SOURCEINFO);
        }

        m_thread = std::thread(
            [&]()
            {
#if defined(AERON_COMPILER_MSVC)
#elif defined(Darwin)
                pthread_setname_np(m_name.c_str());
#else
                char threadName[16UL] = {};
                std::copy(m_name.begin(), m_name.begin() + std::min(m_name.size(), 15UL), threadName);
                pthread_setname_np(pthread_self(), threadName);
#endif
                run();
            });
    }

    /**
     * Run the Agent duty cycle until closed.
     */
    inline void run()
    {
        m_isRunning.store(true, std::memory_order_release);
        bool isRunning = true;

        util::OnScopeExit tidy(
            [&]()
            {
                m_isRunning.store(false, std::memory_order_release);
            });

        try
        {
            m_agent.onStart();
        }
        catch (const util::SourcedException &exception)
        {
            isRunning = false;
            m_exceptionHandler(exception);
        }

        if (isRunning)
        {
            while (!m_isClosed.load(std::memory_order_acquire))
            {
                try
                {
                    m_idleStrategy.idle(m_agent.doWork());
                }
                catch (const util::SourcedException &exception)
                {
                    m_exceptionHandler(exception);
                }
            }
        }

        try
        {
            m_agent.onClose();
        }
        catch (const util::SourcedException &exception)
        {
            m_exceptionHandler(exception);
        }
    }

    /**
     * Close the agent and stop the associated thread from running. This method waits for the thread to join.
     */
    inline void close()
    {
        bool expected = false;
        if (m_isClosed.compare_exchange_strong(expected, true, std::memory_order_seq_cst))
        {
            if (m_thread.joinable())
            {
                m_thread.join();
            }
        }
    }

private:
    Agent &m_agent;
    IdleStrategy &m_idleStrategy;
    const util::exception_handler_t &m_exceptionHandler;
    std::atomic<bool> m_isStarted = { false };
    std::atomic<bool> m_isRunning = { false };
    std::atomic<bool> m_isClosed = { false };
    std::thread m_thread;
    const std::string m_name;
};

}}

#endif
