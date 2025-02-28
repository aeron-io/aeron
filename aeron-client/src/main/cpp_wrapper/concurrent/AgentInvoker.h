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
#ifndef AERON_AGENT_INVOKER_H
#define AERON_AGENT_INVOKER_H

#include <functional>
#include <thread>
#include <atomic>

#include "util/Exceptions.h"
#include "concurrent/logbuffer/TermReader.h"

namespace aeron {

namespace concurrent {

template<typename Agent>
class AgentInvoker
{
public:
    AgentInvoker(Agent &agent, const util::exception_handler_t &exceptionHandler) :
        m_agent(agent),
        m_exceptionHandler(exceptionHandler)
    {
    }

    /**
     * Has the Agent been started?
     *
     * @return has the Agent been started?
     */
    inline bool isStarted() const
    {
        return m_isStarted;
    }

    /**
     * Is the Agent running?
     *
     * @return is the Agent been started successfully and not closed?
     */
    inline bool isRunning() const
    {
        return m_isRunning;
    }

    /**
     * Has the Agent been closed?
     *
     * @return has the Agent been closed?
     */
    inline bool isClosed() const
    {
        return m_isClosed;
    }

    /**
     * Mark the invoker as started and call the Agent::onStart() method.
     * <p>
     * Startup logic will only be performed once.
     */
    inline void start()
    {
        try
        {
            if (!m_isStarted)
            {
                m_isStarted = true;
                m_agent.onStart();
                m_isRunning = true;
            }
        }
        catch (const util::SourcedException &exception)
        {
            m_exceptionHandler(exception);
            close();
        }
    }

    /**
     * Invoke the Agent::doWork() method and return the work count.
     *
     * If not successfully started or after closed then this method will return without invoking the {@link Agent}.
     *
     * @return the work count for the Agent::doWork() method.
     */
    inline int invoke()
    {
        int workCount = 0;

        if (m_isRunning)
        {
            try
            {
                workCount = m_agent.doWork();
            }
            catch (const util::SourcedException &exception)
            {
                m_exceptionHandler(exception);
            }
        }

        return workCount;
    }

    /**
     * Mark the invoker as closed and call the Agent::onClose() logic for clean up.
     *
     * The clean up logic will only be performed once.
     */
    inline void close()
    {
        try
        {
            if (!m_isClosed)
            {
                m_isRunning = false;
                m_isClosed = true;
                m_agent.onClose();
            }
        }
        catch (const util::SourcedException &exception)
        {
            m_exceptionHandler(exception);
        }
    }

private:
    Agent &m_agent;
    const util::exception_handler_t &m_exceptionHandler;
    bool m_isStarted = false;
    bool m_isRunning = false;
    bool m_isClosed = false;
};

}}

#endif
