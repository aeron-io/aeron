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

#include <functional>

#include <gtest/gtest.h>
#include <gmock/gmock.h>

#include "concurrent/AgentRunner.h"
#include "concurrent/NoOpIdleStrategy.h"
#include "util/Exceptions.h"

using aeron::concurrent::AgentRunner;
using aeron::concurrent::NoOpIdleStrategy;
using aeron::util::AgentTerminationException;
using aeron::util::exception_handler_t;

class AgentRunnerTest : public testing::Test
{
public:
    AgentRunnerTest() = default;
    ~AgentRunnerTest() override = default;
};

class TestAgent
{
    std::atomic<int64_t> m_StartCallCount {0};
    std::atomic<int64_t> m_CloseCallCount {0};
    std::atomic<int64_t> m_DoWorkCallCount {0};
public:
    TestAgent() = default;

    void onStart()
    {
        m_StartCallCount.fetch_add(1);
    }

    void onClose()
    {
        m_CloseCallCount.fetch_add(1);
    }

    int doWork()
    {
        m_DoWorkCallCount.fetch_add(1);
        return 0;
    }

    int64_t startCallCount() const
    {
        return m_StartCallCount.load();
    }

    int64_t closeCallCount() const
    {
        return m_CloseCallCount.load();
    }

    int64_t doWorkCallCount() const
    {
        return m_DoWorkCallCount.load();
    }
};

TEST_F(AgentRunnerTest, shouldCloseAgentIfWasNotStarted)
{
    TestAgent agent;
    EXPECT_EQ(0, agent.startCallCount());
    EXPECT_EQ(0, agent.closeCallCount());
    EXPECT_EQ(0, agent.doWorkCallCount());

    NoOpIdleStrategy idleStrategy;
    exception_handler_t exception_handler;
    AgentRunner<TestAgent, NoOpIdleStrategy> agentRunner(agent, idleStrategy, exception_handler, "test");

    EXPECT_FALSE(agentRunner.isStarted());
    EXPECT_FALSE(agentRunner.isRunning());
    EXPECT_FALSE(agentRunner.isClosed());
    EXPECT_EQ("test", agentRunner.name());
    EXPECT_EQ(0, agent.startCallCount());
    EXPECT_EQ(0, agent.closeCallCount());
    EXPECT_EQ(0, agent.doWorkCallCount());

    agentRunner.close();

    EXPECT_FALSE(agentRunner.isStarted());
    EXPECT_FALSE(agentRunner.isRunning());
    EXPECT_TRUE(agentRunner.isClosed());
    EXPECT_EQ(0, agent.startCallCount());
    EXPECT_EQ(1, agent.closeCallCount());
    EXPECT_EQ(0, agent.doWorkCallCount());
}


class ThrowingAgent : public TestAgent
{
public:
    void onStart()
    {
        TestAgent::onStart();
        throw std::logic_error("ThrowingAgent::onStart()");
    }

    void onClose()
    {
        TestAgent::onClose();
        throw std::invalid_argument("ThrowingAgent::onClose()");
    }

    int doWork()
    {
        TestAgent::doWork();
        throw std::domain_error("ThrowingAgent::doWork()");
    }
};

TEST_F(AgentRunnerTest, shouldHandleErrorWhenClosingAgentExplicitly)
{
    ThrowingAgent agent;
    NoOpIdleStrategy idleStrategy;
    bool called = false;
    exception_handler_t exception_handler = [&](const std::exception &ex)
    {
        called = true;
        EXPECT_EQ(typeid(ex), typeid(std::invalid_argument));
        EXPECT_STREQ("ThrowingAgent::onClose()", ex.what());
    };
    AgentRunner<ThrowingAgent, NoOpIdleStrategy> agentRunner(agent, idleStrategy, exception_handler, "x");

    EXPECT_FALSE(agentRunner.isStarted());
    EXPECT_FALSE(agentRunner.isRunning());
    EXPECT_FALSE(agentRunner.isClosed());
    EXPECT_EQ("x", agentRunner.name());

    agentRunner.close();

    EXPECT_FALSE(agentRunner.isStarted());
    EXPECT_FALSE(agentRunner.isRunning());
    EXPECT_TRUE(agentRunner.isClosed());
    EXPECT_TRUE(called);
    EXPECT_EQ(0, agent.startCallCount());
    EXPECT_EQ(0, agent.doWorkCallCount());
    EXPECT_EQ(1, agent.closeCallCount());
}

TEST_F(AgentRunnerTest, shouldNotInvokeDoWorkIfStartFailsWithException)
{
    ThrowingAgent agent;
    NoOpIdleStrategy idleStrategy;
    int called = 0;
    exception_handler_t exception_handler = [&](const std::exception &ex)
    {
        called++;
        EXPECT_NE(typeid(ex), typeid(std::domain_error));
        EXPECT_STRNE("ThrowingAgent::doWork()", ex.what());
    };
    AgentRunner<ThrowingAgent, NoOpIdleStrategy> agentRunner(agent, idleStrategy, exception_handler, "run");

    agentRunner.run();

    EXPECT_FALSE(agentRunner.isStarted());
    EXPECT_FALSE(agentRunner.isRunning());
    EXPECT_TRUE(agentRunner.isClosed());
    EXPECT_EQ(2, called);
    EXPECT_EQ(1, agent.startCallCount());
    EXPECT_EQ(0, agent.doWorkCallCount());
    EXPECT_EQ(1, agent.closeCallCount());
}

class TerminatingAgent : public TestAgent
{
    bool m_expectedTermination;
public:
    TerminatingAgent(bool expectedTermination) : TestAgent(), m_expectedTermination(expectedTermination)
    {
    }

    int doWork()
    {
        TestAgent::doWork();
        throw AgentTerminationException(m_expectedTermination, m_expectedTermination ? "expected termination" : "crash");
    }
};

TEST_F(AgentRunnerTest, runShouldTerminateSilentlyOnExpectedAgentTerminationException)
{
    TerminatingAgent agent(true);
    NoOpIdleStrategy idleStrategy;
    int called = 0;
    exception_handler_t exception_handler = [&](const std::exception &ex)
    {
        called++;
        EXPECT_EQ(typeid(ex), typeid(AgentTerminationException));
        EXPECT_STREQ("expected termination", ex.what());
    };
    AgentRunner<TerminatingAgent, NoOpIdleStrategy> agentRunner(agent, idleStrategy, exception_handler, "run");

    agentRunner.run();

    EXPECT_FALSE(agentRunner.isStarted());
    EXPECT_FALSE(agentRunner.isRunning());
    EXPECT_TRUE(agentRunner.isClosed());
    EXPECT_EQ(0, called);
    EXPECT_EQ(1, agent.startCallCount());
    EXPECT_EQ(1, agent.closeCallCount());
    EXPECT_EQ(1, agent.doWorkCallCount());
}

TEST_F(AgentRunnerTest, runShouldTerminateOnUnexpectedAgentTerminationException)
{
    TerminatingAgent agent(false);
    NoOpIdleStrategy idleStrategy;
    int called = 0;
    exception_handler_t exception_handler = [&](const std::exception &ex)
    {
        called++;
        EXPECT_EQ(typeid(ex), typeid(AgentTerminationException));
        EXPECT_STREQ("crash", ex.what());
    };
    AgentRunner<TerminatingAgent, NoOpIdleStrategy> agentRunner(agent, idleStrategy, exception_handler, "run");

    agentRunner.run();

    EXPECT_FALSE(agentRunner.isStarted());
    EXPECT_FALSE(agentRunner.isRunning());
    EXPECT_TRUE(agentRunner.isClosed());
    EXPECT_EQ(1, called);
    EXPECT_EQ(1, agent.startCallCount());
    EXPECT_EQ(1, agent.closeCallCount());
    EXPECT_EQ(1, agent.doWorkCallCount());
}

class UnknownErrorAgent : public TestAgent
{
public:
    UnknownErrorAgent() = default;

    int doWork()
    {
        TestAgent::doWork();
        if (doWorkCallCount() <= 10)
        {
            throw std::domain_error("this is a test");
        }
        throw AgentTerminationException(true, "done");
    }
};

TEST_F(AgentRunnerTest, runShouldNotTerminateOnNonAgentTerminationExceptions)
{
    UnknownErrorAgent agent;
    NoOpIdleStrategy idleStrategy;
    int called = 0;
    exception_handler_t exception_handler = [&](const std::exception &ex)
    {
        called++;
        EXPECT_EQ(typeid(ex), typeid(std::domain_error));
        EXPECT_STREQ("this is a test", ex.what());
    };
    AgentRunner<UnknownErrorAgent, NoOpIdleStrategy> agentRunner(agent, idleStrategy, exception_handler, "abc");

    agentRunner.run();

    EXPECT_FALSE(agentRunner.isStarted());
    EXPECT_FALSE(agentRunner.isRunning());
    EXPECT_TRUE(agentRunner.isClosed());
    EXPECT_EQ(10, called);
    EXPECT_EQ(1, agent.startCallCount());
    EXPECT_EQ(1, agent.closeCallCount());
    EXPECT_EQ(11, agent.doWorkCallCount());
}

TEST_F(AgentRunnerTest, startThrowsIfAgentWasAlreadyClosed)
{
    TestAgent agent;
    NoOpIdleStrategy idleStrategy;
    exception_handler_t exception_handler;
    AgentRunner<TestAgent, NoOpIdleStrategy> agentRunner(agent, idleStrategy, exception_handler, "abc");

    agentRunner.close();

    EXPECT_THROW(
    {
        agentRunner.start();
    },
    aeron::util::IllegalStateException );

    EXPECT_FALSE(agentRunner.isStarted());
}

TEST_F(AgentRunnerTest, startThrowsIfAlreadyStarted)
{
    TestAgent agent;
    NoOpIdleStrategy idleStrategy;
    exception_handler_t exception_handler;
    AgentRunner<TestAgent, NoOpIdleStrategy> agentRunner(agent, idleStrategy, exception_handler, "abc");

    agentRunner.start();
    EXPECT_TRUE(agentRunner.isStarted());

    while (agent.doWorkCallCount() <= 1)
    {
        std::this_thread::yield();
    }

    EXPECT_TRUE(agentRunner.isStarted());
    EXPECT_TRUE(agentRunner.isRunning());
    EXPECT_FALSE(agentRunner.isClosed());

    EXPECT_THROW(
    {
        try
        {
            agentRunner.start();
            FAIL();
        }
        catch (const std::exception &ex)
        {
            EXPECT_STREQ("AgentRunner already started", ex.what());
            throw;
        }
    },
    aeron::util::IllegalStateException );

    EXPECT_TRUE(agentRunner.isStarted());
    EXPECT_TRUE(agentRunner.isRunning());
    EXPECT_FALSE(agentRunner.isClosed());

    agentRunner.close();

    EXPECT_TRUE(agentRunner.isStarted());
    EXPECT_FALSE(agentRunner.isRunning());
    EXPECT_TRUE(agentRunner.isClosed());
    EXPECT_EQ(1, agent.startCallCount());
    EXPECT_EQ(1, agent.closeCallCount());
    EXPECT_GT(agent.doWorkCallCount(), 1);
}
