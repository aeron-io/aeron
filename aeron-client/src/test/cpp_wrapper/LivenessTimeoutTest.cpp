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
#include <string>
#include <typeinfo>
#include <vector>

#include <gtest/gtest.h>

#include "EmbeddedMediaDriver.h"
#include "Aeron.h"
#include "TestUtil.h"

using namespace aeron;

class SystemTest : public testing::Test
{
public:
    SystemTest()
    {
        m_driver.livenessTimeoutNs(m_livenessTimeoutNs);
        m_driver.start();
    }

    ~SystemTest() override
    {
        m_driver.stop();
    }

    static std::int32_t typeId(CountersReader &reader, std::int32_t counterId)
    {
        const index_t offset = aeron::concurrent::CountersReader::metadataOffset(counterId);
        return reader.metaDataBuffer().getInt32(offset + CountersReader::TYPE_ID_OFFSET);
    }

protected:
    std::uint64_t m_livenessTimeoutNs = 250 * 1000 * 1000LL;
    EmbeddedMediaDriver m_driver;
};

TEST_F(SystemTest, shouldReportClosedIfTimedOut)
{
    Context ctx;
    ctx.useConductorAgentInvoker(true);
    ctx.errorHandler(
        [](const std::exception &ignored)
        {
            // Deliberately ignored.
        });

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();
    invoker.start();
    ASSERT_FALSE(aeron->isClosed());
    invoker.invoke();
    // Ideally we wouldn't sleep here, but didn't want to change the public API to inject a clock.
    std::this_thread::sleep_for(std::chrono::nanoseconds(2 * m_livenessTimeoutNs));
    invoker.invoke();
    ASSERT_TRUE(aeron->isClosed());
}

TEST_F(SystemTest, shouldReportConductorServiceTimeoutExceptionIfTimedOut)
{
    std::vector<std::string> errors;
    bool conductorServiceTimeoutReported = false;

    Context ctx;
    ctx.useConductorAgentInvoker(true);
    ctx.errorHandler(
        [&](const std::exception &exception)
        {
            errors.emplace_back(std::string(typeid(exception).name()) + ": " + exception.what());
            if (nullptr != dynamic_cast<const ConductorServiceTimeoutException *>(&exception))
            {
                conductorServiceTimeoutReported = true;
            }
        });

    std::shared_ptr<Aeron> aeron = Aeron::connect(ctx);
    AgentInvoker<ClientConductor>& invoker = aeron->conductorAgentInvoker();
    invoker.start();
    invoker.invoke();
    std::this_thread::sleep_for(std::chrono::nanoseconds(2 * m_livenessTimeoutNs));
    invoker.invoke();
    ASSERT_TRUE(aeron->isClosed());

    std::string reported;
    for (const std::string &error : errors)
    {
        reported += error + "\n";
    }
    EXPECT_TRUE(conductorServiceTimeoutReported) << reported;
}
