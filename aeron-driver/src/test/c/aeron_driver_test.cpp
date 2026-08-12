/*
* Copyright 2026 Adaptive Financial Consulting Limited.
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

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include <fstream>
#include <string>

extern "C"
{
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
#include "aeron_driver.h"

int aeron_driver_validate_unshared_affinity(aeron_driver_context_t* context, FILE *output);
int aeron_driver_validate_l3_locality(
    aeron_driver_context_t *context, const int **l3_peers, const int *l3_peer_counts, FILE *output);
}

using namespace testing;

class DriverTest : public Test
{
public:
    DriverTest() : m_output(nullptr), m_output_ptr(nullptr), m_output_size(0)
    {
    }

protected:
    void SetUp() override
    {
        m_output = open_memstream(&m_output_ptr, &m_output_size);
    }

    void TearDown() override
    {
    }

    FILE *m_output;
    char *m_output_ptr;
    size_t m_output_size;
};

TEST_F(DriverTest, shouldHaveNoWarningsIfAllUnset)
{
    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = AERON_NULL_VALUE;
    context.sender_cpu_affinity_no = AERON_NULL_VALUE;
    context.receiver_cpu_affinity_no = AERON_NULL_VALUE;
    context.native_resource_agent_cpu_affinity_no = AERON_NULL_VALUE;

    EXPECT_EQ(0, aeron_driver_validate_unshared_affinity(&context, m_output));
    fflush(m_output);

    EXPECT_EQ(0, m_output_size);
}

TEST_F(DriverTest, shouldHaveNoWarningsIfAllDifferent)
{
    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = 0;
    context.sender_cpu_affinity_no = 1;
    context.receiver_cpu_affinity_no = 2;
    context.native_resource_agent_cpu_affinity_no = 3;

    EXPECT_EQ(0, aeron_driver_validate_unshared_affinity(&context, m_output));
    fflush(m_output);

    EXPECT_EQ(0, m_output_size);
}

TEST_F(DriverTest, shouldHaveOneWarningsIfTwoShareACpu)
{
    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = 0;
    context.sender_cpu_affinity_no = 1;
    context.receiver_cpu_affinity_no = 1;
    context.native_resource_agent_cpu_affinity_no = 3;

    EXPECT_EQ(1, aeron_driver_validate_unshared_affinity(&context, m_output));
    fflush(m_output);

    EXPECT_NE(0, m_output_size);
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "sender and receiver"));
}

TEST_F(DriverTest, shouldHaveOneWarningsIfSenderAndReciverShareACpu)
{
    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = 0;
    context.sender_cpu_affinity_no = 1;
    context.receiver_cpu_affinity_no = 1;
    context.native_resource_agent_cpu_affinity_no = 3;

    EXPECT_EQ(1, aeron_driver_validate_unshared_affinity(&context, m_output));
    fflush(m_output);

    EXPECT_NE(0, m_output_size);
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "sender and receiver"));
}

TEST_F(DriverTest, shouldHaveOneWarningsIfSenderAndReciverShareACpuWithOthersNull)
{
    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = -1;
    context.sender_cpu_affinity_no = 1;
    context.receiver_cpu_affinity_no = 1;
    context.native_resource_agent_cpu_affinity_no = -1;

    EXPECT_EQ(1, aeron_driver_validate_unshared_affinity(&context, m_output));
    fflush(m_output);

    EXPECT_NE(0, m_output_size);
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "sender and receiver"));
}

TEST_F(DriverTest, shouldHaveThreeWarningsIfThreeShareACpu)
{
    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = -1;
    context.sender_cpu_affinity_no = 1;
    context.receiver_cpu_affinity_no = 1;
    context.native_resource_agent_cpu_affinity_no = 1;

    EXPECT_EQ(3, aeron_driver_validate_unshared_affinity(&context, m_output));
    fflush(m_output);

    EXPECT_NE(0, m_output_size);
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "sender and receiver"));
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "sender and native_resource_agent"));
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "receiver and native_resource_agent"));
}

TEST_F(DriverTest, shouldHaveTwoWarningsIfTwoPairShareCpus)
{
    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = 1;
    context.sender_cpu_affinity_no = 1;
    context.receiver_cpu_affinity_no = 2;
    context.native_resource_agent_cpu_affinity_no = 2;

    EXPECT_EQ(2, aeron_driver_validate_unshared_affinity(&context, m_output));
    fflush(m_output);

    EXPECT_NE(0, m_output_size);
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "conductor and sender"));
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "receiver and native_resource_agent"));
}

TEST_F(DriverTest, shouldHaveNoL3WarningsIfAllUnset)
{
    const int groupA[] = {0, 1, 2, 3};
    const int groupB[] = {4, 5, 6, 7};
    const int *l3Peers[8] = {groupA, groupA, groupA, groupA, groupB, groupB, groupB, groupB};
    const int l3PeerCounts[8] = {4, 4, 4, 4, 4, 4, 4, 4};

    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = AERON_NULL_VALUE;
    context.conductor_cpu_affinity_resolved = AERON_NULL_VALUE;
    context.sender_cpu_affinity_no = AERON_NULL_VALUE;
    context.sender_cpu_affinity_resolved = AERON_NULL_VALUE;
    context.receiver_cpu_affinity_no = AERON_NULL_VALUE;
    context.receiver_cpu_affinity_resolved = AERON_NULL_VALUE;
    context.native_resource_agent_cpu_affinity_no = AERON_NULL_VALUE;
    context.native_resource_agent_cpu_affinity_resolved = AERON_NULL_VALUE;

    EXPECT_EQ(0, aeron_driver_validate_l3_locality(&context, l3Peers, l3PeerCounts, m_output));
    fflush(m_output);

    EXPECT_EQ(0, m_output_size);
}

TEST_F(DriverTest, shouldHaveNoL3WarningsWhenAllShareL3)
{
    const int groupA[] = {0, 1, 2, 3};
    const int groupB[] = {4, 5, 6, 7};
    const int *l3Peers[8] = {groupA, groupA, groupA, groupA, groupB, groupB, groupB, groupB};
    const int l3PeerCounts[8] = {4, 4, 4, 4, 4, 4, 4, 4};

    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = 0;
    context.conductor_cpu_affinity_resolved = 0;
    context.sender_cpu_affinity_no = 1;
    context.sender_cpu_affinity_resolved = 1;
    context.receiver_cpu_affinity_no = 2;
    context.receiver_cpu_affinity_resolved = 2;
    context.native_resource_agent_cpu_affinity_no = 3;
    context.native_resource_agent_cpu_affinity_resolved = 3;

    EXPECT_EQ(0, aeron_driver_validate_l3_locality(&context, l3Peers, l3PeerCounts, m_output));
    fflush(m_output);

    EXPECT_EQ(0, m_output_size);
}

TEST_F(DriverTest, shouldHaveOneL3WarningWhenTwoHaveNonL3SharedAffinities)
{
    const int groupA[] = {0, 1, 2, 3};
    const int groupB[] = {4, 5, 6, 7};
    const int *l3Peers[8] = {groupA, groupA, groupA, groupA, groupB, groupB, groupB, groupB};
    const int l3PeerCounts[8] = {4, 4, 4, 4, 4, 4, 4, 4};

    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = 1;
    context.conductor_cpu_affinity_resolved = 0;
    context.sender_cpu_affinity_no = 2;
    context.sender_cpu_affinity_resolved = 4;
    context.receiver_cpu_affinity_no = AERON_NULL_VALUE;
    context.receiver_cpu_affinity_resolved = AERON_NULL_VALUE;
    context.native_resource_agent_cpu_affinity_no = AERON_NULL_VALUE;
    context.native_resource_agent_cpu_affinity_resolved = AERON_NULL_VALUE;

    EXPECT_EQ(1, aeron_driver_validate_l3_locality(&context, l3Peers, l3PeerCounts, m_output));
    fflush(m_output);

    EXPECT_NE(0, m_output_size);
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "conductor and sender"));
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "configured=1"));
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "configured=2"));
}

TEST_F(DriverTest, shouldHaveThreeL3WarningsWhenOneIsIsolated)
{
    const int groupA[] = {0, 1, 2, 3};
    const int groupB[] = {4, 5, 6, 7};
    const int *l3Peers[8] = {groupA, groupA, groupA, groupA, groupB, groupB, groupB, groupB};
    const int l3PeerCounts[8] = {4, 4, 4, 4, 4, 4, 4, 4};

    aeron_driver_context_t context;
    context.conductor_cpu_affinity_no = 0;
    context.conductor_cpu_affinity_resolved = 0;
    context.sender_cpu_affinity_no = 4;
    context.sender_cpu_affinity_resolved = 4;
    context.receiver_cpu_affinity_no = 5;
    context.receiver_cpu_affinity_resolved = 5;
    context.native_resource_agent_cpu_affinity_no = 6;
    context.native_resource_agent_cpu_affinity_resolved = 6;

    EXPECT_EQ(3, aeron_driver_validate_l3_locality(&context, l3Peers, l3PeerCounts, m_output));
    fflush(m_output);

    EXPECT_NE(0, m_output_size);
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "conductor and sender"));
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "conductor and receiver"));
    EXPECT_STRNE(nullptr, strstr(m_output_ptr, "conductor and native_resource_agent"));
    EXPECT_EQ(nullptr, strstr(m_output_ptr, "sender and receiver"));
    EXPECT_EQ(nullptr, strstr(m_output_ptr, "sender and native_resource_agent"));
    EXPECT_EQ(nullptr, strstr(m_output_ptr, "receiver and native_resource_agent"));
}
