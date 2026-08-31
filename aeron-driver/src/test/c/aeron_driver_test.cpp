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


extern "C" {
#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
#include "aeron_driver.h"
#include "aeron_topology.h"

int aeron_driver_validate_unshared_affinity(aeron_driver_context_t* context, FILE *output);
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
        fclose(m_output);
        free(m_output_ptr);
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

//
// TEST_F(DriverTest, shouldHaveNoL3WarningsIfAllUnset)
// {
//     aeron_topology_extra_info_t extraInfo[4] = {
//         {"conductor", AERON_NULL_VALUE}, {"sender", AERON_NULL_VALUE},
//         {"receiver", AERON_NULL_VALUE}, {"native_resource_agent", AERON_NULL_VALUE}
//     };
//     aeron_topology_cpu_group_t cpus[4] = {
//         {AERON_NULL_VALUE, AERON_NULL_VALUE, &extraInfo[0]},
//         {AERON_NULL_VALUE, AERON_NULL_VALUE, &extraInfo[1]},
//         {AERON_NULL_VALUE, AERON_NULL_VALUE, &extraInfo[2]},
//         {AERON_NULL_VALUE, AERON_NULL_VALUE, &extraInfo[3]}
//     };
//     const aeron_topology_cpu_info_t cpuInfo = {
//         cpus, 4, nullptr, nullptr, nullptr, 0
//     };
//
//     EXPECT_EQ(0, aeron_driver_validate_group_locality(&cpuInfo, "L3 cache domains", m_output));
//     fflush(m_output);
//
//     EXPECT_EQ(0, m_output_size);
// }
//
// TEST_F(DriverTest, shouldHaveNoL3WarningsWhenAllShareL3)
// {
//     aeron_topology_extra_info_t extraInfo[4] = {
//         {"conductor", 0}, {"sender", 1}, {"receiver", 2}, {"native_resource_agent", 3}
//     };
//     aeron_topology_cpu_group_t cpus[4] = {
//         {0, 0, &extraInfo[0]},
//         {1, 0, &extraInfo[1]},
//         {2, 0, &extraInfo[2]},
//         {3, 0, &extraInfo[3]}
//     };
//     const aeron_topology_cpu_info_t cpuInfo = {
//         cpus, 4, nullptr, nullptr, nullptr, 1
//     };
//
//     EXPECT_EQ(0, aeron_driver_validate_group_locality(&cpuInfo, "L3 cache domains", m_output));
//     fflush(m_output);
//
//     EXPECT_EQ(0, m_output_size);
// }
//
// TEST_F(DriverTest, shouldHaveOneL3WarningWhenTwoHaveNonL3SharedAffinities)
// {
//     aeron_topology_extra_info_t extraInfo[4] = {
//         {"conductor", 1}, {"sender", 2}, {"receiver", AERON_NULL_VALUE}, {"native_resource_agent", AERON_NULL_VALUE}
//     };
//     aeron_topology_cpu_group_t cpus[4] = {
//         {0, 0, &extraInfo[0]},
//         {4, 1, &extraInfo[1]},
//         {AERON_NULL_VALUE, AERON_NULL_VALUE, &extraInfo[2]},
//         {AERON_NULL_VALUE, AERON_NULL_VALUE, &extraInfo[3]}
//     };
//     aeron_topology_cpu_info_t cpuInfo = {
//         cpus, 4, nullptr, nullptr, nullptr, 2
//     };
//
//     EXPECT_EQ(1, aeron_driver_validate_group_locality(&cpuInfo, "L3 cache domains", m_output));
//     fflush(m_output);
//
//     EXPECT_NE(0, m_output_size);
//     EXPECT_STRNE(nullptr, strstr(m_output_ptr, "group 0: conductor (cpu=0 [configured=1])"));
//     EXPECT_STRNE(nullptr, strstr(m_output_ptr, "group 1: sender (cpu=4 [configured=2])"));
// }
//
// TEST_F(DriverTest, shouldHaveOneL3WarningWhenOneIsIsolated)
// {
//     aeron_topology_extra_info_t extraInfo[4] = {
//         {"conductor", 0}, {"sender", 4}, {"receiver", 5}, {"native_resource_agent", 6}
//     };
//     aeron_topology_cpu_group_t cpus[4] = {
//         {0, 0, &extraInfo[0]},
//         {4, 1, &extraInfo[1]},
//         {5, 1, &extraInfo[2]},
//         {6, 1, &extraInfo[3]}
//     };
//     aeron_topology_cpu_info_t cpuInfo = {
//         cpus, 4, nullptr, nullptr, nullptr, 2
//     };
//
//     EXPECT_EQ(1, aeron_driver_validate_group_locality(&cpuInfo, "L3 cache domains", m_output));
//     fflush(m_output);
//
//     EXPECT_NE(0, m_output_size);
//     EXPECT_STRNE(nullptr, strstr(m_output_ptr, "span 2 L3 cache domains"));
//     EXPECT_STRNE(nullptr, strstr(m_output_ptr, "group 0: conductor (cpu=0 [configured=0])"));
//     EXPECT_STRNE(nullptr, strstr(
//             m_output_ptr,
//             "group 1: sender (cpu=4 [configured=4]) receiver (cpu=5 [configured=5]) "
//             "native_resource_agent (cpu=6 [configured=6])"));
// }
//
// TEST_F(DriverTest, shouldHaveOneDieWarningWhenOneIsIsolated)
// {
//     aeron_topology_extra_info_t extraInfo[4] = {
//         {"conductor", 0}, {"sender", 4}, {"receiver", 5}, {"native_resource_agent", 6}
//     };
//     aeron_topology_cpu_group_t cpus[4] = {
//         {0, 0, &extraInfo[0]},
//         {4, 0, &extraInfo[1]},
//         {5, 65535, &extraInfo[2]},
//         {6, 65535, &extraInfo[3]}
//     };
//     int group_ids[2] = {65535, 0};
//     aeron_topology_cpu_info_t cpuInfo = {
//         cpus, 4, nullptr, nullptr, group_ids, 2
//     };
//
//     EXPECT_EQ(1, aeron_driver_validate_group_locality(&cpuInfo, "dies", m_output));
//     fflush(m_output);
//
//     EXPECT_NE(0, m_output_size);
//     EXPECT_STRNE(nullptr, strstr(m_output_ptr, "span 2 dies"));
//     EXPECT_STRNE(nullptr, strstr(m_output_ptr, "0: conductor (cpu=0 [configured=0]) sender (cpu=4 [configured=4])"));
//     EXPECT_STRNE(nullptr, strstr(
//             m_output_ptr, "65535: receiver (cpu=5 [configured=5]) native_resource_agent (cpu=6 [configured=6])"));
// }
