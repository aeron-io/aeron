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
#include <cstdio>
#include <iostream>
#include <fstream>
#include <string>


extern "C"
{
#include "aeron_alloc.h"
#include "aeron_cpuset.h"
#include "aeron_topology.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
}

#define AERON_CPUSET_TEST_TEMPLATE "cpuset_XXXXXX"

using namespace testing;

class TopologyTest : public Test
{
public:
    TopologyTest() : m_tempDir(nullptr), m_output(nullptr), m_output_ptr(nullptr), m_output_size(0)
    {
    }

protected:
    void SetUp() override
    {
        m_tempDir = mkdtemp(m_tempDirArray);
        std::cout << "m_tempDir: " << m_tempDir << " " << m_tempDirArray << std::endl;
        m_output = open_memstream(&m_output_ptr, &m_output_size);
    }

    void TearDown() override
    {
        EXPECT_EQ(0, aeron_delete_directory(m_tempDir)) << aeron_errmsg();
        fclose(m_output);
        free(m_output_ptr);
    }

    char m_tempDirArray[19] = { "/tmp/cpuset_XXXXXX" };
    char *m_tempDir;
    FILE *m_output;
    char *m_output_ptr;
    size_t m_output_size;
};

TEST_F(TopologyTest, shouldReadV2Cgroups)
{
    int *cpus = nullptr;
    int cpu_count = 8;

    aeron_cpuset_cgroup_read_v2(AERON_CPUSET_PROC_SELF_CGROUP, AERON_CPUSET_CGROUP_MOUNT_V2, &cpus, &cpu_count);

    aeron_topology_core_t *topology = nullptr;
    int core_count = 0;

    ASSERT_NE(-1, aeron_topology_read(cpus, cpu_count, &topology, &core_count)) << aeron_errmsg();

    aeron_topology_check_alignment(cpus, cpu_count, m_output);

    printf("%s ", "cpus");
    for (int i = 0; i < cpu_count; i++)
    {
        printf("%d ", cpus[i]);
    }
    printf("\n");

    printf("%s ", "cores");
    for (int i = 0; i < core_count; i++)
    {
        const int *core_cpus = topology[i].cpus;
        const int core_cpu_count = topology[i].cpu_count;

        for (int j = 0; j < core_cpu_count; j++)
        {
            printf("%d ", core_cpus[j]);
        }
        printf("\n");
    }
    printf("\n");

    aeron_topology_check_l3_locality(cpus, cpu_count, m_output);
    aeron_topology_check_cluster_locality(cpus, cpu_count, m_output);

    fflush(m_output);
    printf("%s", m_output_ptr);
}
