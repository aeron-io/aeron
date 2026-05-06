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
        m_output = open_memstream(&m_output_ptr, &m_output_size);
    }

    void TearDown() override
    {
        EXPECT_EQ(0, aeron_delete_directory(m_tempDir)) << aeron_errmsg();
        fclose(m_output);
        free(m_output_ptr);
    }

    static void setupCgroupCpuset(
        std::string &procSelfCgroupFilename,
        std::string &cgroupMountRoot,
        const char *cpuset)
    {
        std::string cgroupRoot = cgroupMountRoot + "/user.slice/user-1000.slice";
        ASSERT_EQ(0, aeron_mkdir_recursive(cgroupRoot.c_str(), 0700));

        std::string effectiveCpusFilename = cgroupMountRoot + "/user.slice/cpuset.cpus.effective";

        std::ofstream cgroupFile(procSelfCgroupFilename.c_str(), std::ios::out);
        cgroupFile << "0::/user.slice/user-1000.slice" << std::endl;
        cgroupFile.close();

        std::ofstream effectiveCgroupFile(effectiveCpusFilename.c_str(), std::ios::out);
        effectiveCgroupFile << cpuset << std::endl;
        effectiveCgroupFile.close();
    }

    static void setupSiblings(std::string &sysfsRoot, std::vector<std::pair<int, int>> &siblings)
    {
        for (int i = 0; i < static_cast<int>(siblings.size()); i++)
        {
            std::string cpuDirectory = sysfsRoot + "/cpu" + std::to_string(i) + "/topology";
            std::string threadSiblingsList = cpuDirectory + "/thread_siblings_list";
            aeron_mkdir_recursive(cpuDirectory.c_str(), 0700);
            std::ofstream os(threadSiblingsList.c_str(), std::ios::out);
            os << siblings[i].first << '-' << siblings[i].second << std::endl;
            os.close();
        }
    }

    static void setupL3Shared(std::string &sysfsRoot, std::vector<std::pair<int, int>> &l3Peers)
    {
        for (int i = 0; i < static_cast<int>(l3Peers.size()); i++)
        {
            std::string cpuDirectory = sysfsRoot + "/cpu" + std::to_string(i) + "/cache/index3";
            std::string threadSiblingsList = cpuDirectory + "/shared_cpu_list";
            aeron_mkdir_recursive(cpuDirectory.c_str(), 0700);
            std::ofstream os(threadSiblingsList.c_str(), std::ios::out);
            os << l3Peers[i].first << '-' << l3Peers[i].second << std::endl;
            os.close();
        }
    }

    static void setupClusterShared(std::string &sysfsRoot, std::vector<int> &clusters)
    {
        for (int i = 0; i < static_cast<int>(clusters.size()); i++)
        {
            std::string cpuDirectory = sysfsRoot + "/cpu" + std::to_string(i) + "/topology";
            std::string threadSiblingsList = cpuDirectory + "/cluster_id";
            aeron_mkdir_recursive(cpuDirectory.c_str(), 0700);
            std::ofstream os(threadSiblingsList.c_str(), std::ios::out);
            os << clusters[i] << std::endl;
            os.close();
        }
    }

    char m_tempDirArray[19] = { "/tmp/cpuset_XXXXXX" };
    char *m_tempDir;
    FILE *m_output;
    char *m_output_ptr;
    size_t m_output_size;
};

TEST_F(TopologyTest, shouldCheckAlignment)
{
#ifndef __linux__
    GTEST_SKIP() << "CGroups only supported on Linux";
#endif

    ASSERT_NE(nullptr, m_output);

    std::string mountRoot = std::string(m_tempDir) + "/cgroup";
    std::string cgroupFilename = std::string(m_tempDir) + "/proc-cgroup";
    std::string sysfsRoot = std::string(m_tempDir) + "/sysfs";
    setupCgroupCpuset(cgroupFilename, mountRoot, "5-10");
    std::vector<std::pair<int, int>> a = {
        {0, 1}, {0, 1}, {2, 3}, {2, 3}, {4, 5}, {4, 5},
        {6, 7}, {6, 7}, {8, 9}, {8, 9}, {10, 11}, {10, 11},
        {12, 13}, {12, 13}, {14, 15}, {14, 15}
    };
    setupSiblings(sysfsRoot, a);

    int *cpus = nullptr;
    int cpu_count = 8;

    aeron_cpuset_cgroup_read_v2(cgroupFilename.c_str(), mountRoot.c_str(), &cpus, &cpu_count);

    const int warnings = aeron_topology_check_alignment(sysfsRoot.c_str(), cpus, cpu_count, m_output);
    ASSERT_NE(-1, warnings) << aeron_errmsg();
    EXPECT_EQ(2, warnings);
    fflush(m_output);

    ASSERT_NE(nullptr, m_output_ptr);
    EXPECT_NE(nullptr, strstr(m_output_ptr, "cpuset is missing sibling CPU(s) 4"));
    EXPECT_NE(nullptr, strstr(m_output_ptr, "cpuset is missing sibling CPU(s) 11"));

    aeron_free(cpus);
}

TEST_F(TopologyTest, shouldCheckL3Locality)
{
#ifndef __linux__
    GTEST_SKIP() << "CGroups only supported on Linux";
#endif

    ASSERT_NE(nullptr, m_output);

    std::string mountRoot = std::string(m_tempDir) + "/cgroup";
    std::string cgroupFilename = std::string(m_tempDir) + "/proc-cgroup";
    std::string sysfsRoot = std::string(m_tempDir) + "/sysfs";
    setupCgroupCpuset(cgroupFilename, mountRoot, "5-10");
    std::vector<std::pair<int, int>> a = {
        {0, 7}, {0, 7}, {0, 7}, {0, 7}, {0, 7}, {0, 7}, {0, 7}, {0, 7},
        {8, 15}, {8, 15}, {8, 15}, {8, 15}, {8, 15}, {8, 15}, {8, 15}, {8, 15}
    };
    setupL3Shared(sysfsRoot, a);

    int *cpus = nullptr;
    int cpu_count = 8;

    aeron_cpuset_cgroup_read_v2(cgroupFilename.c_str(), mountRoot.c_str(), &cpus, &cpu_count);

    const int warnings = aeron_topology_check_l3_locality(sysfsRoot.c_str(), cpus, cpu_count, m_output);
    ASSERT_NE(-1, warnings) << aeron_errmsg();
    EXPECT_EQ(1, warnings);
    fflush(m_output);

    ASSERT_NE(nullptr, m_output_ptr);
    EXPECT_NE(nullptr, strstr(m_output_ptr, "cpuset spans multiple L3 cache domains"));

    aeron_free(cpus);
}

TEST_F(TopologyTest, shouldCheckClusterLocality)
{
#ifndef __linux__
    GTEST_SKIP() << "CGroups only supported on Linux";
#endif

    ASSERT_NE(nullptr, m_output);

    std::string mountRoot = std::string(m_tempDir) + "/cgroup";
    std::string cgroupFilename = std::string(m_tempDir) + "/proc-cgroup";
    std::string sysfsRoot = std::string(m_tempDir) + "/sysfs";
    setupCgroupCpuset(cgroupFilename, mountRoot, "5-10");
    std::vector<int> a = {
        65535, 65535, 65535, 65535, 65535, 65535, 65535, 65535,
        0, 0, 0, 0, 0, 0, 0, 0
    };
    setupClusterShared(sysfsRoot, a);

    int *cpus = nullptr;
    int cpu_count = 8;

    aeron_cpuset_cgroup_read_v2(cgroupFilename.c_str(), mountRoot.c_str(), &cpus, &cpu_count);

    const int warnings = aeron_topology_check_cluster_locality(sysfsRoot.c_str(), cpus, cpu_count, m_output);
    ASSERT_NE(-1, warnings) << aeron_errmsg();
    EXPECT_EQ(1, warnings);
    fflush(m_output);

    ASSERT_NE(nullptr, m_output_ptr);
    EXPECT_NE(nullptr, strstr(m_output_ptr, "cpuset spans 2 CPU clusters"));

    aeron_free(cpus);
}
