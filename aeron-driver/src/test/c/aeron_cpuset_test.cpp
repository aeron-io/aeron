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
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <string>


extern "C"
{
#include "aeron_alloc.h"
#include "aeron_cpuset.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"
}


#define AERON_CPUSET_TEST_TEMPLATE "cpuset_XXXXXX"

using namespace testing;

class CpusetTest : public Test
{
public:
    CpusetTest() : m_tempDir(nullptr)
    {
    }

protected:
    void SetUp() override
    {
        m_tempDir = mkdtemp(m_tempDirArray);
        std::cout << "m_tempDir: " << m_tempDir << " " << m_tempDirArray << std::endl;
    }

    void TearDown() override
    {
        EXPECT_EQ(0, aeron_delete_directory(m_tempDir)) << aeron_errmsg();
    }

    static std::vector<int> parseCpulist(const char* text)
    {
        int *cpus = nullptr;
        int cpu_count = 0;

        EXPECT_EQ(0, aeron_cpuset_parse_cpulist(text, &cpus, &cpu_count)) << aeron_errmsg();
        std::vector<int> actual{cpus, cpus + cpu_count};

        aeron_free(cpus);

        return actual;
    }

    static int parseCpulistError(const char* text)
    {
        int *cpus = nullptr;
        int cpu_count = 0;
        int result = aeron_cpuset_parse_cpulist(text, &cpus, &cpu_count);
        EXPECT_EQ(nullptr, cpus);

        return result;
    }

    char m_tempDirArray[19] = { "/tmp/cpuset_XXXXXX" };
    char *m_tempDir;
};

TEST_F(CpusetTest, shouldReadV2Cgroups)
{
    std::string mountRoot = std::string(m_tempDir) + "/cgroup";
    std::string cgroupRoot = mountRoot + "/user.slice/user-1000.slice";
    ASSERT_EQ(0, aeron_mkdir_recursive(cgroupRoot.c_str(), 0700));

    std::string cgroupFilename = std::string(m_tempDir) + "/proc-cgroup";
    std::string effectiveCpusFilename = cgroupRoot + "/cpuset.cpus.effective";

    std::ofstream cgroupFile(cgroupFilename.c_str(), std::ios::out);
    cgroupFile << "0::/user.slice/user-1000.slice" << std::endl;
    cgroupFile.close();

    std::ofstream effectiveCgroupFile(effectiveCpusFilename.c_str(), std::ios::out);
    effectiveCgroupFile << "5-10" << std::endl;
    effectiveCgroupFile.close();

    int *cpus = nullptr;
    int cpu_count = 0;

    aeron_cpuset_cgroup_read_v2(cgroupFilename.c_str(), mountRoot.c_str(), &cpus, &cpu_count);
    ASSERT_NE(nullptr, cpus) << aeron_errmsg();

    std::vector<int> actual{cpus, cpus + cpu_count};
    EXPECT_THAT(actual, ElementsAre(5, 6, 7, 8, 9, 10));

    aeron_free(cpus);
}

TEST_F(CpusetTest, shouldReadV2CgroupsInParent)
{
    std::string mountRoot = std::string(m_tempDir) + "/cgroup";
    std::string cgroupRoot = mountRoot + "/user.slice/user-1000.slice";
    ASSERT_EQ(0, aeron_mkdir_recursive(cgroupRoot.c_str(), 0700));

    std::string cgroupFilename = std::string(m_tempDir) + "/proc-cgroup";
    std::string effectiveCpusFilename = mountRoot + "/user.slice/cpuset.cpus.effective";

    std::ofstream cgroupFile(cgroupFilename.c_str(), std::ios::out);
    cgroupFile << "0::/user.slice/user-1000.slice" << std::endl;
    cgroupFile.close();

    std::ofstream effectiveCgroupFile(effectiveCpusFilename.c_str(), std::ios::out);
    effectiveCgroupFile << "5-10" << std::endl;
    effectiveCgroupFile.close();

    int *cpus = nullptr;
    int cpu_count = 0;

    aeron_cpuset_cgroup_read_v2(cgroupFilename.c_str(), mountRoot.c_str(), &cpus, &cpu_count);
    ASSERT_NE(nullptr, cpus) << aeron_errmsg();

    std::vector<int> actual{cpus, cpus + cpu_count};
    EXPECT_THAT(actual, ElementsAre(5, 6, 7, 8, 9, 10));

    aeron_free(cpus);
}

TEST_F(CpusetTest, shouldErrorIfNotFoundReadV2Cgroups)
{
    std::string mountRoot = std::string(m_tempDir) + "/cgroup";
    std::string cgroupRoot = mountRoot + "/user.slice/user-1000.slice";
    ASSERT_EQ(0, aeron_mkdir_recursive(cgroupRoot.c_str(), 0700));

    std::string cgroupFilename = std::string(m_tempDir) + "/proc-cgroup";
    // std::string effectiveCpusFilename = cgroupRoot + "/cpuset.cpus.effective";

    std::ofstream cgroupFile(cgroupFilename.c_str(), std::ios::out);
    cgroupFile << "0::/user.slice/user-1000.slice" << std::endl;
    cgroupFile.close();

    // std::ofstream effectiveCgroupFile(effectiveCpusFilename.c_str(), std::ios::out);
    // effectiveCgroupFile << "5-10" << std::endl;
    // effectiveCgroupFile.close();

    int *cpus = nullptr;
    int cpu_count = 0;

    EXPECT_EQ(-1, aeron_cpuset_cgroup_read_v2(cgroupFilename.c_str(), mountRoot.c_str(), &cpus, &cpu_count));
    EXPECT_EQ(nullptr, cpus);
}


TEST_F(CpusetTest, shouldParseCpulist)
{
    EXPECT_THAT(parseCpulist("0"), ElementsAre(0)) << aeron_errmsg();
    EXPECT_THAT(parseCpulist("7"), ElementsAre(7)) << aeron_errmsg();
    EXPECT_THAT(parseCpulist("3,1,2"), ElementsAre(1, 2, 3)) << aeron_errmsg();
    EXPECT_THAT(parseCpulist("0,0,1"), ElementsAre(0, 1)) << aeron_errmsg();
    EXPECT_THAT(parseCpulist("0-3"), ElementsAre(0, 1, 2, 3)) << aeron_errmsg();
    EXPECT_THAT(parseCpulist("4"), ElementsAre(4)) << aeron_errmsg();
    EXPECT_THAT(parseCpulist("0,2,4-6"), ElementsAre(0, 2, 4, 5, 6)) << aeron_errmsg();
    EXPECT_THAT(parseCpulist("0-2,6"), ElementsAre(0, 1, 2, 6)) << aeron_errmsg();
    EXPECT_EQ(2048, parseCpulist("0-2047").size()) << aeron_errmsg();
}

TEST_F(CpusetTest, shouldNotParseCpulist)
{
    EXPECT_EQ(-1, parseCpulistError(""));
    EXPECT_THAT(aeron_errmsg(), ContainsRegex("empty string"));

    EXPECT_EQ(-1, parseCpulistError("0,"));
    EXPECT_THAT(aeron_errmsg(), ContainsRegex("trailing comma"));

    EXPECT_EQ(-1, parseCpulistError(",0"));
    EXPECT_THAT(aeron_errmsg(), ContainsRegex("leading comma"));

    EXPECT_EQ(-1, parseCpulistError("-1"));
    EXPECT_THAT(aeron_errmsg(), ContainsRegex("negative CPU"));

    EXPECT_EQ(-1, parseCpulistError("0,-1"));
    EXPECT_THAT(aeron_errmsg(), ContainsRegex("negative CPU"));

    EXPECT_EQ(-1, parseCpulistError("a"));
    EXPECT_THAT(aeron_errmsg(), ContainsRegex("non-numeric CPU"));

    EXPECT_EQ(-1, parseCpulistError("0-a"));
    EXPECT_THAT(aeron_errmsg(), ContainsRegex("non-numeric CPU"));

    EXPECT_EQ(-1, parseCpulistError("4-2"));
    EXPECT_THAT(aeron_errmsg(), ContainsRegex("range end less than start"));
}

TEST_F(CpusetTest, shouldReadCore)
{
    int *cpus = nullptr;
    int cpu_count = 0;

    aeron_cpuset_cgroup_read_v2(AERON_CPUSET_PROC_SELF_CGROUP, AERON_CPUSET_CGROUP_MOUNT_V2, &cpus, &cpu_count);

    EXPECT_NE(0, cpu_count) << aeron_errmsg();

    for (int i = 0; i < cpu_count; i++)
    {
        printf("%d ", cpus[i]);
    }
    printf("\n");
}
