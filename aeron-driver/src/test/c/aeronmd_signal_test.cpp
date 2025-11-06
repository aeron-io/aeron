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

#include "aeronc.h"

extern "C"
{
int argc;
char **argv;
}

#include <gtest/gtest.h>
#include <random>
#include <sys/syscall.h>
#include <unistd.h>
#include <sys/wait.h>

class AeronmdSignalTest : public testing::Test
{
};

TEST_F(AeronmdSignalTest, shouldSupportSigTerm)
{
#if !defined(__linux__)
    GTEST_SKIP();
#endif

    if (argc < 2)
    {
        FAIL() << "aeronmd path is not set";
    }

    FILE *aeronmdOutput = popen(argv[1], "r");
    ASSERT_NE(nullptr, aeronmdOutput);

    aeron_context_t *context;
    ASSERT_EQ(0, aeron_context_init(&context));

    aeron_t *aeron;
    ASSERT_EQ(0, aeron_init(&aeron, context));

    aeron_close(aeron);
    aeron_context_close(context);

    FILE *psgrepOutput = popen("/usr/bin/pgrep -xn aeronmd", "r");

    char buf[1024] = {};
    if (nullptr == fgets(buf, sizeof(buf) - 1, psgrepOutput))
    {
        FAIL() << "pgrep does not exist";
    }

    int pid = atoi(buf);
    ASSERT_LT(0, pid) << "aeronmd is not running";

    kill(pid, SIGTERM);

    waitpid(pid, nullptr, WSTOPPED);

    std::stringstream ss;
    while (nullptr != fgets(buf, sizeof(buf) - 1, aeronmdOutput))
    {
        ss << std::string(buf) << std::endl;
    }

    ASSERT_NE(std::string::npos, ss.str().find("Shutting down driver"));

    pclose(aeronmdOutput);
}

GTEST_API_ int main(int t_argc, char **t_argv){

    argc = t_argc;
    argv = t_argv;

    printf("Initializing gtest \n");
    ::testing::InitGoogleTest(&argc, argv);

    return RUN_ALL_TESTS();
}