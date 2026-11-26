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

extern "C"
{
#include "aeron_common.h"
#include "aeronc.h"
#include "aeron_version_gitsha.h"
}

class VersionTest : public testing::Test
{
};

TEST_F(VersionTest, shouldReturnFullVersion)
{
    auto full_version = std::string(aeron_version_full());
    auto expected = std::string("aeron version=")
        .append(AERON_VERSION_TXT)
        .append(" commit=")
        .append(AERON_VERSION_GITSHA);

    ASSERT_EQ(expected, full_version);
}

TEST_F(VersionTest, shouldReturnMajorVersion)
{
    EXPECT_EQ(AERON_VERSION_MAJOR, aeron_version_major());
}

TEST_F(VersionTest, shouldReturnMinorVersion)
{
    EXPECT_EQ(AERON_VERSION_MINOR, aeron_version_minor());
}

TEST_F(VersionTest, shouldReturnPatchVersion)
{
    EXPECT_EQ(AERON_VERSION_PATCH, aeron_version_patch());
}

TEST_F(VersionTest, shouldReturnGitSha)
{
    EXPECT_EQ(std::string(AERON_VERSION_GITSHA), std::string(aeron_version_gitsha()));
}

TEST_F(VersionTest, shouldCompileSemanticVersion)
{
    uint8_t major = 42;
    uint8_t minor = 3;
    uint8_t patch = 5;
    EXPECT_EQ(0x2A0305, aeron_semantic_version_compose(major, minor, patch));
}

TEST_F(VersionTest, shouldExtractMajorVersion)
{
    int32_t version = 0xAE0012;
    EXPECT_EQ(0xAE, aeron_semantic_version_major(version));
}

TEST_F(VersionTest, shouldExtractMinorVersion)
{
    int32_t version = 0xFF10AA;
    EXPECT_EQ(0x10, aeron_semantic_version_minor(version));
}

TEST_F(VersionTest, shouldExtractPatchVersion)
{
    int32_t version = 0xBBAA03;
    EXPECT_EQ(0x03, aeron_semantic_version_patch(version));
}
