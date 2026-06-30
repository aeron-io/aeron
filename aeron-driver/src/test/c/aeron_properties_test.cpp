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

extern "C"
{
#include "util/aeron_properties_util.h"
#include "util/aeron_error.h"
#include "util/aeron_http_util.h"
#include "aeronmd.h"
}

class DriverConfigurationTest : public testing::Test
{
public:
    DriverConfigurationTest()
    {
        aeron_properties_parse_init(&m_state);
    }

    static int propertyHandler(void *clientd, const char *name, const char *value)
    {
        auto test = reinterpret_cast<DriverConfigurationTest *>(clientd);

        test->m_name = std::string(name);
        test->m_value = std::string(value);
        return 1;
    }

    int parseLine(const char *line)
    {
        std::string lineStr(line);
        m_name = "";
        m_value = "";

        return aeron_properties_parse_line(&m_state, lineStr.c_str(), lineStr.length(), propertyHandler, this);
    }


protected:
    aeron_properties_parser_state_t m_state = {};
    std::string m_name;
    std::string m_value;
};

TEST_F(DriverConfigurationTest, shouldNotParseMalformedPropertyLine)
{
    EXPECT_EQ(parseLine(" airon"), -1);
    EXPECT_EQ(parseLine("="), -1);
    EXPECT_EQ(parseLine("=val"), -1);
}

TEST_F(DriverConfigurationTest, shouldNotParseTooLongALine)
{
    EXPECT_EQ(aeron_properties_parse_line(&m_state, "line", sizeof(m_state.property_str), propertyHandler, this), -1);
}

TEST_F(DriverConfigurationTest, shouldIgnoreComments)
{
    EXPECT_EQ(parseLine(" #"), 0);
    EXPECT_EQ(parseLine("# comment"), 0);
    EXPECT_EQ(parseLine("! bang"), 0);
    EXPECT_EQ(parseLine("        ! bang"), 0);
}

TEST_F(DriverConfigurationTest, shouldIgnoreBlankLines)
{
    EXPECT_EQ(parseLine(""), 0);
    EXPECT_EQ(parseLine(" "), 0);
}

TEST_F(DriverConfigurationTest, shouldParseSimpleLine)
{
    EXPECT_EQ(parseLine("propertyName=propertyValue"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");

    EXPECT_EQ(parseLine("propertyName:propertyValue"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");
}

TEST_F(DriverConfigurationTest, shouldParseSimpleLineWithNameWhiteSpace)
{
    EXPECT_EQ(parseLine("   propertyName=propertyValue"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");

    EXPECT_EQ(parseLine("propertyName :propertyValue"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");

    EXPECT_EQ(parseLine("\tpropertyName  =propertyValue"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");
}

TEST_F(DriverConfigurationTest, shouldParseSimpleLineWithLeadingValueWhiteSpace)
{
    EXPECT_EQ(parseLine("propertyName=  propertyValue"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");

    EXPECT_EQ(parseLine("propertyName:\tpropertyValue"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");
}

TEST_F(DriverConfigurationTest, shouldParseSimpleLineWithNoValue)
{
    EXPECT_EQ(parseLine("propertyName="), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "");

    EXPECT_EQ(parseLine("   propertyName="), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "");

    EXPECT_EQ(parseLine("propertyName :"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "");
}

TEST_F(DriverConfigurationTest, shouldParseSimpleContinuation)
{
    EXPECT_EQ(parseLine("propertyName=\\"), 0);
    EXPECT_EQ(parseLine("propertyValue"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");
}

TEST_F(DriverConfigurationTest, shouldParseSimpleContinuationWithWhitespace)
{
    EXPECT_EQ(parseLine("propertyName= property\\"), 0);
    EXPECT_EQ(parseLine("   Value"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");
}

TEST_F(DriverConfigurationTest, shouldParseContinuationWithComment)
{
    EXPECT_EQ(parseLine("propertyName= property\\"), 0);
    EXPECT_EQ(parseLine("#"), 0);
    EXPECT_EQ(parseLine("   Value"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");
}

TEST_F(DriverConfigurationTest, shouldParseContinuationWithBlankLine)
{
    EXPECT_EQ(parseLine("propertyName= property\\"), 0);
    EXPECT_EQ(parseLine("\\"), 0);
    EXPECT_EQ(parseLine("   Value"), 1);
    EXPECT_EQ(m_name, "propertyName");
    EXPECT_EQ(m_value, "propertyValue");
}

TEST_F(DriverConfigurationTest, shouldIgnoreMissingPropertiesFile)
{
    EXPECT_EQ(aeron_properties_file_load("/tmp/aeron-nonexistent-properties-file.properties"), 0);
}

TEST_F(DriverConfigurationTest, DISABLED_shouldHttpRetrieve)
{
    aeron_http_response_t *response = nullptr;
    int result = aeron_http_retrieve(&response, "http://localhost:8000/aeron-throughput.properties", -1L);

    if (-1 == result)
    {
        std::cout << aeron_errmsg() << std::endl;
    }
    else
    {
        std::cout << std::to_string(response->status_code) << std::endl;
        std::cout << std::to_string(response->content_length) << std::endl;
        std::cout << std::to_string(response->length) << std::endl;
        std::cout << std::to_string(response->headers_offset) << std::endl;
        std::cout << std::to_string(response->body_offset) << std::endl;
        std::cout << (response->buffer + response->body_offset) << std::endl;
    }
}

TEST_F(DriverConfigurationTest, DISABLED_shouldHttpRetrieveProperties)
{
    int result = aeron_properties_http_load("http://localhost:8000/aeron-throughput.properties");

    if (-1 == result)
    {
        std::cout << aeron_errmsg() << std::endl;
    }

    std::cout << getenv("AERON_MTU_LENGTH") << std::endl;
}
