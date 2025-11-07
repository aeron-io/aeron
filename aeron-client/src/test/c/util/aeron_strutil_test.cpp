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

#include <gtest/gtest.h>

extern "C"
{
#include <stdint.h>
#include "aeron_alloc.h"
#include "util/aeron_strutil.h"
}
#undef max

class StrUtilTest : public testing::Test
{
public:
    StrUtilTest() = default;
};

TEST_F(StrUtilTest, shouldHandleSingleValue)
{
    const int max_tokens = 10;
    char *tokens[max_tokens];
    char input[] = "single_token";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, 1);
    EXPECT_STREQ(tokens[0], "single_token");
}

TEST_F(StrUtilTest, shouldHandleMultipleValues)
{
    const int max_tokens = 10;
    char *tokens[max_tokens];
    char input[] = "token_a,token_b,token_c";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, 3);
    EXPECT_STREQ(tokens[0], "token_c");
    EXPECT_STREQ(tokens[1], "token_b");
    EXPECT_STREQ(tokens[2], "token_a");
}

TEST_F(StrUtilTest, shouldHandleMoreThanSpecifiedTokens)
{
    const int max_tokens = 2;
    char *tokens[max_tokens];
    char input[] = "token_a,token_b,token_c";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, -ERANGE);
    EXPECT_STREQ(tokens[0], "token_c");
    EXPECT_STREQ(tokens[1], "token_b");
}

TEST_F(StrUtilTest, shouldFormatNumericStrings)
{
    char buffer[AERON_FORMAT_NUMBER_TO_LOCALE_STR_LEN];

    EXPECT_STREQ("999", aeron_format_number_to_locale(999, buffer, sizeof(buffer)));
    EXPECT_STREQ("-999", aeron_format_number_to_locale(-999, buffer, sizeof(buffer)));

    if (strcmp(localeconv()->thousands_sep, ".") == 0)
    {
        EXPECT_STREQ("999.999", aeron_format_number_to_locale(999999, buffer, sizeof(buffer)));
        EXPECT_STREQ("-999.999", aeron_format_number_to_locale(-999999, buffer, sizeof(buffer)));
        EXPECT_STREQ("999.999.999", aeron_format_number_to_locale(999999999, buffer, sizeof(buffer)));
        EXPECT_STREQ("-999.999.999", aeron_format_number_to_locale(-999999999, buffer, sizeof(buffer)));
        EXPECT_STREQ("10.000.000.000", aeron_format_number_to_locale(10000000000, buffer, sizeof(buffer)));
        EXPECT_STREQ("-999.9", aeron_format_number_to_locale(-999999999, buffer, 7));
    }
    else
    {
        EXPECT_STREQ("999,999", aeron_format_number_to_locale(999999, buffer, sizeof(buffer)));
        EXPECT_STREQ("-999,999", aeron_format_number_to_locale(-999999, buffer, sizeof(buffer)));
        EXPECT_STREQ("999,999,999", aeron_format_number_to_locale(999999999, buffer, sizeof(buffer)));
        EXPECT_STREQ("-999,999,999", aeron_format_number_to_locale(-999999999, buffer, sizeof(buffer)));
        EXPECT_STREQ("10,000,000,000", aeron_format_number_to_locale(10000000000, buffer, sizeof(buffer)));
        EXPECT_STREQ("-999,9", aeron_format_number_to_locale(-999999999, buffer, 7));
    }

    EXPECT_LT(
        strlen(aeron_format_number_to_locale(INT64_MIN, buffer, sizeof(buffer))),
        (size_t)AERON_FORMAT_NUMBER_TO_LOCALE_STR_LEN);
}

TEST_F(StrUtilTest, shouldHandleConsecutiveDelimeters)
{
    const int max_tokens = 10;
    char *tokens[max_tokens];
    char input[] = ",,token_a,,,,token_b,,token_c";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, 3);
    EXPECT_STREQ(tokens[0], "token_c");
    EXPECT_STREQ(tokens[1], "token_b");
    EXPECT_STREQ(tokens[2], "token_a");
}

TEST_F(StrUtilTest, shouldHandleMaxRangeWithConsecutiveDelimeters)
{
    const int max_tokens = 3;
    char *tokens[max_tokens];
    char input[] = ",,token_a,,,,token_b,,token_c";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, 3);
    EXPECT_STREQ(tokens[0], "token_c");
    EXPECT_STREQ(tokens[1], "token_b");
    EXPECT_STREQ(tokens[2], "token_a");
}

TEST_F(StrUtilTest, shouldHandleTrailingTokens)
{
    const int max_tokens = 6;
    char *tokens[max_tokens];
    char input[] = "token_a,token_b,token_c,,,,";

    int num_tokens = aeron_tokenise(input, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, 3);
    EXPECT_STREQ(tokens[0], "token_c");
    EXPECT_STREQ(tokens[1], "token_b");
    EXPECT_STREQ(tokens[2], "token_a");
}

TEST_F(StrUtilTest, shouldHandleNull)
{
    const int max_tokens = 3;
    char *tokens[max_tokens];

    int num_tokens = aeron_tokenise(nullptr, ',', max_tokens, tokens);

    EXPECT_EQ(num_tokens, -EINVAL);
}

TEST_F(StrUtilTest, checkStringLength)
{
    size_t length_initial_value = 1;
    const char str1[] = {'h', 'e', 'l', 'l', 'o', '\0'};
    EXPECT_FALSE(aeron_str_length(str1, 5, nullptr));
    EXPECT_TRUE(aeron_str_length(str1, 6, nullptr));

    size_t length = length_initial_value;
    EXPECT_FALSE(aeron_str_length(str1, 5, &length));
    EXPECT_EQ(length_initial_value, length);

    length = length_initial_value;
    EXPECT_TRUE(aeron_str_length(str1, 6, &length));
    EXPECT_EQ(5U, length);
}

TEST_F(StrUtilTest, checkStringLengthEmptyAndNull)
{
    size_t length_initial_value = 1;
    const char* str1 = "";
    EXPECT_TRUE(aeron_str_length(str1, 5, nullptr));
    EXPECT_TRUE(aeron_str_length(nullptr, 5, nullptr));

    size_t length = length_initial_value;
    EXPECT_TRUE(aeron_str_length(str1, 5, &length));
    EXPECT_EQ(0U, length);

    length = length_initial_value;
    EXPECT_TRUE(aeron_str_length(nullptr, 5, &length));
    EXPECT_EQ(length_initial_value, length);
}

TEST_F(StrUtilTest, shouldCountNumberOfDigitsIntValue)
{
    EXPECT_EQ(1, aeron_digit_count(0));
    EXPECT_EQ(1, aeron_digit_count(1));
    EXPECT_EQ(1, aeron_digit_count(9));

    EXPECT_EQ(2, aeron_digit_count(10));
    EXPECT_EQ(2, aeron_digit_count(99));

    EXPECT_EQ(3, aeron_digit_count(100));
    EXPECT_EQ(3, aeron_digit_count(999));

    EXPECT_EQ(4, aeron_digit_count(1000));
    EXPECT_EQ(4, aeron_digit_count(9999));

    EXPECT_EQ(5, aeron_digit_count(10000));
    EXPECT_EQ(5, aeron_digit_count(99999));

    EXPECT_EQ(6, aeron_digit_count(100000));
    EXPECT_EQ(6, aeron_digit_count(999999));

    EXPECT_EQ(7, aeron_digit_count(1000000));
    EXPECT_EQ(7, aeron_digit_count(9999999));

    EXPECT_EQ(8, aeron_digit_count(10000000));
    EXPECT_EQ(8, aeron_digit_count(99999999));

    EXPECT_EQ(9, aeron_digit_count(100000000));
    EXPECT_EQ(9, aeron_digit_count(999999999));

    EXPECT_EQ(10, aeron_digit_count(1000000000));
    EXPECT_EQ(10, aeron_digit_count(UINT32_MAX));

    EXPECT_EQ(10, aeron_digit_count(UINT32_MAX - 3));
    EXPECT_EQ(3, aeron_digit_count(UINT8_MAX));
    EXPECT_EQ(5, aeron_digit_count(UINT16_MAX));
}
