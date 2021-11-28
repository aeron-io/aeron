/*
 * Copyright 2014-2021 Real Logic Limited.
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
#include "util/aeron_symbol_table.h"
}
#undef max

class SymbolTableTest : public testing::Test
{
public:
    SymbolTableTest() = default;
};

void foo_function()
{

}

void bar_function()
{

}

const char *foo_object = "hello world";
const char *bar_object = "hello fairyland";

aeron_symbol_table_func_t test_function_table[]
    {
        { "foo", "foo_function", foo_function },
        { "bar", "bar_function", bar_function },
        { NULL, NULL, NULL }
    };

aeron_symbol_table_obj_t test_obj_table[]
    {
        { "foo", "foo_object", (void *)foo_object },
        { "bar", "bar_object", (void *)bar_object },
        { NULL, NULL, NULL }
    };

TEST_F(SymbolTableTest, shouldFindObjects)
{
    EXPECT_EQ(foo_object, aeron_symbol_table_obj_load(test_obj_table, "foo", "object table"));
    EXPECT_EQ(foo_object, aeron_symbol_table_obj_load(test_obj_table, "foo_object", "object table"));
    EXPECT_EQ(bar_object, aeron_symbol_table_obj_load(test_obj_table, "bar", "object table"));
    EXPECT_EQ(bar_object, aeron_symbol_table_obj_load(test_obj_table, "bar_object", "object table"));
    EXPECT_EQ(nullptr, aeron_symbol_table_obj_load(test_obj_table, "baz", "object table"));
}

TEST_F(SymbolTableTest, shouldFindFunctionPointers)
{
    EXPECT_EQ(foo_function, aeron_symbol_table_func_load(test_function_table, "foo", "function table"));
    EXPECT_EQ(foo_function, aeron_symbol_table_func_load(test_function_table, "foo_function", "function table"));
    EXPECT_EQ(bar_function, aeron_symbol_table_func_load(test_function_table, "bar", "function table"));
    EXPECT_EQ(bar_function, aeron_symbol_table_func_load(test_function_table, "bar_function", "function table"));
    EXPECT_EQ(nullptr, aeron_symbol_table_func_load(test_function_table, "baz", "function table"));
}
