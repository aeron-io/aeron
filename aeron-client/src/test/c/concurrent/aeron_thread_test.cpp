/*
 * Copyright 2023 Adaptive Financial Consulting Limited.
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
#include "concurrent/aeron_thread.h"
}

TEST(AeronThreadTest, shouldCreateReentrantMutex)
{
    aeron_mutex_t mutex;
    ASSERT_EQ(0, aeron_mutex_init(&mutex, nullptr));

    EXPECT_EQ(0, aeron_mutex_lock(&mutex));
    EXPECT_EQ(0, aeron_mutex_lock(&mutex));
    EXPECT_EQ(0, aeron_mutex_unlock(&mutex));
    EXPECT_EQ(0, aeron_mutex_unlock(&mutex));

    EXPECT_EQ(0, aeron_mutex_destroy(&mutex));
}
