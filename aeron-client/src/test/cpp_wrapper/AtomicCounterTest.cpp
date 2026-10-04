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

#include <atomic>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "concurrent/AtomicBuffer.h"
#include "concurrent/AtomicCounter.h"

using namespace aeron::concurrent;

static const int NUM_THREADS = 2;
static const int NUM_ITERATIONS = 10 * 1000;

// Uses compareAndSet(0, 1) as a lock acquire. The value goes back to the expected value on release, so a
// compareAndSet that reports success without having written is detected as the lock not being held.
template<typename TryLock, typename IsLocked, typename Unlock>
static int countFalseCompareAndSetSuccesses(TryLock tryLock, IsLocked isLocked, Unlock unlock)
{
    std::atomic<int> countDown(NUM_THREADS);
    std::atomic<int> falseSuccesses(0);
    std::vector<std::thread> threads;

    for (int t = 0; t < NUM_THREADS; t++)
    {
        threads.emplace_back(
            [&]()
            {
                countDown--;
                while (countDown > 0)
                {
                    std::this_thread::yield();
                }

                for (int i = 0; i < NUM_ITERATIONS; i++)
                {
                    while (!tryLock())
                    {
                    }

                    if (!isLocked())
                    {
                        falseSuccesses++;
                    }

                    unlock();
                }
            });
    }

    for (std::thread &thread : threads)
    {
        thread.join();
    }

    return falseSuccesses;
}

TEST(AtomicCounterTest, shouldOnlySucceedCompareAndSetWhenValueWasWritten)
{
    alignas(64) std::int64_t value = 0;
    AtomicCounter counter(&value, 0, 0);

    const int falseSuccesses = countFalseCompareAndSetSuccesses(
        [&]() { return counter.compareAndSet(0, 1); },
        [&]() { return 1 == counter.get(); },
        [&]() { counter.set(0); });

    EXPECT_EQ(0, falseSuccesses);
}

TEST(AtomicCounterTest, shouldOnlySucceedCompareAndSetInt32WhenValueWasWritten)
{
    alignas(64) std::uint8_t bytes[64] = {};
    AtomicBuffer buffer(bytes, sizeof(bytes));

    const int falseSuccesses = countFalseCompareAndSetSuccesses(
        [&]() { return buffer.compareAndSetInt32(0, 0, 1); },
        [&]() { return 1 == buffer.getInt32Volatile(0); },
        [&]() { buffer.putInt32Atomic(0, 0); });

    EXPECT_EQ(0, falseSuccesses);
}
