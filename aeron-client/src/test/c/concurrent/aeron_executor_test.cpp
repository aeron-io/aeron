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
#include <queue>

extern "C"
{
#include "concurrent/aeron_executor.h"
}

typedef struct
{
    int some_value;
    int some_other_value;
}
task_clientd_t;

class ExecutorTest : public testing::Test
{
public:
    void SetUp() override
    {
        if (aeron_executor_init(
            &m_executor,
            be_async(),
            on_execution_complete_cb(),
            this) < 0)
        {
            throw std::runtime_error("could not init q");
        }
    }

    void TearDown() override
    {
        aeron_executor_close(&m_executor);
    }

    virtual bool be_async() = 0;

    virtual aeron_executor_on_execution_complete_func_t on_execution_complete_cb()
    {
        return nullptr;
    }

    static int on_execute(void *task_clientd, void *executor_clientd)
    {
        auto *e = (ExecutorTest *)executor_clientd;

        e->m_on_execute_count++;

        return e->m_on_execute(task_clientd, e);
    }

    static void on_complete(int result, int errcode, const char *errmsg, void *task_clientd, void *executor_clientd)
    {
        auto *e = (ExecutorTest *)executor_clientd;

        e->m_on_complete_count++;

        e->m_on_complete(result, task_clientd, e);
    }

protected:
    aeron_executor_t m_executor = {};
    std::function<int(void *, void *)> m_on_execute;
    std::function<int(int, void *, void *)> m_on_complete;
    int m_on_execute_count = 0;
    int m_on_complete_count = 0;
};

class SyncExecutorTest : public ExecutorTest
{
public:
    bool be_async() override
    {
        return false;
    }
};

TEST_F(SyncExecutorTest, shouldExecuteSynchronously)
{
    task_clientd_t tcd;

    tcd.some_value = 0;

    m_on_execute =
        [&](void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return 1;
        };

    m_on_complete =
        [&](int result, void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return ++result;
        };

    int result = aeron_executor_submit(
        &m_executor,
        SyncExecutorTest::on_execute,
        SyncExecutorTest::on_complete,
        &tcd);

    ASSERT_EQ(result, 0);
    ASSERT_EQ(m_on_execute_count, 1);
    ASSERT_EQ(m_on_complete_count, 1);
    ASSERT_EQ(tcd.some_value, 200);
}

#define TOTAL_TASKS 1000


class AsyncExecutorTest : public ExecutorTest
{
public:
    bool be_async() override
    {
        return true;
    }
};

TEST_F(AsyncExecutorTest, shouldExecuteAsynchronously)
{
    task_clientd_t tcd;

    tcd.some_value = 0;
    tcd.some_other_value = 0;

    m_on_execute =
        [&](void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return 1;
        };

    m_on_complete =
        [&](int result, void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_other_value += 50;

            return ++result;
        };

    for (int i = 0; i < TOTAL_TASKS; i++)
    {
        int result = aeron_executor_submit(
            &m_executor,
            SyncExecutorTest::on_execute,
            SyncExecutorTest::on_complete,
            &tcd);
        ASSERT_EQ(result, 0);
    }

    int work_count = 0;

    while (m_on_complete_count < TOTAL_TASKS)
    {
        work_count += aeron_executor_process_completions(&m_executor, 50);
    }

    ASSERT_EQ(work_count, TOTAL_TASKS);
    ASSERT_EQ(m_on_execute_count, TOTAL_TASKS);
    ASSERT_EQ(m_on_complete_count, TOTAL_TASKS);
    ASSERT_EQ(tcd.some_value, TOTAL_TASKS * 100);
    ASSERT_EQ(tcd.some_other_value, TOTAL_TASKS * 50);
}

class AsyncNoReturnQueueExecutorTest : public AsyncExecutorTest
{
public:
    aeron_executor_on_execution_complete_func_t on_execution_complete_cb() override
    {
        return AsyncNoReturnQueueExecutorTest::on_execution_complete_cb;
    };

    static int on_execution_complete_cb(aeron_executor_task_t *task, void *executor_clientd)
    {
        auto e = ((AsyncNoReturnQueueExecutorTest *)executor_clientd);

        e->tasks.push(task);
        e->m_on_execution_complete_count++;

        return 0;
    }

protected:
    std::queue<aeron_executor_task_t *> tasks;
    int m_on_execution_complete_count = 0;
};


TEST_F(AsyncNoReturnQueueExecutorTest, shouldExecuteAsynchronously)
{
    task_clientd_t tcd;

    tcd.some_value = 0;

    m_on_execute =
        [&](void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return 1;
        };

    m_on_complete =
        [&](int result, void *task_clientd, void *executor_clientd)
        {
            ((task_clientd_t *)task_clientd)->some_value += 100;

            return ++result;
        };

    for (int i = 0; i < TOTAL_TASKS; i++)
    {
        int result = aeron_executor_submit(
            &m_executor,
            SyncExecutorTest::on_execute,
            SyncExecutorTest::on_complete,
            &tcd);
        ASSERT_EQ(result, 0);
    }

    while (m_on_execution_complete_count < TOTAL_TASKS)
    {
        sched_yield();
    }

    ASSERT_EQ(tasks.size(), TOTAL_TASKS);

    while (!tasks.empty())
    {
        aeron_executor_task_do_complete(tasks.front());
        tasks.pop();
    }

    ASSERT_EQ(tasks.size(), 0);
    ASSERT_EQ(m_on_execution_complete_count, TOTAL_TASKS);
    ASSERT_EQ(m_on_execute_count, TOTAL_TASKS);
    ASSERT_EQ(m_on_complete_count, TOTAL_TASKS);
    ASSERT_EQ(tcd.some_value, TOTAL_TASKS * 200);
}
