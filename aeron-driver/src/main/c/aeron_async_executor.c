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

#include "aeron_async_executor.h"
#include "aeron_alloc.h"
#include "aeron_driver_conductor.h"
#include "util/aeron_error.h"
#include "concurrent/aeron_atomic.h"

static aeron_executor_task_t *aeron_executor_task_allocate(
    aeron_executor_t *executor,
    aeron_executor_task_on_execute_func_t on_execute,
    aeron_executor_task_on_complete_func_t on_complete,
    aeron_executor_task_on_cancel_func_t on_cancel,
    void *clientd)
{
    aeron_executor_task_t *task;

    if (aeron_alloc((void **)&task, sizeof(aeron_executor_task_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return NULL;
    }

    task->executor = executor;
    task->on_execute = on_execute;
    task->on_complete = on_complete;
    task->on_cancel = on_cancel;
    task->clientd = clientd;
    task->result = -1;

    return task;
}

void aeron_executor_on_start(void *state, const char *role_name)
{
    aeron_executor_t *executor = (aeron_executor_t *)state;
    if (executor->name_resolver->start_func(executor->name_resolver) < 0)
    {
        AERON_APPEND_ERR("%s", "failed to start name resolver");
        aeron_driver_conductor_log_error(conductor);
    }
}

static void aeron_executor_cancel_all_tasks_and_close_queue(aeron_blocking_linked_queue_t *queue)
{
    while (true)
    {
        aeron_executor_task_t *task = aeron_blocking_linked_queue_poll(queue);
        if (NULL == task)
        {
            break;
        }
        task->on_cancel(task->clientd, task->executor->clientd);
        aeron_free(task);
    }

    aeron_blocking_linked_queue_close(queue); // queue is empty at this point
}

static void aeron_executor_on_close(void *state)
{
    aeron_executor_t *executor = (aeron_executor_t *)state;
    aeron_executor_cancel_all_tasks_and_close_queue(&executor->queue);
    executor->name_resolver->close_func(executor->name_resolver);
}

int aeron_executor_do_work(void *clientd)
{
    aeron_executor_t *executor = (aeron_executor_t *)clientd;

    int work_count = executor->name_resolver->do_work_func(executor->name_resolver, 0 /* FIXME */);

    if (executor->async)
    {
        aeron_executor_task_t *task = (aeron_executor_task_t *)aeron_blocking_linked_queue_poll(&executor->queue);

        if (NULL == task)
        {
            return work_count;
        }

        task->result = (NULL == task->on_execute) ? 0 : task->on_execute(task->clientd, executor->clientd);

        if (task->result < 0)
        {
            task->errcode = aeron_errcode();
            memcpy(task->errmsg, aeron_errmsg(), strlen(aeron_errmsg()));
            aeron_err_clear();
        }

        if (NULL == executor->on_execution_complete)
        {
            aeron_blocking_linked_queue_offer(&executor->return_queue, task);
        }
        else
        {
            executor->on_execution_complete(task, executor->clientd);
            aeron_free(task);
        }
    }

    work_count++;

    return work_count;
}

int aeron_executor_init(aeron_executor_t *executor, aeron_driver_context_t *context, aeron_driver_conductor_t *conductor)
{
    executor->async = context->async_executor_enabled,
    executor->on_execution_complete = NULL;
    executor->clientd = conductor;

    executor->runner.state = AERON_AGENT_STATE_UNUSED;
    executor->runner.role_name = NULL;
    executor->runner.on_close = NULL;

    executor->name_resolver = &conductor->name_resolver;

    if (context->async_executor_enabled)
    {
        if (NULL == executor->on_execution_complete)
        {
            if (aeron_blocking_linked_queue_init(&executor->return_queue) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                return -1;
            }
        }

        if (aeron_blocking_linked_queue_init(&executor->queue) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        if (aeron_agent_init(
            &executor->runner,
            "aeron-executor",
            executor,
            aeron_executor_on_start,
            executor,
            aeron_executor_do_work,
            aeron_executor_on_close,
            context->async_executor_idle_strategy_func,
            context->async_executor_idle_strategy_state) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to init agent runner");
            return -1;
        }

        if (aeron_agent_start(&executor->runner) < 0)
        {
            AERON_APPEND_ERR("%s", "failed to start agent runner");
            return -1;
        }
    }

    return 0;
}

int aeron_executor_close(aeron_executor_t *executor)
{
    if (executor->async)
    {
        if (aeron_agent_stop(&executor->runner))
        {
            AERON_APPEND_ERR("%s", "failed to stop agent runner");
            return -1;
        }

        if (aeron_agent_close(&executor->runner))
        {
            AERON_APPEND_ERR("%s", "failed to close agent runner");
            return -1;
        }

        if (NULL == executor->on_execution_complete)
        {
            aeron_executor_cancel_all_tasks_and_close_queue(&executor->return_queue);
        }
    }
    return 0;
}

int aeron_executor_submit(
    aeron_executor_t *executor,
    aeron_executor_task_on_execute_func_t on_execute,
    aeron_executor_task_on_complete_func_t on_complete,
    aeron_executor_task_on_cancel_func_t on_cancel,
    void *clientd)
{
    if (executor->async)
    {
        aeron_executor_task_t *task;

        task = aeron_executor_task_allocate(executor, on_execute, on_complete, on_cancel, clientd);
        if (NULL == task)
        {
            AERON_APPEND_ERR("%s", "");
            return -1;
        }

        return aeron_blocking_linked_queue_offer(&executor->queue, task);
    }

    /* not async, so just run execute and complete back to back */
    int result = on_execute(clientd, executor->clientd);

    /* error handling must be done inside the on_complete function */
    on_complete(
        result,
        aeron_errcode(),
        aeron_errmsg(),
        clientd,
        executor->clientd);

    return 0;
}

int aeron_executor_process_completions(aeron_executor_t *executor, int limit)
{
    if (!executor->async || NULL != executor->on_execution_complete)
    {
        return 0;
    }

    aeron_executor_task_t *task;
    int count = 0;
    for (; count < limit; count++)
    {
        task = aeron_blocking_linked_queue_poll(&executor->return_queue);
        if (NULL == task)
        {
            break;
        }

        task->on_complete(
            task->result,
            task->errcode,
            task->errmsg,
            task->clientd,
            task->executor->clientd);

        aeron_free(task);
    }

    return count;
}
