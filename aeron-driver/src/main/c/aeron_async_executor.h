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

#ifndef AERON_ASYNC_EXECUTOR_H
#define AERON_ASYNC_EXECUTOR_H

#include "aeron_agent.h"
#include "aeron_name_resolver.h"
#include "concurrent/aeron_blocking_linked_queue.h"
#include "util/aeron_error.h"

typedef struct aeron_async_executor_task_stct aeron_async_executor_task_t;

typedef struct aeron_async_executor_stct
{
    bool async_enabled;
    aeron_clock_func_t aeron_epoch_clock;
    void *clientd;
    aeron_blocking_linked_queue_t queue;
    aeron_blocking_linked_queue_t return_queue;
    aeron_agent_runner_t runner;
    aeron_name_resolver_t *name_resolver;
}
aeron_async_executor_t;

typedef int (*aeron_async_executor_task_on_execute_func_t)(void *task_clientd, void *executor_clientd);
typedef void (*aeron_async_executor_task_on_cancel_func_t)(void *task_clientd, void *executor_clientd);
typedef void (*aeron_async_executor_task_on_complete_func_t)(
    int execution_result, int errcode, const char *errmsg, void *task_clientd, void *executor_clientd);

typedef struct aeron_async_executor_task_stct
{
    aeron_async_executor_t *executor;
    aeron_async_executor_task_on_execute_func_t on_execute;
    aeron_async_executor_task_on_complete_func_t on_complete;
    aeron_async_executor_task_on_cancel_func_t on_cancel;
    void *clientd;
    int result;
    int errcode;
    char errmsg[AERON_ERROR_MAX_TOTAL_LENGTH];
}
aeron_async_executor_task_t;

int aeron_async_executor_init(
    aeron_async_executor_t *executor,
    aeron_driver_context_t *context,
    aeron_name_resolver_t *name_resolver,
    const char *agent_role_name,
    void *clientd);
int aeron_async_executor_do_work(void *clientd);
void aeron_async_executor_on_start(void *state, const char *role_name);

int aeron_async_executor_close(aeron_async_executor_t *executor);

int aeron_async_executor_submit(
    aeron_async_executor_t *executor,
    aeron_async_executor_task_on_execute_func_t on_execute,
    aeron_async_executor_task_on_complete_func_t on_complete,
    aeron_async_executor_task_on_cancel_func_t on_cancel,
    void *clientd);

int aeron_async_executor_process_completions(aeron_async_executor_t *executor, int limit);

void aeron_async_executor_task_do_complete(aeron_async_executor_task_t *task);

#endif //AERON_ASYNC_EXECUTOR_H
