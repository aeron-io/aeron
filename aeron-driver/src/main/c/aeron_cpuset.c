/*
* Copyright 2026 Adaptive Financial Consulting Limited.
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

#include "aeron_cpuset.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <asm-generic/errno-base.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"

char *aeron_cpuset_read_file(const char *filename)
{
    char *file_data = NULL;
    FILE *io_file = fopen(filename, "r");
    if (!io_file)
    {
        AERON_SET_ERR(EINVAL, "unable to open file: %s", filename);
        return NULL;
    }

    if (fseek(io_file, 0, SEEK_END) < 0)
    {
        AERON_SET_ERR(errno, "unable to seek to end of file: %s", filename);
        goto error;
    }

    const int size = ftell(io_file);
    if (size < 0)
    {
        AERON_SET_ERR(errno, "unable to get file length: %s", filename);
        goto error;
    }

    if (aeron_alloc((void **)&file_data, size + 1) < 0)
    {
        AERON_APPEND_ERR("unable to allocate buffer for: %s", filename);
        goto error;
    }

    fseek(io_file, 0, SEEK_SET);
    const size_t read = fread(file_data, 1, size, io_file);
    if (read != (size_t)size)
    {
        AERON_SET_ERR(EINVAL, "unable to read from file: %s", filename);
        goto error;
    }

    fclose(io_file);
    file_data[size] = '\0';
    return file_data;

error:
    fclose(io_file);
    aeron_free(file_data);
    return NULL;
}

typedef enum aeron_cpuset_cgroup_parse_state_en
{
    AERON_CPUSET_CGROUP_PARSE_STATE_ID,
    AERON_CPUSET_CGROUP_PARSE_STATE_CONTROLLERS,
    AERON_CPUSET_CGROUP_PARSE_STATE_PATH,
    AERON_CPUSET_CGROUP_PARSE_STATE_DONE,
}
aeron_cpuset_cgroup_parse_state_t;

static const char *aeron_cpuset_find_cgroup_path(const char *proc_cgroup_file)
{
    char *proc_cgroup_data = aeron_cpuset_read_file(proc_cgroup_file);
    if (NULL == proc_cgroup_data)
    {
        AERON_APPEND_ERR("%s", "");
        return NULL;
    }

    const int proc_cgroup_len = (int)strlen(proc_cgroup_data);

    aeron_cpuset_cgroup_parse_state_t state = AERON_CPUSET_CGROUP_PARSE_STATE_ID;
    const char *id = NULL;
    const char *controllers = NULL;
    const char *path = NULL;
    const char *return_path = NULL;

    for (int i = 0; i < proc_cgroup_len; i++)
    {
        const char c = proc_cgroup_data[i];
        switch (state)
        {
            case AERON_CPUSET_CGROUP_PARSE_STATE_ID:
            {
                if (NULL == id)
                {
                    id = &proc_cgroup_data[i];
                }

                if (':' == c)
                {
                    proc_cgroup_data[i] = '\0';
                    state = AERON_CPUSET_CGROUP_PARSE_STATE_CONTROLLERS;
                }
                break;
            }

            case AERON_CPUSET_CGROUP_PARSE_STATE_CONTROLLERS:
            {
                if (NULL == controllers)
                {
                    controllers = &proc_cgroup_data[i];
                }

                if (':' == c)
                {
                    proc_cgroup_data[i] = '\0';
                    state = AERON_CPUSET_CGROUP_PARSE_STATE_PATH;
                }

                break;
            }

            case AERON_CPUSET_CGROUP_PARSE_STATE_PATH:
            {
                if (NULL == path)
                {
                    path = &proc_cgroup_data[i];
                }

                if ('\n' == c)
                {
                    proc_cgroup_data[i] = '\0';

                    if (0 == strcmp("0", id) && 0 == strlen(controllers))
                    {
                        return_path = strdup(path);
                    }

                    state = AERON_CPUSET_CGROUP_PARSE_STATE_DONE;
                }

                break;
            }

            case AERON_CPUSET_CGROUP_PARSE_STATE_DONE:
            default:
            {
                break;
            }
        }
    }

    aeron_free(proc_cgroup_data);
    return return_path;
}



typedef enum aeron_cpuset_cpulist_parse_state_en
{
    AERON_CPUSET_CPULIST_PARSE_STATE_ID,
    AERON_CPUSET_CPULIST_PARSE_STATE_CONTROLLERS,
    AERON_CPUSET_CPULIST_PARSE_STATE_PATH,
    AERON_CPUSET_CPULIST_PARSE_STATE_DONE,
}
aeron_cpuset_cpulist_parse_state_t;

static int aeron_cpuset_cmp_int(const void *a, const void *b)
{
    const int _a = *(int *)a;
    const int _b = *(int *)b;

    return _a - _b;
}

int aeron_cpuset_parse_cpulist(const char *cpulist_data, int **cpus, int *cpu_count)
{
    int *_cpus;
    int _cpu_count = 0;
    int capacity = 1024;

    if (aeron_alloc((void **)&_cpus, sizeof(int) * capacity) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    const char *curr_ptr = cpulist_data;
    char *next_ptr = NULL;
    char last_char = '\0';

    do
    {
        long cpu = strtol(curr_ptr, &next_ptr, 10);

        if (curr_ptr == next_ptr)
        {
            if ('\0' != *next_ptr)
            {
                last_char = *next_ptr;

                if (',' == last_char)
                {
                    if (0 == _cpu_count)
                    {
                        AERON_SET_ERR(EINVAL, "%s", "leading comma");
                        goto error;
                    }
                }
                else if ('\n' == last_char)
                {
                    // Ignore
                }
                else if (last_char < '0' || '9' < last_char)
                {
                    AERON_SET_ERR(EINVAL, "%s: '%s'", "non-numeric CPU", cpulist_data);
                    goto error;
                }

                curr_ptr++;
            }
        }
        else
        {
            if (0 <= cpu)
            {
                if (capacity == _cpu_count)
                {
                    capacity *= 2;
                    if (aeron_reallocf((void **)&_cpus, sizeof(int) * capacity) < 0)
                    {
                        AERON_APPEND_ERR("%s", "");
                        return -1;
                    }
                }

                _cpus[_cpu_count] = (int)cpu;
                _cpu_count++;
                last_char = '\0';
            }
            else
            {
                if (0 == _cpu_count || '\0' != last_char)
                {
                    AERON_SET_ERR(EINVAL, "%s", "negative CPU");
                    goto error;
                }

                if (-cpu < _cpus[_cpu_count - 1])
                {
                    AERON_SET_ERR(EINVAL, "%s", "range end less than start");
                    goto error;
                }

                for (int i = _cpus[_cpu_count - 1] + 1; i <= -cpu; i++)
                {
                    if (capacity == _cpu_count)
                    {
                        capacity *= 2;
                        if (aeron_reallocf((void **)&_cpus, sizeof(int) * capacity) < 0)
                        {
                            AERON_APPEND_ERR("%s", "");
                            return -1;
                        }
                    }

                    _cpus[_cpu_count] = i;
                    _cpu_count++;
                    last_char = '\0';
                }
            }

            curr_ptr = next_ptr;
        }
    }
    while ('\0' != *next_ptr);

    if (0 == _cpu_count)
    {
        AERON_SET_ERR(EINVAL, "%s", "empty string");
        goto error;
    }

    if (',' == last_char)
    {
        AERON_SET_ERR(EINVAL, "%s", "trailing comma");
        goto error;
    }

    qsort(_cpus, _cpu_count, sizeof(int), aeron_cpuset_cmp_int);

    for (int i = 0; i < _cpu_count; i++)
    {
        for (int j = i + 1; j < _cpu_count; j++)
        {
            if (_cpus[i] == _cpus[j])
            {
                for (int k = j; k < _cpu_count - 1; k++)
                {
                    _cpus[k] = _cpus[k + 1];
                }
                _cpu_count--;
                j--;
            }
        }
    }

    if (aeron_reallocf((void **)&_cpus, sizeof(int) * _cpu_count) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    *cpus = _cpus;
    *cpu_count = _cpu_count;
    return 0;

error:
    aeron_free(_cpus);
    return -1;
}

int aeron_cpuset_parse_cpulist_from_file(const char *cgroup_path, int **cpus, int *cpu_count)
{
    char *cpulist_data = aeron_cpuset_read_file(cgroup_path);
    if (NULL == cpulist_data)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    if (aeron_cpuset_parse_cpulist(cpulist_data, cpus, cpu_count) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    aeron_free(cpulist_data);
    return 0;

error:
    aeron_free(cpulist_data);
    return -1;
}

int aeron_cpuset_cgroup_read_v2(const char *proc_cgroup_file, const char *mount_root, int **cpus, int *cpu_count)
{
    const char *cgroup_path = aeron_cpuset_find_cgroup_path(proc_cgroup_file);
    if (NULL == cgroup_path)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    char absolute_cgroup_path[4096];
    const int written = snprintf(
        absolute_cgroup_path, sizeof(absolute_cgroup_path), "%s%s/cpuset.cpus.effective", mount_root, cgroup_path);

    printf("%s\n", absolute_cgroup_path);

    if (written < 0 || sizeof(absolute_cgroup_path) <= (size_t)written)
    {
        AERON_SET_ERR(EINVAL, "%s", "cgroup path name too long");
        goto error;
    }

    if (aeron_cpuset_parse_cpulist_from_file(absolute_cgroup_path, cpus, cpu_count) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    aeron_free((void *)cgroup_path);
    return 0;

error:
    aeron_free((void *)cgroup_path);
    return -1;
}
