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

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_fileutil.h"

#define AERON_CGROUP_SYS_FILE_LENGTH_MAX (4096)

int aeron_cpuset_read_sysfs_file(const char *filename, char *buf, size_t buf_size)
{
    FILE *f = fopen(filename, "r");
    if (NULL == f)
    {
        AERON_SET_ERR(errno, "unable to open: %s", filename);
        return -1;
    }

    size_t read = fread(buf, 1, buf_size - 1, f);
    fclose(f);
    buf[read] = '\0';

    return (int)read;
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
    char proc_cgroup_data[AERON_CGROUP_SYS_FILE_LENGTH_MAX];
    const int proc_cgroup_len = aeron_cpuset_read_sysfs_file(
        proc_cgroup_file, proc_cgroup_data, sizeof(proc_cgroup_data));

    if (-1 == proc_cgroup_len)
    {
        AERON_APPEND_ERR("%s", "");
        return NULL;
    }

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

static int aeron_cpuset_validate_path_root(const char *path, char *validated_path, size_t validated_path_len)
{
    char tmp_path[AERON_MAX_FILE_PATH_LENGTH];

    const char *realpath = aeron_realpath(path, validated_path, validated_path_len);
    if (NULL == realpath)
    {
        AERON_SET_ERR(errno, "unable to resolve path for %s", path);
        return -1;
    }

    const char *tmpdir = aeron_tmpdir(tmp_path, sizeof(tmp_path));
    if (0 != strncmp("/sys/", realpath, strlen("/sys/")) &&
        (NULL == tmpdir || 0 != strncmp(tmpdir, realpath, strlen(tmpdir))))
    {
        AERON_SET_ERR(EINVAL, "file %s is from an invalid location", path);
        return -1;
    }

    return 0;
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
    char cpulist_data[AERON_CGROUP_SYS_FILE_LENGTH_MAX];

    if (-1 == aeron_cpuset_read_sysfs_file(cgroup_path, cpulist_data, sizeof(cpulist_data)))
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    if (aeron_cpuset_parse_cpulist(cpulist_data, cpus, cpu_count) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    return 0;
}

int aeron_cpuset_cgroup_read_v2(const char *proc_cgroup_file, const char *mount_root, int **cpus, int *cpu_count)
{
    char *cgroup_path = (char *)aeron_cpuset_find_cgroup_path(proc_cgroup_file);
    if (NULL == cgroup_path)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    char absolute_cgroup_path[AERON_MAX_FILE_PATH_LENGTH];
    do
    {
        const int written = snprintf(
            absolute_cgroup_path, sizeof(absolute_cgroup_path), "%s%s/cpuset.cpus.effective", mount_root, cgroup_path);

        if (written < 0 || sizeof(absolute_cgroup_path) <= (size_t)written)
        {
            AERON_SET_ERR(EINVAL, "%s", "cgroup path name too long");
            goto error;
        }

        if (aeron_file_exists(absolute_cgroup_path))
        {
            char validated_path[AERON_MAX_FILE_PATH_LENGTH];
            if (aeron_cpuset_validate_path_root(absolute_cgroup_path, validated_path, sizeof(validated_path)) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                goto error;
            }

            if (aeron_cpuset_parse_cpulist_from_file(validated_path, cpus, cpu_count) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                goto error;
            }

            break;
        }

        if ('\0' == *cgroup_path)
        {
            AERON_APPEND_ERR("unable to find '%s' in path '%s'", "cpuset.cpus.effective", mount_root);
            goto error;
        }

        if (ENOMEM == aeron_errcode())
        {
            AERON_APPEND_ERR("%s", "");
            goto error;
        }

        char *last_slash = strrchr(cgroup_path, '/');
        if (NULL == last_slash)
        {
            *cgroup_path = '\0';
        }
        else
        {
            *last_slash = '\0';
        }
    }
    while (true);

    aeron_free(cgroup_path);
    return 0;

error:
    aeron_free(cgroup_path);
    return -1;
}
