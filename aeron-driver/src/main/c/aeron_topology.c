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

#include "aeron_topology.h"
#include "aeron_cpuset.h"
#include "aeron_alloc.h"
#include "util/aeron_error.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define AERON_TOPOLOGY_MAX_CPU_ID 8192
#define AERON_TOPOLOGY_FILE_BUF_SIZE 4096

const char *aeron_topology_sys_cpu_path = AERON_TOPOLOGY_SYS_CPU_PATH;

typedef struct aeron_topology_core_group_stct
{
    int *present;
    int present_count;
    int *missing;
    int missing_count;
}
aeron_topology_core_group_t;

static int aeron_topology_cmp_int(const void *a, const void *b)
{
    return *(const int *)a - *(const int *)b;
}

static int aeron_topology_cmp_core_by_prime(const void *a, const void *b)
{
    const aeron_topology_core_t *ca = a;
    const aeron_topology_core_t *cb = b;
    if (0 == ca->cpu_count)
    {
        return 1;
    }
    if (0 == cb->cpu_count)
    {
        return -1;
    }
    return ca->cpus[0] - cb->cpus[0];
}

static int aeron_topology_read_sysfs_cpu_file(int cpu, const char *suffix, char *buf, size_t buf_size)
{
    char path[4096];
    int n = snprintf(path, sizeof(path), "%s/cpu%d/%s", aeron_topology_sys_cpu_path, cpu, suffix);
    if (n < 0 || (size_t)n >= sizeof(path))
    {
        AERON_SET_ERR(EINVAL, "path too long for CPU %d", cpu);
        return -1;
    }

    FILE *f = fopen(path, "r");
    if (NULL == f)
    {
        AERON_SET_ERR(errno, "unable to open: %s", path);
        return -1;
    }

    size_t read = fread(buf, 1, buf_size - 1, f);
    fclose(f);
    buf[read] = '\0';
    return (int)read;
}

static int aeron_topology_read_siblings(int cpu, int **siblings, int *sibling_count)
{
    char buf[AERON_TOPOLOGY_FILE_BUF_SIZE];
    if (aeron_topology_read_sysfs_cpu_file(cpu, "topology/thread_siblings_list", buf, sizeof(buf)) < 0)
    {
        AERON_APPEND_ERR("reading topology for CPU %d", cpu);
        return -1;
    }
    if (aeron_cpuset_parse_cpulist(buf, siblings, sibling_count) < 0)
    {
        AERON_APPEND_ERR("parsing sibling list for CPU %d", cpu);
        return -1;
    }
    return 0;
}

static int aeron_topology_read_l3_peers(int cpu, int **peers, int *peer_count)
{
    char buf[AERON_TOPOLOGY_FILE_BUF_SIZE];
    if (aeron_topology_read_sysfs_cpu_file(cpu, "cache/index3/shared_cpu_list", buf, sizeof(buf)) < 0)
    {
        return -1;
    }
    if (aeron_cpuset_parse_cpulist(buf, peers, peer_count) < 0)
    {
        return -1;
    }
    return 0;
}

static int aeron_topology_read_cluster_id(int cpu, int *cluster_id)
{
    char buf[64];
    if (aeron_topology_read_sysfs_cpu_file(cpu, "topology/cluster_id", buf, sizeof(buf)) < 0)
    {
        return -1;
    }
    char *end = buf;
    long val = strtol(buf, &end, 10);
    if (end == buf)
    {
        AERON_SET_ERR(EINVAL, "parsing cluster_id for CPU %d: '%s'", cpu, buf);
        return -1;
    }
    *cluster_id = (int)val;
    return 0;
}

static void aeron_topology_core_groups_free(aeron_topology_core_group_t *groups, int group_count)
{
    if (NULL == groups)
    {
        return;
    }
    for (int i = 0; i < group_count; i++)
    {
        aeron_free(groups[i].present);
        aeron_free(groups[i].missing);
    }
    aeron_free(groups);
}

static int aeron_topology_read_core_groups(
    const int *cpus,
    int cpu_count,
    aeron_topology_core_group_t **groups_out,
    int *group_count_out)
{
    uint8_t cpu_set[AERON_TOPOLOGY_MAX_CPU_ID] = { 0 };
    uint8_t seen[AERON_TOPOLOGY_MAX_CPU_ID] = { 0 };
    aeron_topology_core_group_t *groups = NULL;
    int group_count = 0;
    int group_capacity = 16;
    int result = 0;

    if (aeron_alloc((void **)&groups, sizeof(aeron_topology_core_group_t) * group_capacity) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        result = -1;
        goto done;
    }

    for (int i = 0; i < cpu_count; i++)
    {
        cpu_set[cpus[i]] = 1;
    }

    for (int i = 0; i < cpu_count; i++)
    {
        int cpu = cpus[i];
        if (seen[cpu])
        {
            continue;
        }

        int *siblings = NULL;
        int sibling_count = 0;
        if (aeron_topology_read_siblings(cpu, &siblings, &sibling_count) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            result = -1;
            goto done;
        }

        for (int j = 0; j < sibling_count; j++)
        {
            if (0 <= siblings[j] && siblings[j] < AERON_TOPOLOGY_MAX_CPU_ID)
            {
                seen[siblings[j]] = 1;
            }
        }

        int *present = NULL;
        int present_count = 0;
        int *missing = NULL;
        int missing_count = 0;

        if (0 < sibling_count)
        {
            if (aeron_alloc((void **)&present, sizeof(int) * sibling_count) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                aeron_free(siblings);
                result = -1;
                goto done;
            }
            if (aeron_alloc((void **)&missing, sizeof(int) * sibling_count) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                aeron_free(siblings);
                aeron_free(present);
                result = -1;
                goto done;
            }
        }

        for (int j = 0; j < sibling_count; j++)
        {
            int s = siblings[j];
            if (0 <= s && s < AERON_TOPOLOGY_MAX_CPU_ID && cpu_set[s])
            {
                present[present_count++] = s;
            }
            else
            {
                missing[missing_count++] = s;
            }
        }

        aeron_free(siblings);

        if (0 == present_count)
        {
            aeron_free(present);
            present = NULL;
        }
        else if (aeron_reallocf((void **)&present, sizeof(int) * present_count) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            aeron_free(missing);
            result = -1;
            goto done;
        }

        if (0 == missing_count)
        {
            aeron_free(missing);
            missing = NULL;
        }
        else if (aeron_reallocf((void **)&missing, sizeof(int) * missing_count) < 0)
        {
            AERON_APPEND_ERR("%s", "");
            aeron_free(present);
            result = -1;
            goto done;
        }

        if (group_count == group_capacity)
        {
            group_capacity *= 2;
            if (aeron_reallocf((void **)&groups, sizeof(aeron_topology_core_group_t) * group_capacity) < 0)
            {
                AERON_APPEND_ERR("%s", "");
                aeron_free(present);
                aeron_free(missing);
                result = -1;
                goto done;
            }
        }

        groups[group_count].present = present;
        groups[group_count].present_count = present_count;
        groups[group_count].missing = missing;
        groups[group_count].missing_count = missing_count;
        group_count++;
    }

done:
    *groups_out = groups;
    *group_count_out = group_count;
    return result;
}

static int aeron_topology_format_cpulist(const int *cpus, int cpu_count, char *buf, size_t buf_size)
{
    int written = 0;
    int i = 0;
    while (i < cpu_count)
    {
        int j = i;
        while (j + 1 < cpu_count && cpus[j + 1] == cpus[j] + 1)
        {
            j++;
        }

        if (written > 0)
        {
            if ((size_t)(written + 1) >= buf_size)
            {
                return -1;
            }
            buf[written++] = ',';
        }

        int n;
        if (i == j)
        {
            n = snprintf(buf + written, buf_size - (size_t)written, "%d", cpus[i]);
        }
        else
        {
            n = snprintf(buf + written, buf_size - (size_t)written, "%d-%d", cpus[i], cpus[j]);
        }

        if (n < 0 || (size_t)(written + n) >= buf_size)
        {
            return -1;
        }
        written += n;
        i = j + 1;
    }
    return written;
}

void aeron_topology_cores_free(aeron_topology_core_t *cores, int core_count)
{
    if (NULL == cores)
    {
        return;
    }
    for (int i = 0; i < core_count; i++)
    {
        aeron_free(cores[i].cpus);
    }
    aeron_free(cores);
}

int aeron_topology_read(
    const int *cpus,
    int cpu_count,
    aeron_topology_core_t **cores_out,
    int *core_count_out)
{
    aeron_topology_core_group_t *groups = NULL;
    int group_count = 0;
    aeron_topology_core_t *cores = NULL;
    int core_count = 0;

    if (aeron_topology_read_core_groups(cpus, cpu_count, &groups, &group_count) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_topology_core_groups_free(groups, group_count);
        return -1;
    }

    if (group_count > 0 && aeron_alloc((void **)&cores, sizeof(aeron_topology_core_t) * group_count) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        aeron_topology_core_groups_free(groups, group_count);
        return -1;
    }

    for (int i = 0; i < group_count; i++)
    {
        if (groups[i].present_count > 0)
        {
            cores[core_count].cpus = groups[i].present;
            cores[core_count].cpu_count = groups[i].present_count;
            groups[i].present = NULL; // ownership transferred to cores[]
            core_count++;
        }
        aeron_free(groups[i].missing);
        groups[i].missing = NULL;
    }
    aeron_free(groups);

    qsort(cores, core_count, sizeof(aeron_topology_core_t), aeron_topology_cmp_core_by_prime);

    *cores_out = cores;
    *core_count_out = core_count;
    return 0;
}

int aeron_topology_primes_of(
    const aeron_topology_core_t *cores,
    int core_count,
    int **primes_out,
    int *prime_count_out)
{
    if (0 == core_count)
    {
        *primes_out = NULL;
        *prime_count_out = 0;
        return 0;
    }

    int *primes = NULL;
    if (aeron_alloc((void **)&primes, sizeof(int) * core_count) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int count = 0;
    for (int i = 0; i < core_count; i++)
    {
        if (cores[i].cpu_count > 0)
        {
            primes[count++] = cores[i].cpus[0];
        }
    }

    *primes_out = primes;
    *prime_count_out = count;
    return 0;
}

int aeron_topology_all_of(
    const aeron_topology_core_t *cores,
    int core_count,
    int **cpus_out,
    int *cpu_count_out)
{
    int total = 0;
    for (int i = 0; i < core_count; i++)
    {
        total += cores[i].cpu_count;
    }

    if (0 == total)
    {
        *cpus_out = NULL;
        *cpu_count_out = 0;
        return 0;
    }

    int *cpus = NULL;
    if (aeron_alloc((void **)&cpus, sizeof(int) * total) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    int pos = 0;
    for (int i = 0; i < core_count; i++)
    {
        memcpy(cpus + pos, cores[i].cpus, sizeof(int) * cores[i].cpu_count);
        pos += cores[i].cpu_count;
    }

    *cpus_out = cpus;
    *cpu_count_out = total;
    return 0;
}

int aeron_topology_check_alignment(const int *cpus, const int cpu_count, FILE *output)
{
    int result = 0;

    aeron_topology_core_group_t *groups = NULL;
    int group_count = 0;

    if (AERON_TOPOLOGY_MAX_CPU_ID < cpu_count)
    {
        AERON_SET_ERR(EINVAL, "cpu count %d is greater than max cpu count", cpu_count);
        return -1;
    }

    if (cpu_count < 2)
    {
        return 0;
    }

    aeron_topology_read_core_groups(cpus, cpu_count, &groups, &group_count); // best-effort

    for (int i = 0; i < group_count; i++)
    {
        if (0 < groups[i].missing_count && 0 < groups[i].present_count)
        {
            char cpulist_buf[4096];
            aeron_topology_format_cpulist(groups[i].missing, groups[i].missing_count, cpulist_buf, sizeof(cpulist_buf));

            fprintf(
                output,
                "cpuset is missing sibling CPU(s) %s of the core containing CPU %d (partial core in cpuset)",
                cpulist_buf, groups[i].present[0]);

            result++;
        }
    }

    return result;
}

int aeron_topology_check_l3_locality(const int *cpus, const int cpu_count, FILE* output)
{
    if (AERON_TOPOLOGY_MAX_CPU_ID < cpu_count)
    {
        AERON_SET_ERR(EINVAL, "cpu count %d is greater than max cpu count", cpu_count);
        return -1;
    }

    if (cpu_count < 2)
    {
        return 0;
    }

    uint8_t seen[AERON_TOPOLOGY_MAX_CPU_ID] = { 0 };

    int *peers = NULL;
    int peer_count = 0;

    if (0 <= aeron_topology_read_l3_peers(cpus[0], &peers, &peer_count))
    {
        for (int i = 0; i < peer_count; i++)
        {
            seen[peers[i]] = 1;
        }

        for (int j = 1; j < cpu_count; j++)
        {
            if (!seen[cpus[j]])
            {
                fprintf(output, "%s", "cpuset spans multiple L3 cache domains");
            }
        }
    }
    else
    {
        aeron_err_clear();
    }

    return 0;
}

int aeron_topology_check_cluster_locality(const int *cpus, int cpu_count, FILE* output)
{
    if (AERON_TOPOLOGY_MAX_CPU_ID < cpu_count)
    {
        AERON_SET_ERR(EINVAL, "cpu count %d is greater than max cpu count", cpu_count);
        return -1;
    }

    int result = 0;

    if (cpu_count < 2)
    {
        return result;
    }

    // --- Cluster check ---
    int cluster_ids[256];
    int cluster_id_count = 0;
    int cluster_ok = 1;

    for (int i = 0; i < cpu_count; i++)
    {
        int id = 0;
        if (aeron_topology_read_cluster_id(cpus[i], &id) < 0)
        {
            cluster_ok = 0;
            aeron_err_clear();
            break;
        }

        int found = 0;
        for (int j = 0; j < cluster_id_count; j++)
        {
            if (cluster_ids[j] == id)
            {
                found = 1;
                break;
            }
        }
        if (!found && cluster_id_count < 256)
        {
            cluster_ids[cluster_id_count++] = id;
        }
    }

    if (cluster_ok && cluster_id_count > 1)
    {
        qsort(cluster_ids, cluster_id_count, sizeof(int), aeron_topology_cmp_int);

        char id_list_buf[512];
        int written = 0;
        for (int i = 0; i < cluster_id_count; i++)
        {
            int n = snprintf(
                id_list_buf + written, sizeof(id_list_buf) - (size_t)written,
                "%s%d", i > 0 ? ", " : "", cluster_ids[i]);
            if (n > 0)
            {
                written += n;
            }
        }

        fprintf(output, "cpuset spans %d CPU clusters (cluster IDs: %s)", cluster_id_count, id_list_buf);
        result++;
    }

    return result;
}
