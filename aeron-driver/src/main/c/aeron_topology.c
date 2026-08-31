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

#include "aeronc.h"

#define AERON_TOPOLOGY_FILE_BUF_SIZE 4096

static int aeron_topology_read_sysfs_cpu_file(
    const char *sys_cpu_root,
    const int cpu,
    const char *suffix,
    char *buf,
    const size_t buf_size)
{
    char path[AERON_MAX_PATH];
    int n = snprintf(path, sizeof(path), "%s/cpu%d/%s", sys_cpu_root, cpu, suffix);
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

static int aeron_topology_read_die_id(const char *sys_cpu_root, int cpu, int *cluster_id)
{
    char buf[64];
    if (aeron_topology_read_sysfs_cpu_file(sys_cpu_root, cpu, "topology/die_id", buf, sizeof(buf)) < 0)
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

static int aeron_topology_read_siblings(const char *sys_cpu_root, int cpu, bool *siblings_table, int max_cpu)
{
    char buf[AERON_TOPOLOGY_FILE_BUF_SIZE];
    if (aeron_topology_read_sysfs_cpu_file(sys_cpu_root, cpu, "topology/thread_siblings_list", buf, sizeof(buf)) < 0)
    {
        AERON_APPEND_ERR("reading topology for CPU %d", cpu);
        return -1;
    }

    int *cpus = NULL;
    int cpu_count = 0;

    if (aeron_cpuset_parse_cpulist(buf, &cpus, &cpu_count) < 0)
    {
        AERON_APPEND_ERR("parsing sibling list for CPU %d", cpu);
        return -1;
    }

    for (int i = 0; i < cpu_count; i++)
    {
        const int sibling_cpu = cpus[i];
        if (cpus[i] < max_cpu && sibling_cpu != cpu)
        {
            siblings_table[cpu * max_cpu + sibling_cpu] = true;
            siblings_table[sibling_cpu * max_cpu + cpu] = true;
        }
    }

    aeron_free(cpus);
    return 0;
}

static int aeron_topology_read_l3_peers(const char *sys_cpu_root, int cpu, bool *l3_table, int max_cpu)
{
    char buf[AERON_TOPOLOGY_FILE_BUF_SIZE];
    if (aeron_topology_read_sysfs_cpu_file(sys_cpu_root, cpu, "cache/index3/shared_cpu_list", buf, sizeof(buf)) < 0)
    {
        return -1;
    }

    int *cpus = NULL;
    int cpu_count = 0;

    if (aeron_cpuset_parse_cpulist(buf, &cpus, &cpu_count) < 0)
    {
        AERON_APPEND_ERR("parsing l3 peer list for CPU %d", cpu);
        return -1;
    }

    for (int i = 0; i < cpu_count; i++)
    {
        const int sibling_cpu = cpus[i];
        if (cpus[i] < max_cpu && sibling_cpu != cpu)
        {
            l3_table[cpu * max_cpu + sibling_cpu] = true;
            l3_table[sibling_cpu * max_cpu + cpu] = true;
        }
    }

    aeron_free(cpus);
    return 0;
}

void aeron_topology_print(aeron_topology_t *topology)
{
    printf("%s\n", "siblings");
    for (int i = 0; i < topology->sibling_count; i++)
    {
        for (int j = 0; j < topology->sibling_count; j++)
        {
            printf("%s", topology->sibling_table[i * topology->sibling_count + j] ? "X" : " ");
        }
        printf("\n");
    }
    printf("\n");

    printf("%s\n", "l3");
    for (int i = 0; i < topology->l3_peer_count; i++)
    {
        for (int j = 0; j < topology->l3_peer_count; j++)
        {
            printf("%s", topology->l3_peer_table[i * topology->l3_peer_count + j] ? "X" : " ");
        }
        printf("\n");
    }
    printf("\n");

    for (int i = 0; i < topology->die_id_count; i++)
    {
        printf("%d ", topology->die_ids[i]);
    }
    printf("\n");
}

int aeron_topology_init(
    const char *sys_cpu_root,
    const int *cpus,
    int cpu_count,
    aeron_topology_t** topology)
{
    int max_cpu = AERON_NULL_VALUE;
    for (int i = 0; i < cpu_count; i++)
    {
        max_cpu = max_cpu < cpus[i] ? cpus[i] : max_cpu;
    }

    max_cpu++;

    aeron_topology_t *_topology;

    if (aeron_alloc((void **)&_topology, sizeof(aeron_topology_t)) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        return -1;
    }

    _topology->sibling_count = max_cpu;
    _topology->l3_peer_count = max_cpu;
    _topology->die_id_count = max_cpu;

    if (aeron_alloc((void **)&_topology->l3_peer_table, sizeof(bool) * max_cpu * max_cpu) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    if (aeron_alloc((void **)&_topology->sibling_table, sizeof(bool) * max_cpu * max_cpu) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    if (aeron_alloc((void **)&_topology->die_ids, sizeof(int) * max_cpu) < 0)
    {
        AERON_APPEND_ERR("%s", "");
        goto error;
    }

    memset(_topology->die_ids, -1, max_cpu * sizeof(int));
    for (int i = 0; i < cpu_count; i++)
    {
        const int cpu = cpus[i];
        aeron_topology_read_siblings(sys_cpu_root, cpu, _topology->sibling_table, max_cpu);
        aeron_topology_read_l3_peers(sys_cpu_root, cpu, _topology->l3_peer_table, max_cpu);
        aeron_topology_read_die_id(sys_cpu_root, cpu, &_topology->die_ids[cpu]);
    }

    *topology = _topology;
    return 0;

error:
    aeron_topology_free(_topology);
    return -1;
}

void aeron_topology_free(aeron_topology_t *topology)
{
    if (NULL == topology)
    {
        return;
    }

    aeron_free(topology->l3_peer_table);
    aeron_free(topology->sibling_table);
    aeron_free(topology->die_ids);
    aeron_free(topology);
}

int aeron_topology_check_alignment(
    aeron_topology_t *topology, const int *cpus, const int cpu_count, FILE *output)
{
    int warnings = 0;

    for (int i = 0; i < cpu_count; i++)
    {
        const int cpu = cpus[i];

        for (int j = 0; j < topology->sibling_count; j++)
        {
            const bool has_sibling = topology->sibling_table[cpu * topology->sibling_count + j];
            if (has_sibling)
            {
                const int sibling_cpu = j;
                bool found = false;
                for (int k = 0; k < cpu_count; k++)
                {
                    if (cpus[k] == sibling_cpu)
                    {
                        found = true;
                        break;
                    }
                }

                if (!found)
                {
                    warnings++;
                    fprintf(output, "cpuset is missing sibling CPU(s) %d\n", sibling_cpu);
                }
            }
        }
    }

    return warnings;
}

int aeron_topology_check_l3_locality(
    aeron_topology_t *topology, const int *cpus, int cpu_count, FILE *output)
{
    if (0 == cpu_count)
    {
        return 0;
    }

    int warnings = 0;

    const int cpu = cpus[0];
    for (int i = 1; i < cpu_count; i++)
    {
        const int sibling_cpu = cpus[i];

        const bool found = topology->l3_peer_table[cpu * topology->l3_peer_count + sibling_cpu];

        if (!found)
        {
            warnings++;
            fprintf(output, "cpuset spans multiple L3 cache domains");
            break;
        }
    }

    return warnings;
}

int aeron_topology_check_die_locality(aeron_topology_t *topology, const int *cpus, int cpu_count, FILE *output)
{
    if (0 == cpu_count)
    {
        return 0;
    }

    int warnings = 0;

    const int cpu = cpus[0];
    const int die_id = topology->die_ids[cpu];

    for (int i = 1; i < cpu_count; i++)
    {
        const int other_cpu = cpus[i];
        const int other_die_id = topology->die_ids[other_cpu];

        if (die_id != other_die_id)
        {
            warnings++;
            fprintf(output, "cpuset spans multiple CPU clusters");
            break;
        }
    }

    return warnings;
}

