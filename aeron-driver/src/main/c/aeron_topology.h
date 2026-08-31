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

#ifndef AERON_TOPOLOGY_H
#define AERON_TOPOLOGY_H

#include <stdbool.h>
#include <stdio.h>

#define AERON_TOPOLOGY_SYS_CPU_PATH "/sys/devices/system/cpu"
#define AERON_TOPOLOGY_MAX_CPU_ID 8192

/**
 * The system topology that tracks siblings, l3 locality and die ids.
 */
typedef struct aeron_topology_stct
{
    bool *sibling_table; // Flattened 2-d array
    int sibling_count;
    bool *l3_peer_table; // Flattened 2-d array
    int l3_peer_count;
    int *die_ids;
    int die_id_count;
}
aeron_topology_t;


/**
 *  Build a cpu topology structure based on the set of online cpus.  Will read up to the highest online cpu supplied.
 *
 * @param sys_cpu_root root of the sysfs filesystem.
 * @param cpus list of online cpus.
 * @param cpu_count number of online cpus.
 * @param topology allocated cpu structure.
 * @return -1 on failure 0 on success.
 */
int aeron_topology_init(
    const char *sys_cpu_root,
    const int *cpus,
    int cpu_count,
    aeron_topology_t** topology);

void aeron_topology_free(aeron_topology_t *topology);

/**
 * Check that for every physical core touching cpus, either all or none of its
 * logical sibling threads are in cpus. Returns one warning string per partial
 * core.
 *
 * @param topology      the loaded system topology.
 * @param cpus          input array of CPU IDs
 * @param cpu_count     number of entries in cpus
 * @param output        to write the warnings to.
 * @return the count of the number of warnings or -1 on error.
 */
int aeron_topology_check_alignment(
    const aeron_topology_t *topology, const int *cpus, int cpu_count, FILE *output);

/**
 * Check that all CPUs in cpus share the same die.
 *
 * @param topology      the loaded system topology.
 * @param cpus          input array of CPU IDs
 * @param cpu_count     number of entries in cpus
 * @param output        buffer to write the warning to, if any. Will be length 0 if no warnings.
 * @return the count of the number of warnings or -1 on error.
 */
int aeron_topology_check_l3_locality(
    const aeron_topology_t *topology, const int *cpus, int cpu_count, FILE *output);

/**
 * Check that all CPUs in cpus share the same L3 cache.
 *
 * @param topology      the loaded system topology.
 * @param cpus          input array of CPU IDs
 * @param cpu_count     number of entries in cpus
 * @param output        buffer to write the warning to, if any. Will be length 0 if no warnings.
 * @return the count of the number of warnings or -1 on error.
 */
int aeron_topology_check_die_locality(const aeron_topology_t *topology, const int *cpus, int cpu_count, FILE *output);


#endif //AERON_TOPOLOGY_H
