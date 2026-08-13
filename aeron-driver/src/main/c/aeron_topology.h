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

#include <stdio.h>

#define AERON_TOPOLOGY_SYS_CPU_PATH "/sys/devices/system/cpu"
#define AERON_TOPOLOGY_MAX_CPU_ID 8192

/**
 * The set of logical CPU siblings that share a physical core, filtered to
 * only those present in the cpuset being processed. Sorted ascending;
 * cpus[0] is the prime (lowest-numbered) thread.
 */
typedef struct aeron_topology_core_stct
{
    int *cpus;
    int cpu_count;
}
aeron_topology_core_t;

/**
 * Read one Core per physical core that has at least one CPU in cpus.
 * Each returned Core contains only the CPUs from cpus that belong to that
 * physical core, sorted ascending. The returned array is sorted by prime thread.
 *
 * @param sys_cpu_root of the sys fs filesystem to access cpu information.
 * @param cpus input array of logical CPU IDs to inspect
 * @param cpu_count number of entries in cpus
 * @param cores output array allocated within this function; free with aeron_topology_cores_free
 * @param core_count number of entries in cores
 * @return 0 on success, -1 on failure
 */
int aeron_topology_read(
    const char *sys_cpu_root,
    const int *cpus,
    int cpu_count,
    aeron_topology_core_t **cores,
    int *core_count);

/**
 * Return the prime (lowest-numbered) sibling of each core.
 *
 * @param cores input array of cores
 * @param core_count number of entries in cores
 * @param primes output array allocated within this function
 * @param prime_count number of entries in primes
 * @return 0 on success, -1 on failure
 */
int aeron_topology_primes_of(
    const aeron_topology_core_t *cores,
    int core_count,
    int **primes,
    int *prime_count);

/**
 * Return all logical CPUs across all provided cores, in core-then-sibling order.
 *
 * @param cores input array of cores
 * @param core_count number of entries in cores
 * @param cpus output array allocated within this function
 * @param cpu_count number of entries in cpus
 * @return 0 on success, -1 on failure
 */
int aeron_topology_all_of(
    const aeron_topology_core_t *cores,
    int core_count,
    int **cpus,
    int *cpu_count);

/**
 * Check that for every physical core touching cpus, either all or none of its
 * logical sibling threads are in cpus. Returns one warning string per partial
 * core. Best-effort: if sysfs is unavailable warnings will be empty.
 *
 * @param sys_cpu_root of the sys fs filesystem to access cpu information.
 * @param cpus input array of CPU IDs
 * @param cpu_count number of entries in cpus
 * @param output to write the warnings to.
 * @return the count of the number of warnings or -1 on error.
 */
int aeron_topology_check_alignment(const char* sys_cpu_root, const int *cpus, int cpu_count, FILE *output);

/**
 * Check that all CPUs in cpus share the same die.
 *
 * @param sys_cpu_root  of the sys fs filesystem to access cpu information.
 * @param cpus          input array of CPU IDs
 * @param cpu_count     number of entries in cpus
 * @param output        buffer to write the warning to, if any. Will be length 0 if no warnings.
 * @return the count of the number of warnings or -1 on error.
 */
int aeron_topology_check_die_locality(const char* sys_cpu_root, const int *cpus, int cpu_count, FILE* output);

/**
 * Check that all CPUs in cpus share the same L3 cache.
 *
 * @param sys_cpu_root of the sys fs filesystem to access cpu information.
 * @param cpus input array of CPU IDs
 * @param cpu_count number of entries in cpus
 * @param output
 * @return the count of the number of warnings or -1 on error.
 */
int aeron_topology_check_l3_locality(const char* sys_cpu_root, const int *cpus, int cpu_count, FILE* output);

/**
 * Read the set of CPUs that share an L3 cache domain with cpu (including cpu itself).
 *
 * @param sys_cpu_root of the sys fs filesystem to access cpu information.
 * @param cpu logical CPU id to inspect.
 * @param peers output array allocated within this function
 * @param peer_count number of entries in peers.
 * @return 0 on success, -1 on failure.
 */
int aeron_topology_read_l3_peers(const char *sys_cpu_root, int cpu, int **peers, int *peer_count);

/**
 * Build a table of L3 cache peers for each distinct CPU in cpus, skipping AERON_NULL_VALUE
 * entries and any CPU already populated in l3_peers.
 *
 * @param sys_cpu_root of the sys fs filesystem to access cpu information.
 * @param cpus input array of resolved CPU ids (entries may be AERON_NULL_VALUE to skip).
 * @param cpu_count number of entries in cpus.
 * @param l3_peers output table indexed by CPU id; entries allocated within this function.
 * @param l3_peer_counts output table indexed by CPU id, parallel to l3_peers.
 * @return 0 on success, -1 on failure.
 */
int aeron_topology_build_l3_peer_table(
    const char *sys_cpu_root,
    const int *cpus,
    int cpu_count,
    const int *l3_peers[AERON_TOPOLOGY_MAX_CPU_ID],
    int l3_peer_counts[AERON_TOPOLOGY_MAX_CPU_ID]);

/**
 * Partition CPUs by L3 cache domain.
 *
 * @param cpus input array of resolved CPU ids.
 * @param cpu_count number of entries in cpus.
 * @param l3_peers table indexed by CPU id, as built by aeron_topology_build_l3_peer_table.
 * @param l3_peer_counts table indexed by CPU id, parallel to l3_peers.
 * @param l3_group_members output array of per-group CPU id arrays
 * @param l3_group_member_count output array of per-group member counts
 * @param l3_group_count output number of groups written.
 * @return 0 on success, -1 on failure.
 */
int aeron_topology_build_l3_group_table(
    const int *cpus,
    int cpu_count,
    const int **l3_peers,
    const int *l3_peer_counts,
    int ***l3_group_members,
    int **l3_group_member_count,
    int *l3_group_count);

/**
 * Free an array of cores allocated by aeron_topology_read.
 */
void aeron_topology_cores_free(aeron_topology_core_t *cores, int core_count);

#endif //AERON_TOPOLOGY_H
