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

#define AERON_TOPOLOGY_SYS_CPU_PATH "/sys/devices/system/cpu"

/**
 * Overrideable root of the kernel's per-CPU sysfs tree. Defaults to
 * AERON_TOPOLOGY_SYS_CPU_PATH; tests may redirect to a fake sysfs directory.
 */
extern const char *aeron_topology_sys_cpu_path;

/**
 * The set of logical CPU siblings that share a physical core, filtered to
 * only those present in the cpuset being processed. Sorted ascending;
 * cpus[0] is the prime (lowest-numbered) thread.
 */
typedef struct aeron_topology_core_st
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
 * @param cpus input array of logical CPU IDs to inspect
 * @param cpu_count number of entries in cpus
 * @param cores output array allocated within this function; free with aeron_topology_cores_free
 * @param core_count number of entries in cores
 * @return 0 on success, -1 on failure
 */
int aeron_topology_read(
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
 * @param cpus input array of CPU IDs
 * @param cpu_count number of entries in cpus
 * @param warnings output array of warning strings allocated within this function;
 *                 free with aeron_topology_warnings_free
 * @param warning_count number of warnings
 * @return 0 on success (even with warnings), -1 on memory allocation failure
 */
int aeron_topology_check_alignment(
    const int *cpus,
    int cpu_count,
    char ***warnings,
    int *warning_count);

/**
 * Check that all CPUs in cpus share the same L3 cache domain and CPU cluster.
 * Returns at most one warning per violation (two total). Best-effort: if the
 * relevant sysfs files are absent the corresponding check is skipped silently.
 *
 * @param cpus input array of CPU IDs
 * @param cpu_count number of entries in cpus
 * @param warnings output array of warning strings allocated within this function;
 *                 free with aeron_topology_warnings_free
 * @param warning_count number of warnings
 * @return 0 on success (even with warnings), -1 on memory allocation failure
 */
int aeron_topology_check_locality(
    const int *cpus,
    int cpu_count,
    char ***warnings,
    int *warning_count);

/**
 * Free an array of cores allocated by aeron_topology_read.
 */
void aeron_topology_cores_free(aeron_topology_core_t *cores, int core_count);

/**
 * Free an array of warning strings allocated by aeron_topology_check_*.
 */
void aeron_topology_warnings_free(char **warnings, int warning_count);

#endif //AERON_TOPOLOGY_H
