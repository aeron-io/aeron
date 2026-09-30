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


#ifndef AERON_CPUSET_H
#define AERON_CPUSET_H

#define AERON_CPUSET_CGROUP_MOUNT_V2 "/sys/fs/cgroup"
#define AERON_CPUSET_CGROUP_MOUNT_V1 "/sys/fs/cgroup/cpuset"
#define AERON_CPUSET_PROC_SELF_CGROUP "/proc/self/cgroup"

#include <stdlib.h>

/**
 * Parse a list of cpus, e.g. '1,3,4,5-19'.
 *
 * @param cpulist_data NUL terminated string that represents a cpulist.
 * @param cpus array of cpus, allocated within this function
 * @param cpu_count count of number of cpus
 * @return 0 on success, -1 on failure
 */
int aeron_cpuset_parse_cpulist(const char *cpulist_data, int **cpus, int *cpu_count);

int aeron_cpuset_format_cpulist(const int *cpus, int cpu_count, char *buf, size_t buf_size);

/**
 * Read a list of the online CPUs. Will allocate into the cpus parameter and the user will
 * need to use `aeron_free` when done with it.
 *
 * @param mount_root base file system path to read from.
 * @param online_cpu_file the online cpu file to read.
 * @param cpus out parameter to allocate and fill with cpu ids
 * @param cpu_count out parameter to count the number of cpus.
 * @return -1 on failure, 0 on success.
 */
int aeron_cpuset_read_online(const char *mount_root, const char *online_cpu_file, int **cpus, int *cpu_count);

/**
 * Read the cpuset that this cgroup has been set up with. Will allocate into the cpus parameter and the user will
 * need to use `aeron_free` when done with it.
 *
 * @param mount_root base file system path to read from.
 * @param proc_cgroup_file the cgroup file to read.
 * @param cpus out pararmeter to allocate and fill with cpu ids
 * @param cpu_count out parameter to count the number of cpus
 * @return -1 on failure, 0 on success.
 */
int aeron_cpuset_cgroup_read_v2(const char *mount_root, const char *proc_cgroup_file, int **cpus, int *cpu_count);

#endif //AERON_CPUSET_H
