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

/**
 * Parse a list of cpus, e.g. '1,3,4,5-19'.
 *
 * @param cpulist_data NUL terminated string that represents a cpulist.
 * @param cpus array of cpus, allocated within this function
 * @param cpu_count count of number of cpus
 * @return 0 on success, -1 on failure
 */
int aeron_cpuset_parse_cpulist(const char *cpulist_data, int **cpus, int *cpu_count);

int aeron_cpuset_cgroup_read_v2(const char *proc_cgroup_file, const char *mount_root, int **cpus, int *cpu_count);

#endif //AERON_CPUSET_H
