/*
 * Copyright 2014-2026 Real Logic Limited.
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

package io.aeron.topology;

import org.agrona.collections.IntArrayList;

import java.io.PrintStream;

interface TopologyValidator
{
    /**
     * Validates the given CPU list based on a specific topology constraint.
     *
     * @param cpuList       to validate.
     * @param warningStream to write warnings to.
     * @return number of warnings.
     */
    int validate(IntArrayList cpuList, PrintStream warningStream);
}
