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

import io.aeron.test.CapturingPrintStream;
import io.aeron.topology.AffinityRegistry.CoreClaim;
import io.aeron.topology.CpuSelection.AffinitySelection;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.aeron.topology.TopologyTestUtils.countWarnings;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SharedCpuValidatorTest
{
    private final CapturingPrintStream out = new CapturingPrintStream();
    private final SharedCpuValidator validator = new SharedCpuValidator();

    @Test
    void shouldNotWarnWhenNoCpusShared()
    {
        final AffinitySelection selection = new AffinitySelection(
            List.of(new CoreClaim("aaa", 2), new CoreClaim("bbb", 3)));

        assertEquals(0, validator.validate(selection, out.resetAndGetPrintStream()));
        assertEquals(0, countWarnings(out.flushAndGetContent()));
    }

    @Test
    void shouldWarnWithLabelsWhenCpuShared()
    {
        final AffinitySelection selection = new AffinitySelection(
            List.of(new CoreClaim("aaa", 2), new CoreClaim("bbb", 3), new CoreClaim("ccc", 2)));

        assertEquals(1, validator.validate(selection, out.resetAndGetPrintStream()));
        final String output = out.flushAndGetContent();
        assertEquals(1, countWarnings(output), output);
        assertTrue(output.contains("aaa and ccc are sharing cpu=2"), output);
    }

    @Test
    void shouldWarnForEachPairWhenCpuSharedByThree()
    {
        final AffinitySelection selection = new AffinitySelection(
            List.of(new CoreClaim("aaa", 2), new CoreClaim("bbb", 2), new CoreClaim("ccc", 2)));

        assertEquals(3, validator.validate(selection, out.resetAndGetPrintStream()));
        assertEquals(3, countWarnings(out.flushAndGetContent()));
    }
}
