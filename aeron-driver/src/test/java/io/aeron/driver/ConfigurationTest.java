/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.driver;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConfigurationTest
{
    @Test
    void shouldUseSparseFilesByDefault()
    {
        System.clearProperty(Configuration.TERM_BUFFER_SPARSE_FILE_PROP_NAME);
        assertTrue(Configuration.termBufferSparseFile());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void shouldUseSparseFilesByDefault(final boolean useSparseFiles)
    {
        System.setProperty(Configuration.TERM_BUFFER_SPARSE_FILE_PROP_NAME, String.valueOf(useSparseFiles));
        try
        {
            assertEquals(useSparseFiles, Configuration.termBufferSparseFile());
        }
        finally
        {
            System.clearProperty(Configuration.TERM_BUFFER_SPARSE_FILE_PROP_NAME);
        }
    }
}
