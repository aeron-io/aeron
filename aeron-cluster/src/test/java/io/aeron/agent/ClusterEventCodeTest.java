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
package io.aeron.agent;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.agent.ClusterEventCode.*;
import static org.junit.jupiter.api.Assertions.*;

class ClusterEventCodeTest
{
    @ParameterizedTest
    @EnumSource(ClusterEventCode.class)
    void getCodeById(final ClusterEventCode code)
    {
        assertSame(code, get(code.id()));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, 77, Integer.MIN_VALUE, Integer.MAX_VALUE })
    void getCodeByIdShouldThrowIllegalArgumentExceptionForUnknownId(final int id)
    {
        assertThrows(IllegalArgumentException.class, () -> get(id));
    }

    @ParameterizedTest
    @EnumSource(ClusterEventCode.class)
    void toEventCodeIdComputesEventId(final ClusterEventCode eventCode)
    {
        assertEquals(EVENT_CODE_TYPE << 16 | (eventCode.id() & 0xFFFF), eventCode.toEventCodeId());
    }

    @ParameterizedTest
    @EnumSource(ClusterEventCode.class)
    void fromEventCodeIdLooksUpEventCode(final ClusterEventCode eventCode)
    {
        assertSame(eventCode, fromEventCodeId(eventCode.toEventCodeId()));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, 13, Integer.MIN_VALUE, Integer.MAX_VALUE })
    void fromEventCodeIdShouldThrowIllegalArgumentExceptionForUnknownCodeId(final int eventCodeId)
    {
        assertThrows(IllegalArgumentException.class, () -> fromEventCodeId(eventCodeId));
    }
}
