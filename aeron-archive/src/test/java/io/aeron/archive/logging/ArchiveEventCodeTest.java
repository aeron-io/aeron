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
package io.aeron.archive.logging;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import static io.aeron.archive.logging.ArchiveEventCode.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ArchiveEventCodeTest
{
    @ParameterizedTest
    @EnumSource(ArchiveEventCode.class)
    void getCodeById(final ArchiveEventCode code)
    {
        assertSame(code, get(code.id()));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, 101, Integer.MIN_VALUE, Integer.MAX_VALUE })
    void getShouldThrowIllegalArgumentExceptionIfIdIsUnknown(final int id)
    {
        assertThrows(IllegalArgumentException.class, () -> get(id));
    }

    @ParameterizedTest
    @EnumSource(ArchiveEventCode.class)
    void getCodeByTemplateId(final ArchiveEventCode code)
    {
        final int templateId = code.templateId();
        if (templateId < 0)
        {
            assertNull(getByTemplateId(templateId));
        }
        else
        {
            assertSame(code, getByTemplateId(templateId));
        }
    }

    @ParameterizedTest
    @EnumSource(ArchiveEventCode.class)
    void toEventCodeIdComputesEventId(final ArchiveEventCode eventCode)
    {
        assertEquals((EVENT_CODE_TYPE << 16) | (0xFFFF & eventCode.id()), eventCode.toEventCodeId());
    }

    @ParameterizedTest
    @EnumSource(ArchiveEventCode.class)
    void fromEventCodeIdLooksUpEventCode(final ArchiveEventCode eventCode)
    {
        assertSame(eventCode, fromEventCodeId(eventCode.toEventCodeId()));
    }

    @ParameterizedTest
    @ValueSource(ints = { 0, -1, 13, Integer.MIN_VALUE, Integer.MAX_VALUE })
    void fromEventCodeIdThrowsIllegalArgumentExceptionIfCodeIsInvalid(final int eventCodeId)
    {
        assertThrows(IllegalArgumentException.class, () -> fromEventCodeId(eventCodeId));
    }
}
