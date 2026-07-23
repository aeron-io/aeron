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
package io.aeron.logging;

import io.aeron.Aeron;

/**
 * Utility methods and values for working with CBOR.
 */
public final class CborUtils
{
    private CborUtils()
    {
    }

    // Base bytes for major types
    static final int UNSIGNED_INTEGER_MAJOR_TYPE = 0;
    static final int NEGATIVE_INTEGER_MAJOR_TYPE = 1 << 5;
    static final int TEXT_STRING_MAJOR_TYPE = 3 << 5;
    static final int ARRAY_MAJOR_TYPE = 4 << 5;
    static final int MAP_MAJOR_TYPE = 5 << 5;
    static final int TAG_MAJOR_TYPE = 6 << 5;
    // NOTE: This major type actually includes floats and simple values
    static final int SIMPLE_VALUE_MAJOR_TYPE = 7 << 5;
    static final int ADDITIONAL_CONTENT_1_BYTE = 24;
    static final int ADDITIONAL_CONTENT_2_BYTE = 25;
    static final int ADDITIONAL_CONTENT_4_BYTE = 26;
    static final int ADDITIONAL_CONTENT_8_BYTE = 27;
    static final int ADDITIONAL_CONTENT_INDEFINITE = 31;
    static final int ADDITIONAL_CONTENT_NULL = 22;
    // Simple values
    static final byte NULL_VALUE = typeByte(SIMPLE_VALUE_MAJOR_TYPE, ADDITIONAL_CONTENT_NULL);
    static final int ADDITIONAL_CONTENT_FALSE = 20;
    static final byte FALSE_VALUE = typeByte(SIMPLE_VALUE_MAJOR_TYPE, ADDITIONAL_CONTENT_FALSE);
    static final int ADDITIONAL_CONTENT_TRUE = 21;
    static final byte TRUE_VALUE = typeByte(SIMPLE_VALUE_MAJOR_TYPE, ADDITIONAL_CONTENT_TRUE);
    static final int BREAK = 0xFF;

    static final int TIMESTAMP_INDEX = 0;
    static final int EVENT_CODE_INT_INDEX = TIMESTAMP_INDEX + 1;
    static final int EVENT_CODE_STRING_INDEX = EVENT_CODE_INT_INDEX + 1;
    static final int DATA_INDEX = EVENT_CODE_STRING_INDEX + 1;
    static final int TRUNCATE_FLAG_INDEX = DATA_INDEX + 1;
    static final int ENTRIES_LENGTH = TRUNCATE_FLAG_INDEX + 1;
    /**
     * Indicates that a value is not present.
     */
    // Tags
    public static final long NO_TAG = Aeron.NULL_VALUE;
    /**
     * Indicates that a value came from an enum.
     */
    // Example only
    public static final long ENUM_TAG = 44;

    // This might be useful for checking if a tag is present
    static boolean hasTag(final byte inputTags, final int checkedTag)
    {
        return (inputTags & checkedTag) != 0;
    }

    static byte typeByte(final int majorType, final int modifier)
    {
        return (byte)((0b111_00000 & majorType) | (0b000_11111 & modifier));
    }
}
