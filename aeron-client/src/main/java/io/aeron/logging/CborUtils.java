package io.aeron.logging;

public class CborUtils
{
    // Base bytes for major types
    static final int UNSIGNED_INTEGER_MAJOR_TYPE = 0;
    static final int NEGATIVE_INTEGER_MAJOR_TYPE = 1 << 5;
    static final int TEXT_STRING_MAJOR_TYPE = 3 << 5;
    static final int ARRAY_MAJOR_TYPE = 4 << 5;
    static final int MAP_MAJOR_TYPE = 5 << 5;
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
    static final int ENTRIES_LENGTH = 3;

    static byte typeByte(final int majorType, final int modifier)
    {
        return (byte)((0b111_00000 & majorType) | (0b000_11111 & modifier));
    }
}
