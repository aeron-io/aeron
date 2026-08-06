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
package io.aeron.test.logging;

import io.aeron.eventlog.GeneratedLogger;
import io.aeron.eventlog.LoggerMethod;
import io.aeron.logging.CborDecode;
import io.aeron.logging.EventCode;
import io.aeron.test.Tests;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.ringbuffer.RingBuffer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static io.aeron.logging.CborUtils.ENUM_TAG;
import static io.aeron.logging.CborUtils.IPV4_TAG;
import static io.aeron.logging.CborUtils.IPV6_TAG;
import static io.aeron.logging.CborUtils.NO_TAG;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reflectively drives every {@link LoggerMethod}-annotated method on a {@link GeneratedLogger}-annotated event
 * logger interface (e.g. {@code DriverEventLogger}, {@code ClusterEventLogger}, {@code ArchiveEventLogger|}) with
 * a range of representative, boundary, and {@code null} argument values, decodes the resulting CBOR message with
 * {@link CborDecode}, and asserts that every value survived the round trip unchanged.
 * <p>
 * This class knows nothing about any particular event logger; it discovers everything it needs - the event code
 * enum, the shape of each method, which parameters form a buffer/offset/length triple, which parameter (if any)
 * selects the event code - from the {@link GeneratedLogger} and {@link LoggerMethod} annotations and from the
 * declared parameter types/names. Because {@link io.aeron.eventlog.Tag} and {@link io.aeron.eventlog.AllowTruncate}
 * are source-retention only, this class cannot independently verify the exact CBOR tag used for a raw buffer
 * field; instead it pins whatever tag is observed on the first invocation of a given buffer field and requires
 * every subsequent variant of that field to keep using it.
 */
public final class GenericLoggerEventVerifier
{
    private static final int[] BUFFER_LENGTHS = { 0, 1, 23, 24, 255, 256 };
    private static final int BUFFER_LEADING_PADDING = 5;
    private static final int BUFFER_TRAILING_PADDING = 5;

    private static final List<String> STRING_VARIANTS = List.of(
        "", "typical value", repeat('a', 23), repeat('b', 24), repeat('c', 255), repeat('d', 256));

    private enum ProbeEnum
    {
        PROBE_ALPHA, PROBE_BETA, PROBE_GAMMA
    }

    private GenericLoggerEventVerifier()
    {
    }

    /**
     * Reflectively exercises every {@code log*} method on {@code loggerInterface}, invoking it on
     * {@code loggerImpl} and verifying the resulting CBOR message decodes back to the values supplied.
     *
     * @param loggerInterface the {@link GeneratedLogger}-annotated interface, e.g. {@code DriverEventLogger.class}.
     * @param loggerImpl      an instance implementing {@code loggerInterface}, backed by {@code ringBuffer}.
     * @param ringBuffer      the ring buffer {@code loggerImpl} writes into and this method reads back from.
     */
    public static void verifyAllLogMethods(
        final Class<?> loggerInterface, final Object loggerImpl, final RingBuffer ringBuffer)
    {
        final GeneratedLogger generatedLogger = loggerInterface.getAnnotation(GeneratedLogger.class);
        assertNotNull(generatedLogger, loggerInterface.getName() + " is not @GeneratedLogger annotated");

        final Class<?> eventCodeClass;
        try
        {
            eventCodeClass = Class.forName(generatedLogger.eventCodeType());
        }
        catch (final ClassNotFoundException ex)
        {
            throw new RuntimeException(ex);
        }

        final EventCode[] eventCodeConstants = (EventCode[])eventCodeClass.getEnumConstants();
        assertTrue(0 < eventCodeConstants.length, eventCodeClass.getName() + " has no enum constants");

        final Method[] logMethods = Arrays.stream(loggerInterface.getMethods())
            .filter(method -> method.getName().startsWith("log"))
            .filter(method -> null != method.getAnnotation(LoggerMethod.class))
            .toArray(Method[]::new);

        assertTrue(0 < logMethods.length, "No @LoggerMethod methods found on " + loggerInterface.getName());

        final RecordingLoggerEventCallback callback = new RecordingLoggerEventCallback();
        final CborDecode cborDecode = new CborDecode(List.of(callback));

        for (final Method method : logMethods)
        {
            verifyMethod(method, loggerImpl, ringBuffer, callback, cborDecode, eventCodeClass, eventCodeConstants);
        }
    }

    private static void verifyMethod(
        final Method method,
        final Object loggerImpl,
        final RingBuffer ringBuffer,
        final RecordingLoggerEventCallback callback,
        final CborDecode cborDecode,
        final Class<?> eventCodeClass,
        final EventCode[] eventCodeConstants)
    {
        final LoggerMethod annotation = method.getAnnotation(LoggerMethod.class);
        final List<Slot> slots = buildSlots(method, annotation, method.getParameters(), eventCodeClass);
        final String fixedEventCode = annotation.eventCode();

        final Object[] nominalArgs = new Object[method.getParameterCount()];
        for (final Slot slot : slots)
        {
            slot.applyNominal(nominalArgs, eventCodeConstants);
        }

        final Map<String, Long> bufferTagBaseline = new HashMap<>();

        runScenario(
            method, loggerImpl, ringBuffer, callback, cborDecode, nominalArgs, slots,
            fixedEventCode, eventCodeClass, bufferTagBaseline);

        for (final Slot slot : slots)
        {
            for (final Object[] variant : slot.variants(eventCodeConstants))
            {
                final Object[] args = nominalArgs.clone();
                slot.apply(args, variant);
                runScenario(
                    method, loggerImpl, ringBuffer, callback, cborDecode, args, slots,
                    fixedEventCode, eventCodeClass, bufferTagBaseline);
            }
        }
    }

    private static void runScenario(
        final Method method,
        final Object loggerImpl,
        final RingBuffer ringBuffer,
        final RecordingLoggerEventCallback callback,
        final CborDecode cborDecode,
        final Object[] args,
        final List<Slot> slots,
        final String fixedEventCode,
        final Class<?> eventCodeClass,
        final Map<String, Long> bufferTagBaseline)
    {
        callback.reset();

        try
        {
            method.invoke(loggerImpl, args);
        }
        catch (final IllegalAccessException | InvocationTargetException ex)
        {
            throw new RuntimeException(
                "Failed invoking " + method + " with args " + Arrays.toString(args), ex);
        }

        while (0 == ringBuffer.read(cborDecode, 1))
        {
            Tests.yield();
        }

        final EventCode expectedCode = fixedEventCode.isEmpty() ?
            (EventCode)args[codeIndexOf(method, slots)] :
            enumValueOf(eventCodeClass, fixedEventCode);

        final String scenario = method.getName() + Arrays.toString(args);
        assertHeader(scenario, callback, expectedCode);
        assertFalse(callback.truncated(), scenario + ": unexpectedly truncated");

        for (final Slot slot : slots)
        {
            slot.assertField(scenario, args, callback, bufferTagBaseline);
        }
    }

    private static void assertHeader(
        final String scenario, final RecordingLoggerEventCallback callback, final EventCode expectedCode)
    {
        final RecordingLoggerEventCallback.Header header = callback.header();
        assertNotNull(header, scenario + ": no header decoded");
        final int expectedEventType = (expectedCode.toEventCodeId() >>> 16) & 0xFFFF;
        assertEquals(expectedEventType, header.eventType(), scenario + ": eventType");
        assertEquals(expectedCode.id(), header.eventCode(), scenario + ": eventCode");
        assertEquals(expectedCode.name(), header.eventCodeName(), scenario + ": eventCodeName");
    }

    private static int codeIndexOf(final Method method, final List<Slot> slots)
    {
        for (final Slot slot : slots)
        {
            if (Kind.CODE == slot.kind)
            {
                return slot.indices[0];
            }
        }
        throw new IllegalStateException("No auto-detected event code parameter for " + method);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private static EventCode enumValueOf(final Class<?> enumClass, final String name)
    {
        return (EventCode)Enum.valueOf((Class<Enum>)(Class)enumClass, name);
    }

    // ------------------------------------------------------------------------------------------------------
    // Slot resolution: maps each method parameter (or buffer/offset/length parameter triple) onto a Slot that
    // knows how to generate nominal/variant values for it and how to assert its decoded value.
    // ------------------------------------------------------------------------------------------------------

    private static List<Slot> buildSlots(
        final Method method,
        final LoggerMethod annotation,
        final Parameter[] parameters,
        final Class<?> eventCodeClass)
    {
        final List<Slot> slots = new ArrayList<>();
        final boolean[] consumed = new boolean[parameters.length];

        if (annotation.eventCode().isEmpty())
        {
            int codeIndex = -1;
            for (int i = 0; i < parameters.length; i++)
            {
                if (parameters[i].getType() == eventCodeClass)
                {
                    codeIndex = i;
                    break;
                }
            }
            assertTrue(
                -1 != codeIndex,
                "Could not auto-detect the " + eventCodeClass.getSimpleName() + " parameter for " + method);

            consumed[codeIndex] = true;
            slots.add(new Slot(Kind.CODE, null, new int[]{ codeIndex }, null));
        }

        final String[] bufferView = annotation.bufferView();
        if (3 == bufferView.length)
        {
            final int bufIdx = indexOfParameterNamed(parameters, bufferView[0]);
            final int offIdx = indexOfParameterNamed(parameters, bufferView[1]);
            final int lenIdx = indexOfParameterNamed(parameters, bufferView[2]);
            assertTrue(
                -1 != bufIdx && -1 != offIdx && -1 != lenIdx,
                "bufferView " + Arrays.toString(bufferView) + " did not resolve to parameters of " + method);
            consumed[bufIdx] = true;
            consumed[offIdx] = true;
            consumed[lenIdx] = true;
            slots.add(new Slot(
                Kind.BUFFER_TRIPLE, parameters[bufIdx].getName(), new int[]{ bufIdx, offIdx, lenIdx }, null));
        }
        else
        {
            int candidate = -1;
            for (int i = 0; i < parameters.length; i++)
            {
                if (consumed[i])
                {
                    continue;
                }
                final Class<?> type = parameters[i].getType();
                if (DirectBuffer.class.isAssignableFrom(type) || ByteBuffer.class.isAssignableFrom(type))
                {
                    candidate = (-1 == candidate) ? i : -2;
                }
            }
            if (0 <= candidate)
            {
                consumed[candidate] = true;
                final Class<?> type = parameters[candidate].getType();
                final Kind kind = ByteBuffer.class.isAssignableFrom(type) ?
                    Kind.BUFFER_WHOLE_BYTE_BUFFER : Kind.BUFFER_WHOLE_DIRECT;
                slots.add(new Slot(kind, parameters[candidate].getName(), new int[]{ candidate }, null));
            }
        }

        for (int i = 0; i < parameters.length; i++)
        {
            if (!consumed[i])
            {
                slots.add(resolveFieldSlot(method, parameters[i], i));
            }
        }

        return slots;
    }

    private static int indexOfParameterNamed(final Parameter[] parameters, final String name)
    {
        for (int i = 0; i < parameters.length; i++)
        {
            if (parameters[i].getName().equals(name))
            {
                return i;
            }
        }
        return -1;
    }

    private static Slot resolveFieldSlot(final Method method, final Parameter parameter, final int index)
    {
        final Class<?> type = parameter.getType();
        final String name = parameter.getName();
        final int[] indices = { index };

        if (int.class == type)
        {
            return new Slot(Kind.NUMERIC_INT, name, indices, null);
        }
        if (long.class == type)
        {
            return new Slot(Kind.NUMERIC_LONG, name, indices, null);
        }
        if (short.class == type)
        {
            return new Slot(Kind.NUMERIC_SHORT, name, indices, null);
        }
        if (boolean.class == type)
        {
            return new Slot(Kind.BOOLEAN, name, indices, null);
        }
        if (String.class == type || CharSequence.class.isAssignableFrom(type))
        {
            return new Slot(Kind.STRING, name, indices, null);
        }
        if (InetAddress.class.isAssignableFrom(type))
        {
            return new Slot(Kind.INET, name, indices, null);
        }
        if (type.isEnum() || Enum.class == type)
        {
            return new Slot(Kind.ENUM, name, indices, type);
        }
        if (DirectBuffer.class.isAssignableFrom(type))
        {
            return new Slot(Kind.BUFFER_WHOLE_DIRECT, name, indices, null);
        }
        if (ByteBuffer.class.isAssignableFrom(type))
        {
            return new Slot(Kind.BUFFER_WHOLE_BYTE_BUFFER, name, indices, null);
        }

        throw new IllegalStateException(
            "Unsupported @LoggerMethod parameter type " + type.getName() + " on " + method);
    }

    private enum Kind
    {
        CODE, NUMERIC_INT, NUMERIC_LONG, NUMERIC_SHORT, BOOLEAN, STRING, INET, ENUM,
        BUFFER_TRIPLE, BUFFER_WHOLE_BYTE_BUFFER, BUFFER_WHOLE_DIRECT
    }

    private record Slot(Kind kind, String fieldName, int[] indices, Class<?> enumType)
    {
        void apply(final Object[] args, final Object[] variant)
        {
            for (int i = 0; i < indices.length; i++)
            {
                args[indices[i]] = variant[i];
            }
        }

        void applyNominal(final Object[] args, final EventCode[] codes)
        {
            switch (kind)
            {
                case CODE -> args[indices[0]] = codes[0];
                case NUMERIC_INT -> args[indices[0]] = 42;
                case NUMERIC_LONG -> args[indices[0]] = 424242L;
                case NUMERIC_SHORT -> args[indices[0]] = (short)7;
                case BOOLEAN -> args[indices[0]] = Boolean.TRUE;
                case STRING -> args[indices[0]] = "nominal-value";
                case INET -> args[indices[0]] = ipv4Address();
                case ENUM -> args[indices[0]] = enumConstantsFor(enumType)[0];
                case BUFFER_TRIPLE ->
                {
                    args[indices[0]] = paddedBuffer(BUFFER_LEADING_PADDING, 16, BUFFER_TRAILING_PADDING);
                    args[indices[1]] = BUFFER_LEADING_PADDING;
                    args[indices[2]] = 16;
                }
                case BUFFER_WHOLE_BYTE_BUFFER -> args[indices[0]] = paddedByteBuffer(16);
                case BUFFER_WHOLE_DIRECT -> args[indices[0]] = new UnsafeBuffer(fillPattern(16));
            }
        }

        List<Object[]> variants(final EventCode[] codes)
        {
            final List<Object[]> result = new ArrayList<>();
            switch (kind)
            {
                case CODE ->
                {
                    final LinkedHashSet<EventCode> chosen = new LinkedHashSet<>();
                    chosen.add(codes[0]);
                    chosen.add(codes[codes.length / 2]);
                    chosen.add(codes[codes.length - 1]);
                    for (final EventCode code : chosen)
                    {
                        result.add(new Object[]{ code });
                    }
                }
                case NUMERIC_INT ->
                {
                    for (final long value : new long[]{ 0, 1, -1, Integer.MAX_VALUE, Integer.MIN_VALUE })
                    {
                        result.add(new Object[]{ (int)value });
                    }
                }
                case NUMERIC_LONG ->
                {
                    for (final long value : new long[]{ 0, 1, -1, Long.MAX_VALUE, Long.MIN_VALUE })
                    {
                        result.add(new Object[]{ value });
                    }
                }
                case NUMERIC_SHORT ->
                {
                    for (final long value : new long[]{ 0, 1, -1, Short.MAX_VALUE, Short.MIN_VALUE })
                    {
                        result.add(new Object[]{ (short)value });
                    }
                }
                case BOOLEAN -> result.addAll(List.of(new Object[]{ true }, new Object[]{ false }));
                case STRING ->
                {
                    result.add(new Object[]{ null });
                    for (final String value : STRING_VARIANTS)
                    {
                        result.add(new Object[]{ value });
                    }
                }
                case INET -> result.addAll(List.of(
                    new Object[]{ null }, new Object[]{ ipv4Address() }, new Object[]{ ipv6Address() }));
                case ENUM ->
                {
                    final Object[] constants = enumConstantsFor(enumType);
                    result.add(new Object[]{ null });
                    result.add(new Object[]{ constants[0] });
                    if (1 < constants.length)
                    {
                        result.add(new Object[]{ constants[constants.length - 1] });
                    }
                }
                case BUFFER_TRIPLE ->
                {
                    for (final int length : BUFFER_LENGTHS)
                    {
                        final Object[] bufferTriple = {
                            paddedBuffer(BUFFER_LEADING_PADDING, length, BUFFER_TRAILING_PADDING),
                            BUFFER_LEADING_PADDING,
                            length
                        };

                        result.add(bufferTriple);
                    }
                }
                case BUFFER_WHOLE_BYTE_BUFFER ->
                {
                    for (final int length : BUFFER_LENGTHS)
                    {
                        result.add(new Object[]{ paddedByteBuffer(length) });
                    }
                }
                case BUFFER_WHOLE_DIRECT ->
                {
                    for (final int length : BUFFER_LENGTHS)
                    {
                        result.add(new Object[]{ new UnsafeBuffer(fillPattern(length)) });
                    }
                }
            }
            return result;
        }

        void assertField(
            final String scenario,
            final Object[] args,
            final RecordingLoggerEventCallback callback,
            final Map<String, Long> bufferTagBaseline)
        {
            if (Kind.CODE == kind)
            {
                return;
            }

            final String ctx = scenario + " field '" + fieldName + "'";
            final RecordingLoggerEventCallback.Value actual = callback.values().get(fieldName);
            assertNotNull(actual, ctx + ": no value decoded");

            switch (kind)
            {
                case NUMERIC_INT, NUMERIC_LONG, NUMERIC_SHORT ->
                {
                    assertEquals(RecordingLoggerEventCallback.Kind.NUMBER, actual.kind(), ctx);
                    assertEquals(((Number)args[indices[0]]).longValue(), actual.numberValue(), ctx);
                    assertEquals(NO_TAG, actual.tag(), ctx);
                }
                case BOOLEAN ->
                {
                    assertEquals(RecordingLoggerEventCallback.Kind.BOOLEAN, actual.kind(), ctx);
                    assertEquals(args[indices[0]], actual.booleanValue(), ctx);
                    assertEquals(NO_TAG, actual.tag(), ctx);
                }
                case STRING ->
                {
                    final Object value = args[indices[0]];
                    assertNullableText(ctx, actual, null == value, null == value ? null : value.toString(), NO_TAG);
                }
                case ENUM ->
                {
                    final Object value = args[indices[0]];
                    assertNullableText(
                        ctx, actual, null == value, null == value ? null : ((Enum<?>)value).name(), ENUM_TAG);
                }
                case INET ->
                {
                    final InetAddress address = (InetAddress)args[indices[0]];
                    if (null == address)
                    {
                        assertEquals(RecordingLoggerEventCallback.Kind.NULL, actual.kind(), ctx);
                        assertEquals(NO_TAG, actual.tag(), ctx);
                    }
                    else
                    {
                        assertEquals(RecordingLoggerEventCallback.Kind.BYTES, actual.kind(), ctx);
                        assertArrayEquals(address.getAddress(), actual.bytesValue(), ctx);
                        assertEquals(
                            address instanceof Inet6Address ? IPV6_TAG : IPV4_TAG, actual.tag(), ctx);
                    }
                }
                case BUFFER_TRIPLE -> assertBuffer(ctx, tripleBytes(args, indices), actual, bufferTagBaseline);
                case BUFFER_WHOLE_BYTE_BUFFER ->
                    assertBuffer(ctx, byteBufferBytes((ByteBuffer)args[indices[0]]), actual, bufferTagBaseline);
                case BUFFER_WHOLE_DIRECT ->
                    assertBuffer(ctx, directBufferBytes((DirectBuffer)args[indices[0]]), actual, bufferTagBaseline);
                default -> throw new IllegalStateException("Unexpected kind " + kind);
            }
        }

        private void assertBuffer(
            final String ctx,
            final byte[] expectedBytes,
            final RecordingLoggerEventCallback.Value actual,
            final Map<String, Long> bufferTagBaseline)
        {
            assertEquals(RecordingLoggerEventCallback.Kind.BYTES, actual.kind(), ctx);
            assertArrayEquals(expectedBytes, actual.bytesValue(), ctx);

            // @Tag is source-retention only, so the specific tag used for a raw buffer field can't be
            // independently derived by reflection here. Instead pin the tag observed on the first invocation
            // of this field and require every subsequent variant of the same field to keep using it.
            final Long baseline = bufferTagBaseline.putIfAbsent(fieldName, actual.tag());
            if (null != baseline)
            {
                assertEquals(baseline.longValue(), actual.tag(), ctx + ": tag changed between invocations");
            }
        }
    }

    private static void assertNullableText(
        final String ctx,
        final RecordingLoggerEventCallback.Value actual,
        final boolean expectedNull,
        final String expectedText,
        final long tagIfPresent)
    {
        if (expectedNull)
        {
            assertEquals(RecordingLoggerEventCallback.Kind.NULL, actual.kind(), ctx);
            assertEquals(NO_TAG, actual.tag(), ctx);
        }
        else
        {
            assertEquals(RecordingLoggerEventCallback.Kind.STRING, actual.kind(), ctx);
            assertEquals(expectedText, actual.stringValue(), ctx);
            assertEquals(tagIfPresent, actual.tag(), ctx);
        }
    }

    // ------------------------------------------------------------------------------------------------------
    // Value construction helpers.
    // ------------------------------------------------------------------------------------------------------

    private static Object[] enumConstantsFor(final Class<?> declaredType)
    {
        if (Enum.class == declaredType)
        {
            return ProbeEnum.values();
        }
        return declaredType.getEnumConstants();
    }

    private static String repeat(final char c, final int length)
    {
        return String.valueOf(c).repeat(length);
    }

    private static byte[] fillPattern(final int length)
    {
        final byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++)
        {
            bytes[i] = (byte)(i + 1);
        }
        return bytes;
    }

    private static UnsafeBuffer paddedBuffer(final int leadingPadding, final int length, final int trailingPadding)
    {
        final byte[] backing = new byte[leadingPadding + length + trailingPadding];
        Arrays.fill(backing, (byte)0xEE);
        System.arraycopy(fillPattern(length), 0, backing, leadingPadding, length);
        return new UnsafeBuffer(backing);
    }

    private static ByteBuffer paddedByteBuffer(final int length)
    {
        final byte[] backing = new byte[BUFFER_LEADING_PADDING + length + BUFFER_TRAILING_PADDING];
        Arrays.fill(backing, (byte)0xEE);
        System.arraycopy(fillPattern(length), 0, backing, BUFFER_LEADING_PADDING, length);
        final ByteBuffer buffer = ByteBuffer.wrap(backing);
        buffer.position(BUFFER_LEADING_PADDING);
        buffer.limit(BUFFER_LEADING_PADDING + length);
        return buffer;
    }

    private static byte[] tripleBytes(final Object[] args, final int[] indices)
    {
        final DirectBuffer buffer = (DirectBuffer)args[indices[0]];
        final int offset = (Integer)args[indices[1]];
        final int length = (Integer)args[indices[2]];
        final byte[] expected = new byte[length];
        buffer.getBytes(offset, expected, 0, length);
        return expected;
    }

    private static byte[] byteBufferBytes(final ByteBuffer buffer)
    {
        final ByteBuffer duplicate = buffer.duplicate();
        final byte[] expected = new byte[duplicate.remaining()];
        duplicate.get(expected);
        return expected;
    }

    private static byte[] directBufferBytes(final DirectBuffer buffer)
    {
        final byte[] expected = new byte[buffer.capacity()];
        buffer.getBytes(0, expected);
        return expected;
    }

    private static InetAddress ipv4Address()
    {
        try
        {
            return InetAddress.getByAddress(new byte[]{ 10, 20, 30, 40 });
        }
        catch (final UnknownHostException ex)
        {
            throw new RuntimeException(ex);
        }
    }

    private static InetAddress ipv6Address()
    {
        try
        {
            return InetAddress.getByAddress(new byte[]{
                (byte)0xfe, (byte)0x80, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 });
        }
        catch (final UnknownHostException ex)
        {
            throw new RuntimeException(ex);
        }
    }
}
