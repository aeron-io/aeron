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
package io.aeron.eventlog;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares how a {@link GeneratedLogger}-annotated interface method's ring-buffer claim/encode/commit
 * body should be generated. Every attribute is either a name resolved against real, already-compiled
 * elements (and validated as such) or an ordinary compile-time constant expression - never a raw code
 * snippet pasted verbatim.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.SOURCE)
public @interface LoggerMethod
{
    /**
     * Simple name of the event-code enum constant to use, e.g. {@code "CANVASS_POSITION"}. Leave blank
     * to auto-detect the event code from this method's sole parameter of type
     * {@link GeneratedLogger#eventCodeType()} (e.g. a method that takes the event code as an argument).
     *
     * @return the event code constant name, or empty string to auto-detect from a parameter.
     */
    String eventCode() default "";

    /**
     * Bare (unqualified) static method name on {@link GeneratedLogger#encoder()} used to compute the
     * encoded length. Mutually exclusive with {@link #fixedLength()}; exactly one must be set.
     *
     * @return the length method name, or empty string if {@link #fixedLength()} is used instead.
     */
    String lengthMethod() default "";

    /**
     * Bare (unqualified) static method name on {@link GeneratedLogger#encoder()} used to encode the event.
     * Leave blank to derive it by convention from this method's own name (a {@code logXxx} method derives
     * {@code encodeXxx}); set explicitly only when the encoder's method name genuinely doesn't follow that
     * convention.
     *
     * @return the encode method name, or empty string to derive it from this method's name.
     */
    String encodeMethod() default "";

    /**
     * Ordered list of this method's own parameter names to forward to {@link #lengthMethod()}. Each
     * name is validated against this method's real parameter list at compile time. Empty means
     * {@link #lengthMethod()} is a zero-arg method.
     *
     * @return the parameter names, in call order.
     */
    String[] lengthArgs() default {};

    /**
     * A compile-time constant length (e.g. {@code 3 * SIZE_OF_LONG + 3 * SIZE_OF_INT}), used when the
     * length has no parameter dependency and no dedicated encoder-side length method exists. Resolved
     * by the Java compiler at the annotation call site, not by the annotation processor. Mutually
     * exclusive with {@link #lengthMethod()}; exactly one must be set.
     *
     * @return the fixed length, or -1 if {@link #lengthMethod()} is used instead.
     */
    int fixedLength() default -1;

    /**
     * When {@code true}, {@code captureLength} is set equal to {@code length} directly instead of being
     * computed via {@code CommonEventEncoder.captureLength(length)}.
     *
     * @return whether to skip the capture-length computation.
     */
    boolean skipCaptureLength() default false;

    /**
     * Ordered list of this method's own parameter names to forward to the {@code encodeXxx} method
     * (after the fixed {@code buffer, offset, captureLength, length} prefix). Each name is validated
     * against this method's real parameter list at compile time. Empty means "this method's own
     * parameters, in declared order" - the common case; set explicitly only when the encoder's
     * expected argument order genuinely differs from the logger method's declared parameter order.
     *
     * @return the parameter names, in call order, or empty to use declared parameter order.
     */
    String[] encodeArgs() default {};
}
