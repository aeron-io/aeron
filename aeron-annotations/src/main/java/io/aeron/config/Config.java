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
package io.aeron.config;

import java.lang.annotation.*;
import java.util.concurrent.TimeUnit;

/**
 * Annotation to mark fields and methods that together describe a single Aeron configuration
 * option, and to record the expected corresponding definitions in the C driver.
 *
 * <h2>How a config option is assembled</h2>
 * <p>A single logical config option is typically spread across two or three Java elements, all
 * tied together by a shared {@link #id()} (derived automatically from the field name when not
 * set explicitly):</p>
 * <ol>
 *   <li>A {@code static final String} field whose name ends in {@code _PROP_NAME} holds the
 *       Java system-property key (e.g. {@code "aeron.dir"}).
 *       {@link Type#PROPERTY_NAME}</li>
 *   <li>A {@code static final} field whose name ends in {@code _DEFAULT} or {@code _DEFAULT_NS}
 *       holds the default value.
 *       {@link Type#DEFAULT}</li>
 *   <li>Optionally, a method on a {@code Context} class provides the runtime accessor.  Placing
 *       {@code @Config} on the method records the context class and method name in the
 *       documentation.</li>
 * </ol>
 *
 * <h2>C driver cross-referencing</h2>
 * <p>The C driver exposes each config option as a pair of {@code #define}s:</p>
 * <ul>
 *   <li>An env-var name define, e.g. {@code AERON_DIR_ENV_VAR "AERON_DIR"}</li>
 *   <li>A default-value define, e.g. {@code AERON_DIR_DEFAULT "/dev/shm/aeron"}</li>
 * </ul>
 * <p>The processor derives expected C names automatically from the Java system-property key
 * (upper-cased, dots replaced with underscores).  The {@code expectedC*} attributes let you
 * override the derived names when the C code uses a non-standard convention.
 * Set {@link #existsInC()} to {@code false} for Java-only options that have no C equivalent,
 * and {@link #existsInJava()} to {@code false} for C-only options.</p>
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface Config
{
    /**
     * Type is used to indicate whether the annotation is marking a property name or a default value.
     */
    enum Type
    {
        /**
         * Undefined.
         */
        UNDEFINED,
        /**
         * Property.
         */
        PROPERTY_NAME,
        /**
         * Default value.
         */
        DEFAULT
    }

    /**
     * What type of field is being annotated.
     *
     * @return what type of field is being annotated.
     */
    Type configType() default Type.UNDEFINED;

    /**
     * The unique id that ties together all the usages of the annotation across fields/methods.
     *
     * @return the unique id that ties together all the usages of the annotation across fields/methods.
     */
    String id() default "";

    /**
     * The uri parameter (if any) associated with this option.
     *
     * @return the uri parameter (if any) associated with this option.
     */
    String uriParam() default "";

    /**
     * Whether this config option exists in the Java code.
     *
     * @return whether this config option exists in the Java code.
     */
    boolean existsInJava() default true;

    /**
     * Whether this config option exists in the C code.
     *
     * @return whether this config option exists in the C code.
     */
    boolean existsInC() default true;

    /**
     * The expected C #define name that will be set with the env variable name for this option.
     *
     * @return the expected C #define name that will be set with the env variable name for this option.
     */
    String expectedCEnvVarFieldName() default "";

    /**
     * The expected C env variable name for this option.
     *
     * @return the expected C env variable name for this option.
     */
    String expectedCEnvVar() default "";

    /**
     * The expected C #define name that will be set with the default value for this option.
     *
     * @return the expected C #define name that will be set with the default value for this option.
     */
    String expectedCDefaultFieldName() default "";

    /**
     * The expected C default value for this option.
     *
     * @return the expected C default value for this option.
     */
    String expectedCDefault() default "";

    /**
     * Whether to skip validation of the default in C.
     *
     * @return whether to skip validation of the default in C.
     */
    boolean skipCDefaultValidation() default false;

    /**
     * What's the type of default (string, int, etc...).
     *
     * @return what's the type of default (string, int, etc...).
     */
    DefaultType defaultType() default DefaultType.UNDEFINED;

    /**
     * Specify the default boolean, if defaultType is BOOLEAN.
     *
     * @return specify the default boolean, if defaultType is BOOLEAN.
     */
    boolean defaultBoolean() default false;

    /**
     * Specify the default int, if defaultType is INT.
     *
     * @return specify the default int, if defaultType is INT.
     */
    int defaultInt() default 0;

    /**
     * Specify the default long, if defaultType is LONG.
     *
     * @return specify the default long, if defaultType is LONG.
     */
    long defaultLong() default 0;

    /**
     * Specify the default double, if defaultType is DOUBLE.
     *
     * @return specify the default double, if defaultType is DOUBLE.
     */
    double defaultDouble() default 0.0;

    /**
     * Specify the default string, if defaultType is STRING.
     *
     * @return specify the default string, if defaultType is STRING.
     */
    String defaultString() default "";

    /**
     * Specify a string that acts as a stand-in for the default value when generating documentation.
     *
     * @return specify a string that acts as a stand-in for the default value when generating documentation.
     */
    String defaultValueString() default "";

    /**
     * Used to indicate whether the default value is a time value.
     */
    enum IsTimeValue
    {
        /**
         * Undefined.
         */
        UNDEFINED,
        /**
         * True.
         */
        TRUE,
        /**
         * False.
         */
        FALSE
    }

    /**
     * Whether the default value is a time value.
     *
     * @return whether the default value is a time value.
     */
    IsTimeValue isTimeValue() default IsTimeValue.UNDEFINED;

    /**
     * The time unit if the default value is a time value of some sort.
     *
     * @return the time unit if the default value is a time value of some sort.
     */
    TimeUnit timeUnit() default TimeUnit.NANOSECONDS;

    /**
     * Whether this config option has a 'context'.
     *
     * @return whether this config option has a 'context'.
     */
    boolean hasContext() default true;
}
