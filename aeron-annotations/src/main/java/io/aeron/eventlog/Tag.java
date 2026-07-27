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
package io.aeron.eventlog;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Overrides the CBOR tag {@code CborEventLoggerProcessor} would otherwise infer from a
 * {@link LoggerMethod}-annotated method parameter's Java type. Leave a parameter unannotated to use the
 * inferred default (e.g. the enum tag for enum-typed parameters, the IPv4/IPv6 tag for address-typed
 * parameters); apply {@code @Tag} to state a specific tag explicitly, such as an application-specific tag
 * identifying what kind of payload a raw buffer parameter carries.
 */
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.SOURCE)
public @interface Tag
{
    /**
     * The CBOR tag to use when encoding this parameter, an ordinary compile-time constant expression (e.g.
     * a {@code io.aeron.logging.CborUtils} tag constant), resolved by the Java compiler at the annotation
     * call site, not by the annotation processor.
     *
     * @return the tag value.
     */
    long value();
}
