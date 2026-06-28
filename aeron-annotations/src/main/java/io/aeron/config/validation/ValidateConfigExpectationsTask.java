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
package io.aeron.config.validation;

import io.aeron.utility.ElementIO;

/**
 * A gradle task for validating the C code looks as expected, based on the contents of the @Config java annotations.
 */
public final class ValidateConfigExpectationsTask
{
    private ValidateConfigExpectationsTask()
    {
    }

    /**
     * Launch task to validate config expectations.
     *
     * @param args Arg 0 should be the location of a config-info.xml file with a list of ConfigInfo objects. Arg 1
     *             should be the location of the C source code
     * @throws Exception on IO failure.
     */
    public static void main(final String[] args) throws Exception
    {
        Validator.validate(ElementIO.read(args[0]), args[1]).printFailuresOn(System.err);
    }
}
