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
package io.aeron.test;

import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.PrintStream;

public class HideStdErrExtension implements BeforeEachCallback, AfterEachCallback
{
    private final PrintStream nullPrintStream = new PrintStream(new NullOutputStream());
    private PrintStream originalStream = null;

    @Override
    public void beforeEach(final ExtensionContext context)
    {
        if (context.getTestMethod().isPresent() &&
            null != context.getTestMethod().get().getAnnotation(IgnoreStdErr.class))
        {
            originalStream = System.err;
            System.setErr(nullPrintStream);
        }
    }

    @Override
    public void afterEach(final ExtensionContext context)
    {
        if (null != originalStream)
        {
            System.setErr(originalStream);
            originalStream = null;
        }
    }
}
