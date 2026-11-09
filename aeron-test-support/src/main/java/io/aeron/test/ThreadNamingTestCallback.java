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
import org.junit.jupiter.api.extension.AfterTestExecutionCallback;
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.reflect.Method;

public class ThreadNamingTestCallback
    implements BeforeTestExecutionCallback, AfterTestExecutionCallback, AfterEachCallback
{
    private final ThreadLocal<String> oldThreadName = new ThreadLocal<>();
    {
        oldThreadName.set("UNKNOWN");
    }

    @Override
    public void beforeTestExecution(final ExtensionContext context) throws Exception
    {
        final String className = context.getTestClass().map(Class::getSimpleName).orElse("UNKNOWN");
        final String methodName = context.getTestMethod().map(Method::getName).orElse("UNKNOWN");
        final Thread thread = Thread.currentThread();
        final String existingThreadName = thread.getName();
        final String testThreadName = "TEST::" + className + "." + methodName;
        oldThreadName.set(existingThreadName);
        thread.setName(testThreadName);
    }

    @Override
    public void afterEach(final ExtensionContext context) throws Exception
    {
        Thread.currentThread().setName(oldThreadName.get());
        oldThreadName.remove();
    }

    @Override
    public void afterTestExecution(final ExtensionContext context) throws Exception
    {
        final Thread thread = Thread.currentThread();
        thread.setName(thread.getName() + ":tearDown");
    }
}
