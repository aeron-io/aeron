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
package io.aeron.build;

import org.gradle.api.Project;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.testing.Test;

import java.util.Collections;

/**
 * Generic Gradle task for running logging tests.
 */
public abstract class LoggingTest extends Test
{
    private String testClassName;

    public LoggingTest()
    {
        // Fixed/shared values for all logging tests
        setGroup("verification");
        setForkEvery(1L);
        useJUnitPlatform(options -> options.includeTags("logging"));
        systemProperty("aeron.event.log.reader.classname",
            "io.aeron.test.agent.CountingEventReaderAgent");
    }

    /**
     * Returns the simple name of the test class this task runs.
     *
     * @return the test class name.
     */
    @Input
    public String getTestClassName()
    {
        return testClassName;
    }

    /**
     * Sets the test class this task should run
     *
     * @param testClassName name of the test class.
     */
    public void setTestClassName(final String testClassName)
    {
        this.testClassName = testClassName;
        setIncludes(Collections.singleton("**/" + testClassName + ".class"));
    }

    /**
     * Registers the four logging test variants (all/none/disabled/configured) for the module,
     * and also chains everything to the loggingTest task
     *
     * @param targetProject     project to register the tasks in.
     * @param testClass         name of the test classes.
     * @param enabledEventsKey  system property key used to enable events for this module.
     * @param disabledEventsKey system property key used to disable specific events for this module.
     * @param eventSubset       comma-separated event codes used for configuration
     */
    public static void configureLoggingTests(final Project targetProject, final String testClass,
        final String enabledEventsKey, final String disabledEventsKey, final String eventSubset)
    {
        targetProject.getTasks().register("loggingTestAll", LoggingTest.class, task ->
        {
            task.setTestClassName(testClass);
            task.systemProperty(enabledEventsKey, "all");
        });
        targetProject.getTasks().register("loggingTestNone", LoggingTest.class, task ->
        {
            task.setTestClassName(testClass);
            task.systemProperty(enabledEventsKey, "none");
        });
        targetProject.getTasks().register("loggingTestDisabled", LoggingTest.class, task ->
        {
            task.setTestClassName(testClass);
            task.systemProperty(enabledEventsKey, "all");
            task.systemProperty(disabledEventsKey, eventSubset);
        });
        targetProject.getTasks().register("loggingTestConfigured", LoggingTest.class, task ->
        {
            task.setTestClassName(testClass);
            task.systemProperty(enabledEventsKey, eventSubset);
        });
        targetProject.getTasks().register("loggingTest", task ->
            task.dependsOn(targetProject.getTasks().withType(LoggingTest.class)));
        targetProject.getTasks().named("check").configure(task ->
            task.dependsOn(targetProject.getTasks().named("loggingTest")));
    }
}
