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
package io.aeron.build;

import org.gradle.api.artifacts.Configuration;
import org.gradle.api.attributes.Category;
import org.gradle.api.attributes.TestSuiteName;
import org.gradle.api.attributes.VerificationType;
import org.gradle.api.model.ObjectFactory;

/**
 * Selects the {@code testResultsElementsForTest} variant that the {@code java} plugin exposes for every project: the
 * binary results of its {@code test} task. Used to aggregate test results without reaching into other projects' tasks.
 */
public final class TestResultsVariant
{
    private TestResultsVariant()
    {
    }

    /**
     * Make a resolvable configuration select the {@code test} results of exactly the projects it depends on, not of
     * their dependencies.
     *
     * @param configuration to request the test results with.
     * @param objects       to create attribute values with.
     */
    public static void request(final Configuration configuration, final ObjectFactory objects)
    {
        configuration.setTransitive(false);
        configuration.attributes((attributes) ->
        {
            attributes.attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.class, Category.VERIFICATION));
            attributes.attribute(TestSuiteName.TEST_SUITE_NAME_ATTRIBUTE, objects.named(TestSuiteName.class, "test"));
            attributes.attribute(
                VerificationType.VERIFICATION_TYPE_ATTRIBUTE,
                objects.named(VerificationType.class, VerificationType.TEST_RESULTS));
        });
    }
}
