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
import org.gradle.api.attributes.Bundling;
import org.gradle.api.attributes.Category;
import org.gradle.api.attributes.VerificationType;
import org.gradle.api.model.ObjectFactory;

/**
 * Selects the {@code mainSourceElements} variant that the {@code java} plugin exposes for every project: the source
 * directories of its {@code main} source set, including generated ones. Used to aggregate other projects' sources
 * without reaching into their source sets.
 */
public final class MainSourcesVariant
{
    private MainSourcesVariant()
    {
    }

    /**
     * Make a resolvable configuration select the main source directories of exactly the projects it depends on, not of
     * their dependencies.
     *
     * @param configuration to request the main source directories with.
     * @param objects       to create attribute values with.
     */
    public static void request(final Configuration configuration, final ObjectFactory objects)
    {
        configuration.setTransitive(false);
        configuration.attributes((attributes) ->
        {
            attributes.attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.class, Category.VERIFICATION));
            attributes.attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.class, Bundling.EXTERNAL));
            attributes.attribute(
                VerificationType.VERIFICATION_TYPE_ATTRIBUTE,
                objects.named(VerificationType.class, VerificationType.MAIN_SOURCES));
        });
    }
}
