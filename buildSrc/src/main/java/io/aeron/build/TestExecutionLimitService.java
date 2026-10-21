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

import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;

/**
 * Shared by every {@code Test} task in the build to limit how many run at the same time. Aeron's tests start media
 * drivers and share Aeron directories and ports, so by default only one test task runs at a time, even when Gradle
 * runs other tasks in parallel (as it does with the configuration cache enabled).
 */
public abstract class TestExecutionLimitService implements BuildService<BuildServiceParameters.None>
{
}
