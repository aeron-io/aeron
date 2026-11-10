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

import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.util.Random;

public class RandomWatcher implements TestWatcher
{
    private final Random random;
    private final long seed;

    public RandomWatcher(final long seed)
    {
        this.seed = seed;
        random = new Random(seed);
    }

    public RandomWatcher()
    {
        this(System.nanoTime());
    }

    @Override
    public void testFailed(final ExtensionContext context, final Throwable cause)
    {
        System.err.println(context.getDisplayName() + " failed with random seed: " + seed);
    }

    public Random random()
    {
        return random;
    }
}
