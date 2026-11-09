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

import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

public final class CountingFragmentHandler implements FragmentHandler
{
    private final String name;
    private int lastCheckedTargetValue = 0;
    private int received = 0;

    public CountingFragmentHandler(final String name)
    {
        this.name = name;
    }

    public boolean notDone(final int targetValue)
    {
        lastCheckedTargetValue = targetValue;
        return targetValue != received;
    }

    @Override
    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        received++;
    }

    @Override
    public String toString()
    {
        return "CountingFragmentHandler{" +
            "name='" + name + '\'' +
            ", received=" + received +
            ", lastCheckedTargetValue=" + lastCheckedTargetValue +
            '}';
    }
}
