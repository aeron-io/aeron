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
package io.aeron.archive;

import org.agrona.ErrorHandler;

import java.io.File;
import java.util.ArrayDeque;
import java.util.function.BooleanSupplier;

class GatedDeleteSegmentsSession extends DeleteSegmentsSession
{
    private final BooleanSupplier gate;

    private boolean gatePassed = false;

    GatedDeleteSegmentsSession(
        final long recordingId,
        final long correlationId,
        final ArrayDeque<File> files,
        final ControlSession controlSession,
        final ErrorHandler errorHandler,
        final BooleanSupplier gate)
    {
        super(recordingId, correlationId, files, controlSession, errorHandler);
        this.gate = gate;
    }

    /**
     * {@inheritDoc}
     */
    public int doWork()
    {
        if (!gatePassed)
        {
            gatePassed = gate.getAsBoolean();

            if (!gatePassed)
            {
                return 0;
            }
        }

        return super.doWork();
    }
}
