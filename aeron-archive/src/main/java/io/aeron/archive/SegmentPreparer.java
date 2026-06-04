/*
 * Copyright 2026 Adaptive Financial Consulting Limited.
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

/**
 * Accepts {@link SegmentPrepareRequest}s from a {@link RecordingWriter} so the next recording segment file can
 * be pre-created off the recording thread. Implementations may execute asynchronously; the result is observed
 * through the request's state.
 */
@FunctionalInterface
interface SegmentPreparer
{
    /**
     * Request preparation of the next recording segment file.
     *
     * @param request describing the segment file to prepare.
     */
    void prepareSegment(SegmentPrepareRequest request);
}
