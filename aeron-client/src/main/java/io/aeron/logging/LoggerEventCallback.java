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
package io.aeron.logging;

/**
 * Primary interface that will receive data from a CBOR logged message. Will callback the appropriate
 * methods on each data item for a single message.
 * <code>
 *     onHeader(...);
 *     onValue(...);  // 0-n times
 *     onFooter(...);
 * </code>
 */
public interface LoggerEventCallback
{
    /**
     * Header of the message being decoded.
     *
     * @param eventType of {@link EventCodeType}.
     * @param eventCode of the specific event type, will be the id of the specific event code.
     * @param timestamp of the event.
     */
    void onHeader(int eventType, int eventCode, long timestamp);

    /**
     * A string value of the logging event. Could refer to a string or an enum value.
     *
     * @param name  of the event, note that the supplied {@link CharSequence} is reused across multiple calls, it
     *              is the user's responsibility to copy this value if required as the value is only valid for the
     *              scope of the call.
     * @param tags  of the event, note that the supplied {@link CharSequence} is reused across multiple calls, it
     *              is the user's responsibility to copy this value if required as the value is only valid for the
     *              scope of the call.
     * @param value of the event, note that the supplied {@link CharSequence} is reused across multiple calls, it
     *              is the user's responsibility to copy this value if required as the value is only valid for the
     *              scope of the call.
     */
    void onValue(CharSequence name, long tags, CharSequence value);

    /**
     * A number value of the logging event. Could be of any numeric length (e.g. byte, short, int, long).
     *
     * @param name  of the event, note that the supplied {@link CharSequence} is reused across multiple calls, it
     *              is the user's responsibility to copy this value if required as the value is only valid for the
     *              scope of the call.
     * @param tags  of the event.
     * @param value of the event.
     */
    void onValue(CharSequence name, long tags, long value);

    /**
     * A boolean value of the logging event.
     *
     * @param name  of the event, note that the supplied {@link CharSequence} is reused across multiple calls, it
     *              is the user's responsibility to copy this value if required as the value is only valid for the
     *              scope of the call.
     * @param tags  of the event.
     * @param value of the event.
     */
    void onValue(CharSequence name, long tags, boolean value);

    /**
     * Indicates that the message is complete, and will determine if any fields were dropped due to truncation.
     *
     * @param truncated if this message was truncated when writing.
     */
    void onFooter(boolean truncated);
}
