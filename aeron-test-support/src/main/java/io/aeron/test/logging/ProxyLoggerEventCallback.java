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
package io.aeron.test.logging;

import io.aeron.logging.LoggerEventCallback;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

public class ProxyLoggerEventCallback implements LoggerEventCallback
{
    private final LoggerEventCallback delegate;

    public ProxyLoggerEventCallback(final LoggerEventCallback delegate)
    {
        this.delegate = delegate;
    }

    public void onHeader(
        final int eventType,
        final int eventCode,
        final CharSequence eventCodeName,
        final long timestamp)
    {
        this.delegate.onHeader(eventType, eventCode, eventCodeName.toString(), timestamp);
    }

    public void onValue(final CharSequence name, final long tags, final CharSequence value)
    {
        if (value == null)
        {
            this.delegate.onValue(name.toString(), tags, (CharSequence)null);
            return;
        }

        this.delegate.onValue(name.toString(), tags, value.toString());
    }

    public void onValue(final CharSequence name, final long tags, final long value)
    {
        this.delegate.onValue(name.toString(), tags, value);
    }

    public void onValue(final CharSequence name, final long tags, final boolean value)
    {
        this.delegate.onValue(name.toString(), tags, value);
    }

    public void onValue(final CharSequence name, final long tags, final DirectBuffer value)
    {
        if (value == null)
        {
            this.delegate.onValue(name.toString(), tags, (DirectBuffer)null);
            return;
        }

        final byte[] copy = new byte[value.capacity()];
        value.getBytes(0, copy);
        this.delegate.onValue(name.toString(), tags, new UnsafeBuffer(copy));
    }

    public void onFooter(final boolean truncated)
    {
        this.delegate.onFooter(truncated);
    }
}
