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
package io.aeron.driver;

/**
 * Delay generator that simply returns a constant value for the delay and the retry delay.
 */
public final class StaticDelayGenerator implements FeedbackDelayGenerator
{
    /**
     * {@link StaticDelayGenerator} instance with zero delay.
     */
    public static final StaticDelayGenerator ZERO_DELAY_GENERATOR = new StaticDelayGenerator(0);

    private final long delayInNs;
    private final long retryDelayNs;

    /**
     * Create a delayInNs generator that uses the specified delayNs.
     *
     * @param delayNs      initial delay (nanoseconds)
     * @param retryDelayNs delay for retry (nanoseconds)
     */
    public StaticDelayGenerator(final long delayNs, final long retryDelayNs)
    {
        this.delayInNs = delayNs;
        this.retryDelayNs = retryDelayNs;
    }

    /**
     * Create a delayInNs generator that uses the specified delayNs for both initial and retry delays.
     *
     * @param delayNs      delay (nanoseconds)
     */
    public StaticDelayGenerator(final long delayNs)
    {
        this(delayNs, delayNs);
    }

    /**
     * Deprecated constructor.
     *
     * @param delayNs           delay (nanoseconds)
     * @param immediateFeedback immediately feedback (unused)
     * @deprecated The <code>shouldImmediatelyFeedback</code> method has been removed from the interface so the
     * <code>immediatelyFeedback</code> parameter is ignored. You can emulate the old behaviour with:
     * <pre>
     *     new StaticDelayGenerator(0, MILLISECONDS.toNanos(10));
     * </pre>
     * If used for unicast delays, then this is not recommended and a short delay like the default
     * {@link Configuration#NAK_UNICAST_DELAY_DEFAULT_NS} should be used.
     */
    @Deprecated
    public StaticDelayGenerator(final long delayNs, @SuppressWarnings("unused") final boolean immediateFeedback)
    {
        this(delayNs, delayNs);
    }

    /**
     * {@inheritDoc}
     */
    public long generateDelayNs()
    {
        return delayInNs;
    }

    /**
     * {@inheritDoc}
     */
    public long retryDelayNs()
    {
        return retryDelayNs;
    }

    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return "StaticDelayGenerator{" +
            "delayInNs=" + delayInNs +
            ", retryDelayNs=" + retryDelayNs +
            '}';
    }
}
