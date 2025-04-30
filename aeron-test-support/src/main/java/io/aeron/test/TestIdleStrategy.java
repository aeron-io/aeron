/*
 * Copyright 2024-2025 Adaptive Financial Consulting Limited.
 */
package io.aeron.test;

import org.agrona.concurrent.IdleStrategy;

public class TestIdleStrategy implements IdleStrategy
{
    /**
     * Static instance to reduce allocation.
     */
    public static final TestIdleStrategy INSTANCE = new TestIdleStrategy();

    public void idle(final int i)
    {
        if (i == 0)
        {
            idle();
        }
    }

    public void idle()
    {
        Tests.yield();
    }

    public void reset()
    {

    }
}
