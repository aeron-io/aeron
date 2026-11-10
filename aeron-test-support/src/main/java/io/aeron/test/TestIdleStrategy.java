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

    @Override
    public void idle(final int i)
    {
        if (i == 0)
        {
            idle();
        }
    }

    @Override
    public void idle()
    {
        Tests.yield();
    }

    @Override
    public void reset()
    {

    }
}
