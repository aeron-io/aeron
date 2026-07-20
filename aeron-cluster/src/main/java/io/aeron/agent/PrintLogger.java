package io.aeron.agent;

import java.io.PrintStream;

public class PrintLogger implements LoggerEventCallback
{
    private final PrintStream out;

    public PrintLogger(final PrintStream out)
    {
        this.out = out;
    }

    public void onHeader(final int eventType, final int eventCode, final long timestamp)
    {
        out.print("[");
        out.print(timestamp);
        out.print("] ");
        out.print(" [");
        out.print(eventCode);
        out.print("]");
    }

    public void onValue(final CharSequence name, final CharSequence value)
    {
        out.print(" ");
        out.print(name);
        out.print("=");
        out.print(value);
    }

    public void onValue(final CharSequence name, final long value)
    {
        out.print(" ");
        out.print(name);
        out.print("=");
        out.print(value);
    }

    public void onFooter(final boolean truncated)
    {
        out.flush();
    }
}
