package io.aeron.agent;

public interface LoggerEventCallback
{
    void onHeader(final int eventType, final int eventCode, final long timestamp);

    void onValue(final CharSequence name, final CharSequence value);

    void onValue(final CharSequence name, final long value);

    void onFooter(final boolean truncated);
}
