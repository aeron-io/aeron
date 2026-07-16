package io.aeron.agent;

public interface EventLogCallback
{
    void onHeader(
        final long timestamp,
        final EventCodeType eventCodeType,
        final int eventCode,
        final int captureLength,
        final int messageLength);

    void onField(final CharSequence name, final long value);
    void onField(final CharSequence name, final CharSequence value);
    void onField(final CharSequence name, final int value);

    void onFooter();
}
