package io.aeron.archive.client;

public interface PersistentSubscriptionListener
{
    void onLive();

    void onError(final Exception e);
}
