package io.aeron.archive.client;

import io.aeron.exceptions.ConcurrentConcludeException;
import io.aeron.exceptions.ConfigurationException;
import org.junit.jupiter.api.Test;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PersistentSubscriptionContextTest
{

    @Test
    void canOnlyConcludeOnce()
    {
        final PersistentSubscription.Context context = new PersistentSubscription.Context()
            .recordingId(1)
            .liveChannel(IPC_CHANNEL)
            .liveStreamId(1)
            .aeronArchiveContext(new AeronArchive.Context());
        context.conclude();

        assertThrows(ConcurrentConcludeException.class, context::conclude);
    }

    @Test
    void contextMustHaveRecordingId()
    {
        final PersistentSubscription.Context context = new PersistentSubscription.Context()
            .liveChannel(IPC_CHANNEL)
            .liveStreamId(1)
            .aeronArchiveContext(new AeronArchive.Context());
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextMustHaveLiveChannel()
    {
        final PersistentSubscription.Context context = new PersistentSubscription.Context()
            .recordingId(1)
            .liveStreamId(1)
            .aeronArchiveContext(new AeronArchive.Context());
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextMustHaveLiveStreamId()
    {
        final PersistentSubscription.Context context = new PersistentSubscription.Context()
            .recordingId(1)
            .liveChannel(IPC_CHANNEL)
            .aeronArchiveContext(new AeronArchive.Context());
        assertThrows(ConfigurationException.class, context::conclude);
    }

    @Test
    void contextMustHaveAnArchiveContext()
    {
        final PersistentSubscription.Context context = new PersistentSubscription.Context()
            .recordingId(1)
            .liveChannel(IPC_CHANNEL)
            .liveStreamId(1);
        assertThrows(ConfigurationException.class, context::conclude);
    }

}
