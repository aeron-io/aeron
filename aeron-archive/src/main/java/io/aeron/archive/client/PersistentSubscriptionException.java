package io.aeron.archive.client;

public class PersistentSubscriptionException extends RuntimeException
{
    private static final long serialVersionUID = -6953277621087533657L;

    private final Reason reason;

    public enum Reason {
        RECORDING_NOT_FOUND,
        INVALID_START_POSITION
    }

    public PersistentSubscriptionException(final Reason reason, final String message)
    {
        super(message);
        this.reason = reason;
    }

    public Reason reason()
    {
        return reason;
    }
}
