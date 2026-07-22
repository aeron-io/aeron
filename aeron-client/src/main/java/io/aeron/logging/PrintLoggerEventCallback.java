package io.aeron.logging;

import java.io.PrintStream;


class PrintLoggerEventCallback implements LoggerEventCallback
{
    // [53609.381133403] CLUSTER: ELECTION_STATE_CHANGE [122/122]:
    // memberId=2 CANDIDATE_BALLOT -> LEADER_LOG_REPLICATION leaderId=2 candidateTermId=0 leadershipTermId=0
    // logPosition=0 logLeadershipTermId=-1 appendPosition=0 catchupPosition=-1 reason="unanimous leader"
    private final PrintStream out;
    private final StringBuilder sb = new StringBuilder();

    PrintLoggerEventCallback(final PrintStream out)
    {
        this.out = out;
    }

    public void onHeader(final int eventType, final int eventCode, final long timestamp)
    {
        final EventCodeType eventCodeType = EventCodeType.get(eventType);

        sb.delete(0, sb.length());

        LogUtil.appendTimestamp(sb, timestamp);
        sb.append(eventCodeType.name()).append(": ");
        sb.append(eventCode);
    }

    public void onValue(final CharSequence name, final CharSequence value)
    {
        sb.append(' ').append(name).append("=\"").append(value).append("\"");
    }

    public void onValue(final CharSequence name, final long value)
    {
        sb.append(' ').append(name).append('=').append(value);
    }

    public void onValue(final CharSequence name, final boolean value)
    {
        sb.append(' ').append(name).append('=').append(value);
    }

    public void onFooter(final boolean truncated)
    {
        if (truncated)
        {
            sb.append("truncated");
        }

        out.println(sb);
        sb.delete(0, sb.length());
    }
}
