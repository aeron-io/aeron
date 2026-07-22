/*
 * Copyright 2014-2026 Real Logic Limited.
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
