package io.aeron.driver;

import io.aeron.driver.media.UdpChannel;
import io.aeron.protocol.StatusMessageFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersManager;

import java.nio.ByteBuffer;

/** Adds three receivers to MinMulticastFlowControl, times out the first and the last,
 *  then calls onIdle and reports which receivers survived. */
public class MinFcRemovalProbe
{
    private static final int WINDOW = 16 * 1024;

    private static long sm(
        final MinMulticastFlowControl fc, final long receiverId, final int termOffset, final long timeNs)
    {
        final StatusMessageFlyweight f = new StatusMessageFlyweight();
        f.wrap(new byte[1024]);
        f.receiverId(receiverId);
        f.consumptionTermId(0);
        f.consumptionTermOffset(termOffset);
        f.receiverWindowLength(WINDOW);
        return fc.onStatusMessage(f, null, 0, 0, 0, timeNs);
    }

    public static void main(final String[] args)
    {
        final UnsafeBuffer metadata = new UnsafeBuffer(ByteBuffer.allocateDirect(64 * 1024));
        final UnsafeBuffer values = new UnsafeBuffer(ByteBuffer.allocateDirect(16 * 1024));
        final CountersManager counters = new CountersManager(metadata, values);

        final MinMulticastFlowControl fc = new MinMulticastFlowControl();
        final UdpChannel channel = UdpChannel.parse(
            "aeron:udp?endpoint=224.20.30.39:24326|interface=localhost|fc=min,g:/1,t:100ms");
        final MediaDriver.Context ctx = new MediaDriver.Context();
        ctx.tempBuffer(new UnsafeBuffer(new byte[8192]));
        fc.initialize(ctx, counters, channel, 0, 0, 0, 0, 0);

        final long timeout = fc.receiverTimeoutNs();
        System.out.println("receiverTimeoutNs = " + timeout);

        sm(fc, 1, 1000, 0);            // will time out
        sm(fc, 2, 2000, timeout * 10); // still alive at the idle time
        sm(fc, 3, 3000, 0);            // will time out

        System.out.println("before onIdle: receivers = " + dump(fc));

        final long now = timeout * 10 + 1;   // 1 and 3 are stale, 2 is not
        fc.onIdle(now, 0, 0, false);

        System.out.println("after  onIdle: receivers = " + dump(fc));
        System.out.println("expected: 2 (the only receiver that is still alive)");
    }

    private static String dump(final MinMulticastFlowControl fc)
    {
        try
        {
            java.lang.reflect.Field f = AbstractMinMulticastFlowControl.class.getDeclaredField("receivers");
            f.setAccessible(true);
            final Object[] rs = (Object[])f.get(fc);
            final StringBuilder sb = new StringBuilder();
            for (final Object r : rs) { sb.append(id(r)).append(' '); }
            return sb.toString();
        }
        catch (final Exception e) { return "<" + e + ">"; }
    }

    private static String id(final Object receiver)
    {
        try
        {
            final java.lang.reflect.Field f = receiver.getClass().getDeclaredField("receiverId");
            f.setAccessible(true);
            return String.valueOf(f.getLong(receiver));
        }
        catch (final Exception e) { return "?"; }
    }
}
