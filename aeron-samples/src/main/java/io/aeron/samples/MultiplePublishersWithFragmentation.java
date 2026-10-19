/*
 * Copyright 2014-2025 Real Logic Limited.
 * Copyright 2015 Kaazing Corporation
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
package io.aeron.samples;

import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.BitUtil;
import org.agrona.BufferUtil;
import org.agrona.concurrent.UnsafeBuffer;

/**
 * A publisher application with multiple publications which send fragmented messages to a channel and two different
 * stream IDs. The default STREAM_ID and CHANNEL are configured in {@link SampleConfiguration}.
 * <p>
 * The default channel and stream IDs can be changed by setting Java system properties at the command line, e.g.:
 * {@code -Daeron.sample.channel=aeron:udp?endpoint=localhost:5555 -Daeron.sample.streamId=20}
 */
public class MultiplePublishersWithFragmentation
{
    private static final int STREAM_ID_1 = SampleConfiguration.STREAM_ID;
    private static final int STREAM_ID_2 = SampleConfiguration.STREAM_ID + 1;
    private static final String CHANNEL = SampleConfiguration.CHANNEL;
    private static final UnsafeBuffer BUFFER_1 = new UnsafeBuffer(
        BufferUtil.allocateDirectAligned(10000, BitUtil.CACHE_LINE_LENGTH));
    private static final UnsafeBuffer BUFFER_2 = new UnsafeBuffer(
        BufferUtil.allocateDirectAligned(9000, BitUtil.CACHE_LINE_LENGTH));

    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    public static void main(final String[] args)
    {
        System.out.println(
            "Publishing to " + CHANNEL + " on stream id " + STREAM_ID_1 + " and stream id " + STREAM_ID_2);

        try (Aeron aeron = Aeron.connect();
            Publication publication1 = aeron.addPublication(CHANNEL, STREAM_ID_1);
            Publication publication2 = aeron.addPublication(CHANNEL, STREAM_ID_2))
        {
            int j = 1;
            int k = 1;
            final String message1 = "Hello World! " + j;
            BUFFER_1.putBytes(0, message1.getBytes());
            final String message2 = "Hello World! " + k;
            BUFFER_2.putBytes(0, message2.getBytes());

            while (j <= 5000 || k <= 5000)
            {
                boolean offerStatus1 = false;
                boolean offerStatus2 = false;
                long result1;
                long result2;

                while (!(offerStatus1 || offerStatus2))
                {
                    if (j <= 5000)
                    {
                        result1 = publication1.offer(BUFFER_1, 0, BUFFER_1.capacity());
                        if (result1 > 0)
                        {
                            j++;
                            offerStatus1 = true;
                            System.out.println("Successfully sent data on stream id " +
                                STREAM_ID_1 + " and data length " + BUFFER_1.capacity() + " at offset " + result1);
                        }
                        else
                        {
                            if (result1 == Publication.BACK_PRESSURED)
                            {
                                System.out.println(" Offer failed due to back pressure for stream id " + STREAM_ID_1);
                            }
                            else if (result1 == Publication.NOT_CONNECTED)
                            {
                                System.out.println(" Offer failed because publisher is not yet " +
                                    "connected to subscriber for stream id " + STREAM_ID_1);
                            }
                            else
                            {
                                System.out.println(" Offer failed due to unexpected reason: " + result1);
                            }

                            offerStatus1 = false;
                        }
                    }

                    if (k <= 5000)
                    {
                        result2 = publication2.offer(BUFFER_2, 0, BUFFER_2.capacity());
                        if (result2 > 0)
                        {
                            k++;
                            offerStatus2 = true;
                            System.out.println("Successfully sent data on stream id " + STREAM_ID_2 +
                                " and data length " + BUFFER_2.capacity() + " at offset " + result2);
                        }
                        else
                        {
                            if (result2 == Publication.BACK_PRESSURED)
                            {
                                System.out.println(" Offer failed due to back pressure for stream id " + STREAM_ID_2);
                            }
                            else if (result2 == Publication.NOT_CONNECTED)
                            {
                                System.out.println(
                                    "Offer failed - publisher is not yet connected to subscriber " + STREAM_ID_2);
                            }
                            else
                            {
                                System.out.println("Offer failed due to unexpected reason: " + result2);
                            }
                            offerStatus2 = false;
                        }
                    }
                }
            }

            System.out.println("Done sending total messages for stream id " +
                STREAM_ID_1 + " = " + (j - 1) + " and stream id " + STREAM_ID_2 + " = " + (k - 1));
        }
    }
}
