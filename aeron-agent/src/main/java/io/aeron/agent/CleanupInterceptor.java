/*
 * Copyright 2014-2025 Real Logic Limited.
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
package io.aeron.agent;

import io.aeron.driver.IpcPublication;
import io.aeron.driver.SubscriptionLink;
import io.aeron.driver.NetworkPublication;
import io.aeron.driver.PublicationImage;
import net.bytebuddy.asm.Advice;

import static io.aeron.agent.DriverEventLogger.LOGGER;

class CleanupInterceptor
{
    static class CleanupImage
    {
        @Advice.OnMethodEnter
        static void cleanupImage(final PublicationImage image)
        {
            LOGGER.logImageRemoval(image.channel(), image.sessionId(), image.streamId(), image.correlationId());
        }
    }

    static class CleanupPublication
    {
        @Advice.OnMethodEnter
        static void cleanupPublication(final NetworkPublication publication)
        {
            LOGGER.logPublicationRemoval(publication.channel(), publication.sessionId(), publication.streamId());
        }
    }

    static class CleanupIpcPublication
    {
        @Advice.OnMethodEnter
        static void cleanupIpcPublication(final IpcPublication publication)
        {
            LOGGER.logPublicationRemoval(publication.channel(), publication.sessionId(), publication.streamId());
        }
    }

    static class CleanupSubscriptionLink
    {
        @Advice.OnMethodEnter
        static void cleanupSubscriptionLink(final SubscriptionLink link)
        {
            LOGGER.logSubscriptionRemoval(link.channel(), link.streamId(), link.registrationId());
        }
    }
}
