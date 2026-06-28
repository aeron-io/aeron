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
package io.aeron.driver;

import org.agrona.BitUtil;

import static java.lang.System.getProperty;

/**
 * Older flow control using preferred, should not be used.
 *
 * @deprecated Use {@link TaggedMulticastFlowControl} instead
 */
@Deprecated
public class PreferredMulticastFlowControl extends TaggedMulticastFlowControl
{
    /**
     * Property name used to set Application Specific Feedback (ASF) in Status Messages to identify preferred receivers.
     * <p>
     * Replaced by {@link Configuration#RECEIVER_GROUP_TAG_PROP_NAME}.
     */
    public static final String PREFERRED_ASF_PROP_NAME = "aeron.PreferredMulticastFlowControl.asf";

    /**
     * Default Application Specific Feedback (ASF) value.
     */
    public static final String PREFERRED_ASF_DEFAULT = "FFFFFFFFFFFFFFFF";

    /**
     * Default constructor.
     */
    public PreferredMulticastFlowControl()
    {
    }

    static final String PREFERRED_ASF = getProperty(PREFERRED_ASF_PROP_NAME, PREFERRED_ASF_DEFAULT);
    static final byte[] PREFERRED_ASF_BYTES = BitUtil.fromHex(PREFERRED_ASF);
}
