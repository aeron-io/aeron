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
package io.aeron.cluster.bridge.version;

/**
 * Protocol version for wire format compatibility.
 * <p>
 * The protocol version is separate from the module version to allow
 * independent evolution of:
 * <ul>
 *   <li>Wire format (protocol version)</li>
 *   <li>Implementation (module version)</li>
 * </ul>
 * <p>
 * Protocol version is embedded in every message header to enable:
 * <ul>
 *   <li>Version negotiation during handshake</li>
 *   <li>Backward compatibility detection</li>
 *   <li>Mixed-version cluster support during rolling upgrades</li>
 * </ul>
 */
public final class ProtocolVersion
{
    /**
     * Current protocol version.
     * <p>
     * History:
     * <ul>
     *   <li>1: Initial protocol - basic header with sequence, timestamp, checksum</li>
     * </ul>
     */
    public static final int CURRENT = 1;

    /**
     * Minimum supported protocol version for backward compatibility.
     */
    public static final int MIN_SUPPORTED = 1;

    /**
     * Maximum supported protocol version.
     */
    public static final int MAX_SUPPORTED = 1;

    /**
     * Protocol feature flags for version 1.
     */
    public static final class Features
    {
        /** Basic message header */
        public static final int BASIC_HEADER = 1 << 0;
        /** Checksum verification */
        public static final int CHECKSUM = 1 << 1;
        /** Heartbeat messages */
        public static final int HEARTBEAT = 1 << 2;
        /** Sequence tracking */
        public static final int SEQUENCE_TRACKING = 1 << 3;
        /** Archive replay */
        public static final int ARCHIVE_REPLAY = 1 << 4;
        /** Compression support */
        public static final int COMPRESSION = 1 << 5;
        /** Encryption support */
        public static final int ENCRYPTION = 1 << 6;

        /** Default features for version 1 */
        public static final int V1_FEATURES =
            BASIC_HEADER | CHECKSUM | HEARTBEAT | SEQUENCE_TRACKING | ARCHIVE_REPLAY;

        private Features()
        {
        }

        /**
         * Check if a feature is supported.
         *
         * @param featureSet the feature set
         * @param feature    the feature to check
         * @return true if supported
         */
        public static boolean isSupported(final int featureSet, final int feature)
        {
            return (featureSet & feature) != 0;
        }

        /**
         * Get the feature set for a protocol version.
         *
         * @param protocolVersion the protocol version
         * @return feature set flags
         */
        public static int featuresForVersion(final int protocolVersion)
        {
            switch (protocolVersion)
            {
                case 1:
                    return V1_FEATURES;
                default:
                    return V1_FEATURES;
            }
        }

        /**
         * Get a human-readable description of enabled features.
         *
         * @param featureSet the feature set
         * @return description
         */
        public static String describe(final int featureSet)
        {
            final StringBuilder sb = new StringBuilder();
            sb.append('[');

            if (isSupported(featureSet, BASIC_HEADER))
            {
                sb.append("BASIC_HEADER,");
            }
            if (isSupported(featureSet, CHECKSUM))
            {
                sb.append("CHECKSUM,");
            }
            if (isSupported(featureSet, HEARTBEAT))
            {
                sb.append("HEARTBEAT,");
            }
            if (isSupported(featureSet, SEQUENCE_TRACKING))
            {
                sb.append("SEQUENCE_TRACKING,");
            }
            if (isSupported(featureSet, ARCHIVE_REPLAY))
            {
                sb.append("ARCHIVE_REPLAY,");
            }
            if (isSupported(featureSet, COMPRESSION))
            {
                sb.append("COMPRESSION,");
            }
            if (isSupported(featureSet, ENCRYPTION))
            {
                sb.append("ENCRYPTION,");
            }

            if (sb.length() > 1)
            {
                sb.setLength(sb.length() - 1); // Remove trailing comma
            }
            sb.append(']');

            return sb.toString();
        }
    }

    /**
     * Compatibility result from version check.
     */
    public enum Compatibility
    {
        /** Fully compatible */
        COMPATIBLE,
        /** Backward compatible - older peer */
        BACKWARD_COMPATIBLE,
        /** Forward compatible - newer peer */
        FORWARD_COMPATIBLE,
        /** Incompatible - cannot communicate */
        INCOMPATIBLE
    }

    /**
     * Check compatibility between two protocol versions.
     *
     * @param localVersion  local protocol version
     * @param remoteVersion remote protocol version
     * @return compatibility result
     */
    public static Compatibility checkCompatibility(final int localVersion, final int remoteVersion)
    {
        if (localVersion == remoteVersion)
        {
            return Compatibility.COMPATIBLE;
        }

        if (remoteVersion < MIN_SUPPORTED)
        {
            return Compatibility.INCOMPATIBLE;
        }

        if (remoteVersion > MAX_SUPPORTED)
        {
            // Future version - might be forward compatible if major version unchanged
            // For now, treat as incompatible for safety
            return Compatibility.INCOMPATIBLE;
        }

        if (remoteVersion < localVersion)
        {
            return Compatibility.BACKWARD_COMPATIBLE;
        }
        else
        {
            return Compatibility.FORWARD_COMPATIBLE;
        }
    }

    /**
     * Check if a protocol version is supported.
     *
     * @param version the version to check
     * @return true if supported
     */
    public static boolean isSupported(final int version)
    {
        return version >= MIN_SUPPORTED && version <= MAX_SUPPORTED;
    }

    /**
     * Get the negotiated version between local and remote.
     * Returns the minimum of the two for compatibility.
     *
     * @param localVersion  local version
     * @param remoteVersion remote version
     * @return negotiated version, or -1 if incompatible
     */
    public static int negotiate(final int localVersion, final int remoteVersion)
    {
        final Compatibility compat = checkCompatibility(localVersion, remoteVersion);

        switch (compat)
        {
            case COMPATIBLE:
                return localVersion;

            case BACKWARD_COMPATIBLE:
            case FORWARD_COMPATIBLE:
                return Math.min(localVersion, remoteVersion);

            case INCOMPATIBLE:
            default:
                return -1;
        }
    }

    /**
     * Encode protocol version with feature flags into a single int.
     * Format: VVVVVVVVVVVVVVVVFFFFFFFFFFFFF (16 bits version, 16 bits features)
     *
     * @param version  protocol version
     * @param features feature flags
     * @return encoded value
     */
    public static int encode(final int version, final int features)
    {
        return (version << 16) | (features & 0xFFFF);
    }

    /**
     * Decode version from encoded value.
     *
     * @param encoded the encoded value
     * @return protocol version
     */
    public static int decodeVersion(final int encoded)
    {
        return (encoded >> 16) & 0xFFFF;
    }

    /**
     * Decode features from encoded value.
     *
     * @param encoded the encoded value
     * @return feature flags
     */
    public static int decodeFeatures(final int encoded)
    {
        return encoded & 0xFFFF;
    }

    /**
     * Get a human-readable description of a protocol version.
     *
     * @param version the version
     * @return description
     */
    public static String describe(final int version)
    {
        switch (version)
        {
            case 1:
                return "v1 (Initial protocol: header, checksum, heartbeat, sequence, archive)";
            default:
                return "v" + version + " (Unknown)";
        }
    }

    private ProtocolVersion()
    {
    }
}
