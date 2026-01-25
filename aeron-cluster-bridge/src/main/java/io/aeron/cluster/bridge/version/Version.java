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

import java.util.Objects;

/**
 * Semantic versioning implementation for the Bridge module.
 * <p>
 * Follows Semantic Versioning 2.0.0 (https://semver.org/):
 * <ul>
 *   <li>MAJOR: Incompatible API changes</li>
 *   <li>MINOR: Backward-compatible functionality additions</li>
 *   <li>PATCH: Backward-compatible bug fixes</li>
 * </ul>
 * <p>
 * Version format: MAJOR.MINOR.PATCH[-PRERELEASE][+BUILD]
 * <p>
 * Examples:
 * <ul>
 *   <li>1.0.0 - Initial stable release</li>
 *   <li>1.1.0-alpha - New feature in alpha</li>
 *   <li>1.1.0-beta.1 - Beta release 1</li>
 *   <li>1.1.0+build.123 - Build metadata</li>
 * </ul>
 */
public final class Version implements Comparable<Version>
{
    /** Current Bridge module version */
    public static final Version CURRENT = new Version(1, 0, 0, null, null);

    /** Minimum supported version for backward compatibility */
    public static final Version MIN_SUPPORTED = new Version(1, 0, 0, null, null);

    private final int major;
    private final int minor;
    private final int patch;
    private final String preRelease;
    private final String buildMetadata;

    /**
     * Create a version with major.minor.patch.
     *
     * @param major major version
     * @param minor minor version
     * @param patch patch version
     */
    public Version(final int major, final int minor, final int patch)
    {
        this(major, minor, patch, null, null);
    }

    /**
     * Create a full version with all components.
     *
     * @param major         major version
     * @param minor         minor version
     * @param patch         patch version
     * @param preRelease    pre-release identifier (e.g., "alpha", "beta.1")
     * @param buildMetadata build metadata (e.g., "build.123")
     */
    public Version(
        final int major,
        final int minor,
        final int patch,
        final String preRelease,
        final String buildMetadata)
    {
        if (major < 0 || minor < 0 || patch < 0)
        {
            throw new IllegalArgumentException("Version numbers cannot be negative");
        }

        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.preRelease = preRelease;
        this.buildMetadata = buildMetadata;
    }

    /**
     * Parse a version string.
     *
     * @param versionString the version string (e.g., "1.2.3-alpha+build.123")
     * @return the parsed Version
     * @throws IllegalArgumentException if the version string is invalid
     */
    public static Version parse(final String versionString)
    {
        if (versionString == null || versionString.isEmpty())
        {
            throw new IllegalArgumentException("Version string cannot be null or empty");
        }

        String working = versionString;
        String buildMetadata = null;
        String preRelease = null;

        // Extract build metadata (after +)
        final int buildIndex = working.indexOf('+');
        if (buildIndex >= 0)
        {
            buildMetadata = working.substring(buildIndex + 1);
            working = working.substring(0, buildIndex);
        }

        // Extract pre-release (after -)
        final int preReleaseIndex = working.indexOf('-');
        if (preReleaseIndex >= 0)
        {
            preRelease = working.substring(preReleaseIndex + 1);
            working = working.substring(0, preReleaseIndex);
        }

        // Parse major.minor.patch
        final String[] parts = working.split("\\.");
        if (parts.length < 2 || parts.length > 3)
        {
            throw new IllegalArgumentException(
                "Invalid version format: " + versionString + ". Expected MAJOR.MINOR[.PATCH]");
        }

        try
        {
            final int major = Integer.parseInt(parts[0]);
            final int minor = Integer.parseInt(parts[1]);
            final int patch = parts.length > 2 ? Integer.parseInt(parts[2]) : 0;

            return new Version(major, minor, patch, preRelease, buildMetadata);
        }
        catch (final NumberFormatException e)
        {
            throw new IllegalArgumentException("Invalid version number in: " + versionString, e);
        }
    }

    /**
     * Encode version as a single integer for efficient storage/transmission.
     * Format: MMMMMMMMMMMMNNNNNNNNPPPPPPPP (12 bits major, 10 bits minor, 10 bits patch)
     *
     * @return encoded version
     */
    public int encode()
    {
        return (major << 20) | (minor << 10) | patch;
    }

    /**
     * Decode a version from an encoded integer.
     *
     * @param encoded the encoded version
     * @return the decoded Version
     */
    public static Version decode(final int encoded)
    {
        final int major = (encoded >> 20) & 0xFFF;
        final int minor = (encoded >> 10) & 0x3FF;
        final int patch = encoded & 0x3FF;
        return new Version(major, minor, patch);
    }

    /**
     * Check if this version is compatible with another version.
     * Compatibility rules:
     * <ul>
     *   <li>Same major version = compatible</li>
     *   <li>Different major version = incompatible</li>
     * </ul>
     *
     * @param other the other version
     * @return true if compatible
     */
    public boolean isCompatibleWith(final Version other)
    {
        return this.major == other.major;
    }

    /**
     * Check if this version is backward compatible with a minimum required version.
     *
     * @param minRequired the minimum required version
     * @return true if this version meets or exceeds the minimum
     */
    public boolean meetsMinimum(final Version minRequired)
    {
        return this.compareTo(minRequired) >= 0;
    }

    /**
     * Check if this version is a pre-release.
     *
     * @return true if pre-release
     */
    public boolean isPreRelease()
    {
        return preRelease != null && !preRelease.isEmpty();
    }

    /**
     * Check if this is a stable release (not pre-release and major > 0).
     *
     * @return true if stable
     */
    public boolean isStable()
    {
        return major > 0 && !isPreRelease();
    }

    /**
     * Create the next major version.
     *
     * @return new Version with incremented major
     */
    public Version nextMajor()
    {
        return new Version(major + 1, 0, 0);
    }

    /**
     * Create the next minor version.
     *
     * @return new Version with incremented minor
     */
    public Version nextMinor()
    {
        return new Version(major, minor + 1, 0);
    }

    /**
     * Create the next patch version.
     *
     * @return new Version with incremented patch
     */
    public Version nextPatch()
    {
        return new Version(major, minor, patch + 1);
    }

    /**
     * Create a pre-release version.
     *
     * @param preReleaseId the pre-release identifier
     * @return new Version with pre-release
     */
    public Version withPreRelease(final String preReleaseId)
    {
        return new Version(major, minor, patch, preReleaseId, buildMetadata);
    }

    /**
     * Create a version with build metadata.
     *
     * @param build the build metadata
     * @return new Version with build metadata
     */
    public Version withBuild(final String build)
    {
        return new Version(major, minor, patch, preRelease, build);
    }

    // Getters
    public int major()
    {
        return major;
    }

    public int minor()
    {
        return minor;
    }

    public int patch()
    {
        return patch;
    }

    public String preRelease()
    {
        return preRelease;
    }

    public String buildMetadata()
    {
        return buildMetadata;
    }

    @Override
    public int compareTo(final Version other)
    {
        // Compare major.minor.patch
        int result = Integer.compare(this.major, other.major);
        if (result != 0)
        {
            return result;
        }

        result = Integer.compare(this.minor, other.minor);
        if (result != 0)
        {
            return result;
        }

        result = Integer.compare(this.patch, other.patch);
        if (result != 0)
        {
            return result;
        }

        // Pre-release versions have lower precedence
        if (this.preRelease == null && other.preRelease != null)
        {
            return 1;
        }
        if (this.preRelease != null && other.preRelease == null)
        {
            return -1;
        }
        if (this.preRelease != null)
        {
            return this.preRelease.compareTo(other.preRelease);
        }

        return 0;
    }

    @Override
    public boolean equals(final Object obj)
    {
        if (this == obj)
        {
            return true;
        }
        if (!(obj instanceof Version))
        {
            return false;
        }
        final Version other = (Version)obj;
        return major == other.major &&
            minor == other.minor &&
            patch == other.patch &&
            Objects.equals(preRelease, other.preRelease);
        // Note: buildMetadata is NOT part of equality per semver spec
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(major, minor, patch, preRelease);
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder();
        sb.append(major).append('.').append(minor).append('.').append(patch);

        if (preRelease != null && !preRelease.isEmpty())
        {
            sb.append('-').append(preRelease);
        }

        if (buildMetadata != null && !buildMetadata.isEmpty())
        {
            sb.append('+').append(buildMetadata);
        }

        return sb.toString();
    }

    /**
     * Get a short version string (major.minor.patch only).
     *
     * @return short version string
     */
    public String toShortString()
    {
        return major + "." + minor + "." + patch;
    }
}
