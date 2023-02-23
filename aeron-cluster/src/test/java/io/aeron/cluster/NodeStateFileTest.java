/*
 * Copyright 2014-2023 Real Logic Limited.
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
package io.aeron.cluster;

import io.aeron.Aeron;
import io.aeron.cluster.client.ClusterException;
import io.aeron.cluster.codecs.node.NodeStateHeaderEncoder;
import io.aeron.cluster.service.ClusterMarkFile;
import org.agrona.IoUtil;
import org.agrona.SemanticVersion;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

class NodeStateFileTest
{
    private final int syncLevel = 1;

    @Test
    void shouldFailIfCreateNewFalseAndFileDoesNotExist(@TempDir final File clusterDir)
    {
        assertThrows(IOException.class, () -> new NodeStateFile(clusterDir, false, syncLevel));
    }

    @Test
    void shouldCreateIfCreateNewTrueAndFileDoesNotExist(@TempDir final File clusterDir) throws IOException
    {
        assertEquals(0, Objects.requireNonNull(clusterDir.list()).length);
        try (NodeStateFile ignore = new NodeStateFile(clusterDir, true, syncLevel))
        {
            Objects.requireNonNull(ignore);
            assertTrue(new File(clusterDir, NodeStateFile.FILENAME).exists());
        }
    }

    @Test
    void shouldHaveNullCandidateTermIdOnInitialCreation(@TempDir final File clusterDir) throws IOException
    {
        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            assertEquals(Aeron.NULL_VALUE, nodeStateFile.candidateTerm().candidateTermId());
        }
    }

    @Test
    void shouldPersistCandidateTermId(@TempDir final File clusterDir) throws Exception
    {
        final long candidateTermId = 832234;
        final long timestampMs = 324234;
        final long logPosition = 8923423;
        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            nodeStateFile.updateCandidateTermId(candidateTermId, logPosition, timestampMs);
        }

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, false, syncLevel))
        {
            assertEquals(candidateTermId, nodeStateFile.candidateTerm().candidateTermId());
            assertEquals(timestampMs, nodeStateFile.candidateTerm().timestamp());
            assertEquals(logPosition, nodeStateFile.candidateTerm().logPosition());
        }
    }

    @Test
    void shouldThrowIfVersionMismatch(@TempDir final File clusterDir) throws IOException
    {
        try (NodeStateFile ignore = new NodeStateFile(clusterDir, true, syncLevel))
        {
            Objects.requireNonNull(ignore);
        }

        final int invalidVersion = SemanticVersion.compose(
            ClusterMarkFile.MAJOR_VERSION + 1, ClusterMarkFile.MINOR_VERSION, ClusterMarkFile.PATCH_VERSION);
        forceVersion(clusterDir, invalidVersion);

        try (NodeStateFile ignore = new NodeStateFile(clusterDir, false, syncLevel))
        {
            Objects.requireNonNull(ignore);
            fail("ClusterException should have been thrown");
        }
        catch (final ClusterException ignore)
        {
        }
    }

    @Test
    void shouldProposeNewMaxTermId(@TempDir final File clusterDir) throws IOException
    {
        final long nextCandidateTermId;
        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            nodeStateFile.updateCandidateTermId(5, 10, 10);

            nextCandidateTermId = nodeStateFile.candidateTerm().candidateTermId() + 1;
            assertEquals(nextCandidateTermId, nodeStateFile.proposeMaxCandidateTermId(nextCandidateTermId, 20, 20));
            final long tooLowCandidateTermId = nodeStateFile.candidateTerm().candidateTermId() - 1;
            assertEquals(nextCandidateTermId, nodeStateFile.proposeMaxCandidateTermId(tooLowCandidateTermId, 30, 30));
        }

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, false, syncLevel))
        {
            assertEquals(nextCandidateTermId, nodeStateFile.candidateTerm().candidateTermId());
        }
    }

    @Test
    void shouldHaveNullClusterMembersOnCreation(@TempDir final File clusterDir) throws IOException
    {
        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            assertNull(nodeStateFile.clusterMembers());
        }
    }

    @Test
    void shouldHaveNullClusterMembersIfNotSet(@TempDir final File clusterDir) throws IOException
    {
        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            nodeStateFile.updateCandidateTermId(1, 2, 3);
        }

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            assertNull(nodeStateFile.clusterMembers());
        }
    }

    @Test
    void shouldHandleReloadOfEmptyFile(@TempDir final File clusterDir) throws IOException
    {
        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            Objects.requireNonNull(nodeStateFile);
        }

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            assertEquals(Aeron.NULL_VALUE, nodeStateFile.candidateTerm().candidateTermId());
            assertNull(nodeStateFile.clusterMembers());
        }
    }

    @Test
    void shouldShouldPersistClusterMembers(@TempDir final File clusterDir) throws IOException
    {
        final long candidateTermId = 832234;
        final long timestampMs = 324234;
        final long logPosition = 8923423;
        final long leadershipTermId = 82734982734L;
        final int memberId = 32;
        final int highClusterMemberId = 65;
        final String longClusterMembers =
            "0,host0:20000,host0:20001,host0:20002,host0:220003,host0:20004|" +
            "1,host1:20000,host1:20001,host1:20002,host1:220003,host1:20004|" +
            "2,host2:20000,host2:20001,host2:20002,host2:220003,host2:20004|";
        final String shortClusterMembers =
            "0,host0:20000,host0:20001,host0:20002,host0:220003,host0:20004|";

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            nodeStateFile.updateCandidateTermId(candidateTermId, logPosition, timestampMs);
            nodeStateFile.updateClusterMembers(leadershipTermId, memberId, highClusterMemberId, longClusterMembers);
        }

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            assertEquals(candidateTermId, nodeStateFile.candidateTerm().candidateTermId());
            assertEquals(timestampMs, nodeStateFile.candidateTerm().timestamp());
            assertEquals(logPosition, nodeStateFile.candidateTerm().logPosition());

            assertNotNull(nodeStateFile.clusterMembers());
            assertEquals(memberId, nodeStateFile.clusterMembers().memberId());
            assertEquals(highClusterMemberId, nodeStateFile.clusterMembers().highMemberId());
            assertEquals(longClusterMembers, nodeStateFile.clusterMembers().clusterMembers());
        }

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            nodeStateFile.updateClusterMembers(leadershipTermId, memberId, highClusterMemberId, shortClusterMembers);
        }

        try (NodeStateFile nodeStateFile = new NodeStateFile(clusterDir, true, syncLevel))
        {
            assertNotNull(nodeStateFile.clusterMembers());
            assertEquals(memberId, nodeStateFile.clusterMembers().memberId());
            assertEquals(highClusterMemberId, nodeStateFile.clusterMembers().highMemberId());
            assertEquals(shortClusterMembers, nodeStateFile.clusterMembers().clusterMembers());
        }
    }

    private void forceVersion(final File clusterDir, final int semanticVersion)
    {
        MappedByteBuffer buffer = null;
        try
        {
            buffer = IoUtil.mapExistingFile(
                new File(clusterDir, NodeStateFile.FILENAME), "test node state file");
            final UnsafeBuffer unsafeBuffer = new UnsafeBuffer(buffer);
            unsafeBuffer.putInt(NodeStateHeaderEncoder.versionEncodingOffset(), semanticVersion);
        }
        finally
        {
            if (null != buffer)
            {
                IoUtil.unmap(buffer);
            }
        }
    }
}