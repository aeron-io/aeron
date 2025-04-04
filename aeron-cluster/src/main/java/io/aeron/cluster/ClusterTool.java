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
package io.aeron.cluster;

import static io.aeron.cluster.ClusterToolCommand.*;
import static io.aeron.cluster.ClusterToolOperator.*;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.agrona.SystemUtil.getDurationInNanos;

import java.io.File;
import java.io.PrintStream;
import java.util.Map;

import org.agrona.SystemUtil;
import org.agrona.collections.Object2ObjectHashMap;

import io.aeron.Aeron;
import io.aeron.cluster.service.ClusterMarkFile;
import io.aeron.cluster.service.ClusterNodeControlProperties;
import io.aeron.config.Config;
import io.aeron.config.DefaultType;

/**
 * Tool for control and investigating the state of a cluster node.
 * <pre>
 * Usage: ClusterTool &#60;cluster-dir&#62; &#60;command&#62; [options]
 *                         describe: prints out all descriptors in the mark file.
 *                              pid: prints PID of cluster component.
 *                    recovery-plan: [service count] prints recovery plan of cluster component.
 *                    recording-log: prints recording log of cluster component.
 *               sort-recording-log: reorders entries in the recording log to match the order in memory.
 * seed-recording-log-from-snapshot: creates a new recording log based on the latest valid snapshot.
 *                           errors: prints Aeron and cluster component error logs.
 *                     list-members: prints leader memberId, active members and passive members lists.
 *                     backup-query: [delay] get, or set, time of next backup query.
 *       invalidate-latest-snapshot: marks the latest snapshot as a invalid so the previous is loaded.
 *                         snapshot: triggers a snapshot on the leader.
 *                 standby-snapshot: triggers a snapshot on cluster standby nodes.
 *                          suspend: suspends appending to the log.
 *                           resume: resumes reading from the log.
 *                         shutdown: initiates an orderly stop of the cluster with a snapshot.
 *                            abort: stops the cluster without a snapshot.
 *      describe-latest-cm-snapshot: prints the contents of the latest valid consensus module snapshot.
 *                        is-leader: returns zero if the cluster node is leader, non-zero if not
 * </pre>
 */
@Config(existsInC = false)
public class ClusterTool
{
    /**
     * Timeout in nanoseconds for the tool to wait while trying to perform an operation.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = 0, hasContext = false)
    public static final String AERON_CLUSTER_TOOL_TIMEOUT_PROP_NAME = "aeron.cluster.tool.timeout";

    /**
     * Delay in nanoseconds to be applied to an operation such as when the new cluster backup query will occur.
     */
    @Config(defaultType = DefaultType.LONG, defaultLong = 0, hasContext = false)
    public static final String AERON_CLUSTER_TOOL_DELAY_PROP_NAME = "aeron.cluster.tool.delay";

    /**
     * Property name for setting the channel used for archive replays.
     */
    @Config(hasContext = false)
    public static final String AERON_CLUSTER_TOOL_REPLAY_CHANNEL_PROP_NAME = "aeron.cluster.tool.replay.channel";

    /**
     * Default channel used for archive replays.
     */
    @Config
    public static final String AERON_CLUSTER_TOOL_REPLAY_CHANNEL_DEFAULT = "aeron:ipc";

    /**
     * Channel used for archive replays.
     */
    public static final String AERON_CLUSTER_TOOL_REPLAY_CHANNEL = SystemUtil.getProperty(
        AERON_CLUSTER_TOOL_REPLAY_CHANNEL_PROP_NAME, AERON_CLUSTER_TOOL_REPLAY_CHANNEL_DEFAULT);

    /**
     * Property name for setting the stream id used for archive replays.
     */
    @Config(hasContext = false)
    public static final String AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_PROP_NAME = "aeron.cluster.tool.replay.stream.id";

    /**
     * Default stream id used for archive replays.
     */
    @Config
    public static final int AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_DEFAULT = 103;

    /**
     * Stream id used for archive replays.
     */
    public static final int AERON_CLUSTER_TOOL_REPLAY_STREAM_ID = Integer.getInteger(
        AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_PROP_NAME, AERON_CLUSTER_TOOL_REPLAY_STREAM_ID_DEFAULT);

    /**
     * Timeout in milliseconds used by the tool for operations.
     */
    public static final long TIMEOUT_MS =
        NANOSECONDS.toMillis(getDurationInNanos(AERON_CLUSTER_TOOL_TIMEOUT_PROP_NAME, 0));

    private static final ClusterToolOperator BACKWARD_COMPATIBLE_OPERATIONS = new ClusterToolOperator(
        AERON_CLUSTER_TOOL_REPLAY_CHANNEL,
        AERON_CLUSTER_TOOL_REPLAY_STREAM_ID,
        TIMEOUT_MS);
    private static final String HELP_PREFIX = "Usage: <cluster-dir> <command> [options]";

    private static final Object2ObjectHashMap<String, ClusterToolCommand> COMMANDS = new Object2ObjectHashMap<>();

    static
    {
        final ClusterToolOperator operator = new ClusterToolOperator(
            AERON_CLUSTER_TOOL_REPLAY_CHANNEL,
            AERON_CLUSTER_TOOL_REPLAY_STREAM_ID,
            TIMEOUT_MS);

        COMMANDS.put("describe", new ClusterToolCommand(
            ignoreFailures(action(operator::describeClusterMarkFile)),
            "prints out all descriptors in the mark file."));

        COMMANDS.put("pid", new ClusterToolCommand(
            action(operator::pid),
            "prints PID of cluster component."));

        COMMANDS.put("recovery-plan", new ClusterToolCommand(
            (clusterDir, out, args) ->
            {
                if (args.length < 3)
                {
                    printHelp(COMMANDS, HELP_PREFIX);
                    return -1;
                }
                return operator.recoveryPlan(System.out, clusterDir, Integer.parseInt(args[2]));
            }, "[service count] prints recovery plan of cluster component."));

        COMMANDS.put("recording-log", new ClusterToolCommand(
            action(operator::recordingLog),
            "prints recording log of cluster component."));

        COMMANDS.put("sort-recording-log", new ClusterToolCommand(
            action(operator::sortRecordingLog),
            "reorders entries in the recording log to match the order in memory."));

        COMMANDS.put("seed-recording-log-from-snapshot", new ClusterToolCommand(
            action(operator::seedRecordingLogFromSnapshot),
            "creates a new recording log based on the latest valid snapshot."));

        COMMANDS.put("create-empty-service-snapshot", new ClusterToolCommand(
            action(operator::createEmptyServiceSnapshot),
            "creates an empty service snapshot based on the latest consensus module snapshot."));

        COMMANDS.put("add-service-snapshot", new ClusterToolCommand((clusterDir, out, args) ->
        {
            if (args.length < 3)
            {
                printHelp(COMMANDS, HELP_PREFIX);
                return -1;
            }
            return operator.addServiceSnapshot(out, clusterDir, Integer.parseInt(args[2]));
        }, "Adds a new snapshot with the specified recording id and with the next available service id."));

        COMMANDS.put("errors", new ClusterToolCommand(
            action(operator::errors),
            "prints Aeron and cluster component error logs."));

        COMMANDS.put("list-members", new ClusterToolCommand(
            action(operator::listMembers),
            "prints leader memberId, active members and passive members lists."));

        COMMANDS.put("backup-query", new ClusterToolCommand((clusterDir, out, args) ->
        {
            if (args.length < 3)
            {
                return operator.printNextBackupQuery(clusterDir, System.out);
            }
            else
            {
                return operator.nextBackupQuery(
                    clusterDir,
                    System.out,
                    NANOSECONDS.toMillis(SystemUtil.parseDuration(AERON_CLUSTER_TOOL_DELAY_PROP_NAME, args[2])));
            }
        }, "[delay] get, or set, time of next backup query."));

        COMMANDS.put("invalidate-latest-snapshot", new ClusterToolCommand(
            action(operator::invalidateLatestSnapshot),
            "marks the latest snapshot as a invalid so the previous is loaded."));

        COMMANDS.put("is-leader", new ClusterToolCommand(
            action(operator::isLeader),
            "returns zero if the cluster node is leader, non-zero if not."));

        COMMANDS.put("snapshot", new ClusterToolCommand(
            action(operator::snapshot),
            "triggers a snapshot on the leader."));

        COMMANDS.put("suspend", new ClusterToolCommand(
            action(operator::suspend),
            "suspends appending to the log."));

        COMMANDS.put("resume", new ClusterToolCommand(
            action(operator::resume),
            "resumes appending to the log."));

        COMMANDS.put("shutdown", new ClusterToolCommand(
            action(operator::shutdown),
            "initiates an orderly stop of the cluster with a snapshot."));

        COMMANDS.put("abort", new ClusterToolCommand(
            action(operator::abort),
            "stops the cluster without a snapshot."));

        COMMANDS.put("describe-latest-cm-snapshot", new ClusterToolCommand(
            action((clusterDir, listener) -> operator.describeLatestConsensusModuleSnapshot(
            clusterDir,
            System.out,
            null)),
            "prints the contents of the latest valid consensus module snapshot."));
    }


    /**
     * Main method for launching the process.
     *
     * @param args passed to the process.
     */
    @SuppressWarnings("methodlength")
    public static void main(final String[] args)
    {
        if (args.length < 2)
        {
            printHelp(COMMANDS, HELP_PREFIX);
            System.exit(-1);
        }

        final File clusterDir = new File(args[0]);
        if (!clusterDir.exists())
        {
            System.err.println("ERR: cluster directory not found: " + clusterDir.getAbsolutePath());
            printHelp(COMMANDS, HELP_PREFIX);
            System.exit(-1);
        }
        final ClusterToolCommand command = COMMANDS.get(args[1]);
        if (null == command)
        {
            System.out.println("Unknown command: " + args[1]);
            printHelp(COMMANDS, HELP_PREFIX);
            System.exit(-1);
        }
        else
        {
            command.action().act(clusterDir, System.out, args);
        }
    }

    /**
     * Print out the descriptors in the {@link ClusterMarkFile}s.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     */
    public static void describe(final PrintStream out, final File clusterDir)
    {
        BACKWARD_COMPATIBLE_OPERATIONS.describeClusterMarkFile(clusterDir, out);
    }

    /**
     * Print out the PID of the cluster process.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     */
    public static void pid(final PrintStream out, final File clusterDir)
    {
        BACKWARD_COMPATIBLE_OPERATIONS.pid(clusterDir, out);
    }

    /**
     * Print out the {@link io.aeron.cluster.RecordingLog.RecoveryPlan} for the cluster.
     *
     * @param out          to print the output to.
     * @param clusterDir   where the cluster is running.
     * @param serviceCount of services running in the containers.
     */
    public static void recoveryPlan(final PrintStream out, final File clusterDir, final int serviceCount)
    {
        BACKWARD_COMPATIBLE_OPERATIONS.recoveryPlan(out, clusterDir, serviceCount);
    }

    /**
     * Print out the {@link RecordingLog} for the cluster.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     */
    public static void recordingLog(final PrintStream out, final File clusterDir)
    {
        BACKWARD_COMPATIBLE_OPERATIONS.recordingLog(clusterDir, out);
    }

    /**
     * Re-order entries in thee {@link RecordingLog} file on disc if they are not in a proper order.
     *
     * @param clusterDir where the cluster is running.
     * @return {@code true} if file contents was changed or {@code false} if it was already in the correct order.
     */
    public static boolean sortRecordingLog(final File clusterDir)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.sortRecordingLog(clusterDir) == SUCCESS;
    }

    /**
     * Create a new {@link RecordingLog} based on the latest valid snapshot whose term base and log positions are set
     * to zero. The original recording log file is backed up as {@code recording.log.bak}.
     *
     * @param clusterDir where the cluster is running.
     */
    public static void seedRecordingLogFromSnapshot(final File clusterDir)
    {
        BACKWARD_COMPATIBLE_OPERATIONS.seedRecordingLogFromSnapshot(clusterDir);
    }

    /**
     * Create a new/empty service snapshot recording based on the most recent snapshot.
     *
     * @param clusterDir where the cluster is running.
     * @param out        to print the output to.
     * @return {@code true} if snapshot is created or {@code false} if it is not.
     */
    public static boolean createEmptyServiceSnapshot(final File clusterDir, final PrintStream out)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.createEmptyServiceSnapshot(clusterDir, out) == SUCCESS;
    }

    /**
     * Add a new snapshot entry to the recording log with the specified recording id.
     * The new snapshot entry will use the next highest service id.
     *
     * @param out         to print the output to.
     * @param clusterDir  where the cluster is running.
     * @param recordingId the recording id to associate with the new snapshot entry
     * @return {@code true} if the entry is added or {@code false} if it is not.
     */
    public static boolean addServiceSnapshot(final PrintStream out, final File clusterDir, final int recordingId)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.addServiceSnapshot(out, clusterDir, recordingId) == SUCCESS;
    }

    /**
     * Print out the errors in the error logs for the cluster components.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     */
    public static void errors(final PrintStream out, final File clusterDir)
    {
        BACKWARD_COMPATIBLE_OPERATIONS.errors(clusterDir, out);
    }

    /**
     * Print out a list of the current members of the cluster.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     */
    public static void listMembers(final PrintStream out, final File clusterDir)
    {
        BACKWARD_COMPATIBLE_OPERATIONS.listMembers(clusterDir, out);
    }

    /**
     * Print out the time of the next backup query.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster backup is running.
     */
    public static void printNextBackupQuery(final PrintStream out, final File clusterDir)
    {
        BACKWARD_COMPATIBLE_OPERATIONS.printNextBackupQuery(clusterDir, out);
    }

    /**
     * Set the time of the next backup query for the cluster backup.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     * @param delayMs    from the current time for the next backup query.
     */
    public static void nextBackupQuery(final PrintStream out, final File clusterDir, final long delayMs)
    {
        BACKWARD_COMPATIBLE_OPERATIONS.nextBackupQuery(clusterDir, out, delayMs);
    }

    /**
     * Print out the descriptors in the {@link ClusterMarkFile}s.
     *
     * @param out              to print the output to.
     * @param serviceMarkFiles to query.
     */
    public static void describe(final PrintStream out, final ClusterMarkFile[] serviceMarkFiles)
    {
        BACKWARD_COMPATIBLE_OPERATIONS.describe(out, serviceMarkFiles);
    }

    /**
     * Determine if a given node is a leader.
     *
     * @param out        to print the output to.
     * @param clusterDir where the cluster is running.
     * @return 0 if the node is an active leader in a closed election, 1 if not,
     * -1 if the mark file does not exist.
     */
    public static int isLeader(final PrintStream out, final File clusterDir)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.isLeader(clusterDir, out);
    }

    /**
     * Does a {@link ClusterMarkFile} exist in the cluster directory.
     *
     * @param clusterDir to check for if a mark file exists.
     * @return true if the cluster mark file exists.
     */
    public static boolean markFileExists(final File clusterDir)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.markFileExists(clusterDir);
    }

    /**
     * List the current members of a cluster.
     *
     * @param clusterMembership to populate.
     * @param clusterDir        where the cluster is running.
     * @param timeoutMs         to wait on the query completing.
     * @return true if successful.
     */
    public static boolean listMembers(
        final ClusterMembership clusterMembership, final File clusterDir, final long timeoutMs)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.listMembers(clusterMembership, clusterDir, timeoutMs);
    }

    /**
     * Query the membership of a cluster.
     *
     * @param markFile          for the cluster component.
     * @param timeoutMs         to wait for the query.
     * @param clusterMembership to populate.
     * @return true if the query was successful.
     */
    public static boolean queryClusterMembers(
        final ClusterMarkFile markFile, final long timeoutMs, final ClusterMembership clusterMembership)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.queryClusterMembers(markFile, timeoutMs, clusterMembership);
    }

    /**
     * Query the membership of a cluster.
     *
     * @param controlProperties from a {@link ClusterMarkFile}.
     * @param timeoutMs         to wait for the query.
     * @param clusterMembership to populate.
     * @return true if the query was successful.
     */
    public static boolean queryClusterMembers(
        final ClusterNodeControlProperties controlProperties,
        final long timeoutMs,
        final ClusterMembership clusterMembership)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.queryClusterMembers(controlProperties, timeoutMs, clusterMembership);
    }

    /**
     * Get the deadline time (MS) for the next cluster backup query.
     *
     * @param clusterDir where the cluster component is running.
     * @return the deadline time (MS) for the next cluster backup query, or {@link Aeron#NULL_VALUE} not available.
     */
    public static long nextBackupQueryDeadlineMs(final File clusterDir)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.nextBackupQueryDeadlineMs(clusterDir);
    }

    /**
     * Get the deadline time (MS) for the next cluster backup query.
     *
     * @param markFile for the cluster component.
     * @return the deadline time (MS) for the next cluster backup query, or {@link Aeron#NULL_VALUE} not available.
     */
    public static long nextBackupQueryDeadlineMs(final ClusterMarkFile markFile)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.nextBackupQueryDeadlineMs(markFile);
    }

    /**
     * Set the deadline time (MS) for the next cluster backup query.
     *
     * @param clusterDir where the cluster component is running.
     * @param timeMs     to set for the next deadline.
     * @return true if successful, otherwise false.
     */
    public static boolean nextBackupQueryDeadlineMs(final File clusterDir, final long timeMs)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.nextBackupQueryDeadlineMs(clusterDir, timeMs);
    }

    /**
     * Set the deadline time (MS) for the next cluster backup query.
     *
     * @param markFile for the cluster component.
     * @param timeMs   to set for the next deadline.
     * @return true if successful, otherwise false.
     */
    public static boolean nextBackupQueryDeadlineMs(final ClusterMarkFile markFile, final long timeMs)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.nextBackupQueryDeadlineMs(markFile, timeMs);
    }

    /**
     * Invalidate the latest snapshot so recovery will use an earlier one or log if no earlier one exists.
     *
     * @param out        to print the operation result.
     * @param clusterDir where the cluster component is running.
     * @return true if the latest snapshot was invalidated.
     */
    public static boolean invalidateLatestSnapshot(final PrintStream out, final File clusterDir)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.invalidateLatestSnapshot(clusterDir, out) == SUCCESS;
    }

    /**
     * Print out a summary of the state captured in the latest consensus module snapshot.
     *
     * @param out        to print the operation result.
     * @param clusterDir where the cluster is running.
     * @return <code>true</code> if the snapshot was successfully described <code>false</code> otherwise.
     */
    public static boolean describeLatestConsensusModuleSnapshot(final PrintStream out, final File clusterDir)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.describeLatestConsensusModuleSnapshot(clusterDir, out, null) == SUCCESS;
    }

    /**
     * Instruct the cluster to take a snapshot.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return true is the operation was successfully requested.
     */
    public static boolean snapshot(final File clusterDir, final PrintStream out)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.snapshot(clusterDir, out) == SUCCESS;
    }

    /**
     * Instruct the cluster to suspend operation.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return true is the operation was successfully requested.
     */
    public static boolean suspend(final File clusterDir, final PrintStream out)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.suspend(clusterDir, out) == SUCCESS;
    }

    /**
     * Instruct the cluster to resume operation.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return true is the operation was successfully requested.
     */
    public static boolean resume(final File clusterDir, final PrintStream out)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.resume(clusterDir, out) == SUCCESS;
    }

    /**
     * Instruct the cluster to shut down.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return true is the operation was successfully requested.
     */
    public static boolean shutdown(final File clusterDir, final PrintStream out)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.shutdown(clusterDir, out) == SUCCESS;
    }

    /**
     * Instruct the cluster to abort.
     *
     * @param clusterDir where the consensus module is running.
     * @param out        to print the result of the operation.
     * @return true is the operation was successfully requested.
     */
    public static boolean abort(final File clusterDir, final PrintStream out)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.abort(clusterDir, out) == SUCCESS;
    }

    /**
     * Finds the latest valid snapshot from the log file.
     *
     * @param clusterDir where the cluster node is running.
     * @return entry or {@code null} if not found.
     */
    static RecordingLog.Entry findLatestValidSnapshot(final File clusterDir)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.findLatestValidSnapshot(clusterDir);
    }

    /**
     * Load {@link ClusterNodeControlProperties} from the mark file.
     *
     * @param clusterDir where the cluster node is running.
     * @return control properties.
     */
    static ClusterNodeControlProperties loadControlProperties(final File clusterDir)
    {
        return BACKWARD_COMPATIBLE_OPERATIONS.loadControlProperties(clusterDir);
    }

    /*--------------------------------------------------------------*/

    /**
     * Cluster Tool commands map.
     * This is to allow other tools to simply extend ClusterTool
     * <p>
     * Note that the map is cloned and both key and value are Java immutable objects.
     *
     * @return a clone of Cluster Tool commands map
     */
    public static Map<String, ClusterToolCommand> commands()
    {
        return new Object2ObjectHashMap<>(COMMANDS);
    }

}
