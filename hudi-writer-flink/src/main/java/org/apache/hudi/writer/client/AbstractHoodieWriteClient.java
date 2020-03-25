package org.apache.hudi.writer.client;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRollingStat;
import org.apache.hudi.common.model.HoodieRollingStatMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.exception.HoodieCommitException;
import org.apache.hudi.writer.exception.HoodieRollbackException;
import org.apache.hudi.writer.index.HoodieHBaseIndex;
import org.apache.hudi.writer.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Abstract Write Client providing functionality for performing commit, index updates and rollback
 *  Reused for regular write operations like upsert/insert/bulk-insert.. as well as bootstrap
 */
public class AbstractHoodieWriteClient extends AbstractHoodieClient {

  private static final Logger LOG = LogManager.getLogger(AbstractHoodieWriteClient.class);

  private static final String UPDATE_STR = "update";
  private transient WriteOperationType operationType;
  private final transient HoodieHBaseIndex index;

  protected AbstractHoodieWriteClient(HoodieTable table, FileSystem fs) {
    super(table, fs);
    this.index = table.getIndex();
  }

  public void setOperationType(WriteOperationType operationType) {
    this.operationType = operationType;
  }

  public WriteOperationType getOperationType() {
    return operationType;
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, List<WriteStatus> writeStatuses) {
    return commit(instantTime, writeStatuses, Option.empty());
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, List<WriteStatus> writeStatuses,
                        Option<Map<String, String>> extraMetadata) {
    return commit(instantTime, writeStatuses, extraMetadata, createMetaClient().getCommitActionType());
  }

  private boolean commit(String instantTime, List<WriteStatus> writeStatuses,
                         Option<Map<String, String>> extraMetadata, String actionType) {

    LOG.info("Commiting " + instantTime);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable table = super.table;

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    List<HoodieWriteStat> stats = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());

    updateMetadataAndRollingStats(actionType, metadata, stats);

    // Finalize write
    finalizeWrite(instantTime, stats);

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(metadata::addMetadata);
    }

    HoodieWriteConfig config = getConfig();
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());
    metadata.setOperationType(operationType);

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

      postCommit(metadata, instantTime, extraMetadata);

      LOG.info("Committed " + instantTime);
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
    return true;
  }

  /**
   * Finalize Write operation.
   *
   * @param instantTime Instant Time
   * @param stats       Hoodie Write Stat
   */
  protected void finalizeWrite(String instantTime, List<HoodieWriteStat> stats) {
    try {
      table.finalizeWrite(instantTime, stats);
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  private void updateMetadataAndRollingStats(String actionType, HoodieCommitMetadata metadata,
                                             List<HoodieWriteStat> writeStats) {
    // TODO : make sure we cannot rollback / archive last commit file
    try {
      // 0. All of the rolling stat management is only done by the DELTA commit for MOR and COMMIT for COW other wise
      // there may be race conditions
      HoodieRollingStatMetadata rollingStatMetadata = new HoodieRollingStatMetadata(actionType);
      // 1. Look up the previous compaction/commit and get the HoodieCommitMetadata from there.
      // 2. Now, first read the existing rolling stats and merge with the result of current metadata.

      // Need to do this on every commit (delta or commit) to support COW and MOR.

      for (HoodieWriteStat stat : writeStats) {
        String partitionPath = stat.getPartitionPath();
        // TODO: why is stat.getPartitionPath() null at times here.
        metadata.addWriteStat(partitionPath, stat);
        HoodieRollingStat hoodieRollingStat = new HoodieRollingStat(stat.getFileId(),
            stat.getNumWrites() - (stat.getNumUpdateWrites() - stat.getNumDeletes()), stat.getNumUpdateWrites(),
            stat.getNumDeletes(), stat.getTotalWriteBytes());
        rollingStatMetadata.addRollingStat(partitionPath, hoodieRollingStat);
      }
      // The last rolling stat should be present in the completed timeline
      Option<HoodieInstant> lastInstant =
          table.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant();
      if (lastInstant.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            table.getActiveTimeline().getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
        Option<String> lastRollingStat = Option
            .ofNullable(commitMetadata.getExtraMetadata().get(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY));
        if (lastRollingStat.isPresent()) {
          rollingStatMetadata = rollingStatMetadata
              .merge(HoodieCommitMetadata.fromBytes(lastRollingStat.get().getBytes(), HoodieRollingStatMetadata.class));
        }
      }
      metadata.addMetadata(HoodieRollingStatMetadata.ROLLING_STAT_METADATA_KEY, rollingStatMetadata.toJsonString());
    } catch (IOException io) {
      throw new HoodieCommitException("Unable to save rolling stats");
    }
  }

  public List<WriteStatus> updateIndexAndCommitIfNeeded(List<WriteStatus> writeStatus, HoodieTable hoodieTable, String instantTime) {
    List<WriteStatus> statuses = index.updateLocation(writeStatus, hoodieTable);
    // Trigger the insert and collect statuses
    commitOnAutoCommit(instantTime, statuses, hoodieTable.getMetaClient().getCommitActionType());
    return statuses;
  }

  protected void commitOnAutoCommit(String instantTime, List<WriteStatus> resultRDD, String actionType) {
    if (getConfig().shouldAutoCommit()) {
      LOG.info("Auto commit enabled: Committing " + instantTime);
      boolean commitResult = commit(instantTime, resultRDD, Option.empty(), actionType);
      if (!commitResult) {
        throw new HoodieCommitException("Failed to commit " + instantTime);
      }
    } else {
      LOG.info("Auto commit disabled for " + instantTime);
    }
  }

  /**
   * Post Commit Hook. Derived classes use this method to perform post-commit processing
   *
   * @param metadata      Commit Metadata corresponding to committed instant
   * @param instantTime   Instant Time
   * @param extraMetadata Additional Metadata passed by user
   * @throws IOException in case of error
   */
  protected void postCommit(HoodieCommitMetadata metadata, String instantTime,
                            Option<Map<String, String>> extraMetadata) throws IOException {

  }

  protected void rollbackInternal(String commitToRollback) {
    final String startRollbackTime = HoodieActiveTimeline.createNewInstantTime();
    // Create a Hoodie table which encapsulated the commits and files visible
    try {
      Option<HoodieInstant> rollbackInstantOpt =
          Option.fromJavaOptional(table.getActiveTimeline().getCommitsTimeline().getInstants()
              .filter(instant -> HoodieActiveTimeline.EQUAL.test(instant.getTimestamp(), commitToRollback))
              .findFirst());

      if (rollbackInstantOpt.isPresent()) {
        List<HoodieRollbackStat> stats = doRollbackAndGetStats(rollbackInstantOpt.get());
        finishRollback(stats, Collections.singletonList(commitToRollback), startRollbackTime);
      }
    } catch (IOException e) {
      throw new HoodieRollbackException("Failed to rollback " + getConfig().getBasePath() + " commits " + commitToRollback,
          e);
    }
  }

  protected List<HoodieRollbackStat> doRollbackAndGetStats(final HoodieInstant instantToRollback) throws
      IOException {
    final String commitToRollback = instantToRollback.getTimestamp();

    HoodieTimeline inflightAndRequestedCommitTimeline = table.getPendingCommitTimeline();
    HoodieTimeline commitTimeline = table.getCompletedCommitsTimeline();
    // Check if any of the commits is a savepoint - do not allow rollback on those commits
    List<String> savepoints = table.getCompletedSavepointTimeline().getInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    savepoints.forEach(s -> {
      if (s.contains(commitToRollback)) {
        throw new HoodieRollbackException(
            "Could not rollback a savepointed commit. Delete savepoint first before rolling back" + s);
      }
    });

    if (commitTimeline.empty() && inflightAndRequestedCommitTimeline.empty()) {
      // nothing to rollback
      LOG.info("No commits to rollback " + commitToRollback);
    }

    // Make sure only the last n commits are being rolled back
    // If there is a commit in-between or after that is not rolled back, then abort

    if ((commitToRollback != null) && !commitTimeline.empty()
        && !commitTimeline.findInstantsAfter(commitToRollback, Integer.MAX_VALUE).empty()) {
      throw new HoodieRollbackException(
          "Found commits after time :" + commitToRollback + ", please rollback greater commits first");
    }

    List<String> inflights = inflightAndRequestedCommitTimeline.getInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    if ((commitToRollback != null) && !inflights.isEmpty()
        && (inflights.indexOf(commitToRollback) != inflights.size() - 1)) {
      throw new HoodieRollbackException(
          "Found in-flight commits after time :" + commitToRollback + ", please rollback greater commits first");
    }

    List<HoodieRollbackStat> stats = table.rollback(instantToRollback, true);

    LOG.info("Deleted inflight commits " + commitToRollback);

    // cleanup index entries
    if (index.rollbackCommit(commitToRollback)) {
      throw new HoodieRollbackException("Rollback index changes failed, for time :" + commitToRollback);
    }
    LOG.info("Index rolled back for commits " + commitToRollback);
    return stats;
  }

  private void finishRollback(List<HoodieRollbackStat> rollbackStats,
                              List<String> commitsToRollback, final String startRollbackTime) throws IOException {
    Option<Long> durationInMs = Option.empty();
    long numFilesDeleted = rollbackStats.stream().mapToLong(stat -> stat.getSuccessDeleteFiles().size()).sum();
    HoodieRollbackMetadata rollbackMetadata = AvroUtils
        .convertRollbackMetadata(startRollbackTime, durationInMs, commitsToRollback, rollbackStats);
    //TODO: varadarb - This will be fixed when Rollback transition mimics that of commit
    table.getActiveTimeline().createNewInstant(new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.ROLLBACK_ACTION,
        startRollbackTime));
    table.getActiveTimeline().saveAsComplete(
        new HoodieInstant(true, HoodieTimeline.ROLLBACK_ACTION, startRollbackTime),
        AvroUtils.serializeRollbackMetadata(rollbackMetadata));
    LOG.info("Rollback of Commits " + commitsToRollback + " is complete");

    if (!table.getActiveTimeline().getCleanerTimeline().empty()) {
      LOG.info("Cleaning up older rollback meta files");
      // Cleanup of older cleaner meta files
      // TODO - make the commit archival generic and archive rollback metadata
      FSUtils.deleteOlderRollbackMetaFiles(fs, table.getMetaClient().getMetaPath(),
          table.getActiveTimeline().getRollbackTimeline().getInstants());
    }
  }
}
