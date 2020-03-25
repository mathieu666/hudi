package org.apache.hudi.writer.client;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.config.HoodieCompactionConfig;
import org.apache.hudi.writer.exception.HoodieCommitException;
import org.apache.hudi.writer.exception.HoodieCompactionException;
import org.apache.hudi.writer.exception.HoodieRollbackException;
import org.apache.hudi.writer.table.HoodieCommitArchiveLog;
import org.apache.hudi.writer.table.HoodieTable;
import org.apache.hudi.writer.table.WorkloadProfile;
import org.apache.hudi.writer.table.WorkloadStat;
import org.apache.hudi.writer.utils.ClientUtils;
import org.apache.hudi.writer.utils.ValidationUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * HoodieWriteClient.
 */
public class HoodieWriteClient extends AbstractHoodieWriteClient {
  private static final Logger LOG = LogManager.getLogger(HoodieWriteClient.class);

  private final transient HoodieCleanClient cleanClient;

  public HoodieWriteClient(HoodieTable table, FileSystem fs) {
    super(table, fs);
    cleanClient = new HoodieCleanClient(table,fs);
  }

  /**
   * Save the workload profile in an intermediate file (here re-using commit files) This is useful when performing
   * rollback for MOR tables. Only updates are recorded in the workload profile metadata since updates to log blocks
   * are unknown across batches Inserts (which are new parquet files) are rolled back based on commit time. // TODO :
   * Create a new WorkloadProfile metadata file instead of using HoodieCommitMetadata
   */
  public void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, HoodieTable table, String instantTime)
      throws HoodieCommitException {
    try {
      HoodieCommitMetadata metadata = new HoodieCommitMetadata();
      profile.getPartitionPaths().forEach(path -> {
        WorkloadStat partitionStat = profile.getWorkloadStat(path.toString());
        partitionStat.getUpdateLocationToCount().forEach((key, value) -> {
          HoodieWriteStat writeStat = new HoodieWriteStat();
          writeStat.setFileId(key);
          // TODO : Write baseCommitTime is possible here ?
          writeStat.setPrevCommit(value.getKey());
          writeStat.setNumUpdateWrites(value.getValue());
          metadata.addWriteStat(path.toString(), writeStat);
        });
      });
      metadata.setOperationType(getOperationType());

      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      String commitActionType = table.getMetaClient().getCommitActionType();
      HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, commitActionType, instantTime);
      activeTimeline.transitionRequestedToInflight(requested,
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException io) {
      throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", io);
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
  @Override
  public void postCommit(HoodieCommitMetadata metadata, String instantTime,
                         Option<Map<String, String>> extraMetadata) throws IOException {

    // Do an inline compaction if enabled
    if (getConfig().isInlineCompaction()) {
      metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "true");
      forceCompact(extraMetadata);
    } else {
      metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false");
    }
    // We cannot have unbounded commit files. Archive commits if we have to archive
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(table);
    archiveLog.archiveIfRequired();
    if (getConfig().isAutoClean()) {
      // Call clean to cleanup if there is anything to cleanup after the commit,
      LOG.info("Auto cleaning is enabled. Running cleaner now");
      clean(instantTime);
    } else {
      LOG.info("Auto cleaning is not enabled. Not running cleaner now");
    }
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   *
   * @param startCleanTime Cleaner Instant Timestamp
   * @throws HoodieIOException in case of any IOException
   */
  public HoodieCleanMetadata clean(String startCleanTime) throws HoodieIOException {

    // If there are inflight(failed) or previously requested clean operation, first perform them
    table.getCleanTimeline().filterInflightsAndRequested().getInstants().forEach(hoodieInstant -> {
      LOG.info("There were previously unfinished cleaner operations. Finishing Instant=" + hoodieInstant);
      cleanClient.runClean(hoodieInstant);
    });

    Option<HoodieCleanerPlan> cleanerPlanOpt = cleanClient.scheduleClean(startCleanTime);

    if (cleanerPlanOpt.isPresent()) {
      HoodieCleanerPlan cleanerPlan = cleanerPlanOpt.get();
      if ((cleanerPlan.getFilesToBeDeletedPerPartition() != null)
          && !cleanerPlan.getFilesToBeDeletedPerPartition().isEmpty()) {
        return cleanClient.runClean(HoodieTimeline.getCleanRequestedInstant(startCleanTime), cleanerPlan);
      }
    }
    return null;
  }

  /**
   * Performs a compaction operation on a table, serially before or after an insert/upsert action.
   */
  private Option<String> forceCompact(Option<Map<String, String>> extraMetadata) throws IOException {
    Option<String> compactionInstantTimeOpt = scheduleCompaction(extraMetadata);
    compactionInstantTimeOpt.ifPresent(compactionInstantTime -> {
      try {
        // inline compaction should auto commit as the user is never given control
        compact(compactionInstantTime, true);
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    });
    return compactionInstantTimeOpt;
  }


  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return RDD of Write Status
   */
  public List<WriteStatus> compact(String compactionInstantTime, boolean autoCommit) throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieTimeline pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      // inflight compaction - Needs to rollback first deleting new parquet files before we run compaction.
      rollbackInflightCompaction(inflightInstant);
      // refresh table
      metaClient = ClientUtils.createMetaClient(table.getHadoopConf(), getConfig(), true);
      table = HoodieTable.getHoodieTable(metaClient, getConfig(), table.getHadoopConf(),table.getIndex());
      pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    }

    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(instant)) {
      return runCompaction(instant, metaClient.getActiveTimeline(), autoCommit);
    } else {
      throw new IllegalStateException(
          "No Compaction request available at " + compactionInstantTime + " to run compaction");
    }
  }

  /**
   * Performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return RDD of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> compact(String compactionInstantTime) throws IOException {
    return compact(compactionInstantTime, getConfig().shouldAutoCommit());
  }

  /**
   * Perform compaction operations as specified in the compaction commit file.
   *
   * @param compactionInstant Compaction Instant time
   * @param activeTimeline Active Timeline
   * @param autoCommit Commit after compaction
   * @return RDD of Write Status
   */
  private List<WriteStatus> runCompaction(HoodieInstant compactionInstant, HoodieActiveTimeline activeTimeline,
                                          boolean autoCommit) throws IOException {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.getCompactionPlan(metaClient, compactionInstant.getTimestamp());
    // Mark instant as compaction inflight
    activeTimeline.transitionCompactionRequestedToInflight(compactionInstant);

    List<WriteStatus> statuses = table.compact(compactionInstant.getTimestamp(), compactionPlan);
    // Force compaction action
    // pass extra-metada so that it gets stored in commit file automatically
    commitCompaction(statuses, compactionInstant.getTimestamp(), autoCommit,
        Option.ofNullable(compactionPlan.getExtraMetadata()));
    return statuses;
  }

  /**
   * Commit Compaction and track metrics.
   *
   * @param compactedStatuses Compaction Write status
   * @param compactionCommitTime Compaction Commit Time
   * @param autoCommit Auto Commit
   * @param extraMetadata Extra Metadata to store
   */
  public void commitCompaction(List<WriteStatus> compactedStatuses, String compactionCommitTime,
                                  boolean autoCommit, Option<Map<String, String>> extraMetadata) {
    if (autoCommit) {
      HoodieCommitMetadata metadata = doCompactionCommit(compactedStatuses, compactionCommitTime, extraMetadata);
      LOG.info("Compacted successfully on commit " + compactionCommitTime);
    } else {
      LOG.info("Compaction did not run for commit " + compactionCommitTime);
    }
  }

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @param writeStatuses RDD of WriteStatus to inspect errors and counts
   * @param extraMetadata Extra Metadata to be stored
   */
  public void commitCompaction(String compactionInstantTime, List<WriteStatus> writeStatuses,
                               Option<Map<String, String>> extraMetadata) throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieCompactionPlan compactionPlan = AvroUtils.deserializeCompactionPlan(
        timeline.readCompactionPlanAsBytes(HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get());
    // Merge extra meta-data passed by user with the one already in inflight compaction
    Option<Map<String, String>> mergedMetaData = extraMetadata.map(m -> {
      Map<String, String> merged = new HashMap<>();
      Map<String, String> extraMetaDataFromInstantFile = compactionPlan.getExtraMetadata();
      if (extraMetaDataFromInstantFile != null) {
        merged.putAll(extraMetaDataFromInstantFile);
      }
      // Overwrite/Merge with the user-passed meta-data
      merged.putAll(m);
      return Option.of(merged);
    }).orElseGet(() -> Option.ofNullable(compactionPlan.getExtraMetadata()));
    commitCompaction(writeStatuses, compactionInstantTime, true, mergedMetaData);
  }

  private HoodieCommitMetadata doCompactionCommit(List<WriteStatus> writeStatuses,
                                                  String compactionCommitTime, Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    List<HoodieWriteStat> updateStatusMap = writeStatuses.stream().map(WriteStatus::getStat).collect(Collectors.toList());

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }

    // Finalize write
    finalizeWrite(compactionCommitTime, updateStatusMap);

    // Copy extraMetadata
    extraMetadata.ifPresent(m -> {
      m.forEach(metadata::addMetadata);
    });

    LOG.info("Committing Compaction " + compactionCommitTime + ". Finished with result " + metadata);
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    try {
      activeTimeline.transitionCompactionInflightToComplete(
          new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionCommitTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + metaClient.getBasePath() + " at time " + compactionCommitTime, e);
    }
    return metadata;
  }

  /**
   * Rollback failed compactions. Inflight rollbacks for compactions revert the .inflight file to the .requested file
   *
   * @param inflightInstant Inflight Compaction Instant
   */
  public void rollbackInflightCompaction(HoodieInstant inflightInstant) throws IOException {
    table.rollback(inflightInstant, false);
    // Revert instant state file
    table.getActiveTimeline().revertCompactionInflightToRequested(inflightInstant);
  }

  /**
   * Schedules a new compaction instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleCompaction(Option<Map<String, String>> extraMetadata) throws IOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    LOG.info("Generate a new instant time " + instantTime);
    boolean notEmpty = scheduleCompactionAtInstant(instantTime, extraMetadata);
    return notEmpty ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new compaction instant with passed-in instant time.
   *
   * @param instantTime   Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata)
      throws IOException {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    // if there are inflight writes, their instantTime must not be less than that of compaction instant time
    metaClient.getCommitsTimeline().filterPendingExcludingCompaction().firstInstant().ifPresent(earliestInflight -> {
      ValidationUtils.checkArgument(
          HoodieTimeline.compareTimestamps(earliestInflight.getTimestamp(), instantTime, HoodieTimeline.GREATER),
          "Earliest write inflight instant time must be later than compaction time. Earliest :" + earliestInflight
              + ", Compaction scheduled at " + instantTime);
    });
    // Committed and pending compaction instants should have strictly lower timestamps
    List<HoodieInstant> conflictingInstants = metaClient
        .getActiveTimeline().getCommitsAndCompactionTimeline().getInstants().filter(instant -> HoodieTimeline
            .compareTimestamps(instant.getTimestamp(), instantTime, HoodieTimeline.GREATER_OR_EQUAL))
        .collect(Collectors.toList());
    ValidationUtils.checkArgument(conflictingInstants.isEmpty(),
        "Following instants have timestamps >= compactionInstant (" + instantTime + ") Instants :"
            + conflictingInstants);

    HoodieCompactionPlan workload = table.scheduleCompaction(instantTime);
    if (workload != null && (workload.getOperations() != null) && (!workload.getOperations().isEmpty())) {
      extraMetadata.ifPresent(workload::setExtraMetadata);
      HoodieInstant compactionInstant =
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
      metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
          AvroUtils.serializeCompactionPlan(workload));
      return true;
    }
    return false;
  }

  /**
   * Rollback the (inflight/committed) record changes with the given commit time. Three steps: (1) Atomically unpublish
   * this commit (2) clean indexing data, (3) clean new generated parquet files. (4) Finally delete .commit or .inflight
   * file.
   *
   * @param instantTime Instant time of the commit
   * @return {@code true} If rollback the record changes successfully. {@code false} otherwise
   */
  public boolean rollback(final String instantTime) throws HoodieRollbackException {
    rollbackInternal(instantTime);
    return true;
  }
}
