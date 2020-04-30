package org.apache.hudi.writer;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.hudi.*;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.HoodieIndexV2;
import org.apache.hudi.table.HoodieTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieWriteFlinkClient<T extends HoodieRecordPayload> implements
    HoodieWriteClientV2<
        HoodieWriteInput<DataStream<HoodieRecord<T>>>,
        HoodieWriteKey<DataStream<HoodieKey>>,
        HoodieWriteOutput<DataStream<WriteStatus>>> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieWriteFlinkClient.class);

  private final transient HoodieIndexV2<HoodieWriteInput<DataStream<HoodieRecord<T>>>, HoodieWriteInput<DataStream<HoodieRecord<T>>>> index;
  protected final HoodieWriteConfig config;
  private final transient HoodieEngineContext context;
  private static final String LOOKUP_STR = "lookup";

  public HoodieWriteFlinkClient(HoodieEngineContext context, HoodieWriteConfig config) {
    this.context = context;
    this.config = config;
    this.index = HoodieIndexV2Factory.createHoodieIndex(config, context);
  }

  @Override
  public HoodieWriteOutput<DataStream<WriteStatus>> upsert(HoodieWriteInput<DataStream<HoodieRecord<T>>> hoodieRecords, String instantTime) {
    HoodieTable table = getTableAndInitCtx(WriteOperationType.UPSERT);
    validateSchema(table, true);
    setOperationType(WriteOperationType.UPSERT);
    // problem
    HoodieWriteMetadata result = table.upsert(instantTime, hoodieRecords);
    return postWrite(result, instantTime, table);
  }

  private HoodieWriteOutput<DataStream<WriteStatus>> postWrite(HoodieWriteMetadata result, String instantTime, HoodieTable table) {
    if (result.getIndexLookupDuration().isPresent()) {
      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
    }
    if (result.isCommitted()) {
      // Perform post commit operations.
      if (result.getFinalizeDuration().isPresent()) {
        metrics.updateFinalizeWriteMetrics(result.getFinalizeDuration().get().toMillis(),
            result.getWriteStats().get().size());
      }

      postCommit(result.getCommitMetadata().get(), instantTime, Option.empty());

      emitCommitMetrics(instantTime, result.getCommitMetadata().get(),
          table.getMetaClient().getCommitActionType());
    }
    return result.getWriteStatuses();
  }

  protected void postCommit(HoodieCommitMetadata metadata, String instantTime,
                            Option<Map<String, String>> extraMetadata) {
    try {
      // Do an inline compaction if enabled
      if (config.isInlineCompaction()) {
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "true");
        forceCompact(extraMetadata);
      } else {
        metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false");
      }
      // We cannot have unbounded commit files. Archive commits if we have to archive
      HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(config, createMetaClient(true));
      archiveLog.archiveIfRequired(context);
      if (config.isAutoClean()) {
        // Call clean to cleanup if there is anything to cleanup after the commit,
        LOG.info("Auto cleaning is enabled. Running cleaner now");
        clean(instantTime);
      } else {
        LOG.info("Auto cleaning is not enabled. Not running cleaner now");
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
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
  private DataStream<WriteStatus> compact(String compactionInstantTime, boolean autoCommit) throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieTable<T> table = HoodieTable.create(metaClient, config, context);
    HoodieTimeline pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      rollbackInflightCompaction(inflightInstant, table);
      metaClient.reloadActiveTimeline();
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

  private DataStream<WriteStatus> runCompaction(HoodieInstant compactionInstant, HoodieActiveTimeline activeTimeline,
                                             boolean autoCommit) throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.getCompactionPlan(metaClient, compactionInstant.getTimestamp());
    // Mark instant as compaction inflight
    activeTimeline.transitionCompactionRequestedToInflight(compactionInstant);
    compactionTimer = metrics.getCompactionCtx();
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.create(metaClient, config, jsc);
    DataStream<WriteStatus> statuses = table.compact(context, compactionInstant.getTimestamp(), compactionPlan);
    // Force compaction action
    statuses.persist(SparkConfigUtils.getWriteStatusStorageLevel(config.getProps()));
    // pass extra-metada so that it gets stored in commit file automatically
    commitCompaction(statuses, table, compactionInstant.getTimestamp(), autoCommit,
        Option.ofNullable(compactionPlan.getExtraMetadata()));
    return statuses;
  }

  /**
   * Rollback failed compactions. Inflight rollbacks for compactions revert the .inflight file to the .requested file
   *
   * @param inflightInstant Inflight Compaction Instant
   * @param table Hoodie Table
   */
  public void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieTable table) {
    table.rollback(context, HoodieActiveTimeline.createNewInstantTime(), inflightInstant, false);
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
   * @param instantTime Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata)
      throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
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
    HoodieTable<T> table = HoodieTable.create(metaClient, config, context);
    HoodieCompactionPlan workload = table.scheduleCompaction(context, instantTime);
    if (workload != null && (workload.getOperations() != null) && (!workload.getOperations().isEmpty())) {
      extraMetadata.ifPresent(workload::setExtraMetadata);
      HoodieInstant compactionInstant =
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
      metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
          TimelineMetadataUtils.serializeCompactionPlan(workload));
      return true;
    }
    return false;
  }

  @Override
  public HoodieWriteOutput<DataStream<WriteStatus>> upsertPreppedRecords(HoodieWriteInput<DataStream<HoodieRecord<T>>> preppedRecords, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteInput<DataStream<HoodieRecord<T>>> filterExists(HoodieWriteInput<DataStream<HoodieRecord<T>>> hoodieRecords) {
    return null;
  }

  @Override
  public HoodieWriteOutput<DataStream<WriteStatus>> insert(HoodieWriteInput<DataStream<HoodieRecord<T>>> records, String instantTime) {
    return new HoodieWriteOutput<DataStream<WriteStatus>>(null);
  }

  @Override
  public HoodieWriteOutput<DataStream<WriteStatus>> insertPreppedRecords(HoodieWriteInput<DataStream<HoodieRecord<T>>> preppedRecords, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<DataStream<WriteStatus>> bulkInsert(HoodieWriteInput<DataStream<HoodieRecord<T>>> records, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<DataStream<WriteStatus>> delete(HoodieWriteKey<DataStream<HoodieKey>> keys, String instantTime) {
    return null;
  }

  @Override
  public HoodieEngineContext<HoodieWriteInput<DataStream<HoodieRecord<T>>>, HoodieWriteOutput<DataStream<WriteStatus>>> getEngineContext() {
    return null;
  }


  public HoodieIndexV2<HoodieWriteInput<DataStream<HoodieRecord<T>>>, HoodieWriteInput<DataStream<HoodieRecord<T>>>> getIndex() {
    return index;
  }

  public HoodieWriteConfig getConfig() {
    return config;
  }

  public HoodieEngineContext getContext() {
    return context;
  }

  protected HoodieTableMetaClient createMetaClient(boolean loadActiveTimelineOnLoad) {
    return ClientUtils.createMetaClient(context, config, loadActiveTimelineOnLoad);
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   */
  public HoodieCleanMetadata clean(String cleanInstantTime) throws HoodieIOException {
    LOG.info("Cleaner started");
    final Timer.Context context = metrics.getCleanCtx();

    HoodieCleanMetadata metadata = HoodieTable.create(config, context).clean(context, cleanInstantTime);

    if (context != null) {
      long durationMs = metrics.getDurationInMs(context.stop());
      metrics.updateCleanMetrics(durationMs, metadata.getTotalFilesDeleted());
      LOG.info("Cleaned " + metadata.getTotalFilesDeleted() + " files"
          + " Earliest Retained Instant :" + metadata.getEarliestCommitToRetain()
          + " cleanerElaspsedMs" + durationMs);
    }

    return metadata;
  }

  /**
   * Provides a new commit time for a write operation (insert/update).
   */
  public String startCommit() {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackInFlight) {
      // Only rollback inflight commit/delta-commits. Do not touch compaction commits
      rollbackInflightCommits();
    }
    String commitTime = HoodieActiveTimeline.createNewCommitTime();
    startCommit(commitTime);
    return commitTime;
  }

  private void startCommit(String instantTime) {
    logger.info("Generate a new instant time " + instantTime);
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // if there are pending compactions, their instantTime must not be greater than that of this instant time
    metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().ifPresent(latestPending -> {
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(latestPending.getTimestamp(), instantTime, HoodieTimeline.LESSER),
          "Latest pending compaction instant time must be earlier " + "than this instant time. Latest Compaction :"
              + latestPending + ",  Ingesting at " + instantTime);
    });
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    String commitActionType = table.getMetaClient().getCommitActionType();
    activeTimeline.createInflight(new HoodieInstant(true, commitActionType, instantTime));
  }
}
