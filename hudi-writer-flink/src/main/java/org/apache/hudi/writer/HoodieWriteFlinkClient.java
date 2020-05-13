package org.apache.hudi.writer;

import com.codahale.metrics.Timer;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.hudi.HoodieEngineContext;
import org.apache.hudi.HoodieWriteClientV2;
import org.apache.hudi.HoodieWriteMetadata;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
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
import org.apache.hudi.index.hbase.HBaseIndexV2;
import org.apache.hudi.writer.exception.HoodieInsertException;
import org.apache.hudi.writer.exception.HoodieUpsertException;
import org.apache.hudi.writer.util.ClientUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class HoodieWriteFlinkClient<T extends HoodieRecordPayload> implements
    HoodieWriteClientV2<
        HoodieWriteInput<List<HoodieRecord<T>>>,
        HoodieWriteKey<List<HoodieKey>>,
        HoodieWriteOutput<List<WriteStatus>>> {
  private static final Logger LOG = LoggerFactory.getLogger(HoodieWriteFlinkClient.class);

  private final transient HoodieIndexV2<HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteOutput<List<WriteStatus>>> index;
  protected final HoodieWriteConfig config;
  private final transient HoodieEngineContext context;
  private static final String LOOKUP_STR = "lookup";
  protected final String basePath;

  private transient WriteOperationType operationType;

  public WriteOperationType getOperationType() {
    return this.operationType;
  }

  public void setOperationType(WriteOperationType operationType) {
    this.operationType = operationType;
  }

  public HoodieWriteFlinkClient(HoodieEngineContext context, HoodieWriteConfig config) {
    this.context = context;
    this.config = config;
    this.index = new HBaseIndexV2(context, config);
    this.basePath = config.getBasePath();
  }


  @Override
  public HoodieWriteOutput<List<WriteStatus>> upsert(HoodieWriteInput<List<HoodieRecord<T>>> hoodieRecords, String instantTime) {
    HoodieTableV2 table = getTableAndInitCtx(WriteOperationType.UPSERT);
    validateSchema(table, true);
    setOperationType(WriteOperationType.UPSERT);
    HoodieWriteMetadata result = table.upsert(instantTime, hoodieRecords);
    return postWrite(result, instantTime, table);
  }

  private HoodieWriteOutput<List<WriteStatus>> postWrite(HoodieWriteMetadata result, String instantTime, HoodieTableV2 table) {
//    if (result.getIndexLookupDuration().isPresent()) {
//      metrics.updateIndexMetrics(getOperationType().name(), result.getIndexUpdateDuration().get().toMillis());
//    }
    if (result.isCommitted()) {
      // Perform post commit operations.
//      if (result.getFinalizeDuration().isPresent()) {
//        metrics.updateFinalizeWriteMetrics(result.getFinalizeDuration().get().toMillis(),
//            result.getWriteStats().get().size());
//      }

      postCommit(result.getCommitMetadata().get(), instantTime, Option.empty());

//      emitCommitMetrics(instantTime, result.getCommitMetadata().get(),
//          table.getMetaClient().getCommitActionType());
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
  private List<WriteStatus> compact(String compactionInstantTime, boolean autoCommit) throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieTableV2<T> table = HoodieTableV2.create(metaClient, config, context);
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

  private List<WriteStatus> runCompaction(HoodieInstant compactionInstant, HoodieActiveTimeline activeTimeline,
                                          boolean autoCommit) throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.getCompactionPlan(metaClient, compactionInstant.getTimestamp());
    // Mark instant as compaction inflight
    activeTimeline.transitionCompactionRequestedToInflight(compactionInstant);
    compactionTimer = metrics.getCompactionCtx();
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableV2<T> table = HoodieTableV2.create(metaClient, config, jsc);
    List<WriteStatus> statuses = table.compact(context, compactionInstant.getTimestamp(), compactionPlan);
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
   * @param table           Hoodie Table
   */
  public void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieTableV2 table) {
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
   * @param instantTime   Compaction Instant Time
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
    HoodieTableV2<T> table = HoodieTableV2.create(metaClient, config, context);
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
  public HoodieWriteOutput<List<WriteStatus>> upsertPreppedRecords(HoodieWriteInput<List<HoodieRecord<T>>> preppedRecords, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteInput<List<HoodieRecord<T>>> filterExists(HoodieWriteInput<List<HoodieRecord<T>>> hoodieRecords) {
    return null;
  }

  @Override
  public HoodieWriteOutput<List<WriteStatus>> insert(HoodieWriteInput<List<HoodieRecord<T>>> records, String instantTime) {
    return new HoodieWriteOutput<List<WriteStatus>>(null);
  }

  @Override
  public HoodieWriteOutput<List<WriteStatus>> insertPreppedRecords(HoodieWriteInput<List<HoodieRecord<T>>> preppedRecords, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<List<WriteStatus>> bulkInsert(HoodieWriteInput<List<HoodieRecord<T>>> records, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<List<WriteStatus>> delete(HoodieWriteKey<List<HoodieKey>> keys, String instantTime) {
    return null;
  }

  @Override
  public HoodieEngineContext<HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteOutput<List<WriteStatus>>> getEngineContext() {
    return null;
  }


  public HoodieIndexV2<HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteInput<List<HoodieRecord<T>>>> getIndex() {
    return index;
  }

  public HoodieWriteConfig getConfig() {
    return config;
  }

  public HoodieEngineContext getContext() {
    return context;
  }

  protected HoodieTableMetaClient createMetaClient(boolean loadActiveTimelineOnLoad) {
    return ClientUtils.createMetaClient(context.getHadoopConf().get(), config, loadActiveTimelineOnLoad);
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   */
  public HoodieCleanMetadata clean(String cleanInstantTime) throws HoodieIOException {
    LOG.info("Cleaner started");
    final Timer.Context context = metrics.getCleanCtx();

    HoodieCleanMetadata metadata = HoodieTableV2.create(config, context).clean(context, cleanInstantTime);

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
  @Override
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
    LOG.info("Generate a new instant time " + instantTime);
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // if there are pending compactions, their instantTime must not be greater than that of this instant time
    metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().ifPresent(latestPending -> {
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(latestPending.getTimestamp(), instantTime, HoodieTimeline.LESSER),
          "Latest pending compaction instant time must be earlier " + "than this instant time. Latest Compaction :"
              + latestPending + ",  Ingesting at " + instantTime);
    });
    HoodieTableV2<T> table = HoodieTableV2.getHoodieTable(metaClient, config, jsc);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    String commitActionType = table.getMetaClient().getCommitActionType();
    activeTimeline.createInflight(new HoodieInstant(true, commitActionType, instantTime));
  }

  protected HoodieTableV2 getTableAndInitCtx(WriteOperationType operationType) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaFromLastInstant(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableV2 table = HoodieTableV2.create(metaClient, context);
    return table;
  }

  /**
   * Sets write schema from last instant since deletes may not have schema set in the config.
   */
  private void setWriteSchemaFromLastInstant(HoodieTableMetaClient metaClient) {
    try {
      HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();
      Option<HoodieInstant> lastInstant =
          activeTimeline.filterCompletedInstants().filter(s -> s.getAction().equals(metaClient.getCommitActionType()))
              .lastInstant();
      if (lastInstant.isPresent()) {
        HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
            activeTimeline.getInstantDetails(lastInstant.get()).get(), HoodieCommitMetadata.class);
        if (commitMetadata.getExtraMetadata().containsKey(HoodieCommitMetadata.SCHEMA_KEY)) {
          config.setSchema(commitMetadata.getExtraMetadata().get(HoodieCommitMetadata.SCHEMA_KEY));
        } else {
          throw new HoodieIOException("Latest commit does not have any schema in commit metadata");
        }
      } else {
        throw new HoodieIOException("Deletes issued without any prior commits");
      }
    } catch (IOException e) {
      throw new HoodieIOException("IOException thrown while reading last commit metadata", e);
    }
  }

  /**
   * Ensure that the current writerSchema is compatible with the latest schema of this dataset.
   * <p>
   * When inserting/updating data, we read records using the last used schema and convert them to the
   * GenericRecords with writerSchema. Hence, we need to ensure that this conversion can take place without errors.
   *
   * @param hoodieTableV2 The Hoodie Table
   * @param isUpsert    If this is a check during upserts
   * @throws HoodieUpsertException If schema check fails during upserts
   * @throws HoodieInsertException If schema check fails during inserts
   */
  private void validateSchema(HoodieTableV2 hoodieTableV2, final boolean isUpsert)
      throws HoodieUpsertException, HoodieInsertException {

    if (!getConfig().getAvroSchemaValidate()) {
      // Check not required
      return;
    }

    boolean isValid = false;
    String errorMsg = "WriterSchema is not compatible with the schema present in the Table";
    Throwable internalError = null;
    Schema tableSchema = null;
    Schema writerSchema = null;
    try {
      TableSchemaResolver schemaUtil = new TableSchemaResolver(hoodieTableV2.getMetaClient());
      writerSchema = HoodieAvroUtils.createHoodieWriteSchema(config.getSchema());
      tableSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableSchemaFromCommitMetadata());
      isValid = TableSchemaResolver.isSchemaCompatible(tableSchema, writerSchema);
    } catch (Exception e) {
      // Two error cases are possible:
      // 1. There was no schema as no data has been inserted yet (first time only)
      // 2. Failure in reading the schema
      isValid = hoodieTableV2.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants() == 0;
      errorMsg = "Failed to read latest schema on path " + basePath;
      internalError = e;
    }

    if (!isValid) {
      LOG.error(errorMsg);
      LOG.warn("WriterSchema: " + writerSchema);
      LOG.warn("Table latest schema: " + tableSchema);
      if (isUpsert) {
        throw new HoodieUpsertException(errorMsg, internalError);
      } else {
        throw new HoodieInsertException(errorMsg, internalError);
      }
    }
  }
}
