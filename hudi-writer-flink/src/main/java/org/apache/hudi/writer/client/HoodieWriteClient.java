/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.writer.client;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.writer.common.HoodieWriteInput;
import org.apache.hudi.writer.common.HoodieWriteOutput;
import org.apache.hudi.writer.config.HoodieCompactionConfig;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.exception.HoodieCompactionException;
import org.apache.hudi.writer.exception.HoodieInsertException;
import org.apache.hudi.writer.exception.HoodieRestoreException;
import org.apache.hudi.writer.exception.HoodieRollbackException;
import org.apache.hudi.writer.exception.HoodieUpsertException;
import org.apache.hudi.writer.index.HoodieIndex;
import org.apache.hudi.writer.table.HoodieCommitArchiveLog;
import org.apache.hudi.writer.table.HoodieTable;
import org.apache.hudi.writer.table.action.commit.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Hoodie Write Client helps you build tables on HDFS [insert()] and then perform efficient mutations on an HDFS
 * table [upsert()]
 * <p>
 * Note that, at any given time, there can only be one Spark job performing these operations on a Hoodie table.
 */
public class HoodieWriteClient<T extends HoodieRecordPayload> extends AbstractHoodieWriteClient<T> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieWriteClient.class);
  private static final String LOOKUP_STR = "lookup";
  private final boolean rollbackPending;

  /**
   * Create a write client, without cleaning up failed/inflight commits.
   *
   * @param jsc          Java Spark Context
   * @param clientConfig instance of HoodieWriteConfig
   */
  public HoodieWriteClient(Configuration jsc, HoodieWriteConfig clientConfig) {
    this(jsc, clientConfig, false);
  }

  /**
   * Create a write client, with new hudi index.
   *
   * @param jsc             Java Spark Context
   * @param clientConfig    instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   */
  public HoodieWriteClient(Configuration jsc, HoodieWriteConfig clientConfig, boolean rollbackPending) {
    this(jsc, clientConfig, rollbackPending, HoodieIndex.createIndex(clientConfig));
  }

  /**
   * Create a write client, allows to specify all parameters.
   *
   * @param jsc             Java Spark Context
   * @param clientConfig    instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   */
  public HoodieWriteClient(Configuration jsc, HoodieWriteConfig clientConfig, boolean rollbackPending,
                           HoodieIndex index) {
    super(jsc, index, clientConfig);
    this.rollbackPending = rollbackPending;
  }

  /**
   * Filter out HoodieRecords that already exists in the output folder. This is useful in deduplication.
   *
   * @param hoodieRecords Input RDD of Hoodie records.
   * @return A subset of hoodieRecords RDD, with existing records filtered out.
   */
  public HoodieWriteInput<List<HoodieRecord<T>>> filterExists(HoodieWriteInput<List<HoodieRecord<T>>> hoodieRecords) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.create(config, hadoopConf);
    HoodieWriteInput<List<HoodieRecord<T>>> recordsWithLocation = getIndex().tagLocation(hoodieRecords, hadoopConf, table);
    return new HoodieWriteInput<>(recordsWithLocation.getInputs().stream().filter(v1 -> !v1.isCurrentLocationKnown()).collect(Collectors.toList()));
  }

  /**
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * @param records     List of hoodieRecords to upsert
   * @param instantTime Instant time of the commit
   * @return List[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteOutput<List<WriteStatus>> upsert(HoodieWriteInput<List<HoodieRecord<T>>> records, final String instantTime) {
    HoodieTable<T> table = getTableAndInitCtx(WriteOperationType.UPSERT);
    validateSchema(table, true);
    setOperationType(WriteOperationType.UPSERT);
    HoodieWriteMetadata result = table.upsert(hadoopConf, instantTime, records);
    return postWrite(result, instantTime, table);
  }

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * @param preppedRecords Prepared HoodieRecords to upsert
   * @param instantTime    Instant time of the commit
   * @return List[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteOutput<List<WriteStatus>> upsertPreppedRecords(HoodieWriteInput<List<HoodieRecord<T>>> preppedRecords, final String instantTime) {
    HoodieTable<T> table = getTableAndInitCtx(WriteOperationType.UPSERT_PREPPED);
    validateSchema(table, true);
    setOperationType(WriteOperationType.UPSERT_PREPPED);
    HoodieWriteMetadata result = table.upsertPrepped(hadoopConf, instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  /**
   * Inserts the given HoodieRecords, into the table. This API is intended to be used for normal writes.
   * <p>
   * This implementation skips the index check and is able to leverage benefits such as small file handling/blocking
   * alignment, as with upsert(), by profiling the workload
   *
   * @param records     HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return List[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteOutput<List<WriteStatus>> insert(HoodieWriteInput<List<HoodieRecord<T>>> records, final String instantTime) {
    HoodieTable<T> table = getTableAndInitCtx(WriteOperationType.INSERT);
    validateSchema(table, false);
    setOperationType(WriteOperationType.INSERT);
    HoodieWriteMetadata result = table.insert(hadoopConf, instantTime, records);
    return postWrite(result, instantTime, table);
  }

  public HoodieWriteOutput<List<WriteStatus>> bulkInsert(HoodieWriteInput<List<HoodieRecord<T>>> records, final String instantTime) {
    // TODO
    return new HoodieWriteOutput(new ArrayList<>());
  }

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation skips the index check, skips de-duping and is able to leverage benefits such as small file
   * handling/blocking alignment, as with insert(), by profiling the workload. The prepared HoodieRecords should be
   * de-duped if needed.
   *
   * @param preppedRecords HoodieRecords to insert
   * @param instantTime    Instant time of the commit
   * @return List[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteOutput<List<WriteStatus>> insertPreppedRecords(HoodieWriteInput<List<HoodieRecord<T>>> preppedRecords, final String instantTime) {
    HoodieTable<T> table = getTableAndInitCtx(WriteOperationType.INSERT_PREPPED);
    validateSchema(table, false);
    setOperationType(WriteOperationType.INSERT_PREPPED);
    HoodieWriteMetadata result = table.insertPrepped(hadoopConf, instantTime, preppedRecords);
    return postWrite(result, instantTime, table);
  }

  /**
   * Common method containing steps to be performed after write (upsert/insert/..) operations including auto-commit.
   *
   * @param result      Commit Action Result
   * @param instantTime Instant Time
   * @param hoodieTable Hoodie Table
   * @return Write Status
   */
  private HoodieWriteOutput<List<WriteStatus>> postWrite(HoodieWriteMetadata result, String instantTime, HoodieTable<T> hoodieTable) {

    if (result.isCommitted()) {
      postCommit(result.getCommitMetadata().get(), instantTime, Option.empty());

      hoodieTable.getMetaClient().getCommitActionType();
    }
    return result.getWriteStatuses();
  }

  @Override
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
      archiveLog.archiveIfRequired(hadoopConf);
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
   * Delete a savepoint that was created. Once the savepoint is deleted, the commit can be rolledback and cleaner may
   * clean up data files.
   *
   * @param savepointTime - delete the savepoint
   * @return true if the savepoint was deleted successfully
   */
  public void deleteSavepoint(String savepointTime) {
    HoodieTable<T> table = HoodieTable.create(config, hadoopConf);
    if (table.getMetaClient().getTableType() == HoodieTableType.MERGE_ON_READ) {
      throw new UnsupportedOperationException("Savepointing is not supported or MergeOnRead table types");
    }
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      LOG.warn("No savepoint present " + savepointTime);
      return;
    }

    activeTimeline.revertToInflight(savePoint);
    activeTimeline.deleteInflight(new HoodieInstant(true, HoodieTimeline.SAVEPOINT_ACTION, savepointTime));
    LOG.info("Savepoint " + savepointTime + " deleted");
  }

  /**
   * Delete a compaction request that is pending.
   * <p>
   * NOTE - This is an Admin operation. With async compaction, this is expected to be called with async compaction and
   * write shutdown. Otherwise, async compactor could fail with errors
   *
   * @param compactionTime - delete the compaction time
   */
  private void deleteRequestedCompaction(String compactionTime) {
    HoodieTable<T> table = HoodieTable.create(config, hadoopConf);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieInstant compactionRequestedInstant =
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionTime);
    boolean isCompactionInstantInRequestedState =
        table.getActiveTimeline().filterPendingCompactionTimeline().containsInstant(compactionRequestedInstant);
    HoodieTimeline commitTimeline = table.getCompletedCommitTimeline();
    if (commitTimeline.empty() && !commitTimeline.findInstantsAfter(compactionTime, Integer.MAX_VALUE).empty()) {
      throw new HoodieRollbackException(
          "Found commits after time :" + compactionTime + ", please rollback greater commits first");
    }
    if (isCompactionInstantInRequestedState) {
      activeTimeline.deleteCompactionRequested(compactionRequestedInstant);
    } else {
      throw new IllegalArgumentException("Compaction is not in requested state " + compactionTime);
    }
    LOG.info("Compaction " + compactionTime + " deleted");
  }

  /**
   * Restore the state to the savepoint. WARNING: This rollsback recent commits and deleted data files. Queries
   * accessing the files will mostly fail. This should be done during a downtime.
   *
   * @param savepointTime - savepoint time to rollback to
   * @return true if the savepoint was rollecback to successfully
   */
  public boolean restoreToSavepoint(String savepointTime) {
    HoodieTable<T> table = HoodieTable.create(config, hadoopConf);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();

    // Rollback to savepoint is expected to be a manual operation and no concurrent write or compaction is expected
    // to be running. Rollback to savepoint also removes any pending compaction actions that are generated after
    // savepoint time. Allowing pending compaction to be retained is not safe as those workload could be referencing
    // file-slices that will be rolled-back as part of this operation
    HoodieTimeline instantTimeline = table.getMetaClient().getCommitsAndCompactionTimeline();

    HoodieInstant savePoint = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    boolean isSavepointPresent = table.getCompletedSavepointTimeline().containsInstant(savePoint);
    if (!isSavepointPresent) {
      throw new HoodieRollbackException("No savepoint for instantTime " + savepointTime);
    }

    List<String> commitsToRollback = instantTimeline.findInstantsAfter(savepointTime, Integer.MAX_VALUE).getInstants()
        .map(HoodieInstant::getTimestamp).collect(Collectors.toList());
    LOG.info("Rolling back commits " + commitsToRollback);

    restoreToInstant(savepointTime);

    // Make sure the rollback was successful
    Option<HoodieInstant> lastInstant =
        activeTimeline.reload().getCommitsAndCompactionTimeline().filterCompletedAndCompactionInstants().lastInstant();
    ValidationUtils.checkArgument(lastInstant.isPresent());
    ValidationUtils.checkArgument(lastInstant.get().getTimestamp().equals(savepointTime),
        savepointTime + "is not the last commit after rolling back " + commitsToRollback + ", last commit was "
            + lastInstant.get().getTimestamp());
    return true;
  }

  /**
   * Rollback the inflight record changes with the given commit time.
   *
   * @param commitInstantTime Instant time of the commit
   * @throws HoodieRollbackException if rollback cannot be performed successfully
   */
  public boolean rollback(final String commitInstantTime) throws HoodieRollbackException {
    LOG.info("Begin rollback of instant " + commitInstantTime);
    final String rollbackInstantTime = HoodieActiveTimeline.createNewInstantTime();
    try {
      HoodieTable<T> table = HoodieTable.create(config, hadoopConf);
      Option<HoodieInstant> commitInstantOpt = Option.fromJavaOptional(table.getActiveTimeline().getCommitsTimeline().getInstants()
          .filter(instant -> HoodieActiveTimeline.EQUAL.test(instant.getTimestamp(), commitInstantTime))
          .findFirst());
      if (commitInstantOpt.isPresent()) {
        HoodieRollbackMetadata rollbackMetadata = table.rollback(hadoopConf, rollbackInstantTime, commitInstantOpt.get(), true);
        return true;
      } else {
        LOG.info("Cannot find instant " + commitInstantTime + " in the timeline, for rollback");
        return false;
      }
    } catch (Exception e) {
      throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime, e);
    }
  }

  /**
   * NOTE : This action requires all writers (ingest and compact) to a table to be stopped before proceeding. Revert
   * the (inflight/committed) record changes for all commits after the provided instant time.
   *
   * @param instantTime Instant time to which restoration is requested
   */
  public HoodieRestoreMetadata restoreToInstant(final String instantTime) throws HoodieRestoreException {
    LOG.info("Begin restore to instant " + instantTime);
    final String restoreInstantTime = HoodieActiveTimeline.createNewInstantTime();
    try {
      HoodieTable<T> table = HoodieTable.create(config, hadoopConf);
      HoodieRestoreMetadata restoreMetadata = table.restore(hadoopConf, restoreInstantTime, instantTime);
      return restoreMetadata;
    } catch (Exception e) {
      throw new HoodieRestoreException("Failed to restore to " + instantTime, e);
    }
  }

  /**
   * Releases any resources used by the client.
   */
  @Override
  public void close() {
    // Stop timeline-server if running
    super.close();
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   */
  public HoodieCleanMetadata clean(String cleanInstantTime) throws HoodieIOException {
    LOG.info("Cleaner started");
    HoodieCleanMetadata metadata = HoodieTable.create(config, hadoopConf).clean(hadoopConf, cleanInstantTime);
    return metadata;
  }

  public HoodieCleanMetadata clean() {
    return clean(HoodieActiveTimeline.createNewInstantTime());
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete).
   */
  public String startCommit() {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackPending) {
      // Only rollback pending commit/delta-commits. Do not touch compaction commits
      rollbackPendingCommits();
    }
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    startCommit(instantTime);
    return instantTime;
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete).
   *
   * @param instantTime Instant time to be generated
   */
  public void startCommitWithTime(String instantTime) {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackPending) {
      // Only rollback inflight commit/delta-commits. Do not touch compaction commits
      rollbackPendingCommits();
    }
    startCommit(instantTime);
  }

  public void startCommit(String instantTime) {
    LOG.info("Generate a new instant time " + instantTime);
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // if there are pending compactions, their instantTime must not be greater than that of this instant time
    metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().ifPresent(latestPending ->
        ValidationUtils.checkArgument(
            HoodieTimeline.compareTimestamps(latestPending.getTimestamp(), instantTime, HoodieTimeline.LESSER),
            "Latest pending compaction instant time must be earlier than this instant time. Latest Compaction :"
                + latestPending + ",  Ingesting at " + instantTime));
    HoodieTable<T> table = HoodieTable.create(metaClient, config, hadoopConf);
    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    String commitActionType = table.getMetaClient().getCommitActionType();
    activeTimeline.createNewInstant(new HoodieInstant(State.REQUESTED, commitActionType, instantTime));
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
    HoodieTable<T> table = HoodieTable.create(metaClient, config, hadoopConf);
    HoodieCompactionPlan workload = table.scheduleCompaction(hadoopConf, instantTime);
    if (workload != null && (workload.getOperations() != null) && (!workload.getOperations().isEmpty())) {
      extraMetadata.ifPresent(workload::setExtraMetadata);
      HoodieInstant compactionInstant =
          new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
      metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
          TimelineMetadataUtils.serializeCompactionPlan(workload));
      return true;
    }
    return false;
  }

  /**
   * Performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return RDD of WriteStatus to inspect errors and counts
   */
  public HoodieWriteOutput<List<WriteStatus>> compact(String compactionInstantTime) throws IOException {
    return compact(compactionInstantTime, config.shouldAutoCommit());
  }

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @param writeStatuses         RDD of WriteStatus to inspect errors and counts
   * @param extraMetadata         Extra Metadata to be stored
   */
  public void commitCompaction(String compactionInstantTime, HoodieWriteOutput<List<WriteStatus>> writeStatuses,
                               Option<Map<String, String>> extraMetadata) throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieTable<T> table = HoodieTable.create(metaClient, config, hadoopConf);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieCompactionPlan compactionPlan = TimelineMetadataUtils.deserializeCompactionPlan(
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
    commitCompaction(writeStatuses, table, compactionInstantTime, true, mergedMetaData);
  }

  /**
   * Cleanup all pending commits.
   */
  private void rollbackPendingCommits() {
    HoodieTable<T> table = HoodieTable.create(config, hadoopConf);
    HoodieTimeline inflightTimeline = table.getMetaClient().getCommitsTimeline().filterPendingExcludingCompaction();
    List<String> commits = inflightTimeline.getReverseOrderedInstants().map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
    for (String commit : commits) {
      rollback(commit);
    }
  }

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return RDD of Write Status
   */
  private HoodieWriteOutput<List<WriteStatus>> compact(String compactionInstantTime, boolean autoCommit) throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieTable<T> table = HoodieTable.create(metaClient, config, hadoopConf);
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

  /**
   * Perform compaction operations as specified in the compaction commit file.
   *
   * @param compactionInstant Compaction Instant time
   * @param activeTimeline    Active Timeline
   * @param autoCommit        Commit after compaction
   * @return RDD of Write Status
   */
  private HoodieWriteOutput<List<WriteStatus>> runCompaction(HoodieInstant compactionInstant, HoodieActiveTimeline activeTimeline,
                                                             boolean autoCommit) throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.getCompactionPlan(metaClient, compactionInstant.getTimestamp());
    // Mark instant as compaction inflight
    activeTimeline.transitionCompactionRequestedToInflight(compactionInstant);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.create(metaClient, config, hadoopConf);
    HoodieWriteOutput<List<WriteStatus>> statuses = table.compact(hadoopConf, compactionInstant.getTimestamp(), compactionPlan);
    // pass extra-metada so that it gets stored in commit file automatically
    commitCompaction(new HoodieWriteOutput(statuses), table, compactionInstant.getTimestamp(), autoCommit,
        Option.ofNullable(compactionPlan.getExtraMetadata()));
    return statuses;
  }

  /**
   * Commit Compaction and track metrics.
   *
   * @param compactedStatuses    Compaction Write status
   * @param table                Hoodie Table
   * @param compactionCommitTime Compaction Commit Time
   * @param autoCommit           Auto Commit
   * @param extraMetadata        Extra Metadata to store
   */
  protected void commitCompaction(HoodieWriteOutput<List<WriteStatus>> compactedStatuses, HoodieTable<T> table,
                                  String compactionCommitTime, boolean autoCommit, Option<Map<String, String>> extraMetadata) {
    if (autoCommit) {
      HoodieCommitMetadata metadata = doCompactionCommit(table, compactedStatuses, compactionCommitTime, extraMetadata);
      LOG.info("Compacted successfully on commit " + compactionCommitTime);
    } else {
      LOG.info("Compaction did not run for commit " + compactionCommitTime);
    }
  }

  /**
   * Rollback failed compactions. Inflight rollbacks for compactions revert the .inflight file to the .requested file
   *
   * @param inflightInstant Inflight Compaction Instant
   * @param table           Hoodie Table
   */
  public void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieTable table) {
    table.rollback(hadoopConf, HoodieActiveTimeline.createNewInstantTime(), inflightInstant, false);
    table.getActiveTimeline().revertCompactionInflightToRequested(inflightInstant);
  }

  private HoodieCommitMetadata doCompactionCommit(HoodieTable<T> table, HoodieWriteOutput<List<WriteStatus>> writeStatuses,
                                                  String compactionCommitTime, Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    List<HoodieWriteStat> updateStatusMap = writeStatuses.getOutput().stream().map(WriteStatus::getStat).collect(Collectors.toList());

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }

    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());

    // Finalize write
    finalizeWrite(table, compactionCommitTime, updateStatusMap);

    // Copy extraMetadata
    extraMetadata.ifPresent(m -> {
      m.forEach(metadata::addMetadata);
    });

    LOG.info("Committing Compaction " + compactionCommitTime + ". Finished with result " + metadata);
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    try {
      activeTimeline.transitionCompactionInflightToComplete(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionCommitTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + metaClient.getBasePath() + " at time " + compactionCommitTime, e);
    }
    return metadata;
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
   * Ensure that the current writerSchema is compatible with the latest schema of this dataset.
   * <p>
   * When inserting/updating data, we read records using the last used schema and convert them to the
   * GenericRecords with writerSchema. Hence, we need to ensure that this conversion can take place without errors.
   *
   * @param hoodieTable The Hoodie Table
   * @param isUpsert    If this is a check during upserts
   * @throws HoodieUpsertException If schema check fails during upserts
   * @throws HoodieInsertException If schema check fails during inserts
   */
  private void validateSchema(HoodieTable<T> hoodieTable, final boolean isUpsert)
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
      TableSchemaResolver schemaUtil = new TableSchemaResolver(hoodieTable.getMetaClient());
      writerSchema = HoodieAvroUtils.createHoodieWriteSchema(config.getSchema());
      tableSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableSchemaFromCommitMetadata());
      isValid = TableSchemaResolver.isSchemaCompatible(tableSchema, writerSchema);
    } catch (Exception e) {
      // Two error cases are possible:
      // 1. There was no schema as no data has been inserted yet (first time only)
      // 2. Failure in reading the schema
      isValid = hoodieTable.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().countInstants() == 0;
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

  public List<String> getInflightsAndRequestedInstants(String commitType) {
    HoodieTable<T> table = HoodieTable.create(config, hadoopConf);
    HoodieTimeline unCompletedTimeline = table.getMetaClient().getCommitsTimeline().filterInflightsAndRequested();
    return unCompletedTimeline.getInstants().filter(x -> x.getAction().equals(commitType)).map(HoodieInstant::getTimestamp)
        .collect(Collectors.toList());
  }
}