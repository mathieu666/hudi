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

package org.apache.hudi.client;

import com.codahale.metrics.Timer;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.index.AbstractHoodieIndex;
import org.apache.hudi.metrics.HoodieMetrics;
import org.apache.hudi.table.HoodieMergeOnReadTable;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;
import java.util.Map;

/**
 * Abstract Write Client providing functionality for performing commit, index updates and rollback
 * Reused for regular write operations like upsert/insert/bulk-insert.. as well as bootstrap
 *
 * @param <T> Sub type of HoodieRecordPayload
 */
public abstract class AbstractHoodieWriteClient<T extends HoodieRecordPayload<T>, I, K, O, P> extends AbstractHoodieClient {

  private static final Logger LOG = LogManager.getLogger(AbstractHoodieWriteClient.class);

  private final transient HoodieMetrics metrics;
  public final transient HoodieTable<T, I, K, O, P> table;
  private final transient AbstractHoodieIndex<T, I, K, O, P> index;

  private transient Timer.Context writeContext = null;
  private transient WriteOperationType operationType;

  private static final long serialVersionUID = 1L;
  private static final String LOOKUP_STR = "lookup";
  private final boolean rollbackPending;
//  private final transient HoodieMetrics metrics;
//  private transient Timer.Context compactionTimer;

  public void setOperationType(WriteOperationType operationType) {
    this.operationType = operationType;
  }

  public WriteOperationType getOperationType() {
    return this.operationType;
  }

  protected AbstractHoodieWriteClient(HoodieEngineContext context, HoodieTable<T, I, K, O, P> table,
                                      AbstractHoodieIndex<T, I, K, O, P> index, HoodieWriteConfig clientConfig) {
    this(context, table, index, clientConfig, false);
  }

  protected AbstractHoodieWriteClient(HoodieEngineContext context, HoodieTable<T, I, K, O, P> table,
                                      AbstractHoodieIndex<T, I, K, O, P> index, HoodieWriteConfig clientConfig, boolean rollbackPending) {
    super(context, clientConfig);
    this.metrics = new HoodieMetrics(config, config.getTableName());
    this.table = table;
    this.index = index;
    this.rollbackPending = rollbackPending;
  }

  public abstract HoodieWriteInput<T> filterExists(HoodieWriteInput<T> hoodieRecords, final String instantTime);

  public abstract HoodieWriteOutput<O> upsert(HoodieWriteInput<T> hoodieRecords, final String instantTime);

  public abstract HoodieWriteOutput<O> upsertPreppedRecords(HoodieWriteInput<T> hoodieRecords, final String instantTime);

  public abstract HoodieWriteOutput<O> insert(HoodieWriteInput<T> hoodieRecords, final String instantTime);

  public abstract HoodieWriteOutput<O> insertPreppedRecords(HoodieWriteInput<T> hoodieRecords, final String instantTime);

  public HoodieWriteOutput<O> bulkInsert(HoodieWriteInput<T> hoodieRecords, final String instantTime) {
    return bulkInsert(hoodieRecords, instantTime, Option.empty());
  }

  public abstract HoodieWriteOutput<O> bulkInsert(HoodieWriteInput<T> records, final String instantTime,
                               Option<UserDefinedBulkInsertPartitioner<T>> bulkInsertPartitioner);

  public abstract HoodieWriteOutput<O> bulkInsertPreppedRecords(HoodieWriteInput<T> preppedRecords, final String instantTime,
                                             Option<UserDefinedBulkInsertPartitioner<T>> bulkInsertPartitioner);

  public abstract HoodieWriteOutput<O> delete(HoodieWriteInput<T> hoodieRecords, final String instantTime);

  public abstract HoodieWriteOutput<O> postWrite(HoodieWriteMetadata<O> result, String instantTime, HoodieTable<T, I, K, O, P> hoodieTable);

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, HoodieWriteOutput<O> writeStatuses) {
    return commit(instantTime, writeStatuses, Option.empty());
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, HoodieWriteOutput<O> writeStatuses,
                        Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    return commit(instantTime, writeStatuses, extraMetadata, metaClient.getCommitActionType());
  }

  public abstract boolean commit(String instantTime, HoodieWriteOutput<O> writeStatuses,
                                 Option<Map<String, String>> extraMetadata, String actionType);

  void emitCommitMetrics(String instantTime, HoodieCommitMetadata metadata, String actionType) {
    try {

      if (writeContext != null) {
        long durationInMs = metrics.getDurationInMs(writeContext.stop());
        metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(instantTime).getTime(), durationInMs,
            metadata, actionType);
        writeContext = null;
      }
    } catch (ParseException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime
          + "Instant time is not of valid format", e);
    }
  }

  /**
   * Create a savepoint based on the latest commit action on the timeline.
   *
   * @param user    - User creating the savepoint
   * @param comment - Comment for the savepoint
   */
  public void savepoint(String user, String comment) {
    if (table.getCompletedCommitsTimeline().empty()) {
      throw new HoodieSavepointException("Could not savepoint. Commit timeline is empty");
    }

    String latestCommit = table.getCompletedCommitsTimeline().lastInstant().get().getTimestamp();
    LOG.info("Savepointing latest commit " + latestCommit);
    savepoint(latestCommit, user, comment);
  }

  public void savepoint(String instantTime, String user, String comment) {
    table.savepoint(context, instantTime, user, comment);
  }

  /**
   * Delete a savepoint that was created. Once the savepoint is deleted, the commit can be rolledback and cleaner may
   * clean up data files.
   *
   * @param savepointTime - delete the savepoint
   * @return true if the savepoint was deleted successfully
   */
  public abstract void deleteSavepoint(String savepointTime);

  public abstract void restoreToSavepoint(String savepointTime);

  /**
   * Rollback the inflight record changes with the given commit time.
   *
   * @param commitInstantTime Instant time of the commit
   * @throws HoodieRollbackException if rollback cannot be performed successfully
   */
  public boolean rollback(final String commitInstantTime) throws HoodieRollbackException {
    LOG.info("Begin rollback of instant " + commitInstantTime);
    final String rollbackInstantTime = HoodieActiveTimeline.createNewInstantTime();
    final Timer.Context context = this.metrics.getRollbackCtx();
    try {
      Option<HoodieInstant> commitInstantOpt = Option.fromJavaOptional(table.getActiveTimeline().getCommitsTimeline().getInstants()
          .filter(instant -> HoodieActiveTimeline.EQUALS.test(instant.getTimestamp(), commitInstantTime))
          .findFirst());
      if (commitInstantOpt.isPresent()) {
        HoodieRollbackMetadata rollbackMetadata = table.rollback(super.context, rollbackInstantTime, commitInstantOpt.get(), true);
        if (context != null) {
          long durationInMs = metrics.getDurationInMs(context.stop());
          metrics.updateRollbackMetrics(durationInMs, rollbackMetadata.getTotalFilesDeleted());
        }
        return true;
      } else {
        LOG.warn("Cannot find instant " + commitInstantTime + " in the timeline, for rollback");
        return false;
      }
    } catch (Exception e) {
      throw new HoodieRollbackException("Failed to rollback " + config.getBasePath() + " commits " + commitInstantTime, e);
    }
  }

  /**
   * Post Commit Hook. Derived classes use this method to perform post-commit processing
   *
   * @param metadata      Commit Metadata corresponding to committed instant
   * @param instantTime   Instant Time
   * @param extraMetadata Additional Metadata passed by user
   */
  protected abstract void postCommit(HoodieCommitMetadata metadata, String instantTime, Option<Map<String, String>> extraMetadata);

  /**
   * Finalize Write operation.
   *
   * @param table       HoodieTable
   * @param instantTime Instant Time
   * @param stats       Hoodie Write Stat
   */
  protected void finalizeWrite(HoodieMergeOnReadTable<T, I, K, O, P> table, String instantTime, List<HoodieWriteStat> stats) {
    try {
      final Timer.Context finalizeCtx = metrics.getFinalizeCtx();
      table.finalizeWrite(context, instantTime, stats);
      if (finalizeCtx != null) {
        Option<Long> durationInMs = Option.of(metrics.getDurationInMs(finalizeCtx.stop()));
        durationInMs.ifPresent(duration -> {
          LOG.info("Finalize write elapsed time (milliseconds): " + duration);
          metrics.updateFinalizeWriteMetrics(duration, stats.size());
        });
      }
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  private void updateMetadataAndRollingStats(String actionType, HoodieCommitMetadata metadata,
                                             List<HoodieWriteStat> writeStats) {
    // TODO : make sure we cannot rollback / archive last commit file
    try {
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

  public HoodieMetrics getMetrics() {
    return metrics;
  }

  public AbstractHoodieIndex<T, I, K, O, P> getIndex() {
    return index;
  }

  /**
   * Sets write schema from last instant since deletes may not have schema set in the config.
   */
  private void setWriteSchemaForDeletes(HoodieTableMetaClient metaClient) {
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

  @Override
  public void close() {
    // Stop timeline-server if running
    super.close();
    // Calling this here releases any resources used by your index, so make sure to finish any related operations
    // before this point
    this.index.close();
  }
}
