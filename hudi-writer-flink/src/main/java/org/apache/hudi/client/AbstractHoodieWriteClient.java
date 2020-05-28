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

import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.context.HoodieEngineContext;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.AbstractHoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Abstract Write Client providing functionality for performing commit, index updates and rollback
 * Reused for regular write operations like upsert/insert/bulk-insert.. as well as bootstrap
 *
 * @param <T> Sub type of HoodieRecordPayload
 */
public abstract class AbstractHoodieWriteClient<T extends HoodieRecordPayload, I, K, O> extends AbstractHoodieClient {

  private static final Logger LOG = LogManager.getLogger(AbstractHoodieWriteClient.class);
  private static final String UPDATE_STR = "update";

  private final transient AbstractHoodieIndex<T, I, O> index;

  private transient WriteOperationType operationType;

  public void setOperationType(WriteOperationType operationType) {
    this.operationType = operationType;
  }

  public WriteOperationType getOperationType() {
    return this.operationType;
  }

  protected AbstractHoodieWriteClient(HoodieEngineContext context, AbstractHoodieIndex index, HoodieWriteConfig clientConfig) {
    super(context, clientConfig);
    this.index = index;
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, O writeStatuses) {
    return commit(instantTime, writeStatuses, Option.empty());
  }

  /**
   * Commit changes performed at the given instantTime marker.
   */
  public boolean commit(String instantTime, O writeStatuses,
                                 Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = createMetaClient(false);
    return commit(instantTime, writeStatuses, extraMetadata, metaClient.getCommitActionType());
  };

  public abstract boolean commit(String instantTime, O writeStatuses,
                         Option<Map<String, String>> extraMetadata, String actionType);

  public abstract O updateIndexAndCommitIfNeeded(O writeStatusRDD, HoodieTable table,
                                                              String instantTime);

  public abstract void commitOnAutoCommit(String instantTime, O resultRDD, String actionType);

  /**
   * Post Commit Hook. Derived classes use this method to perform post-commit processing
   * @param metadata      Commit Metadata corresponding to committed instant
   * @param instantTime   Instant Time
   * @param extraMetadata Additional Metadata passed by user
   */
  protected abstract void postCommit(HoodieCommitMetadata metadata, String instantTime,
                                     Option<Map<String, String>> extraMetadata);

  @Override
  public void close() {
    // Stop timeline-server if running
    super.close();
    // Calling this here releases any resources used by your index, so make sure to finish any related operations
    // before this point
    this.index.close();
  }

  /**
   * Finalize Write operation.
   * @param table  HoodieTable
   * @param instantTime Instant Time
   * @param stats Hoodie Write Stat
   */
  protected void finalizeWrite(HoodieTable table, String instantTime, List<HoodieWriteStat> stats) {
    try {
      table.finalizeWrite(context, instantTime, stats);
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  /**
   * Get HoodieTable .
   *
   * @param operationType write operation type
   * @return HoodieTable
   */
  protected HoodieTable getTableAndInitCtx(WriteOperationType operationType) {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    if (operationType == WriteOperationType.DELETE) {
      setWriteSchemaForDeletes(metaClient);
    }
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable table = HoodieTable.create(metaClient, config, context);
//    if (table.getMetaClient().getCommitActionType().equals(HoodieTimeline.COMMIT_ACTION)) {
//      writeContext = metrics.getCommitCtx();
//    } else {
//      writeContext = metrics.getDeltaCommitCtx();
//    }
    return table;
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


  public AbstractHoodieIndex getIndex() {
    return index;
  }

  private void updateMetadataAndRollingStats(String actionType, HoodieCommitMetadata metadata,
                                             List<HoodieWriteStat> writeStats) {
    // TODO : make sure we cannot rollback / archive last commit file
    try {
      // Create a Hoodie table which encapsulated the commits and files visible
      HoodieTable table = HoodieTable.create(config, context);
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
}
