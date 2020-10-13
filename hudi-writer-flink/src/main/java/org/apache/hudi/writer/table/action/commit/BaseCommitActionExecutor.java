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

package org.apache.hudi.writer.table.action.commit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.writer.client.SparkTaskContextSupplier;
import org.apache.hudi.writer.client.WriteStatus;
import org.apache.hudi.writer.common.HoodieWriteInput;
import org.apache.hudi.writer.common.HoodieWriteOutput;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.exception.HoodieCommitException;
import org.apache.hudi.writer.exception.HoodieUpsertException;
import org.apache.hudi.writer.table.HoodieTable;
import org.apache.hudi.writer.table.Partitioner;
import org.apache.hudi.writer.table.WorkloadProfile;
import org.apache.hudi.writer.table.WorkloadStat;
import org.apache.hudi.writer.table.action.BaseActionExecutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class BaseCommitActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseActionExecutor<HoodieWriteMetadata> {

  private static final Logger LOG = LogManager.getLogger(BaseCommitActionExecutor.class);

  private final WriteOperationType operationType;
  protected final SparkTaskContextSupplier sparkTaskContextSupplier = new SparkTaskContextSupplier();

  public BaseCommitActionExecutor(Configuration jsc, HoodieWriteConfig config,
                                  HoodieTable table, String instantTime, WriteOperationType operationType) {
    this(jsc, config, table, instantTime, operationType, null);
  }

  public BaseCommitActionExecutor(Configuration jsc, HoodieWriteConfig config,
                                  HoodieTable table, String instantTime, WriteOperationType operationType,
                                  List<HoodieRecord<T>> inputRecordsRDD) {
    super(jsc, config, table, instantTime);
    this.operationType = operationType;
  }

  public HoodieWriteMetadata execute(HoodieWriteInput<List<HoodieRecord<T>>> inputRecordsRDD) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();

    WorkloadProfile profile = null;
    if (isWorkloadProfileNeeded()) {
      long a = System.currentTimeMillis();
      profile = new WorkloadProfile(inputRecordsRDD);
      long b = System.currentTimeMillis();
      System.out.println("### profile" + (b - a) + " 毫秒");
      LOG.info("Workload profile :" + profile);
      saveWorkloadProfileMetadataToInflight(profile, instantTime);
      long c = System.currentTimeMillis();
      System.out.println(" ### 改状态耗时 ：" + (c - b) + " 毫秒");
    }

    // partition using the insert partitioner
    long d = System.currentTimeMillis();
    final Partitioner partitioner = getPartitioner(profile);
    long e = System.currentTimeMillis();
    System.out.println("### 获取分区器耗时 ： " + (e - d) + " 毫秒");
    Map<Integer, List<HoodieRecord<T>>> partitionedRecords = partition(inputRecordsRDD, partitioner);
    long f = System.currentTimeMillis();
    System.out.println("### 分区耗时 ： " + (f - e) + " 毫秒");
    List<WriteStatus> writeStatusRDD = new LinkedList<>();
    partitionedRecords.forEach((partition, records) -> {
      if (WriteOperationType.isChangingRecords(operationType)) {
        handleUpsertPartition(instantTime, partition, records.iterator(), partitioner).forEachRemaining(writeStatusRDD::addAll);
      } else {
        handleInsertPartition(instantTime, partition, records.iterator(), partitioner).forEachRemaining(writeStatusRDD::addAll);
      }
    });
    long g = System.currentTimeMillis();
    System.out.println("### 写耗时 ： " + (g - f) + " 毫秒");
    updateIndexAndCommitIfNeeded(new HoodieWriteOutput<>(writeStatusRDD), result);
    System.out.println("uodate index 耗时 ： " + (System.currentTimeMillis() - g));
    return result;
  }

  /**
   * Save the workload profile in an intermediate file (here re-using commit files) This is useful when performing
   * rollback for MOR tables. Only updates are recorded in the workload profile metadata since updates to log blocks
   * are unknown across batches Inserts (which are new parquet files) are rolled back based on commit time. // TODO :
   * Create a new WorkloadProfile metadata file instead of using HoodieCommitMetadata
   */
  void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, String instantTime)
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
      metadata.setOperationType(operationType);

      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      String commitActionType = table.getMetaClient().getCommitActionType();
      HoodieInstant requested = new HoodieInstant(State.REQUESTED, commitActionType, instantTime);
      activeTimeline.transitionRequestedToInflight(requested,
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException io) {
      throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", io);
    }
  }

  private Partitioner getPartitioner(WorkloadProfile profile) {
    if (WriteOperationType.isChangingRecords(operationType)) {
      return getUpsertPartitioner(profile);
    } else {
      return getInsertPartitioner(profile);
    }
  }

  private Map<Integer, List<HoodieRecord<T>>> partition(HoodieWriteInput<List<HoodieRecord<T>>> dedupedRecords, Partitioner partitioner) {
    Map<Integer, List<Tuple2<Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>>>> partitionedMidRecords = dedupedRecords.getInputs().stream().map(
        record -> new Tuple2<>(new Tuple2<>(record.getKey(), Option.ofNullable(record.getCurrentLocation())), record))
        .collect(Collectors.groupingBy(x -> partitioner.getPartition(x._1)));
    Map<Integer, List<HoodieRecord<T>>> results = new LinkedHashMap<>();
    partitionedMidRecords.forEach((key, value) -> results.put(key, value.stream().map(x -> x._2).collect(Collectors.toList())));
    return results;
  }

  protected void updateIndexAndCommitIfNeeded(HoodieWriteOutput<List<WriteStatus>> writeStatusRDD, HoodieWriteMetadata result) {
    Instant indexStartTime = Instant.now();
    // Update the index back
    HoodieWriteOutput<List<WriteStatus>> statuses = ((HoodieTable<T>) table).getIndex().updateLocation(writeStatusRDD, hadoopConf,
        (HoodieTable<T>) table);
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setWriteStatuses(statuses);
  }

  protected void commitOnAutoCommit(HoodieWriteMetadata result) {
    if (config.shouldAutoCommit()) {
      LOG.info("Auto commit enabled: Committing " + instantTime);
      commit(Option.empty(), result);
    } else {
      LOG.info("Auto commit disabled for " + instantTime);
    }
  }

  private void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata result) {
    String actionType = table.getMetaClient().getCommitActionType();
    LOG.info("Committing " + instantTime + ", action Type " + actionType);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.create(config, hadoopConf);

    HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    result.setCommitted(true);
    List<HoodieWriteStat> stats = result.getWriteStatuses().getOutput().stream().map(WriteStatus::getStat).collect(Collectors.toList());
    result.setWriteStats(stats);

    updateMetadataAndRollingStats(metadata, stats);

    // Finalize write
    finalizeWrite(instantTime, stats, result);

    // add in extra metadata
    if (extraMetadata.isPresent()) {
      extraMetadata.get().forEach(metadata::addMetadata);
    }
    metadata.addMetadata(HoodieCommitMetadata.SCHEMA_KEY, config.getSchema());
    metadata.setOperationType(operationType);

    try {
      activeTimeline.saveAsComplete(new HoodieInstant(true, actionType, instantTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));

      LOG.info("Committed " + instantTime);
    } catch (IOException e) {
      throw new HoodieCommitException("Failed to complete commit " + config.getBasePath() + " at time " + instantTime,
          e);
    }
    result.setCommitMetadata(Option.of(metadata));
  }

  /**
   * Finalize Write operation.
   *
   * @param instantTime Instant Time
   * @param stats       Hoodie Write Stat
   */
  protected void finalizeWrite(String instantTime, List<HoodieWriteStat> stats, HoodieWriteMetadata result) {
    try {
      Instant start = Instant.now();
      table.finalizeWrite(hadoopConf, instantTime, stats);
      result.setFinalizeDuration(Duration.between(start, Instant.now()));
    } catch (HoodieIOException ioe) {
      throw new HoodieCommitException("Failed to complete commit " + instantTime + " due to finalize errors.", ioe);
    }
  }

  private void updateMetadataAndRollingStats(HoodieCommitMetadata metadata, List<HoodieWriteStat> writeStats) {
    // 1. Look up the previous compaction/commit and get the HoodieCommitMetadata from there.
    // 2. Now, first read the existing rolling stats and merge with the result of current metadata.

    // Need to do this on every commit (delta or commit) to support COW and MOR.
    for (HoodieWriteStat stat : writeStats) {
      String partitionPath = stat.getPartitionPath();
      // TODO: why is stat.getPartitionPath() null at times here.
      metadata.addWriteStat(partitionPath, stat);
    }
  }

  protected boolean isWorkloadProfileNeeded() {
    return true;
  }

  @SuppressWarnings("unchecked")
  protected Iterator<List<WriteStatus>> handleUpsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                              Partitioner partitioner) {
    UpsertPartitioner upsertPartitioner = (UpsertPartitioner) partitioner;
    BucketInfo binfo = upsertPartitioner.getBucketInfo(partition);
    BucketType btype = binfo.bucketType;
    try {
      if (btype.equals(BucketType.INSERT)) {
        return handleInsert(binfo.fileIdPrefix, recordItr);
      } else if (btype.equals(BucketType.UPDATE)) {
        return handleUpdate(binfo.partitionPath, binfo.fileIdPrefix, recordItr);
      } else {
        throw new HoodieUpsertException("Unknown bucketType " + btype + " for partition :" + partition);
      }
    } catch (Throwable t) {
      String msg = "Error upserting bucketType " + btype + " for partition :" + partition;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  protected Iterator<List<WriteStatus>> handleInsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                              Partitioner partitioner) {
    return handleUpsertPartition(instantTime, partition, recordItr, partitioner);
  }

  /**
   * Provides a partitioner to perform the upsert operation, based on the workload profile.
   */
  protected abstract Partitioner getUpsertPartitioner(WorkloadProfile profile);

  /**
   * Provides a partitioner to perform the insert operation, based on the workload profile.
   */
  protected abstract Partitioner getInsertPartitioner(WorkloadProfile profile);

  protected abstract Iterator<List<WriteStatus>> handleInsert(String idPfx,
                                                              Iterator<HoodieRecord<T>> recordItr) throws Exception;

  protected abstract Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
                                                              Iterator<HoodieRecord<T>> recordItr) throws IOException;
}
