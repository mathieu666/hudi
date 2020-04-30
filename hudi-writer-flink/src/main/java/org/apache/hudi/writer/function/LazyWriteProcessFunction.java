package org.apache.hudi.writer.function;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.table.Partitioner;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.commit.BucketInfo;
import org.apache.hudi.table.action.commit.BucketType;
import org.apache.hudi.table.action.commit.UpsertPartitioner;
import org.apache.hudi.writer.exception.HoodieCommitException;
import org.apache.hudi.writer.exception.HoodieUpsertException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

public class LazyWriteProcessFunction extends ProcessFunction<HoodieRecord, WriteStatus> implements CheckpointedFunction {
  private static final Logger LOG = LoggerFactory.getLogger(LazyWriteProcessFunction.class);
  private String instantTime;
  private List<HoodieRecord> recordsToWrite = new LinkedList<>();
  private Collector<WriteStatus> output;

  public LazyWriteProcessFunction(String instantTime) {
    this.instantTime = instantTime;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // TODO
    WorkloadProfile profile = null;
    profile = new WorkloadProfile(recordsToWrite);

    saveWorkloadProfileMetadataToInflight(profile, instantTime);

    // partition using the insert partitioner
    final Partitioner partitioner = getPartitioner(profile);
    Map<Integer, List<HoodieRecord>> partitionedRecords = partition(recordsToWrite, partitioner);
    List<WriteStatus> writeStatus = new LinkedList<>();
    partitionedRecords.forEach((partition, records) -> {
      if (WriteOperationType.isChangingRecords(operationType)) {
        handleUpsertPartition(instantTime, partition, records.iterator(), partitioner).forEachRemaining(writeStatus::addAll);
      } else {
        handleInsertPartition(instantTime, partition, records.iterator(), partitioner).forEachRemaining(writeStatus::addAll);
      }
    });
    writeStatus.stream().forEach(output::collect);
    recordsToWrite.clear();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {

  }

  @Override
  public void processElement(HoodieRecord value, Context ctx, Collector<WriteStatus> out) throws Exception {
    recordsToWrite.add(value);
    if (null == output) {
      output = out;
    }
  }

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
      HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, commitActionType, instantTime);
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

  private Map<Integer, List<HoodieRecord>> partition(List<HoodieRecord> dedupedRecords, Partitioner partitioner) {
    Map<Integer, List<Tuple2<Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord>>> partitionedMidRecords = dedupedRecords.stream().map(
        record -> new Tuple2<>(new Tuple2<>(record.getKey(), Option.ofNullable(record.getCurrentLocation())), record))
        .collect(Collectors.groupingBy(x -> partitioner.getPartition(x._1)));
    Map<Integer, List<HoodieRecord>> results = new LinkedHashMap<>();
    partitionedMidRecords.forEach((key, value) -> results.put(key, value.stream().map(x -> x._2).collect(Collectors.toList())));
    return results;
  }

  public Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    return new UpsertPartitioner(profile, hadoopConf, table, config);
  }

  public Partitioner getInsertPartitioner(WorkloadProfile profile) {
    return getUpsertPartitioner(profile);
  }

  protected Iterator<List<WriteStatus>> handleUpsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                              Partitioner partitioner) {
    UpsertPartitioner upsertPartitioner = (UpsertPartitioner) partitioner;
    BucketInfo binfo = upsertPartitioner.getBucketInfo(partition);
    BucketType btype = binfo.getBucketType();
    try {
      if (btype.equals(BucketType.INSERT)) {
        return handleInsert(binfo.getFileIdPrefix(), recordItr);
      } else if (btype.equals(BucketType.UPDATE)) {
        return handleUpdate(binfo.getPartitionPath(), binfo.getFileIdPrefix(), recordItr);
      } else {
        throw new HoodieUpsertException("Unknown bucketType " + btype + " for partition :" + partition);
      }
    } catch (Throwable t) {
      String msg = "Error upserting bucketType " + btype + " for partition :" + partition;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  public Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord> recordItr)
      throws Exception {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition");
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    return new CopyOnWriteLazyInsertIterable<>(recordItr, config, instantTime, (HoodieTable<T>)table, idPfx,
        sparkTaskContextSupplier);
  }

  public Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
                                                  Iterator<HoodieRecord> recordItr)
      throws IOException {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition with fileId => " + fileId);
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(partitionPath, fileId, recordItr);
    return handleUpdateInternal(upsertHandle, fileId);
  }

  protected Iterator<List<WriteStatus>> handleInsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                              Partitioner partitioner) {
    return handleUpsertPartition(instantTime, partition, recordItr, partitioner);
  }
}
