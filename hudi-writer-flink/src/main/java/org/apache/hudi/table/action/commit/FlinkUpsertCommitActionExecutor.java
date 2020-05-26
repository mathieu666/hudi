package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.context.HoodieEngineContext;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.table.FLinkWorkloadProfile;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.Partitioner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FlinkUpsertCommitActionExecutor<T extends HoodieRecordPayload<T>> extends UpsertCommitActionExecutor<T,
    HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteKey<List<HoodieWriteKey>>, HoodieWriteOutput<List<WriteStatus>>> {
  private static final Logger LOG = LogManager.getLogger(FlinkUpsertCommitActionExecutor.class);
  private HoodieWriteInput<List<HoodieRecord<T>>> inputRecords;

  public FlinkUpsertCommitActionExecutor(HoodieEngineContext<HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteOutput<List<WriteStatus>>> context,
                                         HoodieWriteConfig config,
                                         HoodieTable<T, HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteKey<List<HoodieWriteKey>>, HoodieWriteOutput<List<WriteStatus>>> table,
                                         String instantTime,
                                         HoodieWriteInput<List<HoodieRecord<T>>> inputRecords) {
    super(context, config, table, instantTime);
    this.inputRecords = inputRecords;
  }

  private Map<Integer, List<HoodieRecord<T>>> partition(HoodieWriteInput<List<HoodieRecord<T>>> dedupedRecords, Partitioner partitioner) {
    Map<Integer, List<Tuple2<Tuple2<HoodieKey, Option<HoodieRecordLocation>>, HoodieRecord<T>>>> partitionedMidRecords = dedupedRecords.getInputs().stream().map(
        record -> new Tuple2<>(new Tuple2<>(record.getKey(), Option.ofNullable(record.getCurrentLocation())), record))
        .collect(Collectors.groupingBy(x -> partitioner.getPartition(x._1)));
    Map<Integer, List<HoodieRecord<T>>> results = new LinkedHashMap<>();
    partitionedMidRecords.forEach((key, value) -> results.put(key, value.stream().map(x -> x._2).collect(Collectors.toList())));
    return results;
  }

  @Override
  public void updateIndexAndCommitIfNeeded(HoodieWriteOutput<List<WriteStatus>> writeStatusRDD, HoodieWriteMetadata result) {
    Instant indexStartTime = Instant.now();
    // Update the index back
    HoodieWriteOutput<List<WriteStatus>> statuses = table.getIndex().updateLocation(writeStatusRDD, context, table);
    result.setIndexUpdateDuration(Duration.between(indexStartTime, Instant.now()));
    result.setWriteStatuses(statuses);
  }

  @Override
  public void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata result) {
    // TODO
  }

  @Override
  protected Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    return new UpsertPartitioner(profile, context, table, config);
  }

  @Override
  protected Partitioner getInsertPartitioner(WorkloadProfile profile) {
    return getUpsertPartitioner(profile);
  }

  @Override
  public HoodieWriteMetadata<HoodieWriteOutput<List<WriteStatus>>> execute() {
    try {
      // De-dupe/merge if needed
      HoodieWriteInput<List<HoodieRecord<T>>> dedupedRecords =
          combineOnCondition(config.shouldCombineBeforeUpsert(), inputRecords, config.getUpsertShuffleParallelism(), table);

      Instant lookupBegin = Instant.now();
      HoodieWriteInput<List<HoodieRecord<T>>> taggedRecords = dedupedRecords;
      // perform index loop up to get existing location of records
      taggedRecords = tag(dedupedRecords, context, table);
      Duration indexLookupDuration = Duration.between(lookupBegin, Instant.now());

      // replace "HoodieWriteMetadata result = executor.execute(taggedRecords)"
      HoodieWriteMetadata result = write(taggedRecords);
      result.setIndexLookupDuration(indexLookupDuration);
      return result;
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }
  }

  private HoodieWriteMetadata write(HoodieWriteInput<List<HoodieRecord<T>>> inputRecordsRDD) {
    HoodieWriteMetadata result = new HoodieWriteMetadata();

    WorkloadProfile profile = null;
    if (isWorkloadProfileNeeded()) {
      profile = new FLinkWorkloadProfile(inputRecordsRDD);
      LOG.info("Workload profile :" + profile);
      saveWorkloadProfileMetadataToInflight(profile, instantTime);
    }

    // partition using the insert partitioner
    final Partitioner partitioner = getPartitioner(profile);
    Map<Integer, List<HoodieRecord<T>>> partitionedRecords = partition(inputRecordsRDD, partitioner);
    List<WriteStatus> writeStatusRDD = new LinkedList<>();
    partitionedRecords.forEach((partition, records) -> {
      if (WriteOperationType.isChangingRecords(operationType)) {
        handleUpsertPartition(instantTime, partition, records.iterator(), partitioner).forEachRemaining(writeStatusRDD::addAll);
      } else {
        handleInsertPartition(instantTime, partition, records.iterator(), partitioner).forEachRemaining(writeStatusRDD::addAll);
      }
    });
    updateIndexAndCommitIfNeeded(new HoodieWriteOutput<>(writeStatusRDD), result);
    return result;
  }

  @Override
  public HoodieWriteInput<List<HoodieRecord<T>>> combineOnCondition(boolean condition, HoodieWriteInput<List<HoodieRecord<T>>> records, int parallelism, HoodieTable<T, HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteKey<List<HoodieWriteKey>>, HoodieWriteOutput<List<WriteStatus>>> table) {
    // TODO
    return null;
  }

  @Override
  public HoodieWriteInput<List<HoodieRecord<T>>> tag(HoodieWriteInput<List<HoodieRecord<T>>> dedupedRecords, HoodieEngineContext<HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteOutput<List<WriteStatus>>> context, HoodieTable<T, HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteKey<List<HoodieWriteKey>>, HoodieWriteOutput<List<WriteStatus>>> table) {
    return table.getIndex().tagLocation(dedupedRecords, context, table);
  }
}
