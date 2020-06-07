package org.apache.hudi.client;

import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.AbstractHoodieIndex;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Map;

/**
 * HoodieWriteClient for spark engine.
 * // TODO
 *
 * @param <T>
 */
public class HoodieSparkWriteClient<T extends HoodieRecordPayload<T>> extends AbstractHoodieWriteClient<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> {
  protected HoodieSparkWriteClient(HoodieEngineContext context, HoodieTable table, AbstractHoodieIndex index, HoodieWriteConfig clientConfig) {
    super(context, table, index, clientConfig);
  }

  @Override
  public JavaRDD<HoodieRecord<T>> filterExists(JavaRDD<HoodieRecord<T>> hoodieRecords, String instantTime) {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> upsert(JavaRDD<HoodieRecord<T>> records, String instantTime) {
    setOperationType(WriteOperationType.UPSERT);
    HoodieWriteMetadata result = super.table.upsert(context, instantTime, records);
    return postWrite(result, instantTime, table);
  }

  @Override
  public JavaRDD<WriteStatus> upsertPreppedRecords(JavaRDD<HoodieRecord<T>> hoodieRecords, String instantTime) {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> insert(JavaRDD<HoodieRecord<T>> hoodieRecords, String instantTime) {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> insertPreppedRecords(JavaRDD<HoodieRecord<T>> hoodieRecords, String instantTime) {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsert(JavaRDD<HoodieRecord<T>> records, String instantTime, Option<UserDefinedBulkInsertPartitioner<T>> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> bulkInsertPreppedRecords(JavaRDD<HoodieRecord<T>> preppedRecords, String instantTime, Option<UserDefinedBulkInsertPartitioner<T>> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> delete(JavaRDD<HoodieRecord<T>> hoodieRecords, String instantTime) {
    return null;
  }

  @Override
  public JavaRDD<WriteStatus> postWrite(HoodieWriteMetadata<JavaRDD<WriteStatus>> result, String instantTime, HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> hoodieTable) {

    postCommit(result.getCommitMetadata().get(), instantTime, Option.empty());
    emitCommitMetrics(instantTime, result.getCommitMetadata().get(),
        hoodieTable.getMetaClient().getCommitActionType());
    return result.getWriteStatuses();
  }

  @Override
  public boolean commit(String instantTime, JavaRDD<WriteStatus> writeStatuses, Option<Map<String, String>> extraMetadata, String actionType) {
    return false;
  }

  @Override
  public void deleteSavepoint(String savepointTime) {

  }

  @Override
  public void restoreToSavepoint(String savepointTime) {

  }

  @Override
  protected void postCommit(HoodieCommitMetadata metadata, String instantTime, Option<Map<String, String>> extraMetadata) {

  }
}
