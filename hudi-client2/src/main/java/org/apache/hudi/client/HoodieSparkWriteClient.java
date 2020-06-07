package org.apache.hudi.client;

import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
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
 * @param <T>
 */
public class HoodieSparkWriteClient<T extends HoodieRecordPayload<T>> extends AbstractHoodieWriteClient<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> {
  protected HoodieSparkWriteClient(HoodieEngineContext context, HoodieTable table, AbstractHoodieIndex index, HoodieWriteConfig clientConfig) {
    super(context, table, index, clientConfig);
  }

  @Override
  public HoodieWriteInput<T> filterExists(HoodieWriteInput<T> hoodieRecords, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<JavaRDD<WriteStatus>> upsert(HoodieWriteInput<T> hoodieRecords, String instantTime) {
    // almost the same as before
    // TODO
    return null;
  }

  @Override
  public HoodieWriteOutput<JavaRDD<WriteStatus>> upsertPreppedRecords(HoodieWriteInput<T> hoodieRecords, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<JavaRDD<WriteStatus>> insert(HoodieWriteInput<T> hoodieRecords, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<JavaRDD<WriteStatus>> insertPreppedRecords(HoodieWriteInput<T> hoodieRecords, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<JavaRDD<WriteStatus>> bulkInsert(HoodieWriteInput<T> records, String instantTime, Option<UserDefinedBulkInsertPartitioner<T>> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public HoodieWriteOutput<JavaRDD<WriteStatus>> bulkInsertPreppedRecords(HoodieWriteInput<T> preppedRecords, String instantTime, Option<UserDefinedBulkInsertPartitioner<T>> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public HoodieWriteOutput<JavaRDD<WriteStatus>> delete(HoodieWriteInput<T> hoodieRecords, String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<JavaRDD<WriteStatus>> postWrite(HoodieWriteMetadata<JavaRDD<WriteStatus>> result, String instantTime, HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> hoodieTable) {
    return null;
  }

  @Override
  public boolean commit(String instantTime, HoodieWriteOutput<JavaRDD<WriteStatus>> writeStatuses, Option<Map<String, String>> extraMetadata, String actionType) {
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
