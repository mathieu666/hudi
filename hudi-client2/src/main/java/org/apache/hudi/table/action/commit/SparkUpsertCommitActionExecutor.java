package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.TaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author xianghu.wang
 * @time 2020/6/9
 * @description
 */
public class SparkUpsertCommitActionExecutor<T extends HoodieRecordPayload<T>> extends CommitActionExecutor<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>>  {
  public SparkUpsertCommitActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable table, String instantTime, WriteOperationType operationType, TaskContextSupplier taskContextSupplier) {
    super(context, config, table, instantTime, operationType, taskContextSupplier);
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> execute(HoodieWriteInput<JavaRDD<HoodieRecord<T>>> inputRecordsRDD) {
    return null;
  }

  @Override
  protected void updateIndexAndCommitIfNeeded(HoodieWriteOutput<JavaRDD<WriteStatus>> writeStatusRDD, HoodieWriteMetadata<JavaRDD<WriteStatus>> result) {

  }

  @Override
  protected void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata<JavaRDD<WriteStatus>> result) {

  }

  @Override
  protected Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr) throws Exception {
    return null;
  }

  @Override
  public HoodieWriteMetadata<JavaRDD<WriteStatus>> execute() {
    return null;
  }
}
