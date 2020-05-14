package org.apache.hudi.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.HoodieWriteMetadata;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.action.commit.UpsertCommitActionExecutor;

import java.util.List;
import java.util.Map;

public class HoodieFlinkCopyOnWriteTable<T extends HoodieRecordPayload<?>> extends HoodieTable<T, HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteKey<List<HoodieKey>>, HoodieWriteOutput<List<WriteStatus>>> {


  public HoodieFlinkCopyOnWriteTable(HoodieWriteConfig config, Configuration hadoopConf,
                                     HoodieTableMetaClient metaClient) {
    super(config, hadoopConf, metaClient);
  }

  @Override
  public HoodieWriteMetadata upsert(String instantTime,
                                    HoodieWriteInput<List<HoodieRecord<T>>> records) {
    return new UpsertCommitActionExecutor(super.getHadoopConf(),config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata insert(String instantTime,
                                    HoodieWriteInput<List<HoodieRecord<T>>> records) {
    return null;
  }

  @Override
  public HoodieWriteMetadata bulkInsert(String instantTime,
                                        HoodieWriteInput<List<HoodieRecord<T>>> records,
                                        Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public HoodieWriteMetadata delete(String instantTime, HoodieWriteKey<List<HoodieKey>> keys) {
    return null;
  }

  @Override
  public HoodieWriteMetadata upsertPrepped(String instantTime,
                                           HoodieWriteInput<List<HoodieRecord<T>>> preppedRecords) {
    return null;
  }

  @Override
  public HoodieWriteMetadata insertPrepped(String instantTime,
                                           HoodieWriteInput<List<HoodieRecord<T>>> preppedRecords) {
    return null;
  }

  @Override
  public HoodieWriteMetadata bulkInsertPrepped(String instantTime,
                                               HoodieWriteInput<List<HoodieRecord<T>>> preppedRecords,
                                               Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public HoodieCompactionPlan scheduleCompaction(String instantTime) {
    return null;
  }

  @Override
  public HoodieWriteOutput<List<WriteStatus>> compact(String compactionInstantTime,
                                                      HoodieCompactionPlan compactionPlan) {
    return null;
  }

  @Override
  public HoodieCleanMetadata clean(String cleanInstantTime) {
    return null;
  }

  @Override
  public HoodieRollbackMetadata rollback(String rollbackInstantTime, HoodieInstant commitInstant,
                                         boolean deleteInstants) {
    return null;
  }

  @Override
  public HoodieRestoreMetadata restore(String restoreInstantTime, String instantToRestore) {
    return null;
  }

  @Override
  public void cleanFailedWrites(String instantTs, List<HoodieWriteStat> stats, boolean consistencyCheckEnabled) throws HoodieIOException {

  }

  @Override
  public void waitForAllFiles(Map<String, List<Pair<String, String>>> groupByPartition, ConsistencyGuard.FileVisibility visibility) {

  }
}
