package org.apache.hudi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.table.HoodieTable;

public interface HoodieEngineContext<INPUT extends HoodieWriteInput, OUTPUT extends HoodieWriteOutput> {

  INPUT filterUnknownLocations(INPUT taggedRecords);

  SerializableConfiguration getHadoopConfiguration();

  INPUT combineOnCondition(boolean shouldCombine, INPUT inputRecordsRDD, int shuffleParallelism, HoodieTable table);

  INPUT tag(INPUT dedupedRecords, HoodieEngineContext context, HoodieTable table);

  OUTPUT lazyWrite(INPUT partitionedRecords, String instantTime);

  void updateLocation(OUTPUT writeStatus, HoodieTable table, HoodieEngineContext context);

  void finalCommit(HoodieWriteOutput<DataStream<WriteStatus>> writeStatusDS,HoodieEngineContext context);
}
