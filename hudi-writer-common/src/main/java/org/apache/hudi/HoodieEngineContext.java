package org.apache.hudi;

import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.table.HoodieTable;

public interface HoodieEngineContext<INPUT extends HoodieWriteInput, OUTPUT extends HoodieWriteOutput> {

  INPUT filterUnknownLocations(INPUT taggedRecords);

  SerializableConfiguration getHadoopConf();

  INPUT combineOnCondition(boolean shouldCombine, INPUT inputRecords, int shuffleParallelism, HoodieTable table);

  INPUT tag(INPUT dedupedRecords, HoodieTable table);

  OUTPUT lazyWrite(INPUT partitionedRecords, String instantTime);

  void updateLocation(OUTPUT writeStatus, HoodieTable table);

  void finalCommit(OUTPUT outputs);
}
