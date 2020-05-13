package org.apache.hudi;

import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.config.SerializableConfiguration;

public interface HoodieEngineContext<INPUT extends HoodieWriteInput, OUTPUT extends HoodieWriteOutput> {

  INPUT filterUnknownLocations(INPUT taggedRecords);

  SerializableConfiguration getHadoopConf();

  INPUT combineOnCondition(boolean shouldCombine, INPUT inputRecords, int shuffleParallelism, HoodieTableV2 table);

  INPUT tag(INPUT dedupedRecords, HoodieTableV2 table);

  OUTPUT lazyWrite(INPUT partitionedRecords, String instantTime);

  void updateLocation(OUTPUT writeStatus, HoodieTableV2 table);

  void finalCommit(OUTPUT outputs);
}
