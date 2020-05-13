package org.apache.hudi.writer.context;

import org.apache.hudi.AbstractHoodieEngineContext;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.table.HoodieTable;

import java.util.List;

public class HoodieFlinkEngineContext extends AbstractHoodieEngineContext<HoodieWriteInput<List<HoodieRecord>>, HoodieWriteOutput<List<WriteStatus>>> {

  public HoodieFlinkEngineContext(SerializableConfiguration hadoopConf) {
    super(hadoopConf);
  }

  @Override
  public HoodieWriteInput<List<HoodieRecord>> filterUnknownLocations(HoodieWriteInput<List<HoodieRecord>> taggedRecords) {
    return null;
  }

  @Override
  public HoodieWriteInput<List<HoodieRecord>> combineOnCondition(boolean shouldCombine, HoodieWriteInput<List<HoodieRecord>> inputRecords, int shuffleParallelism, HoodieTable table) {
    return null;
  }

  @Override
  public HoodieWriteInput<List<HoodieRecord>> tag(HoodieWriteInput<List<HoodieRecord>> dedupedRecords, HoodieTable table) {
    return null;
  }

  @Override
  public HoodieWriteOutput<List<WriteStatus>> lazyWrite(HoodieWriteInput<List<HoodieRecord>> taggedRecords, String instantTime) {
    return null;
  }

  @Override
  public void updateLocation(HoodieWriteOutput<List<WriteStatus>> writeStatus, HoodieTable table) {
    // TODO
  }

  @Override
  public void finalCommit(HoodieWriteOutput<List<WriteStatus>> writeStatus) {
    // TODO
  }
}
