package org.apache.hudi.writer.function;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hudi.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.table.HoodieTable;

import java.util.LinkedList;
import java.util.List;

public class TagLocationProcessFunction extends ProcessFunction<HoodieRecord, HoodieRecord> implements CheckpointedFunction {
  private HoodieEngineContext context;
  private HoodieTable table;

  private List<HoodieRecord> recordsToLocate = new LinkedList<>();
  Collector<HoodieRecord> output;

  public TagLocationProcessFunction(HoodieEngineContext context, HoodieTable table) {
    this.context = context;
    this.table = table;
  }

  @Override
  public void processElement(HoodieRecord value, Context ctx, Collector<HoodieRecord> out) throws Exception {
    recordsToLocate.add(value);
    if (null == output) {
      output = out;
    }
}

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // 暂未实现 TODO
    List<HoodieRecord> recordsWithLocation = table.getIndex().tagLocation(recordsToLocate, this.context, table);
    recordsWithLocation.stream().forEach(output::collect);
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {

  }
}
