package org.apache.hudi.writer.function;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.model.HoodieRecord;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class CombineOnConditionProcessFunction extends ProcessFunction<HoodieRecord, HoodieRecord> implements CheckpointedFunction {

  private boolean shouldCombine;
  private HoodieTableV2 table;

  public CombineOnConditionProcessFunction(boolean shouldCombine, HoodieTableV2 table) {
    this.shouldCombine = shouldCombine;
    this.table = table;
  }

  private List<HoodieRecord> recordsToCombine = new LinkedList<>();
  private Collector<HoodieRecord> output;

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    if (null != output) {
      if (shouldCombine) {
        List<HoodieRecord> combinedRecords = deduplicateRecords(recordsToCombine);
        combinedRecords.stream().forEach(output::collect);
      } else {
        recordsToCombine.stream().forEach(output::collect);
      }
    }
  }

  private List<HoodieRecord> deduplicateRecords(List<HoodieRecord> recordsToCombine) {
    // TODO
    return new ArrayList<>();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {

  }

  @Override
  public void processElement(HoodieRecord value, Context ctx, Collector<HoodieRecord> out) throws Exception {
    recordsToCombine.add(value);
    if (null == output) {
      output = out;
    }
  }
}
