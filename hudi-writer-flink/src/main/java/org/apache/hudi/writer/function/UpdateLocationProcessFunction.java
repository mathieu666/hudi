package org.apache.hudi.writer.function;

import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hudi.HoodieEngineContext;
import org.apache.hudi.WriteStatus;

import java.util.LinkedList;
import java.util.List;

public class UpdateLocationProcessFunction extends ProcessFunction<WriteStatus, WriteStatus> implements CheckpointedFunction {
  private HoodieEngineContext context;
  private HoodieTableV2 table;

  Collector<WriteStatus> output;

  private List<WriteStatus> writeStatuses = new LinkedList<>();

  public UpdateLocationProcessFunction(HoodieEngineContext context, HoodieTableV2 table) {
    this.context = context;
    this.table = table;
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    // TODO
    List<WriteStatus> statuses  = table.getIndex().updateLocation(writeStatuses);

    statuses.stream().forEach(output::collect);
    writeStatuses.clear();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {

  }

  @Override
  public void processElement(WriteStatus value, Context ctx, Collector<WriteStatus> out) throws Exception {
    writeStatuses.add(value);
    if (null == output) {
      output = out;
    }
  }
}
