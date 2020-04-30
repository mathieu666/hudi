package org.apache.hudi.writer.context;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.hudi.AbstractHoodieEngineContext;
import org.apache.hudi.HoodieEngineContext;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.writer.function.*;

public class HoodieFlinkEngineContext extends AbstractHoodieEngineContext<HoodieWriteInput<DataStream<HoodieRecord>>, HoodieWriteOutput<DataStream<WriteStatus>>> {
  @Override
  public HoodieWriteInput<DataStream<HoodieRecord>> filterUnknownLocations(HoodieWriteInput<DataStream<HoodieRecord>> taggedRecords) {
    return null;
  }

  @Override
  public HoodieWriteInput<DataStream<HoodieRecord>> combineOnCondition(boolean shouldCombine, HoodieWriteInput<DataStream<HoodieRecord>> inputRecords, int shuffleParallelism, HoodieTable table) {
    DataStream<HoodieRecord> combinedRecords = inputRecords
        .getInputs()
        .process(new CombineOnConditionProcessFunction(shouldCombine, table))
        .setParallelism(shuffleParallelism);
    return new HoodieWriteInput<>(combinedRecords);
  }

  @Override
  public HoodieWriteInput<DataStream<HoodieRecord>> tag(HoodieWriteInput<DataStream<HoodieRecord>> dedupedRecords, HoodieEngineContext context, HoodieTable table) {
    DataStream<HoodieRecord> taggedRecords = dedupedRecords.getInputs().process(new TagLocationProcessFunction(context, table));
    return new HoodieWriteInput<>(taggedRecords);
  }

  @Override
  public HoodieWriteOutput<DataStream<WriteStatus>> lazyWrite(HoodieWriteInput<DataStream<HoodieRecord>> taggedRecords, String instantTime) {
    DataStream<WriteStatus> writeStatusDataStream = taggedRecords.getInputs().process(new LazyWriteProcessFunction(instantTime));
    return new HoodieWriteOutput<>(writeStatusDataStream);
  }

  @Override
  public void updateLocation(HoodieWriteOutput<DataStream<WriteStatus>> writeStatus, HoodieTable table, HoodieEngineContext context) {
    writeStatus.getOutput().process(new UpdateLocationProcessFunction(context,table));
  }

  @Override
  public void finalCommit(HoodieWriteOutput<DataStream<WriteStatus>> writeStatusDS, HoodieEngineContext context) {
    writeStatusDS
        .getOutput()
        .addSink(new FinalCommitSink(context))
        .setParallelism(1);
  }

}
