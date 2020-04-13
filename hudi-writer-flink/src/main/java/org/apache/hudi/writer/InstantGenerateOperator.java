package org.apache.hudi.writer;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.hudi.common.model.HoodieRecord;

public class InstantGenerateOperator extends AbstractStreamOperator<HoodieRecord> implements OneInputStreamOperator<HoodieRecord,HoodieRecord> {
  @Override
  public void processElement(StreamRecord element) throws Exception {
    output.collect(element);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    // emit instantTime
    saveInstantTime();
  }
}
