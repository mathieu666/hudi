package org.apache.hudi.writer.source;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;

/**
 * @author xianghu.wang
 * @time 2020/4/7
 * @description
 */
public class SourceReader<T extends HoodieRecordPayload> implements SourceFunction<HoodieRecord<T>> {
  @Override
  public void run(SourceContext<HoodieRecord<T>> ctx) throws Exception {
    // TODO
  }

  @Override
  public void cancel() {
    // TODO
  }
}
