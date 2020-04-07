package org.apache.hudi.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.model.HoodieRecord;

/**
 *
 */
public class WriteProcessWindowFunction extends ProcessWindowFunction<HoodieRecord, String, String, TimeWindow> {
  private Hoodie

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
  }

  @Override
  public void process(String s, Context context, Iterable<HoodieRecord> elements, Collector<String> out) throws Exception {

  }
}
