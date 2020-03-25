package org.apache.hudi.writer;

import com.alibaba.fastjson.JSON;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * RecordsMocker.
 */
public class RecordsMocker extends RichSourceFunction<HoodieRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(RecordsMocker.class);
  private volatile boolean isRunning = true;
  private HoodieTestDataGenerator dataGen;

  @Override
  public void run(SourceContext<HoodieRecord> ctx) throws Exception {
    int recordsNum = 1000;
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    List<HoodieRecord> records = dataGen.generateInserts(instantTime, recordsNum);
    Random random = new Random();
    HoodieRecord hoodieRecord;
    while (isRunning) {
      hoodieRecord = records.get(random.nextInt(recordsNum));
      ctx.collect(hoodieRecord);
      LOG.info("Mock message : {}", JSON.toJSONString(hoodieRecord));
      TimeUnit.MILLISECONDS.sleep(100);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    dataGen = new HoodieTestDataGenerator();
  }
}
