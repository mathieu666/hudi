package org.apache.hudi.writer.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.writer.common.HoodieWriteInput;
import org.mortbay.util.ajax.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Data mocker.
 */
public class SourceReader<T extends HoodieRecordPayload> extends RichSourceFunction<HoodieWriteInput<HoodieRecord<T>>> {
  private static final Logger LOG = LoggerFactory.getLogger(SourceReader.class);
  private volatile boolean isRunning = true;
  private HoodieTestDataGenerator dataGen;

  @Override
  public void run(SourceContext<HoodieWriteInput<HoodieRecord<T>>> ctx) throws Exception {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmSS");
    String instantTime = sdf.format(new Date());
    List<HoodieRecord> records = dataGen.generateInserts(instantTime, 1000);
    while (isRunning) {
      for (int i = 0; i < records.size(); i++) {
        ctx.collect(new HoodieWriteInput(records.get(i)));
        LOG.info("Mock message : {}", JSON.toString(records.get(i)));
        TimeUnit.MILLISECONDS.sleep(100);
        if (i == records.size() - 1) {
          records = dataGen.generateUniqueUpdates(instantTime, 1000);
          i = 0;
        }
      }
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
