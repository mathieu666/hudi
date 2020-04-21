package org.apache.hudi.writer;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.utils.UtilHelpers;

public class InstantGenerateOperator extends AbstractStreamOperator<HoodieRecord> implements OneInputStreamOperator<HoodieRecord, HoodieRecord> {
  private WriteJob.Config cfg;
  private HoodieWriteConfig writeConfig;
  private HoodieWriteClient writeClient;

  @Override
  public void processElement(StreamRecord element) throws Exception {
    output.collect(element);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    // startCommit
    startCommit();
  }

  @Override
  public void open() throws Exception {
    super.open();
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    writeConfig = UtilHelpers.getHoodieClientConfig(cfg);
    writeClient = new HoodieWriteClient<>(new Configuration(), writeConfig, true);
  }

  private void startCommit() {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        String instantTime = writeClient.startCommit();
        LOG.info("Starting commit  : " + instantTime);
      } catch (IllegalArgumentException ie) {
        lastException = ie;
        LOG.error("Got error trying to start a new commit. Retrying after sleeping for a sec", ie);
        retryNum++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // No-Op
        }
      }
    }
    throw lastException;
  }
}
