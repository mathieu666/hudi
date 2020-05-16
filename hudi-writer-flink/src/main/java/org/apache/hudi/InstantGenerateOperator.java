package org.apache.hudi;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieFlinkWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.context.HoodieEngineContext;
import org.apache.hudi.context.HoodieFlinkEngineContext;
import org.apache.hudi.format.HoodieWriteInput;
import org.apache.hudi.format.HoodieWriteOutput;
import org.apache.hudi.util.UtilHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class InstantGenerateOperator extends AbstractStreamOperator<HoodieWriteInput<HoodieRecord>> implements OneInputStreamOperator<HoodieWriteInput<HoodieRecord>, HoodieWriteInput<HoodieRecord>> {
  private static final Logger LOG = LoggerFactory.getLogger(InstantGenerateOperator.class);
  private WriteJob.Config cfg;
  private HoodieWriteConfig writeConfig;
  private HoodieFlinkWriteClient writeClient;
  private HoodieEngineContext<HoodieWriteInput<List<HoodieRecord>>, HoodieWriteOutput<List<WriteStatus>>> context;
  private transient FileSystem fs;

  @Override
  public void processElement(StreamRecord element) throws Exception {
    LOG.info("Send 1 record");
    output.collect(element);
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    initTable();
    // startCommit
    startCommit();
  }

  @Override
  public void open() throws Exception {
    super.open();
    // get configs from runtimeContext
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    // Engine context
    context = new HoodieFlinkEngineContext(new SerializableConfiguration(new Configuration()));
    // Hadoop FileSystem
    fs = FSUtils.getFs(cfg.targetBasePath, context.getHadoopConf().get());
    // HoodieWriteConfig
    writeConfig = UtilHelpers.getHoodieClientConfig(cfg);
    // writeClient
    writeClient = new HoodieFlinkWriteClient(context, writeConfig);
  }

  private String startCommit() {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        String instantTime = writeClient.startCommit();
        LOG.info("Starting commit : " + instantTime);
        return instantTime;
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

  /**
   * Create table if not exists.
   */
  private void initTable() throws IOException {
    if (!fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient.initTableType(context.getHadoopConf().get(), cfg.targetBasePath,
          cfg.tableType, cfg.targetTableName, "archived", cfg.payloadClassName);
      LOG.info("table initialized");
    }
  }
}
