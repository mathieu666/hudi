package org.apache.hudi.writer;

import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.client.WriteStatus;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.constant.Operation;
import org.apache.hudi.writer.exception.HoodieDeltaStreamerException;
import org.apache.hudi.writer.utils.UtilHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public class WriteProcessWindowFunction extends KeyedProcessFunction<String, HoodieRecord, List<WriteStatus>> implements CheckpointedFunction {

  private static final Logger LOG = LoggerFactory.getLogger(WriteProcessWindowFunction.class);
  /**
   * Job conf.
   */
  private WriteJob.Config cfg;
  /**
   * Serializable hadoop conf.
   */
  private SerializableConfiguration serializableHadoopConf;
  /**
   * HoodieWriteConfig.
   */
  private HoodieWriteConfig writeConfig;

  /**
   * Hadoop FileSystem.
   */
  private transient FileSystem fs;

  /**
   * Timeline with completed commits.
   */
  private transient Option<HoodieTimeline> commitTimelineOpt;

  /**
   * Write Client.
   */
  private transient HoodieWriteClient writeClient;

  /**
   * Incoming records.
   */
  private List<HoodieRecord> records = new LinkedList<>();

  private Collector<List<WriteStatus>> output;

  @Override
  public void processElement(HoodieRecord value, Context ctx, Collector<List<WriteStatus>> out) throws Exception {
    records.add(value);
    LOG.info("Receive 1 record, current records size = [{}]", records.size());
    if (output == null) {
      output = out;
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    if (records.isEmpty()) {
      return;
    }
    // get instantTime
    String instantTime = getInstantTime();
    LOG.info("WriteProcessWindowFunction Get instantTime = {}", instantTime);

    // start write and get the result
    List<WriteStatus> writeStatus;
    if (cfg.operation == Operation.INSERT) {
      writeStatus = writeClient.insert(records, instantTime);
    } else if (cfg.operation == Operation.UPSERT) {
      writeStatus = writeClient.upsert(records, instantTime);
    } else if (cfg.operation == Operation.BULK_INSERT) {
      writeStatus = writeClient.bulkInsert(records, instantTime);
    } else {
      throw new HoodieDeltaStreamerException("Unknown operation :" + cfg.operation);
    }
    if (CollectionUtils.isNotEmpty(writeStatus)) {
      output.collect(writeStatus);
    }
    // 输出writeStatus
    LOG.info("Emit [{}] writeStatus to Sink", writeStatus.size());
    records.clear();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {

  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // get configs from runtimeContext
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    // hadoopConf
    serializableHadoopConf = new SerializableConfiguration(new org.apache.hadoop.conf.Configuration());

    // Hadoop FileSystem
    fs = FSUtils.getFs(cfg.targetBasePath, serializableHadoopConf.get());

    // HoodieWriteConfig
    writeConfig = UtilHelpers.getHoodieClientConfig(cfg);

    // writeClient
    writeClient = new HoodieWriteClient<>(serializableHadoopConf.get(), writeConfig, true);
  }

  private String getInstantTime() {
    HoodieTableMetaClient meta = new HoodieTableMetaClient(serializableHadoopConf.get(), cfg.targetBasePath,
        cfg.payloadClassName);
    return meta.getActiveTimeline().filter(HoodieInstant::isRequested).lastInstant().get().getTimestamp();
  }
}
