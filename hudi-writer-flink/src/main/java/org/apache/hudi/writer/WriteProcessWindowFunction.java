package org.apache.hudi.writer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.client.WriteStatus;
import org.apache.hudi.writer.common.HoodieWriteInput;
import org.apache.hudi.writer.common.HoodieWriteOutput;
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
public class WriteProcessWindowFunction extends KeyedProcessFunction<String, HoodieWriteInput<HoodieRecord>, Tuple2<String, List<WriteStatus>>> implements CheckpointedFunction {

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
   * Write Client.
   */
  private transient HoodieWriteClient writeClient;

  /**
   * Incoming records.
   */
  private List<HoodieRecord> records = new LinkedList<>();

  private Collector<Tuple2<String, List<WriteStatus>>> output;
  private int indexOfThisSubtask;

  @Override
  public void processElement(HoodieWriteInput<HoodieRecord> value, Context ctx, Collector<Tuple2<String, List<WriteStatus>>> out) throws Exception {
    records.add(value.getInputs());
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
    long checkpointId = context.getCheckpointId();

    //执行 snapshot 之前先获取有哪些新的 instant
    HoodieInstant hoodieInstant = getInflightInstantTime();

    String instantTime = "";

    //有数据就一定要有 hoodieInstant
    if (hoodieInstant == null) {
      throw new RuntimeException("执行 snapshot 发现找不到事务 whit index [" + indexOfThisSubtask + "] checkpoint id [" + checkpointId + "]");
    }

    instantTime = hoodieInstant.getTimestamp();
    LOG.warn("执行 snapshotState [" + checkpointId + "] 当前事务为 [" + instantTime + "] 当前批次大小 [" + records.size() + "]");

    // start write and get the result
    HoodieWriteOutput<List<WriteStatus>> writeStatus;
    HoodieWriteInput<List<HoodieRecord>> inputs = new HoodieWriteInput<>(records);
    writeStatus = writeClient.upsert(inputs, instantTime);
    if (null != writeStatus && null != writeStatus.getOutput()) {
      output.collect(new Tuple2<>(instantTime, writeStatus.getOutput()));
      // 输出writeStatus
      LOG.warn("执行 snapshotState [" + checkpointId + "] 当前事务为 [" + instantTime + "] 下发数据条数 [{}]", writeStatus.getOutput().size());
      //清理缓存
    } else {
      LOG.warn("执行 snapshotState [" + checkpointId + "] 当前事务为 [" + instantTime + "]  records.values 为空");
    }
    records.clear();
  }

  @Override
  public void initializeState(FunctionInitializationContext context) throws Exception {

  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    // get configs from runtimeContext
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    // hadoopConf
    serializableHadoopConf = new SerializableConfiguration(UtilHelpers.getHadoopConf());

    // HoodieWriteConfig
    writeConfig = UtilHelpers.getHoodieClientConfig(cfg);

    // writeClient
    writeClient = new HoodieWriteClient<>(serializableHadoopConf.get(), writeConfig, true);
  }

  private HoodieInstant getInflightInstantTime() {
    HoodieTableMetaClient meta = new HoodieTableMetaClient(serializableHadoopConf.get(), cfg.targetBasePath,
        cfg.payloadClassName);
    return meta.getActiveTimeline().filter(HoodieInstant::isRequested).lastInstant().get();
  }
}
