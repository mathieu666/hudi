package org.apache.hudi.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.config.HoodieCompactionConfig;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.index.HoodieIndex;
import org.apache.hudi.writer.utils.UtilHelpers;

import java.io.IOException;

/**
 *
 */
public class WriteProcessWindowFunction extends ProcessWindowFunction<HoodieRecord, String, String, TimeWindow> {

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
   * Bag of properties with source, hoodie client, key generator etc.
   */
  TypedProperties props;

  private HoodieIndex hoodieIndex;

  /**
   * Timeline with completed commits.
   */
  private transient Option<HoodieTimeline> commitTimelineOpt;

  /**
   * Write Client.
   */
  private transient HoodieWriteClient writeClient;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // get configs from runtimeContext
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    // hadoopConf
    serializableHadoopConf = new SerializableConfiguration(new org.apache.hadoop.conf.Configuration());

    // Hadoop FileSystem
    fs = FSUtils.getFs(cfg.targetBasePath, serializableHadoopConf.get());

    // delta streamer conf
    props = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();

    // HoodieWriteConfig
    writeConfig = getHoodieWriteConfig();

    // Index
    hoodieIndex = HoodieIndex.createIndex(writeConfig);

    // writeClient
    writeClient = new HoodieWriteClient<>(serializableHadoopConf.get(), writeConfig, true);
  }


  private HoodieWriteConfig getHoodieWriteConfig() {
    // TODO
    return HoodieWriteConfig.newBuilder().build();
  }

  @Override
  public void process(String s, Context context, Iterable<HoodieRecord> elements, Collector<String> out) throws Exception {
    // Refresh Timeline
    refreshTimeline();

    Option<String> scheduledCompaction = Option.empty();

    scheduledCompaction = writeToSink(elements);



  }

  /**
   * Refresh Timeline.
   */
  private void refreshTimeline() throws IOException {
    if (fs.exists(new Path(cfg.targetBasePath))) {
      HoodieTableMetaClient meta = new HoodieTableMetaClient(new org.apache.hadoop.conf.Configuration(fs.getConf()), cfg.targetBasePath,
          cfg.payloadClassName);
      switch (meta.getTableType()) {
        case COPY_ON_WRITE:
          this.commitTimelineOpt = Option.of(meta.getActiveTimeline().getCommitTimeline().filterCompletedInstants());
          break;
        case MERGE_ON_READ:
          this.commitTimelineOpt = Option.of(meta.getActiveTimeline().getDeltaCommitTimeline().filterCompletedInstants());
          break;
        default:
          throw new HoodieException("Unsupported table type :" + meta.getTableType());
      }
    } else {
      this.commitTimelineOpt = Option.empty();
      HoodieTableMetaClient.initTableType(new org.apache.hadoop.conf.Configuration(jssc.hadoopConfiguration()), cfg.targetBasePath,
          cfg.tableType, cfg.targetTableName, "archived", cfg.payloadClassName);
    }
  }
}
