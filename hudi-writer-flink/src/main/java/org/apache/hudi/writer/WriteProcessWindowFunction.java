package org.apache.hudi.writer;

import org.apache.commons.collections.IteratorUtils;
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
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.constant.Operation;
import org.apache.hudi.writer.exception.HoodieDeltaStreamerException;
import org.apache.hudi.writer.index.HoodieIndex;
import org.apache.hudi.writer.utils.DataSourceUtils;
import org.apache.hudi.writer.utils.UtilHelpers;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 *
 */
public class WriteProcessWindowFunction extends ProcessWindowFunction<HoodieRecord, List<WriteStatus>, String, TimeWindow> {

  private static final Logger LOG = LogManager.getLogger(WriteProcessWindowFunction.class);

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
  public void process(String s, Context context, Iterable<HoodieRecord> inputs, Collector<List<WriteStatus>> out) throws Exception {
    List<HoodieRecord> records = IteratorUtils.toList(inputs.iterator());
    // Refresh Timeline
    refreshTimeline();

    Option<String> scheduledCompaction = Option.empty();


    // filter dupes if needed
    if (cfg.filterDupes) {
      // turn upserts to insert
      cfg.operation = cfg.operation == Operation.UPSERT ? Operation.INSERT : cfg.operation;
      records = DataSourceUtils.dropDuplicates(serializableHadoopConf.get(), records, writeClient.getConfig());
    }

    // try to start commit
    String instantTime = startCommit();
    LOG.info("Starting commit  : " + instantTime);

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
    out.collect(writeStatus);
  }

  private String startCommit() {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        return writeClient.startCommit();
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
      HoodieTableMetaClient.initTableType(new org.apache.hadoop.conf.Configuration(serializableHadoopConf.get()), cfg.targetBasePath,
          cfg.tableType, cfg.targetTableName, "archived", cfg.payloadClassName);
    }
  }
}
