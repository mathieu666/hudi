package org.apache.hudi.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.TypedProperties;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.config.HoodieCompactionConfig;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.index.HoodieHBaseIndex;
import org.apache.hudi.writer.partitioner.PartitionHelper;
import org.apache.hudi.writer.table.HoodieMergeOnReadTable;
import org.apache.hudi.writer.table.HoodieTable;
import org.apache.hudi.writer.table.WorkloadProfile;
import org.apache.hudi.writer.utils.ClientUtils;
import org.apache.hudi.writer.utils.UtilHelpers;
import org.apache.hudi.writer.utils.ValidationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Upsert process.
 */
public class WriteProcessWindowFunction extends ProcessWindowFunction<HoodieRecord, String, String, TimeWindow> {
  /**
   * log service.
   */
  private static final Logger LOG = LoggerFactory.getLogger(WriteProcessWindowFunction.class);

  /**
   * Bag of properties with source, hoodie client, key generator etc.
   */
  TypedProperties props;

  /**
   * HoodieHBaseIndex.
   */
  private HoodieHBaseIndex hoodieHBaseIndex;

  /**
   * HoodieTable.
   */
  private HoodieTable hoodieTable;

  /**
   * HoodieWriteConfig.
   */
  private HoodieWriteConfig writeConfig;

  /**
   * A wrapped configuration which can be serialized.
   */
  private SerializableConfiguration serializableConf;

  /**
   * Hadoop FileSystem.
   */
  private transient FileSystem fs;

  /**
   * HoodieDeltaStreamer config.
   */
  private WriteJob.Config cfg;

  /**
   * Timeline with completed commits.
   */
  private transient Option<HoodieTimeline> commitTimelineOpt;

  private HoodieWriteClient writeClient;

  public static String CHECKPOINT_KEY = "deltastreamer.checkpoint.key";
  public static String CHECKPOINT_RESET_KEY = "deltastreamer.checkpoint.reset_key";

  /**
   * Test data schema.
   */
  public static final String TRIP_EXAMPLE_SCHEMA = "{\"type\": \"record\"," + "\"name\": \"triprec\"," + "\"fields\": [ "
      + "{\"name\": \"timestamp\",\"type\": \"double\"}," + "{\"name\": \"_row_key\", \"type\": \"string\"},"
      + "{\"name\": \"rider\", \"type\": \"string\"}," + "{\"name\": \"driver\", \"type\": \"string\"},"
      + "{\"name\": \"begin_lat\", \"type\": \"double\"}," + "{\"name\": \"begin_lon\", \"type\": \"double\"},"
      + "{\"name\": \"end_lat\", \"type\": \"double\"}," + "{\"name\": \"end_lon\", \"type\": \"double\"},"
      + "{\"name\": \"fare\",\"type\": {\"type\":\"record\", \"name\":\"fare\",\"fields\": ["
      + "{\"name\": \"amount\",\"type\": \"double\"},{\"name\": \"currency\", \"type\": \"string\"}]}},"
      + "{\"name\": \"_hoodie_is_deleted\", \"type\": \"boolean\", \"default\": false} ]}";

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // get configs from runtimeContext
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    // hadoopConf
    serializableConf = new SerializableConfiguration(new org.apache.hadoop.conf.Configuration());

    // delta streamer conf
    props = UtilHelpers.readConfig(fs, new Path(cfg.propsFilePath), cfg.configs).getConfig();

    // Hadoop FileSystem
    fs = FSUtils.getFs(cfg.targetBasePath, serializableConf.get());

    // HoodieWriteConfig
    writeConfig = getHoodieWriteConfig();

    // use HBaseIndex directly
    hoodieHBaseIndex = new HoodieHBaseIndex(writeConfig);

    // use HoodieCopyOnWriteTable directly
    hoodieTable = new HoodieMergeOnReadTable(writeConfig, serializableConf.get(), hoodieHBaseIndex);

    writeClient = new HoodieWriteClient(hoodieTable,fs);
    writeClient.setOperationType(WriteOperationType.UPSERT);
    LOG.info("Context init successfully");
  }

  @Override
  public void process(String hoodieKey, Context context, Iterable<HoodieRecord> incoming, Collector<String> out) throws Exception {
    // Refresh Timeline
    refreshTimeline();

    // get instantTime and try to commit
    String instantTime = startCommit();

    // tag records
    List<HoodieRecord> taggedRecords = hoodieHBaseIndex.tagLocation(incoming, hoodieTable);

    // profile records
    WorkloadProfile workloadProfile = new WorkloadProfile(taggedRecords);

    writeClient.saveWorkloadProfileMetadataToInflight(workloadProfile, hoodieTable, instantTime);

    // prepare to partition
    PartitionHelper partitionHelper = new PartitionHelper(workloadProfile, hoodieTable);
    // partition the records
    Map<Integer, List<HoodieRecord>> partitionedRecords = taggedRecords
        .stream()
        .collect(Collectors.groupingBy(x -> partitionHelper.getPartition(Pair.of(x.getKey(), Option.ofNullable(x.getCurrentLocation())))));

    // fire the final write
    List<WriteStatus> writeResults = new ArrayList<>();
    for (Map.Entry<Integer, List<HoodieRecord>> entry : partitionedRecords.entrySet()) {
      Iterator<List<WriteStatus>> writeStatuses = hoodieTable.handleUpsertPartition(instantTime, entry.getKey(), entry.getValue().iterator(), partitionHelper);
      if (writeStatuses.hasNext()) {
        List<WriteStatus> writeStatus = writeStatuses.next();
        // refresh index id needed
        writeResults.addAll(writeClient.updateIndexAndCommitIfNeeded(writeStatus, hoodieTable, instantTime));
      }
    }

    // commit
    long totalErrorRecords = writeResults.stream().map(WriteStatus::getTotalErrorRecords).count();
    long totalRecords = writeResults.stream().map(WriteStatus::getTotalRecords).count();
    boolean hasErrors = totalErrorRecords > 0;
    Option<String> scheduledCompactionInstant = Option.empty();
    if (!hasErrors || cfg.commitOnErrors) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      checkpointCommitMetadata.put(CHECKPOINT_KEY, checkpointStr);

      boolean success = writeClient.commit(instantTime, writeResults, Option.of(checkpointCommitMetadata));
      if (success) {
        LOG.info("Commit " + instantTime + " successful!");

        // Schedule compaction if needed
        if (cfg.isAsyncCompactionEnabled()) {
          scheduledCompactionInstant = writeClient.scheduleCompaction(Option.empty());
        }
      } else {
        LOG.info("Commit " + instantTime + " failed!");
        throw new HoodieException("Commit " + instantTime + " failed!");
      }
    } else {
      LOG.error("Delta Sync found errors when writing. Errors/Total=" + totalErrorRecords + "/" + totalRecords);
      LOG.error("Printing out the top 100 errors");
      writeResults.stream().filter(WriteStatus::hasErrors).limit(100).forEach(ws -> {
        LOG.error("Global error :", ws.getGlobalError());
        if (ws.getErrors().size() > 0) {
          ws.getErrors().forEach((key, value) -> LOG.trace("Error for key:" + key + " is " + value));
        }
      });
      // Rolling back instant
      writeClient.rollback(instantTime);
      throw new HoodieException("Commit " + instantTime + " failed and rolled-back !");
    }

    out.collect(scheduledCompactionInstant.get());
  }

  private String startCommit() {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        String instantTime = HoodieActiveTimeline.createNewInstantTime();
        startCommit(instantTime);
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

  private void startCommit(String instantTime) {
    LOG.info("Generate a new instant time " + instantTime);
    HoodieTableMetaClient metaClient = ClientUtils.createMetaClient(serializableConf.get(), writeConfig, true);
    // if there are pending compactions, their instantTime must not be greater than that of this instant time
    metaClient.getActiveTimeline().filterPendingCompactionTimeline().lastInstant().ifPresent(latestPending ->
        ValidationUtils.checkArgument(
            HoodieTimeline.compareTimestamps(latestPending.getTimestamp(), instantTime, HoodieTimeline.LESSER),
            "Latest pending compaction instant time must be earlier than this instant time. Latest Compaction :"
                + latestPending + ",  Ingesting at " + instantTime));
    HoodieActiveTimeline activeTimeline = hoodieTable.getActiveTimeline();
    String commitActionType = hoodieTable.getMetaClient().getCommitActionType();
    activeTimeline.createNewInstant(new HoodieInstant(HoodieInstant.State.REQUESTED, commitActionType, instantTime));
  }

  /**
   * Init HoodieWriteConfig.
   */
  private HoodieWriteConfig getHoodieWriteConfig() {
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().withPath(cfg.targetBasePath).combineInput(cfg.filterDupes, true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withPayloadClass(cfg.payloadClassName)
                // Inline compaction is disabled for continuous mode. otherwise enabled for MOR
                .withInlineCompaction(cfg.isInlineCompactionEnabled()).build())
            .forTable(cfg.targetTableName)
            .withAutoCommit(false).withProps(props);

    HoodieWriteConfig config = builder.build();
    config.setSchema(TRIP_EXAMPLE_SCHEMA);

    // Validate what deltastreamer assumes of write-config to be really safe
    ValidationUtils.checkArgument(writeConfig.isInlineCompaction() == cfg.isInlineCompactionEnabled());
    ValidationUtils.checkArgument(!writeConfig.shouldAutoCommit());
    ValidationUtils.checkArgument(writeConfig.shouldCombineBeforeInsert() == cfg.filterDupes);
    ValidationUtils.checkArgument(writeConfig.shouldCombineBeforeUpsert());
    return config;
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
      HoodieTableMetaClient.initTableType(new org.apache.hadoop.conf.Configuration(serializableConf.get()), cfg.targetBasePath,
          cfg.tableType, cfg.targetTableName, "archived", cfg.payloadClassName);
    }
  }

  @Override
  public void close() throws Exception {
    super.close();
  }
}
