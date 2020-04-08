package org.apache.hudi.writer;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;

/**
 * Commit and rollback.
 */
public class CommitAndRollbackProcessFunction extends ProcessFunction<List<WriteStatus>, Object> {
  private static final Logger LOG = LogManager.getLogger(CommitAndRollbackProcessFunction.class);
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

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // get configs from runtimeContext
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    // HoodieWriteConfig
    writeConfig = getHoodieWriteConfig();

    // hadoopConf
    serializableHadoopConf = new SerializableConfiguration(new org.apache.hadoop.conf.Configuration());

    // writeClient
    writeClient = new HoodieWriteClient<>(serializableHadoopConf.get(), writeConfig, true);
  }

  @Override
  public void processElement(List<WriteStatus> writeResults, Context ctx, Collector<Object> out) throws Exception {
    long totalErrorRecords = writeResults.stream().map(WriteStatus::getTotalErrorRecords).count();
    long totalRecords = writeResults.stream().map(WriteStatus::getTotalRecords).count();
    boolean hasErrors = totalErrorRecords > 0;

    Option<String> scheduledCompactionInstant = Option.empty();

    if (!hasErrors || cfg.commitOnErrors) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      checkpointCommitMetadata.put(CHECKPOINT_KEY, checkpointStr);
      if (cfg.checkpoint != null) {
        checkpointCommitMetadata.put(CHECKPOINT_RESET_KEY, cfg.checkpoint);
      }

      if (hasErrors) {
        LOG.warn("Some records failed to be merged but forcing commit since commitOnErrors set. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }

      boolean success = writeClient.commit(instantTime, writeStatus, Option.of(checkpointCommitMetadata));
      if (success) {
        LOG.info("Commit " + instantTime + " successful!");

        // Schedule compaction if needed
        if (cfg.isAsyncCompactionEnabled()) {
          scheduledCompactionInstant = writeClient.scheduleCompaction(Option.empty());
        }

        if (!isEmpty) {
          syncHive();
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
  }

  private HoodieWriteConfig getHoodieWriteConfig() {
    // TODO
    return HoodieWriteConfig.newBuilder().build();
  }
}
