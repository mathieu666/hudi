package org.apache.hudi.writer;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.client.WriteStatus;
import org.apache.hudi.writer.common.HoodieWriteOutput;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.execution.Compactor;
import org.apache.hudi.writer.utils.UtilHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class CommitAndRollbackSink extends RichSinkFunction<Tuple4<String, List<WriteStatus>, Integer, Boolean>> {
  private static final Logger LOG = LoggerFactory.getLogger(CommitAndRollbackSink.class);
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

  private List<List<WriteStatus>> allWriteResults = new LinkedList<>();
  private transient Compactor compactor;

  private Integer upsertParalleSize = 0;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // get configs from runtimeContext
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    upsertParalleSize = getRuntimeContext().getExecutionConfig().getParallelism();
    // hadoopConf
    serializableHadoopConf = new SerializableConfiguration(UtilHelpers.getHadoopConf());

    // HoodieWriteConfig
    writeConfig = UtilHelpers.getHoodieClientConfig(cfg);

    // writeClient
    writeClient = new HoodieWriteClient<>(serializableHadoopConf.get(), writeConfig, true);

    // Compactor
    compactor = new Compactor(writeClient, serializableHadoopConf.get());
  }

  @Override
  public void invoke(Tuple4<String, List<WriteStatus>, Integer, Boolean> value, Context context) throws Exception {
    LOG.info(" Sink 收到数据 instantTime = [{}], subtaskId = [{}] 的数据, size = [{}]", value.f0, value.f2, value.f1.size());
    try {
      allWriteResults.add(value.f1);
      //每次收到数据进行一次检测提交
      checkAndCommit(value.f0);
    } catch (Exception e) {
      LOG.error("Invoke CommitAndRollbackSink error: " + Thread.currentThread().getId() + ";" + this);
      throw e;
    }
  }

  /**
   * 检测 提交.
   *
   * @throws Exception
   */
  private boolean checkAndCommit(String instantTime) throws Exception {
    if (allWriteResults.size() == upsertParalleSize) {
      LOG.info("事务 [{}] 已达到提交标准, 开始提交！", instantTime);
      doCommit(instantTime);
      allWriteResults.clear();
      LOG.info("事务 [{}] 提交完毕", instantTime);
      return true;
    } else {
      LOG.info("事务 [{}] 未达到提交标准, 已到达分区数 = [{}/{}]", instantTime, allWriteResults.size(), upsertParalleSize);
      return false;
    }
  }

  public void doCommit(String instantTime) throws Exception {
    //获取数据
    List<WriteStatus> writeResults = allWriteResults.stream().flatMap(Collection::stream).collect(Collectors.toList());

    //循环对 snapshot 事务 进行提交
    LOG.warn("准备对事务[{}] 进行提交!", instantTime);

    // commit and rollback
    long totalErrorRecords = writeResults.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);
    long totalRecords = writeResults.stream().map(WriteStatus::getTotalRecords).reduce(Long::sum).orElse(0L);
    boolean hasErrors = totalErrorRecords > 0;

    Option<String> scheduledCompactionInstant = Option.empty();

    if (!hasErrors || cfg.commitOnErrors) {
      HashMap<String, String> checkpointCommitMetadata = new HashMap<>();
      if (hasErrors) {
        LOG.warn("Some records failed to be merged but forcing commit since commitOnErrors set. Errors/Total="
            + totalErrorRecords + "/" + totalRecords);
      }

      boolean success = writeClient.commit(instantTime, new HoodieWriteOutput<>(writeResults), Option.of(checkpointCommitMetadata));
      if (success) {
        LOG.warn("Commit " + instantTime + " successful!");
        // Schedule compaction if needed
        if (cfg.isAsyncCompactionEnabled() && HoodieTableType.MERGE_ON_READ.name().equals(cfg.tableType)) {
          scheduledCompactionInstant = writeClient.scheduleCompaction(Option.empty());
          if (scheduledCompactionInstant.isPresent()) {
            compactor.compact(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, scheduledCompactionInstant.get()));
          }
        }
      } else {
        LOG.warn("Commit " + instantTime + " failed!");
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
      //writeClient.rollback(instantTime);
      //失败不能回滚
      throw new HoodieException("Commit " + instantTime + " failed and rolled-back !");
    }
  }
}
