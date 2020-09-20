package org.apache.hudi.writer;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.CollectionUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.common.HoodieWriteInput;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.utils.UtilHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class InstantGenerateOperator extends AbstractStreamOperator<HoodieWriteInput<HoodieRecord>> implements OneInputStreamOperator<HoodieWriteInput<HoodieRecord>, HoodieWriteInput<HoodieRecord>> {
  private static final Logger LOG = LoggerFactory.getLogger(InstantGenerateOperator.class);
  private WriteJob.Config cfg;
  private HoodieWriteConfig writeConfig;
  private HoodieWriteClient writeClient;
  private SerializableConfiguration serializableHadoopConf;
  private transient FileSystem fs;
  private transient Long commitNum = 0L;
  private String latestTimes = "";
  private ListState<String> latestTimesState = null;

  @Override
  public void processElement(StreamRecord element) throws Exception {
    if (element.getValue() != null) {
      commitNum++;
      output.collect(element);
    }
  }

  @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
      super.prepareSnapshotPreBarrier(checkpointId);
    //第一步检测之前的 instant 是否都已提交 等不到就抛异常
    if (!StringUtils.isNullOrEmpty(latestTimes)) {
      doChecker();
      latestTimes = "";
    }

    // 有数据 才开启 INSTANT
    if (commitNum > 0) {
      latestTimes = startNewInstant(checkpointId);
      commitNum = 0L;
    }
    super.prepareSnapshotPreBarrier(checkpointId);
  }

  @Override
  public void open() throws Exception {
    super.open();
    // get configs from runtimeContext
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    // hadoopConf
    serializableHadoopConf = new SerializableConfiguration(UtilHelpers.getHadoopConf());
    // Hadoop FileSystem
    fs = FSUtils.getFs(cfg.targetBasePath, serializableHadoopConf.get());
    // HoodieWriteConfig
    writeConfig = UtilHelpers.getHoodieClientConfig(cfg);
    // writeClient
    writeClient = new HoodieWriteClient<>(serializableHadoopConf.get(), writeConfig, true);
    commitNum = 0L;
    initTable();
  }

  private String startCommit() {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        String instantTime = HoodieActiveTimeline.createNewInstantTime();
        writeClient.startCommit(instantTime);
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
      HoodieTableMetaClient.initTableType(new Configuration(serializableHadoopConf.get()), cfg.targetBasePath,
          cfg.tableType, cfg.targetTableName, "archived", cfg.payloadClassName);
      LOG.info("table initialized");
    }
  }

  /**
   * 开启 N 个 instant
   *
   * @param checkpointId
   */
  private String startNewInstant(long checkpointId) {
    String newTime = startCommit();
    LOG.warn("开启 checkpoint [" + checkpointId + "],启动新事务 [" + newTime + "]");
    return newTime;
  }

  /**
   * 检测上一个checkpoint 的 instant 是否都已提交
   * 如果超时 10s 都存在有 未提交的 instant 就报错
   */
  private void doChecker() throws InterruptedException {
    //获取 未提交的事务
    List<String> rollbackPendingCommits = writeClient.getRollbackPendingCommits();
    int tryTimes = 1;
    boolean hasNoCommit = true;

    while (!CollectionUtil.isNullOrEmpty(rollbackPendingCommits) && hasNoCommit) {
      StringBuffer sb = new StringBuffer();
      if (rollbackPendingCommits.contains(latestTimes)) {
        //清空 sb
        rollbackPendingCommits.forEach(x -> sb.append(x).append(","));

        LOG.error("Latest transaction [{}] is not completed! unCompleted transaction:[{}],try times [{}]", latestTimes, sb.toString(), tryTimes);

        Thread.sleep(1000);
        rollbackPendingCommits = writeClient.getRollbackPendingCommits();
        tryTimes++;
        if (tryTimes >= 10) {
          LOG.error("Latest transaction [{}] is not completed! unCompleted transaction:[{}],try times [{}], throw exception !!", latestTimes, sb.toString(), tryTimes);
          throw new RuntimeException("Lateliest transaction [" + latestTimes + "] is not completed.until try " + tryTimes + " times.");
        }
      }
      //交集 为空 代表 所以事务都不需要回滚（都已提交）
      else {
        LOG.warn("Latest transaction [{}] is completed! Completed transaction,try times [{}]", latestTimes, tryTimes);
        hasNoCommit = false;
      }
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    ListStateDescriptor lateliestTimesStateDescriptor = new ListStateDescriptor<String>("latestTimesState", String.class);
    latestTimesState = context.getOperatorStateStore().getListState(lateliestTimesStateDescriptor);
    if (context.isRestored()) {
      Iterator<String> iterator = latestTimesState.get().iterator();
      if (iterator.hasNext()) {
        latestTimes = iterator.next();
        LOG.warn("InstantGenerateOperator initializeState get latestTimes [{}]", latestTimes);
      }
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext functionSnapshotContext) throws Exception {
    if (!StringUtils.isNullOrEmpty(latestTimes)) {
      List<String> strings = new ArrayList<>(1);
      strings.add(latestTimes);
      latestTimesState.update(strings);
      LOG.warn("InstantGenerateOperator snapshotState update latestTimes [{}]", latestTimes);
    }
  }
}
