package org.apache.hudi.writer;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
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
import java.util.LinkedList;
import java.util.List;

public class InstantGenerateOperator extends AbstractStreamOperator<HoodieWriteInput<HoodieRecord>> implements OneInputStreamOperator<HoodieWriteInput<HoodieRecord>, HoodieWriteInput<HoodieRecord>> {
  private static final Logger LOG = LoggerFactory.getLogger(InstantGenerateOperator.class);
  private WriteJob.Config cfg;
  private HoodieWriteConfig writeConfig;
  private HoodieWriteClient writeClient;
  private SerializableConfiguration serializableHadoopConf;
  private transient FileSystem fs;
  private transient Long commitNum = 0L;
  // 空字符串代表 第一次启动程序或上一个instant已完成
  private String latestInstant = "";
  List<String> latestInstantList = new ArrayList<>(1);
  private transient ListState<String> latestInstantState;
  private List<StreamRecord> records = new LinkedList();
  private transient ListState<StreamRecord> recordsState;

  @Override
  public void processElement(StreamRecord element) throws Exception {
    if (element.getValue() != null) {
      commitNum++;
      records.add(element);
      output.collect(element);
    }
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);
    // 第一步检测之前的 instant 是否都已提交 等不到就抛异常
    if (!StringUtils.isNullOrEmpty(latestInstant)) {
      doChecker();
      // 上一个instant完成，将latestInstant置空
      latestInstant = "";
    }

    // 有数据 才开启 INSTANT
    if (commitNum > 0) {
      latestInstant = startNewInstant(checkpointId);
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
    // init table
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
   * 开启 N 个 instant.
   *
   * @param checkpointId
   */
  private String startNewInstant(long checkpointId) {
    String newTime = startCommit();
    LOG.info("create instant [{}], at checkpoint [{}]", newTime, checkpointId);
    return newTime;
  }

  /**
   * 检测上一个checkpoint 的 instant 是否都已提交.
   */
  private void doChecker() throws InterruptedException {
    // 获取未提交的事务
    String commitType = cfg.tableType.equals("COPY_ON_WRITE") ? "commit" : "deltacommit";
    LOG.info("InstantGenerateOperator query latest instant [{}]", latestInstant);
    List<String> rollbackPendingCommits = writeClient.getInflightsAndRequestedInstants(commitType);
    int tryTimes = 0;
    boolean hasNoCommit = true;
    while (hasNoCommit) {
      tryTimes++;
      StringBuffer sb = new StringBuffer();
      if (rollbackPendingCommits.contains(latestInstant)) {
        //清空 sb
        rollbackPendingCommits.forEach(x -> sb.append(x).append(","));

        LOG.warn("Latest transaction [{}] is not completed! unCompleted transaction:[{}],try times [{}]", latestInstant, sb.toString(), tryTimes);

        Thread.sleep(1000);
        rollbackPendingCommits = writeClient.getInflightsAndRequestedInstants(commitType);
      } else {
        LOG.warn("Latest transaction [{}] is completed! Completed transaction,try times [{}]", latestInstant, tryTimes);
        hasNoCommit = false;
      }
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    // instantState
    ListStateDescriptor<String> latestInstantStateDescriptor = new ListStateDescriptor<String>("latestInstant", String.class);
    latestInstantState = context.getOperatorStateStore().getListState(latestInstantStateDescriptor);

    // recordState
    ListStateDescriptor<StreamRecord> recordsStateDescriptor = new ListStateDescriptor<StreamRecord>("recordsState", StreamRecord.class);
    recordsState = context.getOperatorStateStore().getListState(recordsStateDescriptor);

    if (context.isRestored()) {
      Iterator<String> latestInstantIterator = latestInstantState.get().iterator();
      latestInstantIterator.forEachRemaining(x -> latestInstant = x);
      LOG.info("InstantGenerateOperator initializeState get latestInstant [{}]", latestInstant);

      Iterator<StreamRecord> recordIterator = recordsState.get().iterator();
      records.clear();
      recordIterator.forEachRemaining(x -> records.add(x));
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext functionSnapshotContext) throws Exception {
    if (latestInstantList.isEmpty()) {
      latestInstantList.add(latestInstant);
    } else {
      latestInstantList.set(0, latestInstant);
    }
    latestInstantState.update(latestInstantList);
    LOG.info("InstantGenerateOperator snapshotState update latestInstant [{}]", latestInstant);

    recordsState.update(records);
    LOG.info("InstantGenerateOperator snapshotState update recordsState size = [{}]", records.size());
    records.clear();
  }
}
