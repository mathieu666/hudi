package org.apache.hudi.writer;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.client.WriteStatus;
import org.apache.hudi.writer.common.HoodieWriteInput;
import org.apache.hudi.writer.common.HoodieWriteOutput;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.utils.UtilHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public class WriteProcessOperator extends AbstractStreamOperator<Tuple4<String, List<WriteStatus>, Integer, Boolean>> implements OneInputStreamOperator<HoodieWriteInput<HoodieRecord>, Tuple4<String, List<WriteStatus>, Integer, Boolean>> {

  private static final Logger LOG = LoggerFactory.getLogger(WriteProcessOperator.class);
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

  private int indexOfThisSubtask;
  String instantTime = "";

  private ListState<Tuple4<String, List<WriteStatus>, Integer, Boolean>> upsertState = null;
  private ListState<String> instantState = null;

  @Override
  public void open() throws Exception {
    try {
      cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

      this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
      // hadoopConf
      serializableHadoopConf = new SerializableConfiguration(new org.apache.hadoop.conf.Configuration());
      // HoodieWriteConfig
      writeConfig = UtilHelpers.getHoodieClientConfig(cfg);

      // writeClient
      writeClient = new HoodieWriteClient<>(serializableHadoopConf.get(), writeConfig, true);

    } catch (Exception e) {
      LOG.error("Open WriteProcessWindowFunction error: " + Thread.currentThread().getId() + ";" + this);
      e.printStackTrace();
      throw e;
    }
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    try {
      long checkpointId = context.getCheckpointId();
      List<Tuple4<String, List<WriteStatus>, Integer, Boolean>> objects = new ArrayList<>(1);
      List<String> hoodieInstantList = new ArrayList<>(1);

      //执行 snapshot 之前先获取有哪些新的 instant
      HoodieInstant hoodieInstant = getInflightInstantTime();

      // 没数据，mock数据凑数
      if (records.isEmpty()) {
        if (hoodieInstant != null) {
          // 下发数据凑数  有可能  hoodieInstant 是 历史的 未提交的 hoodieInstant 所以这里过了一下
          String newInstant = hoodieInstant.getTimestamp();
          if (StringUtils.isNullOrEmpty(instantTime) || Long.parseLong(instantTime) <= Long.parseLong(newInstant)) {
            Tuple4 tuple4 = new Tuple4(newInstant, new ArrayList<WriteStatus>(), getRuntimeContext().getIndexOfThisSubtask(), false);
            output.collect(new StreamRecord<>(tuple4));

            objects.add(tuple4);
            hoodieInstantList.add(newInstant);

            upsertState.update(objects);
            instantState.update(hoodieInstantList);
            instantTime = newInstant;
            // 等待 下游 收到数据
            Thread.sleep(100);
            LOG.warn("执行 snapshotState [" + checkpointId + "] 上一个 instant 为 [" + instantTime + "] 发现了新的未提交的 instant [" + newInstant + "] 但是数据为空, 下发数据进行凑数");
          } else {
            LOG.error("执行 snapshotState [" + checkpointId + "] 上一个 instant 为 [" + instantTime + "] 发现了老的未提交的 instant [" + newInstant + "]");
          }
        }
        return;
      }

      //有数据就一定要有 hoodieInstant
      if (hoodieInstant == null) {
        throw new RuntimeException("执行 snapshot 发现找不到事务 whit index [" + indexOfThisSubtask + "] checkpoint id [" + checkpointId + "]");
      }

      instantTime = hoodieInstant.getTimestamp();
      LOG.warn("执行 snapshotState [" + checkpointId + "] 当前事务为 [" + instantTime + "] 当前批次大小 [" + records.size() + "]");

      long t1 = System.currentTimeMillis();

      //hoodieTable.upsert()
      HoodieWriteOutput<List<WriteStatus>> writeStatus = writeClient.upsert(new HoodieWriteInput<>(records), instantTime);
      if (null != writeStatus) {
        List<WriteStatus> writeStatusOutput = writeStatus.getOutput();
        if (null != writeStatusOutput) {
          Tuple4 tuple4 = new Tuple4(instantTime, writeStatusOutput, getRuntimeContext().getIndexOfThisSubtask(), false);
          output.collect(new StreamRecord<>(tuple4));

          objects.add(tuple4);
          hoodieInstantList.add(instantTime);

          upsertState.update(objects);
          instantState.update(hoodieInstantList);
          // 等待 下游 收到数据
          Thread.sleep(100);
          writeStatusOutput.forEach(x -> {
            LOG.warn("执行 snapshotState [" + checkpointId + "] 当前事务为 [" + instantTime + "] 下发 WriteStatus: [{}]", x);
          });
          long t2 = System.currentTimeMillis();
          // 输出writeStatus
          LOG.warn("执行 snapshotState [" + checkpointId + "] 当前事务为 [" + instantTime + "] 下发数据条数 [{}],处理条数 [{}] 耗时 [{}]", writeStatusOutput.size(), records.size(), (t2 - t1));
          //清理缓存
          records.clear();
        }
      }
    } catch (Exception e) {
      LOG.error("SnapshotState WriteProcessWindowFunction error: " + Thread.currentThread().getId() + ";" + this);
      e.printStackTrace();
      throw e;
    }

  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {

    ListStateDescriptor upsertResultStateDescriptor = new ListStateDescriptor("upsertResultState", TypeInformation.of(new TypeHint<Tuple4<String, List<WriteStatus>, Integer, Boolean>>() {
    }));

    ListStateDescriptor upsertInstantStateDescriptor = new ListStateDescriptor("upsertInstantState", TypeInformation.of(new TypeHint<String>() {
    }));

    upsertState = context.getOperatorStateStore().getListState(upsertResultStateDescriptor);
    instantState = context.getOperatorStateStore().getListState(upsertInstantStateDescriptor);
    if (context.isRestored()) {
      Iterator<Tuple4<String, List<WriteStatus>, Integer, Boolean>> iterator = upsertState.get().iterator();
      if (iterator.hasNext()) {
        Tuple4<String, List<WriteStatus>, Integer, Boolean> restoredData = iterator.next();
        restoredData.f1.forEach(x -> {
          LOG.warn("WriteProcessWindowFunction 任务恢复下发数据 事务为  [" + restoredData.f0 + "] 下发 WriteStatus: [{}]", x);
        });
        if (restoredData.f1 != null) {
          output.collect(new StreamRecord<>(new Tuple4(restoredData.f0, restoredData.f1, getRuntimeContext().getIndexOfThisSubtask(), true)));
        }
        Thread.sleep(100);
      }
      Iterator<String> instantItertor = instantState.get().iterator();
      if (instantItertor.hasNext()) {
        instantTime = instantItertor.next();
      }
    }
  }

  private HoodieInstant getInflightInstantTime() {
    HoodieTableMetaClient meta = new HoodieTableMetaClient(serializableHadoopConf.get(), cfg.targetBasePath,
        cfg.payloadClassName);
    Option<HoodieInstant> hoodieInstantOption = meta.getActiveTimeline().filter(HoodieInstant::isRequested).lastInstant();
    if (hoodieInstantOption.isPresent()) {
      return hoodieInstantOption.get();
    } else {
      return null;
    }
  }

  @Override
  public void processElement(StreamRecord<HoodieWriteInput<HoodieRecord>> element) throws Exception {
    records.add(element.getValue().getInputs());
  }
}
