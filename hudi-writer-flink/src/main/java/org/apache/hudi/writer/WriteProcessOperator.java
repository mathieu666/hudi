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
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.client.WriteStatus;
import org.apache.hudi.writer.common.HoodieWriteInput;
import org.apache.hudi.writer.common.HoodieWriteOutput;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.utils.UtilHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
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
  private transient ListState<HoodieRecord> incomeRecordsState;

  private int indexOfThisSubtask;

  @Override
  public void open() throws Exception {
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();

    this.indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
    // hadoopConf
    serializableHadoopConf = new SerializableConfiguration(new org.apache.hadoop.conf.Configuration());
    // HoodieWriteConfig
    writeConfig = UtilHelpers.getHoodieClientConfig(cfg);

    // writeClient
    writeClient = new HoodieWriteClient<>(serializableHadoopConf.get(), writeConfig, true);
  }

  @Override
  public void snapshotState(StateSnapshotContext context) throws Exception {
    long checkpointId = context.getCheckpointId();
    //执行 snapshot 之前先获取有哪些新的 instant
    String commitType = cfg.tableType.equals("COPY_ON_WRITE") ? "commit" : "deltacommit";
    HoodieInstant latestInstant = getLatestRequestedInstantTime(commitType);

    // instant 不为空， 说明上游有数据下发，
    if (latestInstant != null) {
      String instantTimestamp = latestInstant.getTimestamp();
      // 当前算子所在分区没数据，mock 空数据下发
      if (records.isEmpty()) {
        LOG.info("执行 snapshotState, 当前分区无数据 subtask id = [{}] 执行 snapshotState [{}}] 当前事务为 [{}]", indexOfThisSubtask, checkpointId, instantTimestamp);
        Tuple4 tuple4 = new Tuple4(instantTimestamp, new ArrayList<WriteStatus>(), getRuntimeContext().getIndexOfThisSubtask(), false);
        output.collect(new StreamRecord<>(tuple4));
        incomeRecordsState.update(records);
      } else {
        LOG.info("执行 snapshotState, 当前分区有数据 subtask id = [{}] 执行 snapshotState [{}}] 当前事务为 [{}], 数据条数为 [{}]", indexOfThisSubtask, checkpointId, instantTimestamp, records.size());
        long t1 = System.currentTimeMillis();

        HoodieWriteOutput<List<WriteStatus>> writeStatus = writeClient.upsert(new HoodieWriteInput<>(records), instantTimestamp);
        List<WriteStatus> writeStatusOutput = writeStatus.getOutput();
        if (null != writeStatusOutput) {
          Tuple4 tuple4 = new Tuple4(instantTimestamp, writeStatusOutput, getRuntimeContext().getIndexOfThisSubtask(), false);
          output.collect(new StreamRecord<>(tuple4));
          long t2 = System.currentTimeMillis();
          // 输出writeStatus
          LOG.warn("执行 snapshotState [" + checkpointId + "] 当前事务为 [" + instantTimestamp + "] 下发数据条数 [{}],处理条数 [{}] 耗时 [{}]", writeStatusOutput.size(), records.size(), (t2 - t1));
          incomeRecordsState.update(records);
          //清理缓存
          records.clear();
        }
      }
    }
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {

    ListStateDescriptor<HoodieRecord> recordsState = new ListStateDescriptor("recordsState", TypeInformation.of(new TypeHint<HoodieRecord>() {
    }));

    incomeRecordsState = context.getOperatorStateStore().getListState(recordsState);
    if (context.isRestored()) {
      incomeRecordsState.get().forEach(x -> records.add(x));
    }
  }

  private HoodieInstant getLatestRequestedInstantTime(String commitType) {
    LOG.info("WriteProcessOperator query latest requested instant subtask id = [{}]", indexOfThisSubtask);
    HoodieTableMetaClient meta = new HoodieTableMetaClient(serializableHadoopConf.get(), cfg.targetBasePath,
        cfg.payloadClassName);
    Option<HoodieInstant> latestRequestedInstant = meta.getActiveTimeline()
        .filter(i -> (i.getState().equals(HoodieInstant.State.REQUESTED) && (i.getAction().equals(commitType))))
        .lastInstant();
    if (latestRequestedInstant.isPresent()) {
      return latestRequestedInstant.get();
    } else {
      return null;
    }
  }

  @Override
  public void processElement(StreamRecord<HoodieWriteInput<HoodieRecord>> element) throws Exception {
    records.add(element.getValue().getInputs());
  }
}
