package org.apache.hudi.writer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.client.WriteStatus;
import org.apache.hudi.writer.common.HoodieWriteOutput;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.execution.Compactor;
import org.apache.hudi.writer.utils.UtilHelpers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

  private Map<String, List<List<WriteStatus>>> allWriteResults = new LinkedHashMap();
  private transient Compactor compactor;

  private transient Object lock = null;
  private Integer upsertParalleSize = 0;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // get configs from runtimeContext
    cfg = (WriteJob.Config) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
    upsertParalleSize = cfg.parallelism;
    // hadoopConf
    serializableHadoopConf = new SerializableConfiguration(UtilHelpers.getHadoopConf());

    // HoodieWriteConfig
    writeConfig = UtilHelpers.getHoodieClientConfig(cfg);

    // writeClient
    writeClient = new HoodieWriteClient<>(serializableHadoopConf.get(), writeConfig, true);

    // Compactor
    compactor = new Compactor(writeClient, serializableHadoopConf.get());

    lock = new Object();
  }

  @Override
  public void invoke(Tuple4<String, List<WriteStatus>, Integer, Boolean> value, Context context) throws Exception {
    try {
      String key = value.f0;
      synchronized (lock) {
        // 缓存数据
        if (allWriteResults.containsKey(key)) {
          // 把数据 数据放到对应的位置上
          allWriteResults.get(key).set(value.f2, value.f1);
        } else {
          //固定长度
          List<List<WriteStatus>> lists = new ArrayList<>(upsertParalleSize);
          for (int i = 0; i < upsertParalleSize; i++) {
            lists.add(null);
          }
          lists.set(value.f2, value.f1);
          allWriteResults.put(key, lists);
        }
        //每次收到数据进行一次检测提交
        checkAndCommit();
      }
    } catch (Exception e) {
      LOG.error("Invoke CommitAndRollbackSink error: " + Thread.currentThread().getId() + ";" + this);
      e.printStackTrace();
      throw e;
    }
  }

  /**
   * 检测 提交.
   *
   * @throws Exception
   */
  private boolean checkAndCommit() throws Exception {
    if (allWriteResults.isEmpty()) {
      return false;
    }

    // 对hashMap 进行检查看是否有 数据已经到齐，如果到齐了则进行提交
    List<Tuple2<String, Integer>> instants = new ArrayList<>(allWriteResults.size());
    allWriteResults.forEach((k, v) -> {
      int i = 0;
      for (List<WriteStatus> ls : v) {
        if (ls != null) {
          i++;
        }
      }
      instants.add(new Tuple2<>(k, i));
    });

    if (instants.isEmpty()) {
      instants.forEach(x -> {
        LOG.warn("CheckerAdCommit 当前事务 [" + x.f0 + "] 到达条数 [" + x.f1 + "] 并行行度为 [" + upsertParalleSize + "] 是否可以提交 [" + (x.f1 >= upsertParalleSize) + "]");
      });

      List<String> collect = instants.stream().filter(x -> x.f1 >= upsertParalleSize).map(x -> x.f0).sorted().collect(Collectors.toList());
      for (String instantTime : collect) {
        doCommit(instantTime);
        //移除数据
        allWriteResults.remove(instantTime);
        return true;
      }
    }
    return false;

  }

  public void doCommit(String instantTime) throws Exception {

    if (StringUtils.isNullOrEmpty(instantTime)) {
      Set<String> strings = allWriteResults.keySet();
      StringBuffer sb = new StringBuffer();
      strings.forEach(x -> sb.append(x).append(" "));
      throw new RuntimeException("Do commit but instant is empty! instantTime: [" + instantTime + "] buffer instantTime [" + sb + "]");
    }

    if (!allWriteResults.containsKey(instantTime)) {
      Set<String> strings = allWriteResults.keySet();
      StringBuffer sb = new StringBuffer();
      strings.forEach(x -> sb.append(x).append(" "));
      throw new RuntimeException("Commit 当前事务,但是收到的数据时间戳 [" + sb.toString() + "] 不包含待提交时间戳 [" + instantTime + "]");
    }

    List<List<WriteStatus>> lists = allWriteResults.get(instantTime);
    //获取数据
    List<WriteStatus> writeResults = lists.stream().flatMap(Collection::stream).collect(Collectors.toList());

    if (lists.size() != upsertParalleSize) {
      LOG.error("Commit 当前事务但是数据到达条数 [" + lists.size() + "] 与数据并行度 [" + upsertParalleSize + "] 不一致 继续等待");
      throw new RuntimeException("Commit 当前事务但是数据到达条数 [" + lists.size() + "] 与数据并行度 [" + upsertParalleSize + "] 不一致 继续等待");
    }

    StringBuffer sb = new StringBuffer("Commit 当前集合数据为 ");
    allWriteResults.forEach((k, v) -> {
      sb.append("[").append(k).append("->").append(v.size()).append("]").append(" ");
    });
    LOG.warn(sb.toString());

    if (writeResults.isEmpty()) {
      LOG.warn("Commit 当前事务 [{}] 但是数据为空 ", instantTime);
      return;
    }

    //循环对 snapshot 事务 进行提交
    LOG.warn("准备对事务[{}] 进行周期提交!", instantTime);

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

    // clear writeStatuses and wait for next checkpoint
    writeResults.clear();
    allWriteResults.remove(instantTime);
    sb.delete(0, sb.length());
    allWriteResults.forEach((k, v) -> {
      sb.append("[").append(k).append("->").append(v.size()).append("]").append(" ");
    });
    LOG.warn("结束对事务[{}] 进行周期提交! 剩余 instant {}", instantTime, sb);
  }
}
