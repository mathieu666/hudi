/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table.action.compact;

import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCompactionException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ScheduleCompactionActionExecutor extends BaseActionExecutor<Option<HoodieCompactionPlan>> {

  private static final Logger LOG = LogManager.getLogger(ScheduleCompactionActionExecutor.class);

  private final Option<Map<String, String>> extraMetadata;

  public ScheduleCompactionActionExecutor(JavaSparkContext jsc,
                                          HoodieWriteConfig config,
                                          HoodieTable<?> table,
                                          String instantTime,
                                          Option<Map<String, String>> extraMetadata) {
    super(jsc, config, table, instantTime);
    this.extraMetadata = extraMetadata;
  }

  private HoodieCompactionPlan scheduleCompaction() {
    LOG.info("Checking if compaction needs to be run on " + config.getBasePath());
    // 找到上一次压缩的时间
    Option<HoodieInstant> lastCompaction = table.getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant();
    String deltaCommitsSinceTs = "0";
    if (lastCompaction.isPresent()) {
      deltaCommitsSinceTs = lastCompaction.get().getTimestamp();
    }
    // 查询自上次压缩之后，执行了多少个deltacommit
    int deltaCommitsSinceLastCompaction = table.getActiveTimeline().getDeltaCommitTimeline()
        .findInstantsAfter(deltaCommitsSinceTs, Integer.MAX_VALUE).countInstants();
    // 如果还未压缩的deltacommit数小于配置的最大排队deltacommit数，则不生成压缩计划
    if (config.getInlineCompactDeltaCommitMax() > deltaCommitsSinceLastCompaction) {
      LOG.info("Not running compaction as only " + deltaCommitsSinceLastCompaction
          + " delta commits was found since last compaction " + deltaCommitsSinceTs + ". Waiting for "
          + config.getInlineCompactDeltaCommitMax());
      return new HoodieCompactionPlan();
    }

    LOG.info("Compacting merge on read table " + config.getBasePath());
    // 初始化压缩器，生成压缩计划并返回
    HoodieMergeOnReadTableCompactor compactor = new HoodieMergeOnReadTableCompactor();
    try {
      return compactor.generateCompactionPlan(jsc, table, config, instantTime,
          ((SyncableFileSystemView) table.getSliceView()).getPendingCompactionOperations()
              .map(instantTimeOpPair -> instantTimeOpPair.getValue().getFileGroupId())
              .collect(Collectors.toSet()));

    } catch (IOException e) {
      throw new HoodieCompactionException("Could not schedule compaction " + config.getBasePath(), e);
    }
  }

  @Override
  public Option<HoodieCompactionPlan> execute() {
    // 获取inflight状态的commit/deltacommit.如果有，那么他们中第一个instant的时间戳应该比压缩的大
    table.getActiveTimeline().getCommitsTimeline().filterPendingExcludingCompaction().firstInstant()
        .ifPresent(earliestInflight -> ValidationUtils.checkArgument(
            HoodieTimeline.compareTimestamps(earliestInflight.getTimestamp(), HoodieTimeline.GREATER_THAN, instantTime),
            "Earliest write inflight instant time must be later than compaction time. Earliest :" + earliestInflight
                + ", Compaction scheduled at " + instantTime));

    // Committed and pending compaction instants should have strictly lower timestamps
    // 获取冲突的Instant => 类型为commit/deltacommit/compact，时间戳比当前调度的compact时间戳大（正常情况下应该没有）。
    List<HoodieInstant> conflictingInstants = table.getActiveTimeline()
        .getCommitsAndCompactionTimeline().getInstants()
        .filter(instant -> HoodieTimeline.compareTimestamps(
            instant.getTimestamp(), HoodieTimeline.GREATER_THAN_OR_EQUALS, instantTime))
        .collect(Collectors.toList());
    // 验证冲突的instant应该为空
    ValidationUtils.checkArgument(conflictingInstants.isEmpty(),
        "Following instants have timestamps >= compactionInstant (" + instantTime + ") Instants :"
            + conflictingInstants);

    // 生成压缩计划
    HoodieCompactionPlan plan = scheduleCompaction();
    // 如果压缩计划的操作集合不为空，则生成compactionInstant 保存到时间轴
    if (plan != null && (plan.getOperations() != null) && (!plan.getOperations().isEmpty())) {
      extraMetadata.ifPresent(plan::setExtraMetadata);
      HoodieInstant compactionInstant =
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
      try {
        table.getActiveTimeline().saveToCompactionRequested(compactionInstant,
            TimelineMetadataUtils.serializeCompactionPlan(plan));
      } catch (IOException ioe) {
        throw new HoodieIOException("Exception scheduling compaction", ioe);
      }
      return Option.of(plan);
    }
    return Option.empty();
  }
}
