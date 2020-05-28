package org.apache.hudi.table.action.commit;

import org.apache.hudi.client.TaskContextSupplier;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieCommitException;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.WorkloadStat;
import org.apache.hudi.table.action.BaseActionExecutor;
import org.apache.hudi.table.action.HoodieWriteMetadata;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public abstract class BaseCommitActionExecutor<T extends HoodieRecordPayload<T>, I extends HoodieWriteInput, K extends HoodieWriteKey, O extends HoodieWriteOutput, P>
    extends BaseActionExecutor<HoodieWriteMetadata<O>, T, I, K, O, P> {

  private static final Logger LOG = LogManager.getLogger(BaseCommitActionExecutor.class);

  private final WriteOperationType operationType;
  protected final TaskContextSupplier taskContextSupplier;


  public BaseCommitActionExecutor(HoodieEngineContext context, HoodieWriteConfig config, HoodieTable<T, I, K, O, P> table, String instantTime, WriteOperationType operationType) {
    super(context, config, table, instantTime);
    this.operationType = operationType;
    this.taskContextSupplier = table.getTaskContextSupplier();
  }

  public abstract HoodieWriteMetadata<O> execute(I inputs);

  /**
   * Save the workload profile in an intermediate file (here re-using commit files) This is useful when performing
   * rollback for MOR tables. Only updates are recorded in the workload profile metadata since updates to log blocks
   * are unknown across batches Inserts (which are new parquet files) are rolled back based on commit time. // TODO :
   * Create a new WorkloadProfile metadata file instead of using HoodieCommitMetadata
   */
  void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, String instantTime)
      throws HoodieCommitException {
//    try {
//      HoodieCommitMetadata metadata = new HoodieCommitMetadata();
//      profile.getPartitionPaths().forEach(path -> {
//        WorkloadStat partitionStat = profile.getWorkloadStat(path.toString());
//        partitionStat.getUpdateLocationToCount().forEach((key, value) -> {
//          HoodieWriteStat writeStat = new HoodieWriteStat();
//          writeStat.setFileId(key);
//          // TODO : Write baseCommitTime is possible here ?
//          writeStat.setPrevCommit(value.getKey());
//          writeStat.setNumUpdateWrites(value.getValue());
//          metadata.addWriteStat(path.toString(), writeStat);
//        });
//      });
//      metadata.setOperationType(operationType);
//
//      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
//      String commitActionType = table.getMetaClient().getCommitActionType();
//      HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, commitActionType, instantTime);
//      activeTimeline.transitionRequestedToInflight(requested,
//          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)),
//          config.shouldAllowMultiWriteOnSameInstant());
//    } catch (IOException io) {
//      throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", io);
//    }
  }
}
