package org.apache.hudi.writer.function;

import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hudi.HoodieEngineContext;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class FinalCommitSink extends RichSinkFunction<List<WriteStatus>> implements CheckpointListener {
  private HoodieEngineContext context;
  private List<WriteStatus> writeResults = new ArrayList<>();

  public FinalCommitSink(HoodieEngineContext context) {
    this.context = context;
  }

  @Override
  public void notifyCheckpointComplete(long l) throws Exception {
    if (writeResults.isEmpty()) {
      return;
    }
    String instantTime = getInstantTime();
    LOG.info("CommitAndRollbackSink Get instantTime = {}", instantTime);

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

      boolean success = writeClient.commit(instantTime, writeResults, Option.of(checkpointCommitMetadata));
      if (success) {
        LOG.info("Commit " + instantTime + " successful!");
        // Schedule compaction if needed
        if (cfg.isAsyncCompactionEnabled() && HoodieTableType.MERGE_ON_READ.name().equals(cfg.tableType)) {
          scheduledCompactionInstant = writeClient.scheduleCompaction(Option.empty());
          if (scheduledCompactionInstant.isPresent()) {
            compactor.compact(new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, scheduledCompactionInstant.get()));
          }
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

    // clear writeStatuses and wait for next checkpoint
    writeResults.clear();
  }

  @Override
  public void invoke(List<WriteStatus> value, Context context) throws Exception {
    writeResults.addAll(value);
  }
}
