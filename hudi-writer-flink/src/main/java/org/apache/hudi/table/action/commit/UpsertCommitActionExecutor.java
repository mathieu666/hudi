package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.context.HoodieEngineContext;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.table.HoodieTable;

public abstract class UpsertCommitActionExecutor<T extends HoodieRecordPayload<T>, I extends HoodieWriteInput, K extends HoodieWriteKey, O extends HoodieWriteOutput> extends CommitActionExecutor<T,
    I, K, O> {
  public UpsertCommitActionExecutor(HoodieEngineContext<I, O> context, HoodieWriteConfig config, HoodieTable<T, I, K, O> table, String instantTime) {
    super(context, config, table, instantTime, WriteOperationType.UPSERT);
  }

  public abstract I combineOnCondition(
      boolean condition, I records, int parallelism, HoodieTable<T, I, K, O> table);


  public abstract I tag(I dedupedRecords, HoodieEngineContext<I, O> context, HoodieTable<T, I, K, O> table);
}
