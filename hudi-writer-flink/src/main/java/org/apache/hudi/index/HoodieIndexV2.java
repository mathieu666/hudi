package org.apache.hudi.index;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.context.HoodieEngineContext;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.format.HoodieWriteInput;
import org.apache.hudi.format.HoodieWriteKey;
import org.apache.hudi.format.HoodieWriteOutput;
import org.apache.hudi.index.hbase.HBaseIndex;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;

public abstract class HoodieIndexV2<T extends HoodieRecordPayload, I extends HoodieWriteInput, K extends HoodieWriteKey, O extends HoodieWriteOutput> implements Serializable {

  protected final HoodieWriteConfig config;

  protected HoodieIndexV2(HoodieWriteConfig config) {
    this.config = config;
  }

  public static <T extends HoodieRecordPayload> HoodieIndexV2 createIndex(HoodieWriteConfig config) throws HoodieIndexException {
    switch (config.getIndexType()) {
      case HBASE:
        return new HBaseIndex<>(config);
//      case INMEMORY:
//        return new InMemoryHashIndex<>(config);
//      case BLOOM:
//        return new HoodieBloomIndex<>(config);
//      case GLOBAL_BLOOM:
//        return new HoodieGlobalBloomIndex<>(config);
      default:
        throw new HoodieIndexException("Index type unspecified, set " + config.getIndexType());
    }
  }

  public abstract I fetchRecordLocation(I inputs, final HoodieEngineContext context, HoodieTable<T, I, K, O> hoodieTable);

  public abstract I tagLocation(I inputs, HoodieEngineContext<I, O> context, HoodieTable table);

  public abstract O updateLocation(O inputs, HoodieEngineContext context, HoodieTable table);

  /**
   * Rollback the efffects of the commit made at commitTime.
   */
  public abstract boolean rollbackCommit(String commitTime);

  /**
   * An index is `global` if {@link HoodieKey} to fileID mapping, does not depend on the `partitionPath`. Such an
   * implementation is able to obtain the same mapping, for two hoodie keys with same `recordKey` but different
   * `partitionPath`
   *
   * @return whether or not, the index implementation is global in nature
   */
  public abstract boolean isGlobal();

  /**
   * This is used by storage to determine, if its safe to send inserts, straight to the log, i.e having a
   * {@link FileSlice}, with no data file.
   *
   * @return Returns true/false depending on whether the impl has this capability
   */
  public abstract boolean canIndexLogFiles();

  /**
   * An index is "implicit" with respect to storage, if just writing new data to a file slice, updates the index as
   * well. This is used by storage, to save memory footprint in certain cases.
   */
  public abstract boolean isImplicitWithStorage();

  /**
   * Each index type should implement it's own logic to release any resources acquired during the process.
   */
  public void close() {
  }

  public enum IndexType {
    HBASE, INMEMORY, BLOOM, GLOBAL_BLOOM
  }
}
