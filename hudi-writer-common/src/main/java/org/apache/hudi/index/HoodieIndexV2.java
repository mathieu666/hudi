package org.apache.hudi.index;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.table.HoodieTable;

public interface HoodieIndexV2<IN, OUT> {
  IN fetchRecordLocation(IN inputs);

  IN tagLocation(IN inputs, HoodieTable table);

  OUT updateLocation(OUT inputs);

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
  public void close();

  public enum IndexType {
    HBASE, INMEMORY, BLOOM, GLOBAL_BLOOM
  }
}
