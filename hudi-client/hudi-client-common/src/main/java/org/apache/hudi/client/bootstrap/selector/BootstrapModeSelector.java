package org.apache.hudi.client.bootstrap.selector;

import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Pluggable Partition Selector for selecting partitions to perform full or metadata-only bootstrapping.
 */
public abstract class BootstrapModeSelector implements Serializable {
  protected final HoodieWriteConfig writeConfig;

  public BootstrapModeSelector(HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
  }

  /**
   * Classify partitions for the purpose of bootstrapping. For a non-partitioned source, input list will be one entry.
   *
   * @param partitions List of partitions with files present in each partitions
   * @return a partitions grouped by bootstrap mode
   */
  public abstract Map<BootstrapMode, List<String>> select(List<Pair<String, List<HoodieFileStatus>>> partitions);
}
