package org.apache.hudi.writer.index.bloom;

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.writer.config.HoodieWriteConfig;

/**
 * This filter will only work with hoodie table since it will only load partitions with .hoodie_partition_metadata
 * file in it.
 */
public class HoodieGlobalBloomIndex <T extends HoodieRecordPayload> extends HoodieBloomIndex<T> {
  public HoodieGlobalBloomIndex(HoodieWriteConfig config) {
    super(config);
  }
}
