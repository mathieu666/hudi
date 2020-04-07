package org.apache.hudi.writer.index.bloom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.index.HoodieIndex;
import org.apache.hudi.writer.table.HoodieTable;

import java.util.List;

/**
 * Indexing mechanism based on bloom filter. Each parquet file includes its row_key bloom filter in its metadata.
 */
public class HoodieBloomIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {
  public HoodieBloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public List<HoodieKey> fetchRecordLocation(List<HoodieKey> hoodieKeys, Configuration hadoopConf, HoodieTable<T> hoodieTable) {
    return null;
  }

  @Override
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> recordRDD, Configuration hadoopConf, HoodieTable<T> hoodieTable) throws HoodieIndexException {
    return null;
  }

  @Override
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatusRDD, Configuration hadoopConf, HoodieTable<T> hoodieTable) throws HoodieIndexException {
    return null;
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    return false;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }
}
