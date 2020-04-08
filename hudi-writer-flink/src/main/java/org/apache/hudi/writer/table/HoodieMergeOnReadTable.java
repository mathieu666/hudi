package org.apache.hudi.writer.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.writer.config.HoodieWriteConfig;

public class HoodieMergeOnReadTable <T extends HoodieRecordPayload> extends HoodieCopyOnWriteTable<T> {

  public HoodieMergeOnReadTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    super(config, hadoopConf, metaClient);
  }
}
