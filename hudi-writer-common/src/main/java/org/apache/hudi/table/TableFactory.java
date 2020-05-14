package org.apache.hudi.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.EngineType;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;

/**
 * Class help to create HoodieTable.
 */
public class TableFactory {

  public static <T extends HoodieRecordPayload,
      INPUT extends HoodieWriteInput,
      KEY extends HoodieWriteKey,
      OUTPUT extends HoodieWriteOutput> HoodieTable<T, INPUT, KEY, OUTPUT> createCopyOnWriteTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    EngineType engineType = EngineType.valueOf(config.getEngineType());
    switch (engineType) {
      case SPARK:
        // TODO
        return null;
      case JAVA:
        // TODO
        return null;
      case FLINK:
        return new HoodieFlinkCopyOnWriteTable(config, hadoopConf, metaClient);
      default:
        throw new HoodieException("Unsupported engine type :" + config.getEngineType());
    }
  }

  public static <T extends HoodieRecordPayload,
      INPUT extends HoodieWriteInput,
      KEY extends HoodieWriteKey,
      OUTPUT extends HoodieWriteOutput> HoodieTable<T, INPUT, KEY, OUTPUT> createMergeOnReadTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    EngineType engineType = EngineType.valueOf(config.getEngineType());
    switch (engineType) {
      case SPARK:
        // TODO
        return null;
      case JAVA:
        // TODO
        return null;
      case FLINK:
        return new HoodieFlinkMergeOnReadTable(config, hadoopConf, metaClient);
      default:
        throw new HoodieException("Unsupported engine type :" + config.getEngineType());
    }
  }
}
