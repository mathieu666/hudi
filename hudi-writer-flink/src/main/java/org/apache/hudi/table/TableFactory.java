package org.apache.hudi.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.EngineType;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.context.HoodieEngineContext;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.format.HoodieWriteInput;
import org.apache.hudi.format.HoodieWriteKey;
import org.apache.hudi.format.HoodieWriteOutput;

/**
 * Class help to create HoodieTable.
 */
public class TableFactory {

  public static <T extends HoodieRecordPayload,
      I extends HoodieWriteInput,
      K extends HoodieWriteKey,
      O extends HoodieWriteOutput> HoodieTable<T, I, K, O> createCopyOnWriteTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient, HoodieEngineContext context) {
    EngineType engineType = EngineType.valueOf(config.getEngineType());
    switch (engineType) {
      case SPARK:
        // TODO
        return null;
      case JAVA:
        // TODO
        return null;
      case FLINK:
        return new HoodieFlinkCopyOnWriteTable(config, metaClient, context);
      default:
        throw new HoodieException("Unsupported engine type :" + config.getEngineType());
    }
  }

  public static <T extends HoodieRecordPayload,
      I extends HoodieWriteInput,
      K extends HoodieWriteKey,
      O extends HoodieWriteOutput> HoodieTable<T, I, K, O> createMergeOnReadTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient, HoodieEngineContext context) {
    EngineType engineType = EngineType.valueOf(config.getEngineType());
    switch (engineType) {
      case SPARK:
        // TODO
        return null;
      case JAVA:
        // TODO
        return null;
      case FLINK:
        return new HoodieFlinkMergeOnReadTable(config, metaClient, context);
      default:
        throw new HoodieException("Unsupported engine type :" + config.getEngineType());
    }
  }
}
