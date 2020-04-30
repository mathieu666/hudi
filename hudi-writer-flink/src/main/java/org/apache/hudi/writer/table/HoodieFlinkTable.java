package org.apache.hudi.writer.table;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.hudi.HoodieEngineContext;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;

public abstract class HoodieFlinkTable<T extends HoodieRecordPayload>
    extends HoodieTable<T, HoodieWriteInput<DataStream<HoodieRecord<T>>>, HoodieWriteKey<DataStream<HoodieKey>>, HoodieWriteOutput<DataStream<WriteStatus>>> {

  public HoodieFlinkTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient, HoodieEngineContext context) {
    super(config, metaClient, context);
  }

}
