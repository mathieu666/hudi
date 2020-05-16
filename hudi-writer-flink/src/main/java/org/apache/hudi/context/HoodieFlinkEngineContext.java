package org.apache.hudi.context;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.format.HoodieWriteInput;
import org.apache.hudi.format.HoodieWriteOutput;

import java.util.List;

public class HoodieFlinkEngineContext implements HoodieEngineContext<HoodieWriteInput<List<HoodieRecord>>, HoodieWriteOutput<List<WriteStatus>>> {
  private SerializableConfiguration hadoopConf;
  public HoodieFlinkEngineContext(SerializableConfiguration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  @Override
  public SerializableConfiguration getHadoopConf() {
    return this.hadoopConf;
  }

}
