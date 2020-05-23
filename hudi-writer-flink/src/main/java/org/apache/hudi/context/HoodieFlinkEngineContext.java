package org.apache.hudi.context;

import org.apache.hudi.common.config.SerializableConfiguration;

public class HoodieFlinkEngineContext implements HoodieEngineContext<Object> {
  private SerializableConfiguration hadoopConf;
  public HoodieFlinkEngineContext(SerializableConfiguration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  @Override
  public Object getConext() {
    return null;
  }

  @Override
  public SerializableConfiguration getHadoopConf() {
    return this.hadoopConf;
  }

}
