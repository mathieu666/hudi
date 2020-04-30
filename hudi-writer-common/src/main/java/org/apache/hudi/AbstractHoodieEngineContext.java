package org.apache.hudi;

import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.config.SerializableConfiguration;

public abstract class AbstractHoodieEngineContext<INPUT extends HoodieWriteInput, OUTPUT extends HoodieWriteOutput> implements HoodieEngineContext<INPUT, OUTPUT> {
  private SerializableConfiguration hadoopConfiguration;
  @Override
  public SerializableConfiguration getHadoopConfiguration() {
    return hadoopConfiguration;
  }

}
