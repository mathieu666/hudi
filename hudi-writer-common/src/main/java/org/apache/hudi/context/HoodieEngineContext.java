package org.apache.hudi.context;

import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.format.HoodieWriteInput;
import org.apache.hudi.format.HoodieWriteOutput;

public interface HoodieEngineContext<I extends HoodieWriteInput, O extends HoodieWriteOutput> {
  SerializableConfiguration getHadoopConf();
}
