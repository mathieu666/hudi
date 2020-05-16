package org.apache.hudi.context;

import org.apache.hudi.common.config.SerializableConfiguration;

public interface HoodieEngineContext<C> {
  C getConext();

  SerializableConfiguration getHadoopConf();
}
