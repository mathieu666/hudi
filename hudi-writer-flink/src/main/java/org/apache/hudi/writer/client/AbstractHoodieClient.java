package org.apache.hudi.writer.client;

import java.io.Serializable;

/**
 * Abstract class taking care of holding common member variables (FileSystem, SparkContext, HoodieConfigs) Also, manages
 * embedded timeline-server if enabled.
 */
public class AbstractHoodieClient implements Serializable, AutoCloseable {
  @Override
  public void close() throws Exception {

  }
}
