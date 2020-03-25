package org.apache.hudi.writer.client;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.table.HoodieTable;
import org.apache.hudi.writer.utils.ClientUtils;

import java.io.Serializable;

/**
 * Abstract class taking care of holding common member variables (FileSystem, SparkContext, HoodieConfigs) Also, manages
 * embedded timeline-server if enabled.
 */
public class AbstractHoodieClient implements Serializable {
  protected HoodieTable table;
  protected final transient FileSystem fs;

  protected AbstractHoodieClient(HoodieTable table, FileSystem fs) {
    this.table = table;
    this.fs = fs;
  }

  public HoodieWriteConfig getConfig() {
    return table.getConfig();
  }

  public HoodieTable getTable() {
    return table;
  }

  public void setTable(HoodieTable table) {
    this.table = table;
  }

  protected HoodieTableMetaClient createMetaClient() {
    return  table.getMetaClient();
  }

  protected HoodieTableMetaClient createMetaClient(boolean loadActiveTimelineOnLoad) {
    return ClientUtils.createMetaClient(table.getHadoopConf(), table.getConfig(), loadActiveTimelineOnLoad);
  }
}
