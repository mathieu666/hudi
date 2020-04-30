package org.apache.hudi;

import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

public class ActionContext {

  private final transient HoodieEngineContext context;
  private final HoodieWriteConfig config;
  private final HoodieTable table;
  private final String instantTime;

  public ActionContext(HoodieEngineContext context, HoodieWriteConfig config,
      HoodieTable table, String instantTime) {
    this.context = context;
    this.config = config;
    this.table = table;
    this.instantTime = instantTime;
  }

  public HoodieEngineContext getJsc() {
    return context;
  }

  public HoodieWriteConfig getConfig() {
    return config;
  }

  public HoodieTable getTable() {
    return table;
  }

  public String getInstantTime() {
    return instantTime;
  }
}
