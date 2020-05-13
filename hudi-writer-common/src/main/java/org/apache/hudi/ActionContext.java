package org.apache.hudi;

import org.apache.hudi.config.HoodieWriteConfig;

public class ActionContext {

  private final transient HoodieEngineContext context;
  private final HoodieWriteConfig config;
  private final HoodieTableV2 table;
  private final String instantTime;

  public ActionContext(HoodieEngineContext context, HoodieWriteConfig config,
                       HoodieTableV2 table, String instantTime) {
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

  public HoodieTableV2 getTable() {
    return table;
  }

  public String getInstantTime() {
    return instantTime;
  }
}
