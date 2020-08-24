package org.apache.hudi.table;

import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class HoodieSparkTimelineArchiveLog extends BaseHoodieTimelineArchiveLog {

  private static final Logger LOG = LogManager.getLogger(HoodieSparkTimelineArchiveLog.class);

  public HoodieSparkTimelineArchiveLog(HoodieWriteConfig config, HoodieEngineContext context) {
    super(config, context);
  }

  @Override
  protected HoodieTable createTable(HoodieWriteConfig config, HoodieEngineContext context) {
    return HoodieSparkTable.create(config, context);
  }

  @Override
  protected void deleteAnyLeftOverMarkerFiles(HoodieEngineContext context, HoodieInstant instant) {
    BaseMarkerFiles markerFiles = new SparkMarkerFiles(table, instant.getTimestamp());
    if (markerFiles.deleteMarkerDir(context, config.getMarkersDeleteParallelism())) {
      LOG.info("Cleaned up left over marker directory for instant :" + instant);
    }
  }
}
