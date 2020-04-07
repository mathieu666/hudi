package org.apache.hudi.writer.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.writer.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.index.HoodieIndex;

/**
 * HoodieWriteClient.
 */
public class HoodieWriteClient<T extends HoodieRecordPayload> extends AbstractHoodieWriteClient<T> {
  /**
   * Create a write client, with new hudi index.
   *
   * @param hadoopConf HadoopConf
   * @param clientConfig instance of HoodieWriteConfig
   * @param rollbackPending whether need to cleanup pending commits
   */
  public HoodieWriteClient(Configuration hadoopConf, HoodieWriteConfig clientConfig, boolean rollbackPending) {
    this(hadoopConf, clientConfig, rollbackPending, HoodieIndex.createIndex(clientConfig));
  }

  HoodieWriteClient(Configuration hadoopConf, HoodieWriteConfig clientConfig, boolean rollbackPending, HoodieIndex index) {
    this(hadoopConf, clientConfig, rollbackPending, index, Option.empty());
  }

  public HoodieWriteClient(Configuration hadoopConf, HoodieWriteConfig clientConfig, boolean rollbackPending,
                           HoodieIndex index, Option<EmbeddedTimelineService> timelineService) {
  }
}
