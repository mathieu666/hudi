package org.apache.hudi.writer.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.writer.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.utils.ClientUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;

/**
 * Abstract class taking care of holding common member variables (FileSystem, SparkContext, HoodieConfigs) Also, manages
 * embedded timeline-server if enabled.
 */
public class AbstractHoodieClient implements Serializable, AutoCloseable {

  private static final Logger LOG = LogManager.getLogger(AbstractHoodieClient.class);

  protected final transient FileSystem fs;
  protected final transient Configuration hadoopConf;
  protected final HoodieWriteConfig config;
  protected final String basePath;

  /**
   * Timeline Server has the same lifetime as that of Client. Any operations done on the same timeline service will be
   * able to take advantage of the cached file-system view. New completed actions will be synced automatically in an
   * incremental fashion.
   */
  private transient Option<EmbeddedTimelineService> timelineServer;
  private final boolean shouldStopTimelineServer;

  protected AbstractHoodieClient(Configuration hadoopConf, HoodieWriteConfig clientConfig) {
    this(hadoopConf, clientConfig, Option.empty());
  }

  protected AbstractHoodieClient(Configuration hadoopConf, HoodieWriteConfig clientConfig,
                                 Option<EmbeddedTimelineService> timelineServer) {
    this.fs = FSUtils.getFs(clientConfig.getBasePath(), hadoopConf);
    this.hadoopConf = hadoopConf;
    this.basePath = clientConfig.getBasePath();
    this.config = clientConfig;
    this.timelineServer = timelineServer;
    shouldStopTimelineServer = !timelineServer.isPresent();
    startEmbeddedServerView();
  }

  private synchronized void startEmbeddedServerView() {
    if (config.isEmbeddedTimelineServerEnabled()) {
      if (!timelineServer.isPresent()) {
        // Run Embedded Timeline Server
        LOG.info("Starting Timeline service !!");
        timelineServer = Option.of(new EmbeddedTimelineService(hadoopConf,
            config.getClientSpecifiedViewStorageConfig()));
        try {
          timelineServer.get().startServer();
          // Allow executor to find this newly instantiated timeline service
          config.setViewStorageConfig(timelineServer.get().getRemoteFileSystemViewConfig());
        } catch (IOException e) {
          LOG.warn("Unable to start timeline service. Proceeding as if embedded server is disabled", e);
          stopEmbeddedServerView(false);
        }
      } else {
        LOG.info("Timeline Server already running. Not restarting the service");
      }
    } else {
      LOG.info("Embedded Timeline Server is disabled. Not starting timeline service");
    }
  }

  /**
   * Releases any resources used by the client.
   */
  @Override
  public void close() {
    stopEmbeddedServerView(true);
  }

  private synchronized void stopEmbeddedServerView(boolean resetViewStorageConfig) {
    if (timelineServer.isPresent() && shouldStopTimelineServer) {
      // Stop only if owner
      LOG.info("Stopping Timeline service !!");
      timelineServer.get().stop();
    }

    timelineServer = Option.empty();
    // Reset Storage Config to Client specified config
    if (resetViewStorageConfig) {
      config.resetViewStorageConfig();
    }
  }

  public HoodieWriteConfig getConfig() {
    return config;
  }

  protected HoodieTableMetaClient createMetaClient(boolean loadActiveTimelineOnLoad) {
    return ClientUtils.createMetaClient(hadoopConf, config, loadActiveTimelineOnLoad);
  }
}
