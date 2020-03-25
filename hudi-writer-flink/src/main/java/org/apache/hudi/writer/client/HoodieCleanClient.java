package org.apache.hudi.writer.client;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.AvroUtils;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.writer.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * @author xianghu.wang
 * @time 2020/4/2
 * @description
 */
public class HoodieCleanClient extends AbstractHoodieClient {
  private static final Logger LOG = LogManager.getLogger(HoodieCleanClient.class);
  protected HoodieCleanClient(HoodieTable table, FileSystem fs) {
    super(table, fs);
  }

  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   *
   * @param startCleanTime Cleaner Instant Timestamp
   * @throws HoodieIOException in case of any IOException
   */
  public HoodieCleanMetadata clean(String startCleanTime) throws HoodieIOException {

    // If there are inflight(failed) or previously requested clean operation, first perform them
    table.getCleanTimeline().filterInflightsAndRequested().getInstants().forEach(hoodieInstant -> {
      LOG.info("There were previously unfinished cleaner operations. Finishing Instant=" + hoodieInstant);
      runClean(hoodieInstant);
    });

    Option<HoodieCleanerPlan> cleanerPlanOpt = scheduleClean(startCleanTime);

    if (cleanerPlanOpt.isPresent()) {
      HoodieCleanerPlan cleanerPlan = cleanerPlanOpt.get();
      if ((cleanerPlan.getFilesToBeDeletedPerPartition() != null)
          && !cleanerPlan.getFilesToBeDeletedPerPartition().isEmpty()) {
        return runClean(HoodieTimeline.getCleanRequestedInstant(startCleanTime), cleanerPlan);
      }
    }
    return null;
  }

  /**
   * Executes the Cleaner plan stored in the instant metadata.
   *
   * @param cleanInstant Cleaner Instant
   */
  public HoodieCleanMetadata runClean(HoodieInstant cleanInstant) {
    try {
      HoodieCleanerPlan cleanerPlan = CleanerUtils.getCleanerPlan(table.getMetaClient(), cleanInstant);
      return runClean(cleanInstant, cleanerPlan);
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
  }

  public HoodieCleanMetadata runClean(HoodieInstant cleanInstant,
                                       HoodieCleanerPlan cleanerPlan) {
    Preconditions.checkArgument(
        cleanInstant.getState().equals(HoodieInstant.State.REQUESTED) || cleanInstant.getState().equals(HoodieInstant.State.INFLIGHT));

    try {
      LOG.info("Cleaner started");

      if (!cleanInstant.isInflight()) {
        // Mark as inflight first
        cleanInstant = table.getActiveTimeline().transitionCleanRequestedToInflight(cleanInstant,
            AvroUtils.serializeCleanerPlan(cleanerPlan));
      }

      List<HoodieCleanStat> cleanStats = table.clean(cleanInstant, cleanerPlan);

      if (cleanStats.isEmpty()) {
        return HoodieCleanMetadata.newBuilder().build();
      }

      // Emit metrics (duration, numFilesDeleted) if needed
      Option<Long> durationInMs = Option.empty();

      HoodieTableMetaClient metaClient = createMetaClient();
      // Create the metadata and save it
      HoodieCleanMetadata metadata =
          CleanerUtils.convertCleanMetadata(metaClient, cleanInstant.getTimestamp(), durationInMs, cleanStats);
      LOG.info("Cleaned " + metadata.getTotalFilesDeleted() + " files. Earliest Retained :" + metadata.getEarliestCommitToRetain());

      table.getActiveTimeline().transitionCleanInflightToComplete(
          new HoodieInstant(true, HoodieTimeline.CLEAN_ACTION, cleanInstant.getTimestamp()),
          AvroUtils.serializeCleanMetadata(metadata));
      LOG.info("Marked clean started on " + cleanInstant.getTimestamp() + " as complete");
      return metadata;
    } catch (IOException e) {
      throw new HoodieIOException("Failed to clean up after commit", e);
    }
  }

  /**
   * Creates a Cleaner plan if there are files to be cleaned and stores them in instant file.
   *
   * @param startCleanTime Cleaner Instant Time
   * @return Cleaner Plan if generated
   */
  protected Option<HoodieCleanerPlan> scheduleClean(String startCleanTime) {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieCleanerPlan cleanerPlan = table.scheduleClean();

    if ((cleanerPlan.getFilesToBeDeletedPerPartition() != null)
        && !cleanerPlan.getFilesToBeDeletedPerPartition().isEmpty()) {

      HoodieInstant cleanInstant = new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.CLEAN_ACTION, startCleanTime);
      // Save to both aux and timeline folder
      try {
        table.getActiveTimeline().saveToCleanRequested(cleanInstant, AvroUtils.serializeCleanerPlan(cleanerPlan));
        LOG.info("Requesting Cleaning with instant time " + cleanInstant);
      } catch (IOException e) {
        LOG.error("Got exception when saving cleaner requested file", e);
        throw new HoodieIOException(e.getMessage(), e);
      }
      return Option.of(cleanerPlan);
    }
    return Option.empty();
  }
}
