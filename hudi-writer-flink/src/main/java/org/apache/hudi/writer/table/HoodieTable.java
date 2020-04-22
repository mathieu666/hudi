package org.apache.hudi.writer.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.client.SparkTaskContextSupplier;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.exception.HoodieSavepointException;
import org.apache.hudi.writer.index.HoodieIndex;
import org.apache.hudi.writer.table.commit.HoodieWriteMetadata;
import org.apache.hudi.writer.table.partitioner.Partitioner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstract implementation of a HoodieTable.
 */
public abstract class HoodieTable<T extends HoodieRecordPayload> implements Serializable {
  private static final Logger LOG = LogManager.getLogger(HoodieTable.class);

  protected final HoodieWriteConfig config;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieIndex<T> index;

  private SerializableConfiguration hadoopConfiguration;
  private transient FileSystemViewManager viewManager;
  protected final SparkTaskContextSupplier sparkTaskContextSupplier = new SparkTaskContextSupplier();

  protected HoodieTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    this.config = config;
    this.hadoopConfiguration = new SerializableConfiguration(hadoopConf);
    this.viewManager = FileSystemViewManager.createViewManager(new SerializableConfiguration(hadoopConf),
        config.getViewStorageConfig());
    this.metaClient = metaClient;
    this.index = HoodieIndex.createIndex(config);
  }

  public static <T extends HoodieRecordPayload> HoodieTable<T> create(HoodieWriteConfig config, Configuration hadoopConf) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(
        hadoopConf,
        config.getBasePath(),
        true,
        config.getConsistencyGuardConfig(),
        Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))
    );
    return HoodieTable.create(metaClient, config, hadoopConf);
  }

  public static <T extends HoodieRecordPayload> HoodieTable<T> create(HoodieTableMetaClient metaClient,
                                                                      HoodieWriteConfig config,
                                                                      Configuration hadoopConf) {
    switch (metaClient.getTableType()) {
      case COPY_ON_WRITE:
        return new HoodieCopyOnWriteTable<>(config, hadoopConf, metaClient);
      case MERGE_ON_READ:
        return new HoodieMergeOnReadTable<>(config, hadoopConf, metaClient);
      default:
        throw new HoodieException("Unsupported table type :" + metaClient.getTableType());
    }
  }

  /**
   * Finalize the written data onto storage. Perform any final cleanups.
   *
   * @param hadoopConf hadoopConf
   * @param stats      List of HoodieWriteStats
   * @throws HoodieIOException if some paths can't be finalized on storage
   */
  public void finalizeWrite(Configuration hadoopConf, String instantTs, List<HoodieWriteStat> stats)
      throws HoodieIOException {
    cleanFailedWrites(hadoopConf, instantTs, stats, config.getConsistencyGuardConfig().isConsistencyCheckEnabled());
  }

  /**
   * Reconciles WriteStats and marker files to detect and safely delete duplicate data files created because of Spark
   * retries.
   *
   * @param jsc                     Spark Context
   * @param instantTs               Instant Timestamp
   * @param stats                   Hoodie Write Stat
   * @param consistencyCheckEnabled Consistency Check Enabled
   * @throws HoodieIOException
   */
  protected void cleanFailedWrites(Configuration jsc, String instantTs, List<HoodieWriteStat> stats,
                                   boolean consistencyCheckEnabled) throws HoodieIOException {
    try {
      // Reconcile marker and data files with WriteStats so that partially written data-files due to failed
      // (but succeeded on retry) tasks are removed.
      String basePath = getMetaClient().getBasePath();
      FileSystem fs = getMetaClient().getFs();
      Path markerDir = new Path(metaClient.getMarkerFolderPath(instantTs));

      if (!fs.exists(markerDir)) {
        // Happens when all writes are appends
        return;
      }

      List<String> invalidDataPaths = FSUtils.getAllDataFilesForMarkers(fs, basePath, instantTs, markerDir.toString());
      List<String> validDataPaths = stats.stream().map(w -> String.format("%s/%s", basePath, w.getPath()))
          .filter(p -> p.endsWith(".parquet")).collect(Collectors.toList());
      // Contains list of partially created files. These needs to be cleaned up.
      invalidDataPaths.removeAll(validDataPaths);
      if (!invalidDataPaths.isEmpty()) {
        LOG.info(
            "Removing duplicate data files created due to spark retries before committing. Paths=" + invalidDataPaths);
      }

      Map<String, List<Pair<String, String>>> groupByPartition = invalidDataPaths.stream()
          .map(dp -> Pair.of(new Path(dp).getParent().toString(), dp)).collect(Collectors.groupingBy(Pair::getKey));

      if (!groupByPartition.isEmpty()) {
        // Ensure all files in delete list is actually present. This is mandatory for an eventually consistent FS.
        // Otherwise, we may miss deleting such files. If files are not found even after retries, fail the commit
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are present.
          waitForAllFiles(jsc, groupByPartition, ConsistencyGuard.FileVisibility.APPEAR);
        }

        // Now delete partially written files
        new ArrayList<>(groupByPartition.values())
            .stream()
            .map(partitionWithFileList -> {
              final FileSystem fileSystem = metaClient.getFs();
              LOG.info("Deleting invalid data files=" + partitionWithFileList);
              if (partitionWithFileList.isEmpty()) {
                return true;
              }
              // Delete
              partitionWithFileList.stream().map(Pair::getValue).forEach(file -> {
                try {
                  fileSystem.delete(new Path(file), false);
                } catch (IOException e) {
                  throw new HoodieIOException(e.getMessage(), e);
                }
              });

              return true;
            });

        // Now ensure the deleted files disappear
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are absent.
          waitForAllFiles(jsc, groupByPartition, ConsistencyGuard.FileVisibility.DISAPPEAR);
        }
      }
      // Now delete the marker directory
      deleteMarkerDir(instantTs);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }


  /**
   * Delete Marker directory corresponding to an instant.
   *
   * @param instantTs Instant Time
   */
  protected void deleteMarkerDir(String instantTs) {
    try {
      FileSystem fs = getMetaClient().getFs();
      Path markerDir = new Path(metaClient.getMarkerFolderPath(instantTs));
      if (fs.exists(markerDir)) {
        // For append only case, we do not write to marker dir. Hence, the above check
        LOG.info("Removing marker directory=" + markerDir);
        fs.delete(markerDir, true);
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Ensures all files passed either appear or disappear.
   *
   * @param hadoopConf       hadoopConf
   * @param groupByPartition Files grouped by partition
   * @param visibility       Appear/Disappear
   */
  private void waitForAllFiles(Configuration hadoopConf, Map<String, List<Pair<String, String>>> groupByPartition,
                               ConsistencyGuard.FileVisibility visibility) {
    // This will either ensure all files to be deleted are present.
    boolean checkPassed = new ArrayList<>(groupByPartition.entrySet())
        .stream()
        .map(partitionWithFileList -> waitForCondition(partitionWithFileList.getKey(),
            partitionWithFileList.getValue().stream(), visibility))
        .allMatch(x -> x);
    if (!checkPassed) {
      throw new HoodieIOException("Consistency check failed to ensure all files " + visibility);
    }
  }

  private synchronized FileSystemViewManager getViewManager() {
    if (null == viewManager) {
      viewManager = FileSystemViewManager.createViewManager(hadoopConfiguration, config.getViewStorageConfig());
    }
    return viewManager;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  public HoodieActiveTimeline getActiveTimeline() {
    return metaClient.getActiveTimeline();
  }

  /**
   * Return the index.
   */
  public HoodieIndex<T> getIndex() {
    return index;
  }

  /**
   * Get only the completed (no-inflights) savepoint timeline.
   */
  public HoodieTimeline getCompletedSavepointTimeline() {
    return getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
  }

  /**
   * Get only the completed (no-inflights) commit + deltacommit timeline.
   */
  public HoodieTimeline getCompletedCommitsTimeline() {
    return metaClient.getCommitsTimeline().filterCompletedInstants();
  }

  /**
   * Get the base file only view of the file system for this table.
   */
  public TableFileSystemView.BaseFileOnlyView getBaseFileOnlyView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  public Configuration getHadoopConf() {
    return metaClient.getHadoopConf();
  }

  /**
   * Run Compaction on the table. Compaction arranges the data so that it is optimized for data access.
   *
   * @param hadoopConf            Spark Context
   * @param compactionInstantTime Instant Time
   * @param compactionPlan        Compaction Plan
   */
  public abstract List<WriteStatus> compact(Configuration hadoopConf, String compactionInstantTime,
                                            HoodieCompactionPlan compactionPlan);

  /**
   * Rollback the (inflight/committed) record changes with the given commit time. Four steps: (1) Atomically unpublish
   * this commit (2) clean indexing data (3) clean new generated parquet files / log blocks (4) Finally, delete
   * .<action>.commit or .<action>.inflight file if deleteInstants = true
   */
  public abstract List<HoodieRollbackStat> rollback(Configuration hadoopConf, HoodieInstant instant, boolean deleteInstants)
      throws IOException;

  /**
   * Executes a new clean action.
   *
   * @return information on cleaned file slices
   */
  public abstract HoodieCleanMetadata clean(Configuration hadoopConf, String cleanInstantTime);

  /**
   * Get complete view of the file system for this table with ability to force sync.
   */
  public SyncableFileSystemView getHoodieView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get only the completed (no-inflights) commit timeline.
   */
  public HoodieTimeline getCompletedCommitTimeline() {
    return metaClient.getCommitTimeline().filterCompletedInstants();
  }

  /**
   * Get the full view of the file system for this table.
   */
  public TableFileSystemView.SliceView getSliceView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get clean timeline.
   */
  public HoodieTimeline getCleanTimeline() {
    return getActiveTimeline().getCleanerTimeline();
  }

  /**
   * Get the list of savepoints in this table.
   */
  public List<String> getSavepoints() {
    return getCompletedSavepointTimeline().getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
  }

  /**
   * Get only the inflights (no-completed) commit timeline.
   */
  public HoodieTimeline getPendingCommitTimeline() {
    return metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
  }

  /**
   * Get the list of data file names savepointed.
   */
  public Stream<String> getSavepointedDataFiles(String savepointTime) {
    if (!getSavepoints().contains(savepointTime)) {
      throw new HoodieSavepointException(
          "Could not get data files for savepoint " + savepointTime + ". No such savepoint.");
    }
    HoodieInstant instant = new HoodieInstant(false, HoodieTimeline.SAVEPOINT_ACTION, savepointTime);
    HoodieSavepointMetadata metadata;
    try {
      metadata = TimelineMetadataUtils.deserializeHoodieSavepointMetadata(getActiveTimeline().getInstantDetails(instant).get());
    } catch (IOException e) {
      throw new HoodieSavepointException("Could not get savepointed data files for savepoint " + savepointTime, e);
    }
    return metadata.getPartitionMetadata().values().stream().flatMap(s -> s.getSavepointDataFile().stream());
  }

  /**
   * Provides a partitioner to perform the upsert operation, based on the workload profile.
   */
  public abstract Partitioner getUpsertPartitioner(WorkloadProfile profile, Configuration hadoopConf);

  /**
   * Provides a partitioner to perform the insert operation, based on the workload profile.
   */
  public abstract Partitioner getInsertPartitioner(WorkloadProfile profile, Configuration hadoopConf);

  /**
   * Perform the ultimate IO for a given upserted (RDD) partition.
   */
  public abstract Iterator<List<WriteStatus>> handleUpsertPartition(String instantTime, Integer partition,
                                                                    Iterator<HoodieRecord<T>> recordIterator, Partitioner partitioner);

  /**
   * Perform the ultimate IO for a given inserted (RDD) partition.
   */
  public abstract Iterator<List<WriteStatus>> handleInsertPartition(String instantTime, Integer partition,
                                                                    Iterator<HoodieRecord<T>> recordIterator, Partitioner partitioner);

  /**
   * Return whether this HoodieTable implementation can benefit from workload profiling.
   */
  public abstract boolean isWorkloadProfileNeeded();

  /**
   * Schedule compaction for the instant time.
   *
   * @param hadoopConf  hadoopConf
   * @param instantTime Instant Time for scheduling compaction
   * @return
   */
  public abstract HoodieCompactionPlan scheduleCompaction(Configuration hadoopConf, String instantTime);

  private boolean waitForCondition(String partitionPath, Stream<Pair<String, String>> partitionFilePaths,
                                   ConsistencyGuard.FileVisibility visibility) {
    final FileSystem fileSystem = metaClient.getRawFs();
    List<String> fileList = partitionFilePaths.map(Pair::getValue).collect(Collectors.toList());
    try {
      getFailSafeConsistencyGuard(fileSystem).waitTill(partitionPath, fileList, visibility);
    } catch (IOException | TimeoutException ioe) {
      LOG.error("Got exception while waiting for files to show up", ioe);
      return false;
    }
    return true;
  }

  private ConsistencyGuard getFailSafeConsistencyGuard(FileSystem fileSystem) {
    return new FailSafeConsistencyGuard(fileSystem, config.getConsistencyGuardConfig());
  }
}
