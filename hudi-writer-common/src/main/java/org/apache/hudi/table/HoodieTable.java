/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.HoodieWriteMetadata;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.avro.model.HoodieSavepointMetadata;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.ConsistencyGuard;
import org.apache.hudi.common.fs.ConsistencyGuard.FileVisibility;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.fs.FailSafeConsistencyGuard;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.table.view.SyncableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView;
import org.apache.hudi.common.table.view.TableFileSystemView.BaseFileOnlyView;
import org.apache.hudi.common.table.view.TableFileSystemView.SliceView;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieSavepointException;
import org.apache.hudi.index.HoodieIndexV2;
import org.apache.hudi.index.hbase.HBaseIndexV2;
import org.apache.hudi.writer.table.HoodieMergeOnReadTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Abstract implementation of a HoodieTable.
 */
public abstract class HoodieTable<T extends HoodieRecordPayload, INPUT extends HoodieWriteInput, KEY extends HoodieWriteKey, OUTPUT extends HoodieWriteOutput> implements Serializable {

  private static final Logger LOG = LogManager.getLogger(HoodieTable.class);

  protected final HoodieWriteConfig config;
  protected final HoodieTableMetaClient metaClient;
  protected final HoodieIndexV2 index;

  private SerializableConfiguration hadoopConfiguration;
  private transient FileSystemViewManager viewManager;

  protected final SparkTaskContextSupplier sparkTaskContextSupplier = new SparkTaskContextSupplier();

  protected HoodieTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    this.config = config;
    this.hadoopConfiguration = new SerializableConfiguration(hadoopConf);
    this.viewManager = FileSystemViewManager.createViewManager(new SerializableConfiguration(hadoopConf),
        config.getViewStorageConfig());
    this.metaClient = metaClient;
    this.index = new HBaseIndexV2(hadoopConf, config);
  }

  public static HoodieTable create(HoodieWriteConfig config, Configuration hadoopConf) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(
        hadoopConf,
        config.getBasePath(),
        true,
        config.getConsistencyGuardConfig(),
        Option.of(new TimelineLayoutVersion(config.getTimelineLayoutVersion()))
    );
    return HoodieTable.create(metaClient, config, hadoopConf);
  }

  public static HoodieTable create(HoodieTableMetaClient metaClient,
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

  private synchronized FileSystemViewManager getViewManager() {
    if (null == viewManager) {
      viewManager = FileSystemViewManager.createViewManager(hadoopConfiguration, config.getViewStorageConfig());
    }
    return viewManager;
  }

  /**
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * @param hadoopConf  Configuration
   * @param instantTime Instant Time for the action
   * @param records     List of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata upsert(Configuration hadoopConf, String instantTime,
                                             INPUT records);

  /**
   * Insert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * @param hadoopConf  Java Spark Context jsc
   * @param instantTime Instant Time for the action
   * @param records     List of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata insert(Configuration hadoopConf, String instantTime,
                                             INPUT records);

  /**
   * Bulk Insert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * @param hadoopConf            Java Spark Context jsc
   * @param instantTime           Instant Time for the action
   * @param records               List of hoodieRecords to upsert
   * @param bulkInsertPartitioner User Defined Partitioner
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata bulkInsert(Configuration hadoopConf, String instantTime,
                                                 INPUT records, Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner);

  /**
   * Deletes a list of {@link HoodieKey}s from the Hoodie table, at the supplied instantTime {@link HoodieKey}s will be
   * de-duped and non existent keys will be removed before deleting.
   *
   * @param hadoopConf  Java Spark Context jsc
   * @param instantTime Instant Time for the action
   * @param keys        {@link List} of {@link HoodieKey}s to be deleted
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata delete(Configuration hadoopConf, String instantTime, KEY keys);

  /**
   * Upserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * @param jsc            Java Spark Context jsc
   * @param instantTime    Instant Time for the action
   * @param preppedRecords List of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata upsertPrepped(Configuration jsc, String instantTime,
                                                    INPUT preppedRecords);

  /**
   * Inserts the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * @param jsc            Java Spark Context jsc
   * @param instantTime    Instant Time for the action
   * @param preppedRecords List of hoodieRecords to upsert
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata insertPrepped(Configuration jsc, String instantTime,
                                                    INPUT preppedRecords);

  /**
   * Bulk Insert the given prepared records into the Hoodie table, at the supplied instantTime.
   * <p>
   * This implementation requires that the input records are already tagged, and de-duped if needed.
   *
   * @param jsc                   Java Spark Context jsc
   * @param instantTime           Instant Time for the action
   * @param preppedRecords        List of hoodieRecords to upsert
   * @param bulkInsertPartitioner User Defined Partitioner
   * @return HoodieWriteMetadata
   */
  public abstract HoodieWriteMetadata bulkInsertPrepped(Configuration jsc, String instantTime,
                                                        INPUT preppedRecords, Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner);

  public HoodieWriteConfig getConfig() {
    return config;
  }

  public HoodieTableMetaClient getMetaClient() {
    return metaClient;
  }

  public Configuration getHadoopConf() {
    return metaClient.getHadoopConf();
  }

  /**
   * Get the view of the file system for this table.
   */
  public TableFileSystemView getFileSystemView() {
    return new HoodieTableFileSystemView(metaClient, getCompletedCommitsTimeline());
  }

  /**
   * Get the base file only view of the file system for this table.
   */
  public BaseFileOnlyView getBaseFileOnlyView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get the full view of the file system for this table.
   */
  public SliceView getSliceView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get complete view of the file system for this table with ability to force sync.
   */
  public SyncableFileSystemView getHoodieView() {
    return getViewManager().getFileSystemView(metaClient.getBasePath());
  }

  /**
   * Get only the completed (no-inflights) commit + deltacommit timeline.
   */
  public HoodieTimeline getCompletedCommitsTimeline() {
    return metaClient.getCommitsTimeline().filterCompletedInstants();
  }

  /**
   * Get only the completed (no-inflights) commit timeline.
   */
  public HoodieTimeline getCompletedCommitTimeline() {
    return metaClient.getCommitTimeline().filterCompletedInstants();
  }

  /**
   * Get only the inflights (no-completed) commit timeline.
   */
  public HoodieTimeline getPendingCommitTimeline() {
    return metaClient.getCommitsTimeline().filterPendingExcludingCompaction();
  }

  /**
   * Get only the completed (no-inflights) clean timeline.
   */
  public HoodieTimeline getCompletedCleanTimeline() {
    return getActiveTimeline().getCleanerTimeline().filterCompletedInstants();
  }

  /**
   * Get clean timeline.
   */
  public HoodieTimeline getCleanTimeline() {
    return getActiveTimeline().getCleanerTimeline();
  }

  /**
   * Get only the completed (no-inflights) savepoint timeline.
   */
  public HoodieTimeline getCompletedSavepointTimeline() {
    return getActiveTimeline().getSavePointTimeline().filterCompletedInstants();
  }

  /**
   * Get the list of savepoints in this table.
   */
  public List<String> getSavepoints() {
    return getCompletedSavepointTimeline().getInstants().map(HoodieInstant::getTimestamp).collect(Collectors.toList());
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

  public HoodieActiveTimeline getActiveTimeline() {
    return metaClient.getActiveTimeline();
  }

  /**
   * Return the index.
   */
  public HoodieIndexV2 getIndex() {
    return index;
  }

  /**
   * Schedule compaction for the instant time.
   *
   * @param jsc         Spark Context
   * @param instantTime Instant Time for scheduling compaction
   * @return
   */
  public abstract HoodieCompactionPlan scheduleCompaction(Configuration jsc, String instantTime);

  /**
   * Run Compaction on the table. Compaction arranges the data so that it is optimized for data access.
   *
   * @param jsc                   Spark Context
   * @param compactionInstantTime Instant Time
   * @param compactionPlan        Compaction Plan
   */
  public abstract OUTPUT compact(Configuration jsc, String compactionInstantTime,
                                 HoodieCompactionPlan compactionPlan);

  /**
   * Executes a new clean action.
   *
   * @return information on cleaned file slices
   */
  public abstract HoodieCleanMetadata clean(Configuration jsc, String cleanInstantTime);

  /**
   * Rollback the (inflight/committed) record changes with the given commit time.
   * <pre>
   *   Three steps:
   *   (1) Atomically unpublish this commit
   *   (2) clean indexing data
   *   (3) clean new generated parquet files.
   *   (4) Finally delete .commit or .inflight file, if deleteInstants = true
   * </pre>
   */
  public abstract HoodieRollbackMetadata rollback(Configuration jsc,
                                                  String rollbackInstantTime,
                                                  HoodieInstant commitInstant,
                                                  boolean deleteInstants);

  /**
   * Restore the table to the given instant. Note that this is a admin table recovery operation
   * that would cause any running queries that are accessing file slices written after the instant to fail.
   */
  public abstract HoodieRestoreMetadata restore(Configuration jsc,
                                                String restoreInstantTime,
                                                String instantToRestore);

  /**
   * Finalize the written data onto storage. Perform any final cleanups.
   *
   * @param jsc   Spark Context
   * @param stats List of HoodieWriteStats
   * @throws HoodieIOException if some paths can't be finalized on storage
   */
  public void finalizeWrite(Configuration jsc, String instantTs, List<HoodieWriteStat> stats) throws HoodieIOException {
    cleanFailedWrites(jsc, instantTs, stats, config.getConsistencyGuardConfig().isConsistencyCheckEnabled());
  }

  /**
   * Delete Marker directory corresponding to an instant.
   *
   * @param instantTs Instant Time
   */
  public void deleteMarkerDir(String instantTs) {
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
          waitForAllFiles(jsc, groupByPartition, FileVisibility.APPEAR);
        }

        // Now delete partially written files
        groupByPartition.values().stream()
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
            }).collect(Collectors.toList());

        // Now ensure the deleted files disappear
        if (consistencyCheckEnabled) {
          // This will either ensure all files to be deleted are absent.
          waitForAllFiles(jsc, groupByPartition, FileVisibility.DISAPPEAR);
        }
      }
      // Now delete the marker directory
      deleteMarkerDir(instantTs);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Ensures all files passed either appear or disappear.
   *
   * @param jsc              Configuration
   * @param groupByPartition Files grouped by partition
   * @param visibility       Appear/Disappear
   */
  private void waitForAllFiles(Configuration jsc, Map<String, List<Pair<String, String>>> groupByPartition, FileVisibility visibility) {
    // This will either ensure all files to be deleted are present.
    boolean checkPassed =
        groupByPartition.entrySet().stream()
            .map(partitionWithFileList -> waitForCondition(partitionWithFileList.getKey(),
                partitionWithFileList.getValue().stream(), visibility))
            .allMatch(x -> x);
    if (!checkPassed) {
      throw new HoodieIOException("Consistency check failed to ensure all files " + visibility);
    }
  }

  private boolean waitForCondition(String partitionPath, Stream<Pair<String, String>> partitionFilePaths, FileVisibility visibility) {
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

  public SparkTaskContextSupplier getSparkTaskContextSupplier() {
    return sparkTaskContextSupplier;
  }
}
