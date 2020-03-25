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

package org.apache.hudi.writer.table;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.avro.model.HoodieActionInstant;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.HoodieCleanStat;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.SyncableFileSystemView;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.client.ParquetReaderIterator;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.exception.HoodieCompactionException;
import org.apache.hudi.writer.exception.HoodieUpsertException;
import org.apache.hudi.writer.execution.CopyOnWriteLazyInsertIterable;
import org.apache.hudi.writer.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.writer.index.HoodieHBaseIndex;
import org.apache.hudi.writer.io.HoodieAppendHandle;
import org.apache.hudi.writer.io.HoodieMergeHandle;
import org.apache.hudi.writer.model.BucketInfo;
import org.apache.hudi.writer.model.BucketType;
import org.apache.hudi.writer.partitioner.PartitionHelper;
import org.apache.hudi.writer.roolback.RollbackHelper;
import org.apache.hudi.writer.roolback.RollbackRequest;
import org.apache.hudi.writer.utils.ValidationUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Implementation of a more real-time Hoodie Table the provides tradeoffs on read and write cost/amplification.
 *
 * <p>
 * INSERTS - Same as HoodieCopyOnWriteTable - Produce new files, block aligned to desired size (or) Merge with the
 * smallest existing file, to expand it
 * </p>
 * <p>
 * UPDATES - Appends the changes to a rolling log file maintained per file Id. Compaction merges the log file into the
 * base file.
 * </p>
 * <p>
 * WARNING - MOR table type does not support nested rollbacks, every rollback must be followed by an attempted commit
 * action
 * </p>
 */
public class HoodieMergeOnReadTable<T extends HoodieRecordPayload> extends HoodieTable<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieMergeOnReadTable.class);

  public HoodieMergeOnReadTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieHBaseIndex index) {
    super(config, hadoopConf, index);
  }

  public Iterator<List<WriteStatus>> handleInsert(String instantTime, String idPfx, Iterator<HoodieRecord> recordItr)
      throws Exception {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition");
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    return new CopyOnWriteLazyInsertIterable(recordItr, config, instantTime, this, idPfx);
  }

  public Iterator<List<WriteStatus>> handleUpdate(String instantTime, String partitionPath, String fileId,
                                                  Iterator<HoodieRecord> recordItr)
      throws IOException {
    // This is needed since sometimes some buckets are never picked in getPartition() and end up with 0 records
    if (!recordItr.hasNext()) {
      LOG.info("Empty partition with fileId => " + fileId);
      return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
    }
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(instantTime, partitionPath, fileId, recordItr);
    return handleUpdateInternal(upsertHandle, instantTime, fileId);
  }

  public Iterator<List<WriteStatus>> handleUpdate(String commitTime, String fileId, Iterator<HoodieRecord<T>> recordItr)
      throws IOException {
    LOG.info("Merging updates for commit " + commitTime + " for file " + fileId);

    if (!index.canIndexLogFiles() && getSmallFileIds().contains(fileId)) {
      LOG.info("Small file corrections for updates for commit " + commitTime + " for file " + fileId);
      return super.handleUpdate(commitTime, fileId, recordItr);
    } else {
      HoodieAppendHandle<T> appendHandle = new HoodieAppendHandle<>(config, commitTime, this, fileId, recordItr);
      appendHandle.doAppend();
      appendHandle.close();
      return Collections.singletonList(Collections.singletonList(appendHandle.getWriteStatus())).iterator();
    }
  }

  public Iterator<List<WriteStatus>> handleUpdateInternal(HoodieMergeHandle upsertHandle, String instantTime,
                                                          String fileId) throws IOException {
    if (upsertHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException(
          "Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
    } else {
      AvroReadSupport.setAvroReadSchema(getHadoopConf(), upsertHandle.getWriterSchema());
      BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;
      try (ParquetReader<IndexedRecord> reader =
               AvroParquetReader.<IndexedRecord>builder(upsertHandle.getOldFilePath()).withConf(getHadoopConf()).build()) {
        wrapper = new SparkBoundedInMemoryExecutor(getConfig(), new ParquetReaderIterator(reader),
            new UpdateHandler(upsertHandle), x -> x);
        wrapper.execute();
      } catch (Exception e) {
        throw new HoodieException(e);
      } finally {
        upsertHandle.close();
        if (null != wrapper) {
          wrapper.shutdownNow();
        }
      }
    }

    if (upsertHandle.getWriteStatus().getPartitionPath() == null) {
      LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
          + upsertHandle.getWriteStatus());
    }
    return Collections.singletonList(Collections.singletonList(upsertHandle.getWriteStatus())).iterator();
  }

  protected HoodieMergeHandle getUpdateHandle(String instantTime, String partitionPath, String fileId, Iterator<HoodieRecord> recordItr) {
    return new HoodieMergeHandle(getConfig(), instantTime, this, recordItr, partitionPath, fileId);
  }

  @Override
  public Iterator<List<WriteStatus>> handleUpsertPartition(String instantTime, Integer partition,
                                                           Iterator<HoodieRecord> recordItr, PartitionHelper partitioner) {
    BucketInfo binfo = partitioner.getBucketInfo(partition);
    BucketType btype = binfo.getBucketType();

    try {
      if (btype.equals(BucketType.INSERT)) {
        return handleInsert(instantTime, binfo.getFileIdPrefix(), recordItr);
      } else if (btype.equals(BucketType.UPDATE)) {
        return handleUpdate(instantTime, binfo.getPartitionPath(), binfo.getFileIdPrefix(), recordItr);
      } else {
        throw new HoodieUpsertException("Unknown bucketType " + btype + " for partition :" + partition);
      }
    } catch (Throwable t) {
      String msg = "Error upserting bucketType " + btype + " for partition :" + partition;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  @Override
  public List<HoodieCleanStat> clean(HoodieInstant cleanInstant, HoodieCleanerPlan cleanerPlan) {
    List<Tuple2<String, String>> fileToBeCleaned = cleanerPlan.getFilesToBeDeletedPerPartition().entrySet().stream()
        .flatMap(x -> x.getValue().stream()
            .map(y -> new Tuple2<>(x.getKey(), y))).collect(Collectors.toList());

    // Map<String, List<Pair<String, PartitionCleanStat>>> collect =
    Map<String, List<Pair<String, PartitionCleanStat>>> fileToPartitionCleanStat = deleteFilesFunc(fileToBeCleaned)
        .stream()
        .collect(Collectors.groupingBy(Pair::getLeft));

    List<Tuple2<String, PartitionCleanStat>> partitionCleanStats = new ArrayList<>();
    for (Map.Entry<String, List<Pair<String, PartitionCleanStat>>> entry : fileToPartitionCleanStat.entrySet()) {
      Optional<PartitionCleanStat> reducePartitionCleanStat = entry.getValue().stream().map(Pair::getRight).reduce(PartitionCleanStat::merge);
      reducePartitionCleanStat.ifPresent(partitionCleanStat -> partitionCleanStats.add(Tuple2.apply(entry.getKey(), partitionCleanStat)));
    }

    Map<String, PartitionCleanStat> partitionCleanStatsMap =
        partitionCleanStats.stream().collect(Collectors.toMap(Tuple2::_1, Tuple2::_2));

    // Return PartitionCleanStat for each partition passed.
    return cleanerPlan.getFilesToBeDeletedPerPartition().keySet().stream().map(partitionPath -> {
      PartitionCleanStat partitionCleanStat =
          (partitionCleanStatsMap.containsKey(partitionPath)) ? partitionCleanStatsMap.get(partitionPath)
              : new PartitionCleanStat(partitionPath);
      HoodieActionInstant actionInstant = cleanerPlan.getEarliestInstantToRetain();
      return HoodieCleanStat.newBuilder().withPolicy(config.getCleanerPolicy()).withPartitionPath(partitionPath)
          .withEarliestCommitRetained(Option.ofNullable(
              actionInstant != null
                  ? new HoodieInstant(HoodieInstant.State.valueOf(actionInstant.getState()),
                  actionInstant.getAction(), actionInstant.getTimestamp())
                  : null))
          .withDeletePathPattern(partitionCleanStat.deletePathPatterns)
          .withSuccessfulDeletes(partitionCleanStat.successDeleteFiles)
          .withFailedDeletes(partitionCleanStat.failedDeleteFiles).build();
    }).collect(Collectors.toList());
  }

  private List<Pair<String, PartitionCleanStat>> deleteFilesFunc(List<Tuple2<String, String>> fileToBeCleaned) {
    Iterator<Tuple2<String, String>> iter = fileToBeCleaned.iterator();
    Map<String, PartitionCleanStat> partitionCleanStatMap = new HashMap<>();

    FileSystem fs = getMetaClient().getFs();
    Path basePath = new Path(getMetaClient().getBasePath());
    while (iter.hasNext()) {
      Tuple2<String, String> partitionDelFileTuple = iter.next();
      String partitionPath = partitionDelFileTuple._1();
      String delFileName = partitionDelFileTuple._2();
      Path deletePath = FSUtils.getPartitionPath(FSUtils.getPartitionPath(basePath, partitionPath), delFileName);
      String deletePathStr = deletePath.toString();
      Boolean deletedFileResult = null;
      try {
        deletedFileResult = deleteFileAndGetResult(fs, deletePathStr);
      } catch (IOException e) {
        LOG.error("Delete file failed", e);
      }
      if (!partitionCleanStatMap.containsKey(partitionPath)) {
        partitionCleanStatMap.put(partitionPath, new PartitionCleanStat(partitionPath));
      }
      PartitionCleanStat partitionCleanStat = partitionCleanStatMap.get(partitionPath);
      partitionCleanStat.addDeleteFilePatterns(deletePath.getName());
      partitionCleanStat.addDeletedFileResult(deletePath.getName(), deletedFileResult);
    }
    return partitionCleanStatMap.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue()))
        .collect(Collectors.toList());

  }

  private static Boolean deleteFileAndGetResult(FileSystem fs, String deletePathStr) throws IOException {
    Path deletePath = new Path(deletePathStr);
    LOG.debug("Working on delete path :" + deletePath);
    try {
      boolean deleteResult = fs.delete(deletePath, false);
      if (deleteResult) {
        LOG.debug("Cleaned file at path :" + deletePath);
      }
      return deleteResult;
    } catch (FileNotFoundException fio) {
      // With cleanPlan being used for retried cleaning operations, its possible to clean a file twice
      return false;
    }
  }

  @Override
  public HoodieCompactionPlan scheduleCompaction(String instantTime) {
    LOG.info("Checking if compaction needs to be run on " + config.getBasePath());
    Option<HoodieInstant> lastCompaction =
        getActiveTimeline().getCommitTimeline().filterCompletedInstants().lastInstant();
    String deltaCommitsSinceTs = "0";
    if (lastCompaction.isPresent()) {
      deltaCommitsSinceTs = lastCompaction.get().getTimestamp();
    }

    int deltaCommitsSinceLastCompaction = getActiveTimeline().getDeltaCommitTimeline()
        .findInstantsAfter(deltaCommitsSinceTs, Integer.MAX_VALUE).countInstants();
    if (config.getInlineCompactDeltaCommitMax() > deltaCommitsSinceLastCompaction) {
      LOG.info("Not running compaction as only " + deltaCommitsSinceLastCompaction
          + " delta commits was found since last compaction " + deltaCommitsSinceTs + ". Waiting for "
          + config.getInlineCompactDeltaCommitMax());
      return new HoodieCompactionPlan();
    }

    LOG.info("Compacting merge on read table " + config.getBasePath());
    HoodieMergeOnReadTableCompactor compactor = new HoodieMergeOnReadTableCompactor(this);
    try {
      return compactor.generateCompactionPlan(this, config, instantTime,
          ((SyncableFileSystemView) getSliceView()).getPendingCompactionOperations()
              .map(instantTimeCompactionopPair -> instantTimeCompactionopPair.getValue().getFileGroupId())
              .collect(Collectors.toSet()));

    } catch (IOException e) {
      throw new HoodieCompactionException("Could not schedule compaction " + config.getBasePath(), e);
    }
  }

  @Override
  public HoodieCleanerPlan scheduleClean() {
    try {
      CleanHelper cleaner = new CleanHelper(this, config);
      Option<HoodieInstant> earliestInstant = cleaner.getEarliestCommitToRetain();

      List<String> partitionsToClean = cleaner.getPartitionPathsToClean(earliestInstant);

      if (partitionsToClean.isEmpty()) {
        LOG.info("Nothing to clean here. It is already clean");
        return HoodieCleanerPlan.newBuilder().setPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS.name()).build();
      }
      LOG.info(
          "Total Partitions to clean : " + partitionsToClean.size() + ", with policy " + config.getCleanerPolicy());
      int cleanerParallelism = Math.min(partitionsToClean.size(), config.getCleanerParallelism());
      LOG.info("Using cleanerParallelism: " + cleanerParallelism);

      Map<String, List<String>> cleanOps = partitionsToClean.stream().map(partitionPathToClean -> Pair.of(partitionPathToClean, cleaner.getDeletePaths(partitionPathToClean)))
          .collect(Collectors.toMap(Pair::getKey, Pair::getValue));

      return new HoodieCleanerPlan(earliestInstant
          .map(x -> new HoodieActionInstant(x.getTimestamp(), x.getAction(), x.getState().name())).orElse(null),
          config.getCleanerPolicy().name(), cleanOps, 1);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to schedule clean operation", e);
    }
  }

  @Override
  public List<HoodieRollbackStat> rollback(HoodieInstant instant,
                                           boolean deleteInstants) throws IOException {
    long startTime = System.currentTimeMillis();

    String commit = instant.getTimestamp();
    LOG.error("Rolling back instant " + instant);

    // Atomically un-publish all non-inflight commits
    if (instant.isCompleted()) {
      LOG.error("Un-publishing instant " + instant + ", deleteInstants=" + deleteInstants);
      instant = this.getActiveTimeline().revertToInflight(instant);
    }

    List<HoodieRollbackStat> allRollbackStats = new ArrayList<>();

    // At the moment, MOR table type does not support bulk nested rollbacks. Nested rollbacks is an experimental
    // feature that is expensive. To perform nested rollbacks, initiate multiple requests of client.rollback
    // (commitToRollback).
    // NOTE {@link HoodieCompactionConfig#withCompactionLazyBlockReadEnabled} needs to be set to TRUE. This is
    // required to avoid OOM when merging multiple LogBlocks performed during nested rollbacks.
    // Atomically un-publish all non-inflight commits
    // Atomically un-publish all non-inflight commits
    // For Requested State (like failure during index lookup), there is nothing to do rollback other than
    // deleting the timeline file
    if (!instant.isRequested()) {
      LOG.info("Unpublished " + commit);
      List<RollbackRequest> rollbackRequests = generateRollbackRequests(instant);
      // TODO: We need to persist this as rollback workload and use it in case of partial failures
      allRollbackStats = new RollbackHelper(metaClient, config).performRollback(instant, rollbackRequests);
    }

    // Delete Inflight instants if enabled
    deleteInflightAndRequestedInstant(deleteInstants, this.getActiveTimeline(), instant);

    LOG.info("Time(in ms) taken to finish rollback " + (System.currentTimeMillis() - startTime));

    return allRollbackStats;
  }

  /**
   * Delete Inflight instant if enabled.
   *
   * @param deleteInstant      Enable Deletion of Inflight instant
   * @param activeTimeline     Hoodie active timeline
   * @param instantToBeDeleted Instant to be deleted
   */
  protected void deleteInflightAndRequestedInstant(boolean deleteInstant, HoodieActiveTimeline activeTimeline,
                                                   HoodieInstant instantToBeDeleted) {
    // Remove marker files always on rollback
    deleteMarkerDir(instantToBeDeleted.getTimestamp());

    // Remove the rolled back inflight commits
    if (deleteInstant) {
      LOG.info("Deleting instant=" + instantToBeDeleted);
      activeTimeline.deletePending(instantToBeDeleted);
      if (instantToBeDeleted.isInflight() && !metaClient.getTimelineLayoutVersion().isNullVersion()) {
        // Delete corresponding requested instant
        instantToBeDeleted = new HoodieInstant(HoodieInstant.State.REQUESTED, instantToBeDeleted.getAction(),
            instantToBeDeleted.getTimestamp());
        activeTimeline.deletePending(instantToBeDeleted);
      }
      LOG.info("Deleted pending commit " + instantToBeDeleted);
    } else {
      LOG.warn("Rollback finished without deleting inflight instant file. Instant=" + instantToBeDeleted);
    }
  }

  /**
   * Generate all rollback requests that we need to perform for rolling back this action without actually performing
   * rolling back.
   *
   * @param instantToRollback Instant to Rollback
   * @return list of rollback requests
   * @throws IOException
   */
  private List<RollbackRequest> generateRollbackRequests(HoodieInstant instantToRollback)
      throws IOException {
    String commit = instantToRollback.getTimestamp();
    List<String> partitions = FSUtils.getAllPartitionPaths(this.metaClient.getFs(), this.getMetaClient().getBasePath(),
        config.shouldAssumeDatePartitioning());

    return partitions.stream().flatMap(partitionPath -> {
      HoodieActiveTimeline activeTimeline = this.getActiveTimeline().reload();
      List<RollbackRequest> partitionRollbackRequests = new ArrayList<>();
      switch (instantToRollback.getAction()) {
        case HoodieTimeline.COMMIT_ACTION:
          LOG.info(
              "Rolling back commit action. There are higher delta commits. So only rolling back this instant");
          partitionRollbackRequests.add(
              RollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath, instantToRollback));
          break;
        case HoodieTimeline.COMPACTION_ACTION:
          // If there is no delta commit present after the current commit (if compaction), no action, else we
          // need to make sure that a compaction commit rollback also deletes any log files written as part of the
          // succeeding deltacommit.
          boolean higherDeltaCommits =
              !activeTimeline.getDeltaCommitTimeline().filterCompletedInstants().findInstantsAfter(commit, 1).empty();
          if (higherDeltaCommits) {
            // Rollback of a compaction action with no higher deltacommit means that the compaction is scheduled
            // and has not yet finished. In this scenario we should delete only the newly created parquet files
            // and not corresponding base commit log files created with this as baseCommit since updates would
            // have been written to the log files.
            LOG.info("Rolling back compaction. There are higher delta commits. So only deleting data files");
            partitionRollbackRequests.add(
                RollbackRequest.createRollbackRequestWithDeleteDataFilesOnlyAction(partitionPath, instantToRollback));
          } else {
            // No deltacommits present after this compaction commit (inflight or requested). In this case, we
            // can also delete any log files that were created with this compaction commit as base
            // commit.
            LOG.info("Rolling back compaction plan. There are NO higher delta commits. So deleting both data and"
                + " log files");
            partitionRollbackRequests.add(
                RollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath, instantToRollback));
          }
          break;
        case HoodieTimeline.DELTA_COMMIT_ACTION:
          // --------------------------------------------------------------------------------------------------
          // (A) The following cases are possible if index.canIndexLogFiles and/or index.isGlobal
          // --------------------------------------------------------------------------------------------------
          // (A.1) Failed first commit - Inserts were written to log files and HoodieWriteStat has no entries. In
          // this scenario we would want to delete these log files.
          // (A.2) Failed recurring commit - Inserts/Updates written to log files. In this scenario,
          // HoodieWriteStat will have the baseCommitTime for the first log file written, add rollback blocks.
          // (A.3) Rollback triggered for first commit - Inserts were written to the log files but the commit is
          // being reverted. In this scenario, HoodieWriteStat will be `null` for the attribute prevCommitTime and
          // and hence will end up deleting these log files. This is done so there are no orphan log files
          // lying around.
          // (A.4) Rollback triggered for recurring commits - Inserts/Updates are being rolled back, the actions
          // taken in this scenario is a combination of (A.2) and (A.3)
          // ---------------------------------------------------------------------------------------------------
          // (B) The following cases are possible if !index.canIndexLogFiles and/or !index.isGlobal
          // ---------------------------------------------------------------------------------------------------
          // (B.1) Failed first commit - Inserts were written to parquet files and HoodieWriteStat has no entries.
          // In this scenario, we delete all the parquet files written for the failed commit.
          // (B.2) Failed recurring commits - Inserts were written to parquet files and updates to log files. In
          // this scenario, perform (A.1) and for updates written to log files, write rollback blocks.
          // (B.3) Rollback triggered for first commit - Same as (B.1)
          // (B.4) Rollback triggered for recurring commits - Same as (B.2) plus we need to delete the log files
          // as well if the base parquet file gets deleted.
          try {
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                metaClient.getCommitTimeline()
                    .getInstantDetails(
                        new HoodieInstant(true, instantToRollback.getAction(), instantToRollback.getTimestamp()))
                    .get(),
                HoodieCommitMetadata.class);

            // In case all data was inserts and the commit failed, delete the file belonging to that commit
            // We do not know fileIds for inserts (first inserts are either log files or parquet files),
            // delete all files for the corresponding failed commit, if present (same as COW)
            partitionRollbackRequests.add(
                RollbackRequest.createRollbackRequestWithDeleteDataAndLogFilesAction(partitionPath, instantToRollback));

            // append rollback blocks for updates
            if (commitMetadata.getPartitionToWriteStats().containsKey(partitionPath)) {
              partitionRollbackRequests
                  .addAll(generateAppendRollbackBlocksAction(partitionPath, instantToRollback, commitMetadata));
            }
            break;
          } catch (IOException io) {
            throw new UncheckedIOException("Failed to collect rollback actions for commit " + commit, io);
          }
        default:
          break;
      }
      return partitionRollbackRequests.stream();
    }).filter(Objects::nonNull).collect(Collectors.toList());
  }

  private List<RollbackRequest> generateAppendRollbackBlocksAction(String partitionPath, HoodieInstant rollbackInstant,
                                                                   HoodieCommitMetadata commitMetadata) {
    ValidationUtils.checkArgument(rollbackInstant.getAction().equals(HoodieTimeline.DELTA_COMMIT_ACTION));

    // wStat.getPrevCommit() might not give the right commit time in the following
    // scenario : If a compaction was scheduled, the new commitTime associated with the requested compaction will be
    // used to write the new log files. In this case, the commit time for the log file is the compaction requested time.
    // But the index (global) might store the baseCommit of the parquet and not the requested, hence get the
    // baseCommit always by listing the file slice
    Map<String, String> fileIdToBaseCommitTimeForLogMap = this.getSliceView().getLatestFileSlices(partitionPath)
        .collect(Collectors.toMap(FileSlice::getFileId, FileSlice::getBaseInstantTime));
    return commitMetadata.getPartitionToWriteStats().get(partitionPath).stream().filter(wStat -> {

      // Filter out stats without prevCommit since they are all inserts
      boolean validForRollback = (wStat != null) && (!wStat.getPrevCommit().equals(HoodieWriteStat.NULL_COMMIT))
          && (wStat.getPrevCommit() != null) && fileIdToBaseCommitTimeForLogMap.containsKey(wStat.getFileId());

      if (validForRollback) {
        // For sanity, log instant time can never be less than base-commit on which we are rolling back
        ValidationUtils
            .checkArgument(HoodieTimeline.compareTimestamps(fileIdToBaseCommitTimeForLogMap.get(wStat.getFileId()),
                rollbackInstant.getTimestamp(), HoodieTimeline.LESSER_OR_EQUAL));
      }

      return validForRollback && HoodieTimeline.compareTimestamps(fileIdToBaseCommitTimeForLogMap.get(
          // Base Ts should be strictly less. If equal (for inserts-to-logs), the caller employs another option
          // to delete and we should not step on it
          wStat.getFileId()), rollbackInstant.getTimestamp(), HoodieTimeline.LESSER);
    }).map(wStat -> {
      String baseCommitTime = fileIdToBaseCommitTimeForLogMap.get(wStat.getFileId());
      return RollbackRequest.createRollbackRequestWithAppendRollbackBlockAction(partitionPath, wStat.getFileId(),
          baseCommitTime, rollbackInstant);
    }).collect(Collectors.toList());
  }

  @Override
  public List<WriteStatus> compact(String compactionInstantTime,
                                   HoodieCompactionPlan compactionPlan) {
    HoodieMergeOnReadTableCompactor compactor = new HoodieMergeOnReadTableCompactor(this);
    try {
      return compactor.compact(compactionPlan, this, config, compactionInstantTime);
    } catch (IOException e) {
      throw new HoodieCompactionException("Could not compact " + config.getBasePath(), e);
    }
  }

  private static class PartitionCleanStat implements Serializable {

    private final String partitionPath;
    private final List<String> deletePathPatterns = new ArrayList<>();
    private final List<String> successDeleteFiles = new ArrayList<>();
    private final List<String> failedDeleteFiles = new ArrayList<>();

    private PartitionCleanStat(String partitionPath) {
      this.partitionPath = partitionPath;
    }

    private void addDeletedFileResult(String deletePathStr, Boolean deletedFileResult) {
      if (deletedFileResult) {
        successDeleteFiles.add(deletePathStr);
      } else {
        failedDeleteFiles.add(deletePathStr);
      }
    }

    private void addDeleteFilePatterns(String deletePathStr) {
      deletePathPatterns.add(deletePathStr);
    }

    private PartitionCleanStat merge(PartitionCleanStat other) {
      if (!this.partitionPath.equals(other.partitionPath)) {
        throw new RuntimeException(
            String.format("partitionPath is not a match: (%s, %s)", partitionPath, other.partitionPath));
      }
      successDeleteFiles.addAll(other.successDeleteFiles);
      deletePathPatterns.addAll(other.deletePathPatterns);
      failedDeleteFiles.addAll(other.failedDeleteFiles);
      return this;
    }
  }
}
