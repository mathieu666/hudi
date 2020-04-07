package org.apache.hudi.writer.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.util.CompactionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.client.embedded.EmbeddedTimelineService;
import org.apache.hudi.writer.config.HoodieCompactionConfig;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.exception.*;
import org.apache.hudi.writer.index.HoodieIndex;
import org.apache.hudi.writer.table.*;
import org.apache.hudi.writer.table.partitioner.Partitioner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * HoodieWriteClient.
 */
public class HoodieWriteClient<T extends HoodieRecordPayload> extends AbstractHoodieWriteClient<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieWriteClient.class);
  private static final String LOOKUP_STR = "lookup";
  private final boolean rollbackPending;

  /**
   * Create a write client, with new hudi index.
   *
   * @param hadoopConf      HadoopConf
   * @param clientConfig    instance of HoodieWriteConfig
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
    super(hadoopConf, index, clientConfig, timelineService);
    this.rollbackPending = rollbackPending;
  }

  /**
   * Inserts the given HoodieRecords, into the table. This API is intended to be used for normal writes.
   * <p>
   * This implementation skips the index check and is able to leverage benefits such as small file handling/blocking
   * alignment, as with upsert(), by profiling the workload
   *
   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> insert(List<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable<T> table = getTableAndInitCtx(WriteOperationType.INSERT);
    setOperationType(WriteOperationType.INSERT);
    try {
      // De-dupe/merge if needed
      List<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(config.shouldCombineBeforeInsert(), records, config.getInsertShuffleParallelism());

      return upsertRecordsInternal(dedupedRecords, instantTime, table, false);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to insert for commit time " + instantTime, e);
    }
  }

  /**
   * Upsert a batch of new records into Hoodie table at the supplied instantTime.
   *
   * @param records JavaRDD of hoodieRecords to upsert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> upsert(List<HoodieRecord<T>> records, final String instantTime) {
    HoodieTable<T> table = getTableAndInitCtx(WriteOperationType.UPSERT);
    setOperationType(WriteOperationType.UPSERT);
    try {
      // De-dupe/merge if needed
      List<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(config.shouldCombineBeforeUpsert(), records, config.getUpsertShuffleParallelism());

      // perform index loop up to get existing location of records
      List<HoodieRecord<T>> taggedRecords = getIndex().tagLocation(dedupedRecords, hadoopConf, table);
      return upsertRecordsInternal(taggedRecords, instantTime, table, true);
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }
  }

  private List<WriteStatus> upsertRecordsInternal(List<HoodieRecord<T>> preppedRecords, String instantTime,
                                                     HoodieTable<T> hoodieTable, final boolean isUpsert) {

    WorkloadProfile profile = null;
    if (hoodieTable.isWorkloadProfileNeeded()) {
      profile = new WorkloadProfile(preppedRecords);
      LOG.info("Workload profile :" + profile);
      saveWorkloadProfileMetadataToInflight(profile, hoodieTable, instantTime);
    }

    // partition using the insert partitioner
    final Partitioner partitioner = getPartitioner(hoodieTable, isUpsert, profile);
    List<HoodieRecord<T>> partitionedRecords = partition(preppedRecords, partitioner);
    List<WriteStatus> writeStatusRDD = partitionedRecords.mapPartitionsWithIndex((partition, recordItr) -> {
      if (isUpsert) {
        return hoodieTable.handleUpsertPartition(instantTime, partition, recordItr, partitioner);
      } else {
        return hoodieTable.handleInsertPartition(instantTime, partition, recordItr, partitioner);
      }
    }, true).flatMap(List::iterator);

    return updateIndexAndCommitIfNeeded(writeStatusRDD, hoodieTable, instantTime);
  }

  private List<HoodieRecord<T>> combineOnCondition(boolean condition, List<HoodieRecord<T>> records,
                                                      int parallelism) {
    return condition ? deduplicateRecords(records, parallelism) : records;
  }

  private Partitioner getPartitioner(HoodieTable table, boolean isUpsert, WorkloadProfile profile) {
    if (isUpsert) {
      return table.getUpsertPartitioner(profile, hadoopConf);
    } else {
      return table.getInsertPartitioner(profile, hadoopConf);
    }
  }


  private List<HoodieRecord<T>> partition(List<HoodieRecord<T>> dedupedRecords, Partitioner partitioner) {
    // todo
    return new ArrayList<>();
  }

  /**
   * Loads the given HoodieRecords, as inserts into the table. This is suitable for doing big bulk loads into a Hoodie
   * table for the very first time (e.g: converting an existing table to Hoodie).
   *
   * @param records HoodieRecords to insert
   * @param instantTime Instant time of the commit
   * @return JavaRDD[WriteStatus] - RDD of WriteStatus to inspect errors and counts
   */
  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, final String instantTime) {
    return bulkInsert(records, instantTime, Option.empty());
  }

  public List<WriteStatus> bulkInsert(List<HoodieRecord<T>> records, final String instantTime,
                                         Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    HoodieTable<T> table = getTableAndInitCtx(WriteOperationType.BULK_INSERT);
    setOperationType(WriteOperationType.BULK_INSERT);
    try {
      // De-dupe/merge if needed
      List<HoodieRecord<T>> dedupedRecords =
          combineOnCondition(config.shouldCombineBeforeInsert(), records, config.getInsertShuffleParallelism());

      return bulkInsertInternal(dedupedRecords, instantTime, table, bulkInsertPartitioner);
    } catch (Throwable e) {
      if (e instanceof HoodieInsertException) {
        throw e;
      }
      throw new HoodieInsertException("Failed to bulk insert for commit time " + instantTime, e);
    }
  }

  private List<WriteStatus> bulkInsertInternal(List<HoodieRecord<T>> dedupedRecords, String instantTime,
                                                  HoodieTable<T> table, Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    final List<HoodieRecord<T>> repartitionedRecords;
    final int parallelism = config.getBulkInsertShuffleParallelism();
    if (bulkInsertPartitioner.isPresent()) {
      repartitionedRecords = bulkInsertPartitioner.get().repartitionRecords(dedupedRecords, parallelism);
    } else {
      // Now, sort the records and line them up nicely for loading.
      repartitionedRecords = dedupedRecords.sortBy(record -> {
        // Let's use "partitionPath + key" as the sort key. Spark, will ensure
        // the records split evenly across RDD partitions, such that small partitions fit
        // into 1 RDD partition, while big ones spread evenly across multiple RDD partitions
        return String.format("%s+%s", record.getPartitionPath(), record.getRecordKey());
      }, true, parallelism);
    }

    // generate new file ID prefixes for each output partition
    final List<String> fileIDPrefixes =
        IntStream.range(0, parallelism).mapToObj(i -> FSUtils.createNewFileIdPfx()).collect(Collectors.toList());

    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(HoodieInstant.State.REQUESTED,
        table.getMetaClient().getCommitActionType(), instantTime), Option.empty());

    List<WriteStatus> writeStatusRDD = repartitionedRecords
        .mapPartitionsWithIndex(new BulkInsertMapFunction<T>(instantTime, config, table, fileIDPrefixes), true)
        .flatMap(List::iterator);

    return updateIndexAndCommitIfNeeded(writeStatusRDD, table, instantTime);
  }

  @Override
  protected void postCommit(HoodieCommitMetadata metadata, String instantTime,
                            Option<Map<String, String>> extraMetadata) throws IOException {

    // Do an inline compaction if enabled
    if (config.isInlineCompaction()) {
      metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "true");
      forceCompact(extraMetadata);
    } else {
      metadata.addMetadata(HoodieCompactionConfig.INLINE_COMPACT_PROP, "false");
    }
    // We cannot have unbounded commit files. Archive commits if we have to archive
    HoodieCommitArchiveLog archiveLog = new HoodieCommitArchiveLog(config, createMetaClient(true));
    archiveLog.archiveIfRequired(hadoopConf);
    if (config.isAutoClean()) {
      // Call clean to cleanup if there is anything to cleanup after the commit,
      LOG.info("Auto cleaning is enabled. Running cleaner now");
      clean(instantTime);
    } else {
      LOG.info("Auto cleaning is not enabled. Not running cleaner now");
    }
  }

  /**
   * Rollback the (inflight/committed) record changes with the given commit time. Three steps: (1) Atomically unpublish
   * this commit (2) clean indexing data, (3) clean new generated parquet files. (4) Finally delete .commit or .inflight
   * file.
   *
   * @param instantTime Instant time of the commit
   * @return {@code true} If rollback the record changes successfully. {@code false} otherwise
   */
  public boolean rollback(final String instantTime) throws HoodieRollbackException {
    rollbackInternal(instantTime);
    return true;
  }
  /**
   * Clean up any stale/old files/data lying around (either on file storage or index storage) based on the
   * configurations and CleaningPolicy used. (typically files that no longer can be used by a running query can be
   * cleaned)
   */
  public HoodieCleanMetadata clean(String cleanInstantTime) throws HoodieIOException {
    LOG.info("Cleaner started");

    HoodieCleanMetadata metadata = HoodieTable.create(config, hadoopConf).clean(hadoopConf, cleanInstantTime);

    return metadata;
  }

  /**
   * Save the workload profile in an intermediate file (here re-using commit files) This is useful when performing
   * rollback for MOR tables. Only updates are recorded in the workload profile metadata since updates to log blocks
   * are unknown across batches Inserts (which are new parquet files) are rolled back based on commit time. // TODO :
   * Create a new WorkloadProfile metadata file instead of using HoodieCommitMetadata
   */
  private void saveWorkloadProfileMetadataToInflight(WorkloadProfile profile, HoodieTable<T> table, String instantTime)
      throws HoodieCommitException {
    try {
      HoodieCommitMetadata metadata = new HoodieCommitMetadata();
      profile.getPartitionPaths().forEach(path -> {
        WorkloadStat partitionStat = profile.getWorkloadStat(path.toString());
        partitionStat.getUpdateLocationToCount().forEach((key, value) -> {
          HoodieWriteStat writeStat = new HoodieWriteStat();
          writeStat.setFileId(key);
          // TODO : Write baseCommitTime is possible here ?
          writeStat.setPrevCommit(value.getKey());
          writeStat.setNumUpdateWrites(value.getValue());
          metadata.addWriteStat(path.toString(), writeStat);
        });
      });
      metadata.setOperationType(getOperationType());

      HoodieActiveTimeline activeTimeline = table.getActiveTimeline();
      String commitActionType = table.getMetaClient().getCommitActionType();
      HoodieInstant requested = new HoodieInstant(HoodieInstant.State.REQUESTED, commitActionType, instantTime);
      activeTimeline.transitionRequestedToInflight(requested,
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException io) {
      throw new HoodieCommitException("Failed to commit " + instantTime + " unable to save inflight metadata ", io);
    }
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param records hoodieRecords to deduplicate
   * @param parallelism parallelism or partitions to be used while reducing/deduplicating
   * @return RDD of HoodieRecord already be deduplicated
   */
  List<HoodieRecord<T>> deduplicateRecords(List<HoodieRecord<T>> records, int parallelism) {
    boolean isIndexingGlobal = getIndex().isGlobal();
    return records.mapToPair(record -> {
      HoodieKey hoodieKey = record.getKey();
      // If index used is global, then records are expected to differ in their partitionPath
      Object key = isIndexingGlobal ? hoodieKey.getRecordKey() : hoodieKey;
      return new Tuple2<>(key, record);
    }).reduceByKey((rec1, rec2) -> {
      @SuppressWarnings("unchecked")
      T reducedData = (T) rec1.getData().preCombine(rec2.getData());
      // we cannot allow the user to change the key or partitionPath, since that will affect
      // everything
      // so pick it from one of the records.
      return new HoodieRecord<T>(rec1.getKey(), reducedData);
    }, parallelism).map(Tuple2::_2);
  }

  /**
   * Provides a new commit time for a write operation (insert/update/delete).
   */
  public String startCommit() {
    // NOTE : Need to ensure that rollback is done before a new commit is started
    if (rollbackPending) {
      // Only rollback pending commit/delta-commits. Do not touch compaction commits
      rollbackPendingCommits();
    }
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    startCommit(instantTime);
    return instantTime;
  }

  private void startCommit(String instantTime) {
    //todo
  }


  /**
   * Cleanup all pending commits.
   */
  private void rollbackPendingCommits() {
    //todo
  }

  /**
   * Schedules a new compaction instant.
   *
   * @param extraMetadata Extra Metadata to be stored
   */
  public Option<String> scheduleCompaction(Option<Map<String, String>> extraMetadata) throws IOException {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    LOG.info("Generate a new instant time " + instantTime);
    boolean notEmpty = scheduleCompactionAtInstant(instantTime, extraMetadata);
    return notEmpty ? Option.of(instantTime) : Option.empty();
  }

  /**
   * Schedules a new compaction instant with passed-in instant time.
   *
   * @param instantTime Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleCompactionAtInstant(String instantTime, Option<Map<String, String>> extraMetadata)
      throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    // if there are inflight writes, their instantTime must not be less than that of compaction instant time
    metaClient.getCommitsTimeline().filterPendingExcludingCompaction().firstInstant().ifPresent(earliestInflight -> {
      ValidationUtils.checkArgument(
          HoodieTimeline.compareTimestamps(earliestInflight.getTimestamp(), instantTime, HoodieTimeline.GREATER),
          "Earliest write inflight instant time must be later than compaction time. Earliest :" + earliestInflight
              + ", Compaction scheduled at " + instantTime);
    });
    // Committed and pending compaction instants should have strictly lower timestamps
    List<HoodieInstant> conflictingInstants = metaClient
        .getActiveTimeline().getCommitsAndCompactionTimeline().getInstants().filter(instant -> HoodieTimeline
            .compareTimestamps(instant.getTimestamp(), instantTime, HoodieTimeline.GREATER_OR_EQUAL))
        .collect(Collectors.toList());
    ValidationUtils.checkArgument(conflictingInstants.isEmpty(),
        "Following instants have timestamps >= compactionInstant (" + instantTime + ") Instants :"
            + conflictingInstants);
    HoodieTable<T> table = HoodieTable.create(metaClient, config, hadoopConf);
    HoodieCompactionPlan workload = table.scheduleCompaction(hadoopConf, instantTime);
    if (workload != null && (workload.getOperations() != null) && (!workload.getOperations().isEmpty())) {
      extraMetadata.ifPresent(workload::setExtraMetadata);
      HoodieInstant compactionInstant =
          new HoodieInstant(HoodieInstant.State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
      metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
          TimelineMetadataUtils.serializeCompactionPlan(workload));
      return true;
    }
    return false;
  }

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time.
   *
   * @param compactionInstantTime Compaction Instant Time
   * @return RDD of Write Status
   */
  private List<WriteStatus> compact(String compactionInstantTime, boolean autoCommit) throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieTable<T> table = HoodieTable.create(metaClient, config, hadoopConf);
    HoodieTimeline pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      // inflight compaction - Needs to rollback first deleting new parquet files before we run compaction.
      rollbackInflightCompaction(inflightInstant, table);
      // refresh table
      metaClient = createMetaClient(true);
      table = HoodieTable.create(metaClient, config, hadoopConf);
      pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    }

    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(instant)) {
      return runCompaction(instant, metaClient.getActiveTimeline(), autoCommit);
    } else {
      throw new IllegalStateException(
          "No Compaction request available at " + compactionInstantTime + " to run compaction");
    }
  }

  /**
   * Perform compaction operations as specified in the compaction commit file.
   *
   * @param compactionInstant Compaction Instant time
   * @param activeTimeline Active Timeline
   * @param autoCommit Commit after compaction
   * @return RDD of Write Status
   */
  private List<WriteStatus> runCompaction(HoodieInstant compactionInstant, HoodieActiveTimeline activeTimeline,
                                             boolean autoCommit) throws IOException {
    HoodieTableMetaClient metaClient = createMetaClient(true);
    HoodieCompactionPlan compactionPlan =
        CompactionUtils.getCompactionPlan(metaClient, compactionInstant.getTimestamp());
    // Mark instant as compaction inflight
    activeTimeline.transitionCompactionRequestedToInflight(compactionInstant);
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTable<T> table = HoodieTable.create(metaClient, config, hadoopConf);
    List<WriteStatus> statuses = table.compact(hadoopConf, compactionInstant.getTimestamp(), compactionPlan);
    // pass extra-metada so that it gets stored in commit file automatically
    commitCompaction(statuses, table, compactionInstant.getTimestamp(), autoCommit,
        Option.ofNullable(compactionPlan.getExtraMetadata()));
    return statuses;
  }

  /**
   * Commit Compaction and track metrics.
   *
   * @param compactedStatuses Compaction Write status
   * @param table Hoodie Table
   * @param compactionCommitTime Compaction Commit Time
   * @param autoCommit Auto Commit
   * @param extraMetadata Extra Metadata to store
   */
  protected void commitCompaction(List<WriteStatus> compactedStatuses, HoodieTable<T> table,
                                  String compactionCommitTime, boolean autoCommit, Option<Map<String, String>> extraMetadata) {
    if (autoCommit) {
      HoodieCommitMetadata metadata = doCompactionCommit(table, compactedStatuses, compactionCommitTime, extraMetadata);
      LOG.info("Compacted successfully on commit " + compactionCommitTime);
    } else {
      LOG.info("Compaction did not run for commit " + compactionCommitTime);
    }
  }

  private HoodieCommitMetadata doCompactionCommit(HoodieTable<T> table, List<WriteStatus> writeStatuses,
                                                  String compactionCommitTime, Option<Map<String, String>> extraMetadata) {
    HoodieTableMetaClient metaClient = table.getMetaClient();
    List<HoodieWriteStat> updateStatusMap = writeStatuses.map(WriteStatus::getStat).collect();

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }

    // Finalize write
    finalizeWrite(table, compactionCommitTime, updateStatusMap);

    // Copy extraMetadata
    extraMetadata.ifPresent(m -> {
      m.forEach(metadata::addMetadata);
    });

    LOG.info("Committing Compaction " + compactionCommitTime + ". Finished with result " + metadata);
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    try {
      activeTimeline.transitionCompactionInflightToComplete(
          new HoodieInstant(HoodieInstant.State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionCommitTime),
          Option.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + metaClient.getBasePath() + " at time " + compactionCommitTime, e);
    }
    return metadata;
  }

  /**
   * Rollback failed compactions. Inflight rollbacks for compactions revert the .inflight file to the .requested file
   *
   * @param inflightInstant Inflight Compaction Instant
   * @param table Hoodie Table
   */
  public void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieTable table) throws IOException {
    table.rollback(hadoopConf, inflightInstant, false);
    // Revert instant state file
    table.getActiveTimeline().revertCompactionInflightToRequested(inflightInstant);
  }

  /**
   * Performs a compaction operation on a table, serially before or after an insert/upsert action.
   */
  private Option<String> forceCompact(Option<Map<String, String>> extraMetadata) throws IOException {
    Option<String> compactionInstantTimeOpt = scheduleCompaction(extraMetadata);
    compactionInstantTimeOpt.ifPresent(compactionInstantTime -> {
      try {
        // inline compaction should auto commit as the user is never given control
        compact(compactionInstantTime, true);
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    });
    return compactionInstantTimeOpt;
  }
}
