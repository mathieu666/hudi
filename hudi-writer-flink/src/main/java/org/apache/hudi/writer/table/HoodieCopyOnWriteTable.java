package org.apache.hudi.writer.table;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.exception.HoodieUpsertException;
import org.apache.hudi.writer.io.HoodieMergeHandle;
import org.apache.hudi.writer.table.action.clean.CleanActionExecutor;
import org.apache.hudi.writer.table.partitioner.Partitioner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class HoodieCopyOnWriteTable <T extends HoodieRecordPayload> extends HoodieTable<T> {
  private static final Logger LOG = LogManager.getLogger(HoodieCopyOnWriteTable.class);

  public HoodieCopyOnWriteTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    super(config, hadoopConf, metaClient);
  }

  @Override
  public List<WriteStatus> compact(Configuration hadoopConf, String compactionInstantTime, HoodieCompactionPlan compactionPlan) {
    return null;
  }

  @Override
  public List<HoodieRollbackStat> rollback(Configuration hadoopConf, HoodieInstant instant, boolean deleteInstants) throws IOException {
    return null;
  }

  @Override
  public HoodieCleanMetadata clean(Configuration hadoopConf, String cleanInstantTime) {
    return new CleanActionExecutor(hadoopConf, config, this, cleanInstantTime).execute();
  }

  @Override
  public HoodieCompactionPlan scheduleCompaction(Configuration hadoopConf, String instantTime) {
    throw new HoodieNotSupportedException("Compaction is not supported from a CopyOnWrite table");
  }

  @Override
  public boolean isWorkloadProfileNeeded() {
    return true;
  }

  @Override
  public Partitioner getUpsertPartitioner(WorkloadProfile profile, Configuration hadoopConf) {
    if (profile == null) {
      throw new HoodieUpsertException("Need workload profile to construct the upsert partitioner.");
    }
    return new UpsertPartitioner(profile);
  }

  @Override
  public Partitioner getInsertPartitioner(WorkloadProfile profile, Configuration hadoopConf) {
    return getUpsertPartitioner(profile, hadoopConf);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<List<WriteStatus>> handleUpsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                           Partitioner partitioner) {
    UpsertPartitioner upsertPartitioner = (UpsertPartitioner) partitioner;
    BucketInfo binfo = upsertPartitioner.getBucketInfo(partition);
    BucketType btype = binfo.bucketType;
    try {
      if (btype.equals(BucketType.INSERT)) {
        return handleInsert(instantTime, binfo.fileIdPrefix, recordItr);
      } else if (btype.equals(BucketType.UPDATE)) {
        return handleUpdate(instantTime, binfo.partitionPath, binfo.fileIdPrefix, recordItr);
      } else {
        throw new HoodieUpsertException("Unknown bucketType " + btype + " for partition :" + partition);
      }
    } catch (Throwable t) {
      String msg = "Error upserting bucketType " + btype + " for partition :" + partition;
      LOG.error(msg, t);
      throw new HoodieUpsertException(msg, t);
    }
  }

  public Iterator<List<WriteStatus>> handleInsert(String instantTime, String idPfx, Iterator<HoodieRecord<T>> recordItr)
      throws Exception {

    return Collections.singletonList((List<WriteStatus>) Collections.EMPTY_LIST).iterator();
  }

  public Iterator<List<WriteStatus>> handleUpdate(String instantTime, String partitionPath, String fileId,
                                                  Iterator<HoodieRecord<T>> recordItr)
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

  protected Iterator<List<WriteStatus>> handleUpdateInternal(HoodieMergeHandle upsertHandle, String instantTime,
                                                             String fileId) throws IOException {
//    if (upsertHandle.getOldFilePath() == null) {
//      throw new HoodieUpsertException(
//          "Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
//    } else {
//      AvroReadSupport.setAvroReadSchema(getHadoopConf(), upsertHandle.getWriterSchema());
//      BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;
//      try (ParquetReader<IndexedRecord> reader =
//               AvroParquetReader.<IndexedRecord>builder(upsertHandle.getOldFilePath()).withConf(getHadoopConf()).build()) {
//        wrapper = new SparkBoundedInMemoryExecutor(config, new ParquetReaderIterator(reader),
//            new UpdateHandler(upsertHandle), x -> x);
//        wrapper.execute();
//      } catch (Exception e) {
//        throw new HoodieException(e);
//      } finally {
//        upsertHandle.close();
//        if (null != wrapper) {
//          wrapper.shutdownNow();
//        }
//      }
//    }
//
//    // TODO(vc): This needs to be revisited
//    if (upsertHandle.getWriteStatus().getPartitionPath() == null) {
//      LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
//          + upsertHandle.getWriteStatus());
//    }
//    return Collections.singletonList(Collections.singletonList(upsertHandle.getWriteStatus())).iterator();
    return null;
  }


  /**
   * Consumer that dequeues records from queue and sends to Merge Handle.
   */
  private static class UpdateHandler extends BoundedInMemoryQueueConsumer<GenericRecord, Void> {

    private final HoodieMergeHandle upsertHandle;

    private UpdateHandler(HoodieMergeHandle upsertHandle) {
      this.upsertHandle = upsertHandle;
    }

    @Override
    protected void consumeOneRecord(GenericRecord record) {
      upsertHandle.write(record);
    }

    @Override
    protected void finish() {}

    @Override
    protected Void getResult() {
      return null;
    }
  }

  protected HoodieMergeHandle getUpdateHandle(String instantTime, String partitionPath, String fileId, Iterator<HoodieRecord<T>> recordItr) {
    return new HoodieMergeHandle<>(config, instantTime, this, recordItr, partitionPath, fileId, sparkTaskContextSupplier);
  }

  @Override
  public Iterator<List<WriteStatus>> handleInsertPartition(String instantTime, Integer partition, Iterator recordItr,
                                                           Partitioner partitioner) {
    return handleUpsertPartition(instantTime, partition, recordItr, partitioner);
  }

  /**
   * Helper class for a small file's location and its actual size on disk.
   */
  static class SmallFile implements Serializable {

    HoodieRecordLocation location;
    long sizeBytes;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("SmallFile {");
      sb.append("location=").append(location).append(", ");
      sb.append("sizeBytes=").append(sizeBytes);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Helper class for an insert bucket along with the weight [0.0, 1.0] that defines the amount of incoming inserts that
   * should be allocated to the bucket.
   */
  class InsertBucket implements Serializable {

    int bucketNumber;
    // fraction of total inserts, that should go into this bucket
    double weight;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("WorkloadStat {");
      sb.append("bucketNumber=").append(bucketNumber).append(", ");
      sb.append("weight=").append(weight);
      sb.append('}');
      return sb.toString();
    }
  }

  /**
   * Helper class for a bucket's type (INSERT and UPDATE) and its file location.
   */
  class BucketInfo implements Serializable {

    BucketType bucketType;
    String fileIdPrefix;
    String partitionPath;

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("BucketInfo {");
      sb.append("bucketType=").append(bucketType).append(", ");
      sb.append("fileIdPrefix=").append(fileIdPrefix).append(", ");
      sb.append("partitionPath=").append(partitionPath);
      sb.append('}');
      return sb.toString();
    }
  }

  enum BucketType {
    UPDATE, INSERT
  }

  /**
   * Packs incoming records to be upserted, into buckets (1 bucket = 1 RDD partition).
   */
  class UpsertPartitioner implements Partitioner {

    /**
     * List of all small files to be corrected.
     */
    List<SmallFile> smallFiles = new ArrayList<>();
    /**
     * Total number of RDD partitions, is determined by total buckets we want to pack the incoming workload into.
     */
    private int totalBuckets = 0;
    /**
     * Stat for the current workload. Helps in determining total inserts, upserts etc.
     */
    private WorkloadStat globalStat;
    /**
     * Helps decide which bucket an incoming update should go to.
     */
    private HashMap<String, Integer> updateLocationToBucket;
    /**
     * Helps us pack inserts into 1 or more buckets depending on number of incoming records.
     */
    private HashMap<String, List<InsertBucket>> partitionPathToInsertBuckets;
    /**
     * Remembers what type each bucket is for later.
     */
    private HashMap<Integer, BucketInfo> bucketInfoMap;

    /**
     * Rolling stats for files.
     */
    protected HoodieRollingStatMetadata rollingStatMetadata;

    UpsertPartitioner(WorkloadProfile profile) {
      updateLocationToBucket = new HashMap<>();
      partitionPathToInsertBuckets = new HashMap<>();
      bucketInfoMap = new HashMap<>();
      globalStat = profile.getGlobalStat();
      rollingStatMetadata = getRollingStats();
      assignUpdates(profile);
      assignInserts(profile);

      LOG.info("Total Buckets :" + totalBuckets + ", buckets info => " + bucketInfoMap + ", \n"
          + "Partition to insert buckets => " + partitionPathToInsertBuckets + ", \n"
          + "UpdateLocations mapped to buckets =>" + updateLocationToBucket);
    }
    protected HoodieRollingStatMetadata getRollingStats() {
      return null;
    }

    private void assignUpdates(WorkloadProfile profile) {
      // each update location gets a partition
      Set<Map.Entry<String, WorkloadStat>> partitionStatEntries = profile.getPartitionPathStatMap().entrySet();
      for (Map.Entry<String, WorkloadStat> partitionStat : partitionStatEntries) {
        for (Map.Entry<String, Pair<String, Long>> updateLocEntry :
            partitionStat.getValue().getUpdateLocationToCount().entrySet()) {
          addUpdateBucket(partitionStat.getKey(), updateLocEntry.getKey());
        }
      }
    }

    private int addUpdateBucket(String partitionPath, String fileIdHint) {
      int bucket = totalBuckets;
      updateLocationToBucket.put(fileIdHint, bucket);
      BucketInfo bucketInfo = new BucketInfo();
      bucketInfo.bucketType = BucketType.UPDATE;
      bucketInfo.fileIdPrefix = fileIdHint;
      bucketInfo.partitionPath = partitionPath;
      bucketInfoMap.put(totalBuckets, bucketInfo);
      totalBuckets++;
      return bucket;
    }

    private void assignInserts(WorkloadProfile profile) {
      // for new inserts, compute buckets depending on how many records we have for each partition
      Set<String> partitionPaths = profile.getPartitionPaths();
      long averageRecordSize =
          averageBytesPerRecord(metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
              config.getCopyOnWriteRecordSizeEstimate());
      LOG.info("AvgRecordSize => " + averageRecordSize);

      Map<String, List<SmallFile>> partitionSmallFilesMap =
          getSmallFilesForPartitions(new ArrayList<String>(partitionPaths));

      for (String partitionPath : partitionPaths) {
        WorkloadStat pStat = profile.getWorkloadStat(partitionPath);
        if (pStat.getNumInserts() > 0) {

          List<SmallFile> smallFiles = partitionSmallFilesMap.get(partitionPath);
          this.smallFiles.addAll(smallFiles);

          LOG.info("For partitionPath : " + partitionPath + " Small Files => " + smallFiles);

          long totalUnassignedInserts = pStat.getNumInserts();
          List<Integer> bucketNumbers = new ArrayList<>();
          List<Long> recordsPerBucket = new ArrayList<>();

          // first try packing this into one of the smallFiles
          for (SmallFile smallFile : smallFiles) {
            long recordsToAppend = Math.min((config.getParquetMaxFileSize() - smallFile.sizeBytes) / averageRecordSize,
                totalUnassignedInserts);
            if (recordsToAppend > 0 && totalUnassignedInserts > 0) {
              // create a new bucket or re-use an existing bucket
              int bucket;
              if (updateLocationToBucket.containsKey(smallFile.location.getFileId())) {
                bucket = updateLocationToBucket.get(smallFile.location.getFileId());
                LOG.info("Assigning " + recordsToAppend + " inserts to existing update bucket " + bucket);
              } else {
                bucket = addUpdateBucket(partitionPath, smallFile.location.getFileId());
                LOG.info("Assigning " + recordsToAppend + " inserts to new update bucket " + bucket);
              }
              bucketNumbers.add(bucket);
              recordsPerBucket.add(recordsToAppend);
              totalUnassignedInserts -= recordsToAppend;
            }
          }

          // if we have anything more, create new insert buckets, like normal
          if (totalUnassignedInserts > 0) {
            long insertRecordsPerBucket = config.getCopyOnWriteInsertSplitSize();
            if (config.shouldAutoTuneInsertSplits()) {
              insertRecordsPerBucket = config.getParquetMaxFileSize() / averageRecordSize;
            }

            int insertBuckets = (int) Math.ceil((1.0 * totalUnassignedInserts) / insertRecordsPerBucket);
            LOG.info("After small file assignment: unassignedInserts => " + totalUnassignedInserts
                + ", totalInsertBuckets => " + insertBuckets + ", recordsPerBucket => " + insertRecordsPerBucket);
            for (int b = 0; b < insertBuckets; b++) {
              bucketNumbers.add(totalBuckets);
              recordsPerBucket.add(totalUnassignedInserts / insertBuckets);
              BucketInfo bucketInfo = new BucketInfo();
              bucketInfo.bucketType = BucketType.INSERT;
              bucketInfo.partitionPath = partitionPath;
              bucketInfo.fileIdPrefix = FSUtils.createNewFileIdPfx();
              bucketInfoMap.put(totalBuckets, bucketInfo);
              totalBuckets++;
            }
          }

          // Go over all such buckets, and assign weights as per amount of incoming inserts.
          List<InsertBucket> insertBuckets = new ArrayList<>();
          for (int i = 0; i < bucketNumbers.size(); i++) {
            InsertBucket bkt = new InsertBucket();
            bkt.bucketNumber = bucketNumbers.get(i);
            bkt.weight = (1.0 * recordsPerBucket.get(i)) / pStat.getNumInserts();
            insertBuckets.add(bkt);
          }
          LOG.info("Total insert buckets for partition path " + partitionPath + " => " + insertBuckets);
          partitionPathToInsertBuckets.put(partitionPath, insertBuckets);
        }
      }
    }



    private Map<String, List<SmallFile>> getSmallFilesForPartitions(List<String> partitionPaths) {
      Map<String, List<SmallFile>> partitionSmallFilesMap = new HashMap<>();
      return partitionSmallFilesMap;
    }

    /**
     * Returns a list of small files in the given partition path.
     */
    protected List<SmallFile> getSmallFiles(String partitionPath) {

      // smallFiles only for partitionPath
      List<SmallFile> smallFileLocations = new ArrayList<>();

      HoodieTimeline commitTimeline = getCompletedCommitsTimeline();

      if (!commitTimeline.empty()) { // if we have some commits
        HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
        List<HoodieBaseFile> allFiles = getBaseFileOnlyView()
            .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp()).collect(Collectors.toList());

        for (HoodieBaseFile file : allFiles) {
          if (file.getFileSize() < config.getParquetSmallFileLimit()) {
            String filename = file.getFileName();
            SmallFile sf = new SmallFile();
            sf.location = new HoodieRecordLocation(FSUtils.getCommitTime(filename), FSUtils.getFileId(filename));
            sf.sizeBytes = file.getFileSize();
            smallFileLocations.add(sf);
          }
        }
      }

      return smallFileLocations;
    }

    public BucketInfo getBucketInfo(int bucketNumber) {
      return bucketInfoMap.get(bucketNumber);
    }

    public List<InsertBucket> getInsertBuckets(String partitionPath) {
      return partitionPathToInsertBuckets.get(partitionPath);
    }

    @Override
    public int numPartitions() {
      return totalBuckets;
    }

    @Override
    public int getPartition(Object key) {
      Tuple2<HoodieKey, Option<HoodieRecordLocation>> keyLocation =
          (Tuple2<HoodieKey, Option<HoodieRecordLocation>>) key;
      if (keyLocation._2().isPresent()) {
        HoodieRecordLocation location = keyLocation._2().get();
        return updateLocationToBucket.get(location.getFileId());
      } else {
        List<InsertBucket> targetBuckets = partitionPathToInsertBuckets.get(keyLocation._1().getPartitionPath());
        // pick the target bucket to use based on the weights.
        double totalWeight = 0.0;
        final long totalInserts = Math.max(1, globalStat.getNumInserts());
        final long hashOfKey = NumericUtils.getMessageDigestHash("MD5", keyLocation._1().getRecordKey());
        final double r = 1.0 * Math.floorMod(hashOfKey, totalInserts) / totalInserts;
        for (InsertBucket insertBucket : targetBuckets) {
          totalWeight += insertBucket.weight;
          if (r <= totalWeight) {
            return insertBucket.bucketNumber;
          }
        }
        // return first one, by default
        return targetBuckets.get(0).bucketNumber;
      }
    }
  }
  /**
   * Obtains the average record size based on records written during previous commits. Used for estimating how many
   * records pack into one file.
   */
  protected static long averageBytesPerRecord(HoodieTimeline commitTimeline, int defaultRecordSizeEstimate) {
    long avgSize = defaultRecordSizeEstimate;
    try {
      if (!commitTimeline.empty()) {
        // Go over the reverse ordered commits to get a more recent estimate of average record size.
        Iterator<HoodieInstant> instants = commitTimeline.getReverseOrderedInstants().iterator();
        while (instants.hasNext()) {
          HoodieInstant instant = instants.next();
          HoodieCommitMetadata commitMetadata = HoodieCommitMetadata
              .fromBytes(commitTimeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
          long totalBytesWritten = commitMetadata.fetchTotalBytesWritten();
          long totalRecordsWritten = commitMetadata.fetchTotalRecordsWritten();
          if (totalBytesWritten > 0 && totalRecordsWritten > 0) {
            avgSize = (long) Math.ceil((1.0 * totalBytesWritten) / totalRecordsWritten);
            break;
          }
        }
      }
    } catch (Throwable t) {
      // make this fail safe.
      LOG.error("Error trying to compute average bytes/record ", t);
    }
    return avgSize;
  }
}
