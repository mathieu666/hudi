package org.apache.hudi.writer.partitioner;

import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.FSUtils;
import org.apache.hudi.common.util.NumericUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.model.BucketInfo;
import org.apache.hudi.writer.model.BucketType;
import org.apache.hudi.writer.model.InsertBucket;
import org.apache.hudi.writer.model.SmallFile;
import org.apache.hudi.writer.table.HoodieTable;
import org.apache.hudi.writer.table.WorkloadProfile;
import org.apache.hudi.writer.table.WorkloadStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * PartitionHelper.
 */
public class PartitionHelper {
  private static final Logger LOG = LoggerFactory.getLogger(PartitionHelper.class);

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

  private HoodieTable hoodieTable;

  private HoodieWriteConfig config;

  public PartitionHelper(WorkloadProfile profile, HoodieTable table) {
    updateLocationToBucket = new HashMap<>();
    partitionPathToInsertBuckets = new HashMap<>();
    bucketInfoMap = new HashMap<>();
    globalStat = profile.getGlobalStat();
    hoodieTable = table;
    config = table.getConfig();
    assignUpdates(profile);
    assignInserts(profile);

    LOG.info("Total Buckets :" + totalBuckets + ", buckets info => " + bucketInfoMap + ", \n"
        + "Partition to insert buckets => " + partitionPathToInsertBuckets + ", \n"
        + "UpdateLocations mapped to buckets =>" + updateLocationToBucket);
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
    bucketInfo.setBucketType(BucketType.UPDATE);
    bucketInfo.setFileIdPrefix(fileIdHint);
    bucketInfo.setPartitionPath(partitionPath);
    bucketInfoMap.put(totalBuckets, bucketInfo);
    totalBuckets++;
    return bucket;
  }

  private void assignInserts(WorkloadProfile profile) {
    // for new inserts, compute buckets depending on how many records we have for each partition
    Set<String> partitionPaths = profile.getPartitionPaths();
    long averageRecordSize =
        averageBytesPerRecord(hoodieTable.getMetaClient().getActiveTimeline().getCommitTimeline().filterCompletedInstants(),
            config.getCopyOnWriteRecordSizeEstimate());
    LOG.info("AvgRecordSize => " + averageRecordSize);
    for (String partitionPath : partitionPaths) {
      WorkloadStat pStat = profile.getWorkloadStat(partitionPath);
      if (pStat.getNumInserts() > 0) {

        List<SmallFile> smallFiles = getSmallFiles(partitionPath);
        LOG.info("For partitionPath : " + partitionPath + " Small Files => " + smallFiles);

        long totalUnassignedInserts = pStat.getNumInserts();
        List<Integer> bucketNumbers = new ArrayList<>();
        List<Long> recordsPerBucket = new ArrayList<>();

        // first try packing this into one of the smallFiles
        for (SmallFile smallFile : smallFiles) {
          long recordsToAppend = Math.min((config.getParquetMaxFileSize() - smallFile.getSizeBytes()) / averageRecordSize,
              totalUnassignedInserts);
          if (recordsToAppend > 0 && totalUnassignedInserts > 0) {
            // create a new bucket or re-use an existing bucket
            int bucket;
            if (updateLocationToBucket.containsKey(smallFile.getLocation().getFileId())) {
              bucket = updateLocationToBucket.get(smallFile.getLocation().getFileId());
              LOG.info("Assigning " + recordsToAppend + " inserts to existing update bucket " + bucket);
            } else {
              bucket = addUpdateBucket(partitionPath, smallFile.getLocation().getFileId());
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
            bucketInfo.setBucketType(BucketType.INSERT);
            bucketInfo.setPartitionPath(partitionPath);
            bucketInfo.setFileIdPrefix(FSUtils.createNewFileIdPfx());
            bucketInfoMap.put(totalBuckets, bucketInfo);
            totalBuckets++;
          }
        }

        // Go over all such buckets, and assign weights as per amount of incoming inserts.
        List<InsertBucket> insertBuckets = new ArrayList<>();
        for (int i = 0; i < bucketNumbers.size(); i++) {
          InsertBucket bkt = new InsertBucket();
          bkt.setBucketNumber(bucketNumbers.get(i));
          bkt.setWeight((1.0 * recordsPerBucket.get(i)) / pStat.getNumInserts());
          insertBuckets.add(bkt);
        }
        LOG.info("Total insert buckets for partition path " + partitionPath + " => " + insertBuckets);
        partitionPathToInsertBuckets.put(partitionPath, insertBuckets);
      }
    }
  }

  /**
   * Returns a list of small files in the given partition path.
   */
  public List<SmallFile> getSmallFiles(String partitionPath) {

    // smallFiles only for partitionPath
    List<SmallFile> smallFileLocations = new ArrayList<>();

    HoodieTimeline commitTimeline = getCompletedCommitsTimeline();

    if (!commitTimeline.empty()) { // if we have some commits
      HoodieInstant latestCommitTime = commitTimeline.lastInstant().get();
      List<HoodieBaseFile> allFiles = hoodieTable.getBaseFileOnlyView()
          .getLatestBaseFilesBeforeOrOn(partitionPath, latestCommitTime.getTimestamp()).collect(Collectors.toList());

      for (HoodieBaseFile file : allFiles) {
        if (file.getFileSize() < config.getParquetSmallFileLimit()) {
          String filename = file.getFileName();
          SmallFile sf = new SmallFile();
          sf.setLocation(new HoodieRecordLocation(FSUtils.getCommitTime(filename), FSUtils.getFileId(filename)));
          sf.setSizeBytes(file.getFileSize());
          smallFileLocations.add(sf);
          // Update the global small files list
          smallFiles.add(sf);
        }
      }
    }

    return smallFileLocations;
  }

  public List<String> getSmallFileIds() {
    return (List<String>) smallFiles.stream().map(smallFile -> ((SmallFile) smallFile).getLocation().getFileId())
        .collect(Collectors.toList());
  }

  /**
   * Get only the completed (no-inflights) commit + deltacommit timeline.
   */
  public HoodieTimeline getCompletedCommitsTimeline() {
    return hoodieTable.getMetaClient().getCommitsTimeline().filterCompletedInstants();
  }

  public BucketInfo getBucketInfo(int bucketNumber) {
    return bucketInfoMap.get(bucketNumber);
  }

  public List<InsertBucket> getInsertBuckets(String partitionPath) {
    return partitionPathToInsertBuckets.get(partitionPath);
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

  public int numPartitions() {
    return totalBuckets;
  }

  public int getPartition(Object key) {
    Pair<HoodieKey, Option<HoodieRecordLocation>> keyLocation =
        (Pair<HoodieKey, Option<HoodieRecordLocation>>) key;
    if (keyLocation.getRight().isPresent()) {
      HoodieRecordLocation location = keyLocation.getRight().get();
      return updateLocationToBucket.get(location.getFileId());
    } else {
      List<InsertBucket> targetBuckets = partitionPathToInsertBuckets.get(keyLocation.getLeft().getPartitionPath());
      // pick the target bucket to use based on the weights.
      double totalWeight = 0.0;
      final long totalInserts = Math.max(1, globalStat.getNumInserts());
      final long hashOfKey = NumericUtils.getMessageDigestHash("MD5", keyLocation.getLeft().getRecordKey());
      final double r = 1.0 * Math.floorMod(hashOfKey, totalInserts) / totalInserts;
      for (InsertBucket insertBucket : targetBuckets) {
        totalWeight += insertBucket.getWeight();
        if (r <= totalWeight) {
          return insertBucket.getBucketNumber();
        }
      }
      // return first one, by default
      return targetBuckets.get(0).getBucketNumber();
    }
  }
}
