package org.apache.hudi.table;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.HoodieWriteMetadata;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCompactionPlan;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.ParquetReaderIterator;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.exception.HoodieUpsertException;
import org.apache.hudi.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieMergeHandle;
import org.apache.hudi.table.action.commit.UpsertCommitActionExecutor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetReader;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class HoodieCopyOnWriteTable<T extends HoodieRecordPayload> extends HoodieTable<T, HoodieWriteInput<List<HoodieRecord<T>>>, HoodieWriteKey<HoodieKey>,
    HoodieWriteOutput<List<WriteStatus>>> {

  private static final Logger LOG = LogManager.getLogger(HoodieCopyOnWriteTable.class);

  public HoodieCopyOnWriteTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient) {
    super(config, hadoopConf, metaClient);
  }

  @Override
  public HoodieWriteMetadata upsert(Configuration hadoopConf, String instantTime, HoodieWriteInput<List<HoodieRecord<T>>> records) {
    return new UpsertCommitActionExecutor(hadoopConf, config, this, instantTime, records).execute();
  }

  @Override
  public HoodieWriteMetadata insert(Configuration hadoopConf, String instantTime, HoodieWriteInput<List<HoodieRecord<T>>> records) {
    return null;
  }

  @Override
  public HoodieWriteMetadata bulkInsert(Configuration jsc, String instantTime, HoodieWriteInput<List<HoodieRecord<T>>> records, Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public HoodieWriteMetadata delete(Configuration hadoopConf, String instantTime, HoodieWriteKey<HoodieKey> keys) {
    return null;
  }

  @Override
  public HoodieWriteMetadata upsertPrepped(Configuration jsc, String instantTime, HoodieWriteInput<List<HoodieRecord<T>>> preppedRecords) {
    return null;
  }

  @Override
  public HoodieWriteMetadata insertPrepped(Configuration jsc, String instantTime, HoodieWriteInput<List<HoodieRecord<T>>> preppedRecords) {
    return null;
  }

  @Override
  public HoodieWriteMetadata bulkInsertPrepped(Configuration jsc, String instantTime, HoodieWriteInput<List<HoodieRecord<T>>> preppedRecords, Option<UserDefinedBulkInsertPartitioner> bulkInsertPartitioner) {
    return null;
  }

  @Override
  public HoodieCompactionPlan scheduleCompaction(Configuration jsc, String instantTime) {
    throw new HoodieNotSupportedException("Compaction is not supported from a CopyOnWrite table");
  }

  @Override
  public HoodieWriteOutput<List<WriteStatus>> compact(Configuration jsc, String compactionInstantTime, HoodieCompactionPlan compactionPlan) {
    throw new HoodieNotSupportedException("Compaction is not supported from a CopyOnWrite table");
  }

  public Iterator<List<WriteStatus>> handleUpdate(String instantTime, String partitionPath, String fileId,
                                                  Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile oldDataFile) throws IOException {
    // these are updates
    HoodieMergeHandle upsertHandle = getUpdateHandle(instantTime, partitionPath, fileId, keyToNewRecords, oldDataFile);
    return handleUpdateInternal(upsertHandle, instantTime, fileId);
  }

  protected Iterator<List<WriteStatus>> handleUpdateInternal(HoodieMergeHandle upsertHandle, String instantTime,
                                                             String fileId) throws IOException {
    if (upsertHandle.getOldFilePath() == null) {
      throw new HoodieUpsertException(
          "Error in finding the old file path at commit " + instantTime + " for fileId: " + fileId);
    } else {
      AvroReadSupport.setAvroReadSchema(getHadoopConf(), upsertHandle.getWriterSchema());
      BoundedInMemoryExecutor<GenericRecord, GenericRecord, Void> wrapper = null;
      try (ParquetReader<IndexedRecord> reader =
               AvroParquetReader.<IndexedRecord>builder(upsertHandle.getOldFilePath()).withConf(getHadoopConf()).build()) {
        wrapper = new SparkBoundedInMemoryExecutor(config, new ParquetReaderIterator(reader),
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

    // TODO(vc): This needs to be revisited
    if (upsertHandle.getWriteStatus().getPartitionPath() == null) {
      LOG.info("Upsert Handle has partition path as null " + upsertHandle.getOldFilePath() + ", "
          + upsertHandle.getWriteStatus());
    }
    return Collections.singletonList(Collections.singletonList(upsertHandle.getWriteStatus())).iterator();
  }

  protected HoodieMergeHandle getUpdateHandle(String instantTime, String partitionPath, String fileId,
                                              Map<String, HoodieRecord<T>> keyToNewRecords, HoodieBaseFile dataFileToBeMerged) {
    return new HoodieMergeHandle<>(config, instantTime, this, keyToNewRecords,
        partitionPath, fileId, dataFileToBeMerged, sparkTaskContextSupplier);
  }

  @Override
  public HoodieCleanMetadata clean(Configuration jsc, String cleanInstantTime) {
    return null;
  }

  @Override
  public HoodieRollbackMetadata rollback(Configuration jsc, String rollbackInstantTime, HoodieInstant commitInstant, boolean deleteInstants) {
    return null;
  }

  @Override
  public HoodieRestoreMetadata restore(Configuration jsc, String restoreInstantTime, String instantToRestore) {
    return null;
  }

  enum BucketType {
    UPDATE, INSERT
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
