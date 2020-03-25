package org.apache.hudi.writer.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.exception.HoodieDependentSystemUnavailableException;
import org.apache.hudi.writer.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


/**
 * HoodieHBaseIndex.
 */
public class HoodieHBaseIndex implements Serializable {
  private static final byte[] SYSTEM_COLUMN_FAMILY = Bytes.toBytes("_s");
  private static final byte[] COMMIT_TS_COLUMN = Bytes.toBytes("commit_ts");
  private static final byte[] FILE_NAME_COLUMN = Bytes.toBytes("file_name");
  private static final byte[] PARTITION_PATH_COLUMN = Bytes.toBytes("partition_path");
  private static final int SLEEP_TIME_MILLISECONDS = 100;

  private static final Logger LOG = LogManager.getLogger(HoodieHBaseIndex.class);
  private static Connection hbaseConnection = null;

  private Integer multiPutBatchSize;
  private String tableName;
  private String quorum;
  private String port;
  private static int multiGetBatchSize = 100;

  public HoodieHBaseIndex(HoodieWriteConfig config) {
    this.tableName = config.getHbaseTableName();
    this.quorum = config.getHbaseZkQuorum();
    this.port = String.valueOf(config.getHbaseZkPort());
    this.multiPutBatchSize = config.getHbaseIndexGetBatchSize();
    addShutDownHook();
  }

  private Connection getHBaseConnection() {
    Configuration hbaseConfig = HBaseConfiguration.create();
    hbaseConfig.set("hbase.zookeeper.quorum", quorum);
    hbaseConfig.set("hbase.zookeeper.property.clientPort", port);
    try {
      return ConnectionFactory.createConnection(hbaseConfig);
    } catch (IOException e) {
      throw new HoodieDependentSystemUnavailableException(HoodieDependentSystemUnavailableException.HBASE,
          quorum + ":" + port);
    }
  }

  /**
   * Since we are sharing the HBaseConnection across tasks in a JVM, make sure the HBaseConnection is closed when JVM
   * exits.
   */
  private void addShutDownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        hbaseConnection.close();
      } catch (Exception e) {
        // fail silently for any sort of exception
      }
    }));
  }

  /**
   * Looks up the index and tags each incoming record with a location of a file that contains the row (if it is actually
   * present).
   */
  public List<HoodieRecord> tagLocation(Iterable<HoodieRecord> inputs, HoodieTable hoodieTable) {

    // Grab the global HBase connection
    synchronized (HoodieHBaseIndex.class) {
      if (hbaseConnection == null || hbaseConnection.isClosed()) {
        hbaseConnection = getHBaseConnection();
      }
    }
    List<HoodieRecord> taggedRecords = new ArrayList<>();
    try (HTable hTable = (HTable) hbaseConnection.getTable(TableName.valueOf(tableName))) {
      List<Get> statements = new ArrayList<>();
      List<HoodieRecord> currentBatchOfRecords = new LinkedList<>();
      // Do the tagging.
      Iterator<HoodieRecord> hoodieRecordIterator = inputs.iterator();
      while (hoodieRecordIterator.hasNext()) {
        HoodieRecord rec = hoodieRecordIterator.next();
        statements.add(generateStatement(rec.getRecordKey()));
        currentBatchOfRecords.add(rec);
        // iterator till we reach batch size
        if (statements.size() >= multiGetBatchSize || !hoodieRecordIterator.hasNext()) {
          // get results for batch from Hbase
          Result[] results = doGet(hTable, statements);
          // clear statements to be GC'd
          statements.clear();
          for (Result result : results) {
            // first, attempt to grab location from HBase
            HoodieRecord currentRecord = currentBatchOfRecords.remove(0);
            if (result.getRow() != null) {
              String keyFromResult = Bytes.toString(result.getRow());
              String commitTs = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN));
              String fileId = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN));
              String partitionPath = Bytes.toString(result.getValue(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN));

              if (checkIfValidCommit(hoodieTable.getMetaClient(), commitTs)) {
                currentRecord = new HoodieRecord(new HoodieKey(currentRecord.getRecordKey(), partitionPath),
                    currentRecord.getData());
                currentRecord.unseal();
                currentRecord.setCurrentLocation(new HoodieRecordLocation(commitTs, fileId));
                currentRecord.seal();
                taggedRecords.add(currentRecord);
                // the key from Result and the key being processed should be same
                assert (currentRecord.getRecordKey().contentEquals(keyFromResult));
              } else { // if commit is invalid, treat this as a new taggedRecord
                taggedRecords.add(currentRecord);
              }
            } else {
              taggedRecords.add(currentRecord);
            }
          }
        }
      }
    } catch (IOException e) {
      throw new HoodieIndexException("Failed to Tag indexed locations because of exception with HBase Client", e);
    }
    return taggedRecords;
  }

  private Get generateStatement(String key) throws IOException {
    return new Get(Bytes.toBytes(key)).setMaxVersions(1).addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN)
        .addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN).addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN);
  }

  private Result[] doGet(HTable hTable, List<Get> keys) throws IOException {
    sleepForTime(SLEEP_TIME_MILLISECONDS);
    return hTable.get(keys);
  }

  private static void sleepForTime(int sleepTimeMs) {
    try {
      Thread.sleep(sleepTimeMs);
    } catch (InterruptedException e) {
      LOG.error("Sleep interrupted during throttling", e);
      throw new RuntimeException(e);
    }
  }

  private boolean checkIfValidCommit(HoodieTableMetaClient metaClient, String commitTs) {
    HoodieTimeline commitTimeline = metaClient.getActiveTimeline().filterCompletedInstants();
    // Check if the last commit ts for this row is 1) present in the timeline or
    // 2) is less than the first commit ts in the timeline
    return !commitTimeline.empty()
        && (commitTimeline.containsInstant(new HoodieInstant(false, HoodieTimeline.COMMIT_ACTION, commitTs))
        || HoodieTimeline.compareTimestamps(commitTimeline.firstInstant().get().getTimestamp(), commitTs,
        HoodieTimeline.GREATER));
  }

  /**
   * Extracts the location of written records, and updates the index.
   * <p>
   */
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatuss, HoodieTable hoodieTable) {
    List<WriteStatus> writeStatusList = new ArrayList<>();

    // Grab the global HBase connection
    synchronized (HoodieHBaseIndex.class) {
      if (hbaseConnection == null || hbaseConnection.isClosed()) {
        hbaseConnection = getHBaseConnection();
      }

      Iterator<WriteStatus> statusIterator = writeStatuss.iterator();
      try (BufferedMutator mutator = hbaseConnection.getBufferedMutator(TableName.valueOf(tableName))) {
        while (statusIterator.hasNext()) {
          WriteStatus writeStatus = statusIterator.next();
          List<Mutation> mutations = new ArrayList<>();
          try {
            for (HoodieRecord rec : writeStatus.getWrittenRecords()) {
              if (!writeStatus.isErrored(rec.getKey())) {
                Option<HoodieRecordLocation> loc = rec.getNewLocation();
                if (loc.isPresent()) {
                  if (rec.getCurrentLocation() != null) {
                    // This is an update, no need to update index
                    continue;
                  }
                  Put put = new Put(Bytes.toBytes(rec.getRecordKey()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, COMMIT_TS_COLUMN, Bytes.toBytes(loc.get().getInstantTime()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, FILE_NAME_COLUMN, Bytes.toBytes(loc.get().getFileId()));
                  put.addColumn(SYSTEM_COLUMN_FAMILY, PARTITION_PATH_COLUMN, Bytes.toBytes(rec.getPartitionPath()));
                  mutations.add(put);
                } else {
                  // Delete existing index for a deleted record
                  Delete delete = new Delete(Bytes.toBytes(rec.getRecordKey()));
                  mutations.add(delete);
                }
              }
              if (mutations.size() < multiPutBatchSize) {
                continue;
              }
              doMutations(mutator, mutations);
            }
            // process remaining puts and deletes, if any
            doMutations(mutator, mutations);
          } catch (Exception e) {
            Exception we = new Exception("Error updating index for " + writeStatus, e);
            LOG.error(we);
            writeStatus.setGlobalError(we);
          }
          writeStatusList.add(writeStatus);
        }
      } catch (IOException e) {
        throw new HoodieIndexException("Failed to Update Index locations because of exception with HBase Client", e);
      }
    }
    return writeStatusList;
  }

  /**
   * Helper method to facilitate performing mutations (including puts and deletes) in Hbase.
   */
  private void doMutations(BufferedMutator mutator, List<Mutation> mutations) throws IOException {
    if (mutations.isEmpty()) {
      return;
    }
    mutator.mutate(mutations);
    mutator.flush();
    mutations.clear();
    sleepForTime(SLEEP_TIME_MILLISECONDS);
  }

  public boolean canIndexLogFiles() {
    return true;
  }

  public boolean isImplicitWithStorage() {
    return false;
  }

  public boolean rollbackCommit(String instantTime) {
    // Rollback in HbaseIndex is managed via method {@link #checkIfValidCommit()}
    return true;
  }
}
