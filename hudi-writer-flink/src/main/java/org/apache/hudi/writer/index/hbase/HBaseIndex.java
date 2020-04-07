package org.apache.hudi.writer.index.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.index.HoodieIndex;
import org.apache.hudi.writer.table.HoodieTable;

import java.util.List;

/**
 * @author xianghu.wang
 * @time 2020/4/7
 * @description
 */
public class HBaseIndex <T extends HoodieRecordPayload> extends HoodieIndex<T> {
  private final String tableName;
  private static Connection hbaseConnection = null;
  public HBaseIndex(HoodieWriteConfig config) {
    super(config);
    this.tableName = config.getHbaseTableName();
    addShutDownHook();
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

  @Override
  public List<HoodieKey> fetchRecordLocation(List<HoodieKey> hoodieKeys, Configuration hadoopConf, HoodieTable<T> hoodieTable) {
    return null;
  }

  @Override
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> recordRDD, Configuration hadoopConf, HoodieTable<T> hoodieTable) throws HoodieIndexException {
    return null;
  }

  @Override
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatusRDD, Configuration hadoopConf, HoodieTable<T> hoodieTable) throws HoodieIndexException {
    return null;
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    return false;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }
}
