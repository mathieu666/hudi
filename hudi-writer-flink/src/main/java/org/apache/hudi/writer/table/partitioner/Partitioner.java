package org.apache.hudi.writer.table.partitioner;

import java.io.Serializable;

public interface Partitioner extends Serializable {
  int numPartitions();

  int getPartition(Object key);
}
