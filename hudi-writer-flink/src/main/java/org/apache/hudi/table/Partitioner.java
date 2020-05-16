package org.apache.hudi.table;

import java.io.Serializable;

public interface Partitioner extends Serializable {
  int numPartitions();

  int getPartition(Object key);
}
