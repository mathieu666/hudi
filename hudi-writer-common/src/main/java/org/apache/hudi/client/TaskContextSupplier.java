package org.apache.hudi.client;

public interface TaskContextSupplier {
  Integer getPartitionId();

  Integer getStageId();

  Long getAttempId();
}
