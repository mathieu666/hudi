package org.apache.hudi.writer.model;

import java.io.Serializable;

/**
 * Helper class for a bucket's type (INSERT and UPDATE) and its file location.
 */
public class BucketInfo implements Serializable {

  private BucketType bucketType;
  private String fileIdPrefix;
  private String partitionPath;

  public BucketType getBucketType() {
    return bucketType;
  }

  public void setBucketType(BucketType bucketType) {
    this.bucketType = bucketType;
  }

  public String getFileIdPrefix() {
    return fileIdPrefix;
  }

  public void setFileIdPrefix(String fileIdPrefix) {
    this.fileIdPrefix = fileIdPrefix;
  }

  public String getPartitionPath() {
    return partitionPath;
  }

  public void setPartitionPath(String partitionPath) {
    this.partitionPath = partitionPath;
  }

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