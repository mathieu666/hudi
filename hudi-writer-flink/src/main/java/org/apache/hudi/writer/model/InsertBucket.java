package org.apache.hudi.writer.model;

import java.io.Serializable;

/**
 * Helper class for an insert bucket along with the weight [0.0, 0.1] that defines the amount of incoming inserts that
 * should be allocated to the bucket.
 */
public class InsertBucket implements Serializable {

  private int bucketNumber;
  // fraction of total inserts, that should go into this bucket
  private double weight;

  public int getBucketNumber() {
    return bucketNumber;
  }

  public void setBucketNumber(int bucketNumber) {
    this.bucketNumber = bucketNumber;
  }

  public double getWeight() {
    return weight;
  }

  public void setWeight(double weight) {
    this.weight = weight;
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("WorkloadStat {");
    sb.append("bucketNumber=").append(bucketNumber).append(", ");
    sb.append("weight=").append(weight);
    sb.append('}');
    return sb.toString();
  }
}