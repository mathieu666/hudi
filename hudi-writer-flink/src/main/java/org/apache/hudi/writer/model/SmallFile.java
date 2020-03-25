package org.apache.hudi.writer.model;

import org.apache.hudi.common.model.HoodieRecordLocation;

import java.io.Serializable;

/**
 * Helper class for a small file's location and its actual size on disk.
 */
public class SmallFile implements Serializable {

  private HoodieRecordLocation location;
  private long sizeBytes;

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("SmallFile {");
    sb.append("location=").append(location).append(", ");
    sb.append("sizeBytes=").append(sizeBytes);
    sb.append('}');
    return sb.toString();
  }

  public HoodieRecordLocation getLocation() {
    return location;
  }

  public void setLocation(HoodieRecordLocation location) {
    this.location = location;
  }

  public long getSizeBytes() {
    return sizeBytes;
  }

  public void setSizeBytes(long sizeBytes) {
    this.sizeBytes = sizeBytes;
  }
}