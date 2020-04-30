package org.apache.hudi.common;

public class HoodieWriteKey<K> {

  private K keys;

  public HoodieWriteKey(K keys) {
    this.keys = keys;
  }

  public K getKeys() {
    return keys;
  }
}
