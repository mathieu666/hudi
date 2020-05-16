package org.apache.hudi.format;

public class HoodieWriteOutput<O> {

  private O output;

  public HoodieWriteOutput(O output) {
    this.output = output;
  }

  public O getOutput() {
    return output;
  }
}
