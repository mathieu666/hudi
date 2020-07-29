package org.apache.hudi.writer.common;

public class HoodieWriteInput<I> {

  private I inputs;

  public HoodieWriteInput(I inputs) {
    this.inputs = inputs;
  }

  public I getInputs() {
    return inputs;
  }
}
