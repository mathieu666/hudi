package org.apache.hudi.common;

public class HoodieWriteOutput<OUT> {

    private OUT output;

    public HoodieWriteOutput(OUT output) {
        this.output = output;
    }

    public OUT getOutput() {
        return output;
    }
}
