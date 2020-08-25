package org.apache.hudi.table.action.commit;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.io.HoodieSparkMergeHandle;

public class SparkUpdateHandler extends BaseUpdateHandler {


  public SparkUpdateHandler(HoodieSparkMergeHandle upsertHandle) {
    super(upsertHandle);
  }

  @Override
  protected void consumeOneRecord(GenericRecord record) {
    HoodieSparkMergeHandle mergeHandle = (HoodieSparkMergeHandle) this.upsertHandle;
    mergeHandle.write(record);
  }
}
