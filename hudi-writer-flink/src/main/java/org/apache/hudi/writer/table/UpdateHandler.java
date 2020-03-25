package org.apache.hudi.writer.table;

import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.common.util.queue.BoundedInMemoryQueueConsumer;
import org.apache.hudi.writer.io.HoodieMergeHandle;

/**
 * Consumer that dequeues records from queue and sends to Merge Handle.
 */
public class UpdateHandler extends BoundedInMemoryQueueConsumer<GenericRecord, Void> {

  private final HoodieMergeHandle upsertHandle;

  public UpdateHandler(HoodieMergeHandle upsertHandle) {
    this.upsertHandle = upsertHandle;
  }

  @Override
  protected void consumeOneRecord(GenericRecord record) {
    upsertHandle.write(record);
  }

  @Override
  protected void finish() {}

  @Override
  protected Void getResult() {
    return null;
  }
}