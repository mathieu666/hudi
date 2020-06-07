package org.apache.hudi.execution;

import org.apache.avro.Schema;
import org.apache.hudi.client.TaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.io.WriteHandleFactory;
import org.apache.hudi.table.HoodieTable;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Iterator;
import java.util.List;

public class SparkLazyInsertIterable<T extends HoodieRecordPayload<T>> extends LazyInsertIterable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> {
  public SparkLazyInsertIterable(Iterator sortedRecordItr, HoodieWriteConfig config, String instantTime, HoodieTable hoodieTable, String idPrefix, TaskContextSupplier taskContextSupplier, WriteHandleFactory writeHandleFactory) {
    super(sortedRecordItr, config, instantTime, hoodieTable, idPrefix, taskContextSupplier, writeHandleFactory);
  }

  @Override
  protected List<WriteStatus> computeNext() {
    // Executor service used for launching writer thread.
    BoundedInMemoryExecutor<HoodieRecord<T>, HoodieInsertValueGenResult<HoodieRecord>, List<WriteStatus>> bufferedIteratorExecutor =
        null;
    try {
      final Schema schema = new Schema.Parser().parse(hoodieConfig.getSchema());
      bufferedIteratorExecutor =
          new SparkBoundedInMemoryExecutor<>(hoodieConfig, inputItr, getInsertHandler(), getTransformFunction(schema));
      final List<WriteStatus> result = bufferedIteratorExecutor.execute();
      assert result != null && !result.isEmpty() && !bufferedIteratorExecutor.isRemaining();
      return result;
    } catch (Exception e) {
      throw new HoodieException(e);
    } finally {
      if (null != bufferedIteratorExecutor) {
        bufferedIteratorExecutor.shutdownNow();
      }
    }
  }
}
