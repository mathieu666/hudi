package org.apache.hudi.index;

import org.apache.hudi.HoodieEngineContext;
import org.apache.hudi.table.HoodieTable;

public interface HoodieIndexV2<IN, OUT> {
  IN fetchRecordLocation(IN inputs);

  IN tagLocation(IN inputs, HoodieEngineContext context, HoodieTable table);

  OUT updateLocation(IN inputs);
}
