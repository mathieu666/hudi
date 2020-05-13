package org.apache.hudi;

import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;

public interface HoodieWriteClientV2<INPUT extends HoodieWriteInput, Key extends HoodieWriteKey, OUTPUT extends HoodieWriteOutput> {

  OUTPUT upsert(INPUT hoodieRecords, final String instantTime);

  OUTPUT upsertPreppedRecords(INPUT preppedRecords, final String instantTime);

  INPUT filterExists(INPUT hoodieRecords);

  OUTPUT insert(INPUT records, final String instantTime);

  OUTPUT insertPreppedRecords(INPUT preppedRecords, final String instantTime);

  OUTPUT bulkInsert(INPUT records, final String instantTime);

  OUTPUT delete(Key keys, final String instantTime);

  HoodieEngineContext<INPUT, OUTPUT> getEngineContext();

  String startCommit();

}
