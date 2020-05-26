package org.apache.hudi.table.action.commit;

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;

/**
 * @author xianghu.wang
 * @time 2020/5/18
 * @description
 */
public class FlinkWriteHelper<T extends HoodieRecordPayload, I extends HoodieWriteInput, K extends HoodieWriteKey, O extends HoodieWriteOutput> extends WriteHelper<T, I, K, O> {

}
