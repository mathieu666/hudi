package org.apache.hudi.writer.table;

import org.apache.hudi.common.model.HoodieRecordPayload;

import java.io.Serializable;

/**
 * Abstract implementation of a HoodieTable.
 */
public abstract class HoodieTable<T extends HoodieRecordPayload> implements Serializable {
}
