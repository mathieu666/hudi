package org.apache.hudi.writer.client;

import org.apache.hudi.common.model.HoodieRecordPayload;

/**
 *  Abstract Write Client providing functionality for performing commit, index updates and rollback
 *  Reused for regular write operations like upsert/insert/bulk-insert.. as well as bootstrap
 * @param <T> Sub type of HoodieRecordPayload
 */
public class AbstractHoodieWriteClient <T extends HoodieRecordPayload> extends AbstractHoodieClient {
}
