package org.apache.hudi.writer.utils;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

/**
 * Run one round of compaction.
 */
public class Compactor implements Serializable {

  private static final Logger LOG = LogManager.getLogger(Compactor.class);
  private transient HoodieWriteClient compactionClient;
  public Compactor() {
  }

  public void compact(HoodieInstant instant) throws IOException {
    LOG.info("Compactor executing compaction " + instant);
    List<WriteStatus> res = compactionClient.compact(instant.getTimestamp());
    long numWriteErrors = res.stream().filter(WriteStatus::hasErrors).count();
    if (numWriteErrors != 0) {
      // We treat even a single error in compaction as fatal
      LOG.error("Compaction for instant (" + instant + ") failed with write errors. Errors :" + numWriteErrors);
      throw new HoodieException(
          "Compaction for instant (" + instant + ") failed with write errors. Errors :" + numWriteErrors);
    }
    // Commit compaction
    compactionClient.commitCompaction(instant.getTimestamp(), res, Option.empty());
  }
}

