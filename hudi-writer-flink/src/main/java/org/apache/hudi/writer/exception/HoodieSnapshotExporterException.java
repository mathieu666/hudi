package org.apache.hudi.writer.exception;

import org.apache.hudi.exception.HoodieException;

public class HoodieSnapshotExporterException extends HoodieException {

  public HoodieSnapshotExporterException(String msg) {
    super(msg);
  }
}
