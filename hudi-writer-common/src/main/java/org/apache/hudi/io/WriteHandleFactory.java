/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.io;

import org.apache.hudi.client.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.context.HoodieEngineContext;
import org.apache.hudi.format.HoodieWriteInput;
import org.apache.hudi.format.HoodieWriteKey;
import org.apache.hudi.format.HoodieWriteOutput;
import org.apache.hudi.table.HoodieTable;

public abstract class WriteHandleFactory<T extends HoodieRecordPayload, C extends HoodieEngineContext, I extends HoodieWriteInput, K extends HoodieWriteKey, O extends HoodieWriteOutput, P> {
  private int numFilesWritten = 0;

  public abstract HoodieWriteHandle<T, C, I, K, O, P> create(HoodieWriteConfig config, String commitTime, HoodieTable<T, C, I, K, O, P> hoodieTable,
                                                             String partitionPath, String fileIdPrefix, TaskContextSupplier taskContextSupplier);

  protected String getNextFileId(String idPfx) {
    return String.format("%s-%d", idPfx, numFilesWritten++);
  }
}