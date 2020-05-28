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

package org.apache.hudi.index;

import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteKey;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.table.HoodieTable;

import java.io.Serializable;

public abstract class HoodieIndexV2<T extends HoodieRecordPayload,
    I extends HoodieWriteInput, K extends HoodieWriteKey, O extends HoodieWriteOutput, P> implements Serializable {

  protected final HoodieWriteConfig config;

  protected HoodieIndexV2(HoodieWriteConfig config) {
    this.config = config;
  }

  public abstract P fetchRecordLocation(I inputs, final HoodieEngineContext context, HoodieTable<T, I, K, O, P> hoodieTable);

  public abstract I tagLocation(I inputs, HoodieEngineContext context, HoodieTable<T, I, K, O, P> table);

  public abstract O updateLocation(O inputs, HoodieEngineContext context, HoodieTable<T, I, K, O, P> table);

  /**
   * Rollback the efffects of the commit made at commitTime.
   */
  public abstract boolean rollbackCommit(String commitTime);

  /**
   * An index is `global` if {@link HoodieKey} to fileID mapping, does not depend on the `partitionPath`. Such an
   * implementation is able to obtain the same mapping, for two hoodie keys with same `recordKey` but different
   * `partitionPath`
   *
   * @return whether or not, the index implementation is global in nature
   */
  public abstract boolean isGlobal();

  /**
   * This is used by storage to determine, if its safe to send inserts, straight to the log, i.e having a
   * {@link FileSlice}, with no data file.
   *
   * @return Returns true/false depending on whether the impl has this capability
   */
  public abstract boolean canIndexLogFiles();

  /**
   * An index is "implicit" with respect to storage, if just writing new data to a file slice, updates the index as
   * well. This is used by storage, to save memory footprint in certain cases.
   */
  public abstract boolean isImplicitWithStorage();

  /**
   * Each index type should implement it's own logic to release any resources acquired during the process.
   */
  public void close() {
  }

  public enum IndexType {
    HBASE, INMEMORY, BLOOM, GLOBAL_BLOOM
  }

}
