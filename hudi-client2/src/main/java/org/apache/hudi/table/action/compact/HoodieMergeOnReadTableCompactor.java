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

package org.apache.hudi.table.action.compact;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.CompactionOperation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieCopyOnWriteTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Compacts a hoodie table with merge on read storage. Computes all possible compactions,
 * passes it through a CompactionFilter and executes all the compactions and writes a new version of base files and make
 * a normal commit
 *
 */
public abstract class HoodieMergeOnReadTableCompactor<T extends HoodieRecordPayload<T>, I, K, O, P> implements HoodieCompactor<T, I, K, O, P> {

  private static final Logger LOG = LogManager.getLogger(HoodieMergeOnReadTableCompactor.class);
  // Accumulator to keep track of total log files for a table
//  private AccumulatorV2<Long, Long> totalLogFiles;
  // Accumulator to keep track of total log file slices for a table
//  private AccumulatorV2<Long, Long> totalFileSlices;

  public abstract List<WriteStatus> compact(HoodieCopyOnWriteTable hoodieCopyOnWriteTable, HoodieTableMetaClient metaClient,
                                            HoodieWriteConfig config, CompactionOperation operation, String instantTime) throws IOException;


}
