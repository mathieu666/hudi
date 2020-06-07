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

package org.apache.hudi.table;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.TaskContextSupplier;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.AbstractHoodieIndex;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Implementation of a more real-time Hoodie Table the provides tradeoffs on read and write cost/amplification.
 *
 * <p>
 * INSERTS - Same as HoodieCopyOnWriteTable - Produce new files, block aligned to desired size (or) Merge with the
 * smallest existing file, to expand it
 * </p>
 * <p>
 * UPDATES - Appends the changes to a rolling log file maintained per file Id. Compaction merges the log file into the
 * base file.
 * </p>
 * <p>
 * WARNING - MOR table type does not support nested rollbacks, every rollback must be followed by an attempted commit
 * action
 * </p>
 */
public abstract class HoodieMergeOnReadTable<T extends HoodieRecordPayload<T>, I, K, O, P> extends HoodieCopyOnWriteTable<T, I, K, O, P> {

  private static final Logger LOG = LogManager.getLogger(HoodieMergeOnReadTable.class);

  public HoodieMergeOnReadTable(HoodieWriteConfig config, Configuration hadoopConf, HoodieTableMetaClient metaClient, AbstractHoodieIndex<T, I, K, O, P> inedx, TaskContextSupplier taskContextSupplier) {
    super(config, hadoopConf, metaClient, inedx, taskContextSupplier);
  }

  public abstract void finalizeWrite(HoodieEngineContext context, String instantTs, List<HoodieWriteStat> stats);
}
