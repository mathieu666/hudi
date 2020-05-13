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

package org.apache.hudi.writer.table;

import org.apache.hudi.HoodieEngineContext;
import org.apache.hudi.common.model.*;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Implementation of a very heavily read-optimized Hoodie Table where, all data is stored in base files, with
 * zero read amplification.
 *
 * <p>
 * INSERTS - Produce new files, block aligned to desired size (or) Merge with the smallest existing file, to expand it
 * <p>
 * UPDATES - Produce a new version of the file, just replacing the updated records with new values
 */
public class HoodieFlinkCopyOnWriteTable<T extends HoodieRecordPayload> extends HoodieTableV2<T> {

  private static final Logger LOG = LogManager.getLogger(HoodieFlinkCopyOnWriteTable.class);

  public HoodieFlinkCopyOnWriteTable(HoodieWriteConfig config, HoodieTableMetaClient metaClient, HoodieEngineContext context) {
    super(config,metaClient,context);
  }


}
