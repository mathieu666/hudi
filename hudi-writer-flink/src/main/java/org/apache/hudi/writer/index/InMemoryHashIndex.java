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

package org.apache.hudi.writer.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.writer.WriteStatus;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.table.HoodieTable;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Hoodie Index implementation backed by an in-memory Hash map.
 * <p>
 * ONLY USE FOR LOCAL TESTING
 */
public class InMemoryHashIndex<T extends HoodieRecordPayload> extends HoodieIndex<T> {

  private static ConcurrentMap<HoodieKey, HoodieRecordLocation> recordLocationMap;

  public InMemoryHashIndex(HoodieWriteConfig config) {
    super(config);
    synchronized (InMemoryHashIndex.class) {
      if (recordLocationMap == null) {
        recordLocationMap = new ConcurrentHashMap<>();
      }
    }
  }

  @Override
  public List<HoodieKey> fetchRecordLocation(List<HoodieKey> hoodieKeys, Configuration hadoopConf, HoodieTable<T> hoodieTable) {
    return null;
  }

  @Override
  public List<HoodieRecord<T>> tagLocation(List<HoodieRecord<T>> recordRDD, Configuration hadoopConf, HoodieTable<T> hoodieTable) throws HoodieIndexException {
    return null;
  }

  @Override
  public List<WriteStatus> updateLocation(List<WriteStatus> writeStatusRDD, Configuration hadoopConf, HoodieTable<T> hoodieTable) throws HoodieIndexException {
    return null;
  }


  @Override
  public boolean rollbackCommit(String commitTime) {
    return true;
  }

  /**
   * Only looks up by recordKey.
   */
  @Override
  public boolean isGlobal() {
    return true;
  }

  /**
   * Mapping is available in HBase already.
   */
  @Override
  public boolean canIndexLogFiles() {
    return true;
  }

  /**
   * Index needs to be explicitly updated after storage write.
   */
  @Override
  public boolean isImplicitWithStorage() {
    return false;
  }

}

