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

package org.apache.hudi.table.action.commit;

import org.apache.hudi.HoodieEngineContext;
import org.apache.hudi.HoodieWriteMetadata;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieUpsertException;

import java.time.Duration;
import java.time.Instant;

public class WriteHelper<T extends HoodieRecordPayload<T>> {

  public static <T extends HoodieRecordPayload<T>> HoodieWriteMetadata write(String instantTime,
                                                                             HoodieWriteInput inputRecordsRDD, HoodieEngineContext context,
                                                                             HoodieTableV2 table, boolean shouldCombine,
                                                                             int shuffleParallelism, CommitActionExecutor<T> executor, boolean performTagging) {
    try {
      // De-dupe/merge if needed
      HoodieWriteInput dedupedRecords =
          context.combineOnCondition(shouldCombine, inputRecordsRDD, shuffleParallelism, table);

      Instant lookupBegin = Instant.now();
      HoodieWriteInput taggedRecords = dedupedRecords;
      if (performTagging) {
        // perform index loop up to get existing location of records
        taggedRecords = context.tag(dedupedRecords, context, table);
      }
      Duration indexLookupDuration = Duration.between(lookupBegin, Instant.now());

      HoodieWriteMetadata result = executor.execute(taggedRecords);
      result.setIndexLookupDuration(indexLookupDuration);
      return result;
    } catch (Throwable e) {
      if (e instanceof HoodieUpsertException) {
        throw (HoodieUpsertException) e;
      }
      throw new HoodieUpsertException("Failed to upsert for commit time " + instantTime, e);
    }
  }

}
