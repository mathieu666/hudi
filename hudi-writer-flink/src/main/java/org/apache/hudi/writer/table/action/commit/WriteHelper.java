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

package org.apache.hudi.writer.table.action.commit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.writer.common.HoodieWriteInput;
import org.apache.hudi.writer.exception.HoodieUpsertException;
import org.apache.hudi.writer.index.HoodieIndex;
import org.apache.hudi.writer.table.HoodieTable;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class WriteHelper<T extends HoodieRecordPayload<T>> {

  public static <T extends HoodieRecordPayload<T>> HoodieWriteMetadata write(String instantTime,
                                                                             HoodieWriteInput<List<HoodieRecord<T>>> inputRecordsRDD, Configuration hadoopConf,
                                                                             HoodieTable<T> table, boolean shouldCombine,
                                                                             int shuffleParallelism, CommitActionExecutor<T> executor, boolean performTagging) {
    try {
      Instant lookupBegin = Instant.now();
      HoodieWriteInput<List<HoodieRecord<T>>> taggedRecords = inputRecordsRDD;
      if (performTagging) {
        // perform index loop up to get existing location of records
        long start = System.currentTimeMillis();
        taggedRecords = tag(inputRecordsRDD, hadoopConf, table);
        System.out.println("### tag 耗时 ：" + (System.currentTimeMillis() - start) + "毫秒");
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

  private static <T extends HoodieRecordPayload<T>> HoodieWriteInput<List<HoodieRecord<T>>> tag(
      HoodieWriteInput<List<HoodieRecord<T>>> dedupedRecords, Configuration hadoopConf, HoodieTable<T> table) {
    // perform index loop up to get existing location of records
    return table.getIndex().tagLocation(dedupedRecords, hadoopConf, table);
  }

  public static <T extends HoodieRecordPayload<T>> List<HoodieRecord<T>> combineOnCondition(
      boolean condition, List<HoodieRecord<T>> records, int parallelism, HoodieTable<T> table) {
    return condition ? deduplicateRecords(records, table, parallelism) : records;
  }

  /**
   * Deduplicate Hoodie records, using the given deduplication function.
   *
   * @param records     hoodieRecords to deduplicate
   * @param parallelism parallelism or partitions to be used while reducing/deduplicating
   * @return RDD of HoodieRecord already be deduplicated
   */
  public static <T extends HoodieRecordPayload<T>> List<HoodieRecord<T>> deduplicateRecords(
      List<HoodieRecord<T>> records, HoodieTable<T> table, int parallelism) {
    return deduplicateRecords(records, table.getIndex());
  }

  public static <T extends HoodieRecordPayload<T>> List<HoodieRecord<T>> deduplicateRecords(
      List<HoodieRecord<T>> records, HoodieIndex<T> index) {
    Map<String, List<Pair<String, HoodieRecord<T>>>> keyedRecords = records.stream()
        .map(record -> Pair.of(record.getRecordKey(), record))
        .collect(Collectors.groupingBy(Pair::getLeft));
    return keyedRecords.values().stream().map(x -> x.stream().map(Pair::getRight).reduce((rec1, rec2) -> {
      @SuppressWarnings("unchecked")
      T reducedData = (T) rec1.getData().preCombine(rec2.getData());
      // we cannot allow the user to change the key or partitionPath, since that will affect
      // everything
      // so pick it from one of the records.
      return new HoodieRecord<T>(rec1.getKey(), reducedData);
    }).orElse(null)).filter(Objects::nonNull).collect(Collectors.toList());
  }
}
