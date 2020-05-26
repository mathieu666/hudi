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

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.HoodieWriteInput;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Information about incoming records for upsert/insert obtained either via sampling or introspecting the data fully.
 * <p>
 * TODO(vc): Think about obtaining this directly from index.tagLocation
 */
public class FLinkWorkloadProfile<T extends HoodieRecordPayload> extends WorkloadProfile<T, HoodieWriteInput<List<HoodieRecord>>> {
  public FLinkWorkloadProfile(HoodieWriteInput<List<HoodieRecord>> taggedRecords) {
    super(taggedRecords);
  }

  @Override
  public void buildProfile() {

    Map<Pair<String, Option<HoodieRecordLocation>>, Long> partitionLocationCounts = super.taggedRecords.getInputs()
        .stream()
        .map(record -> Pair.of(
            Pair.of(record.getPartitionPath(), Option.ofNullable(record.getCurrentLocation())), record))
        .collect(Collectors.groupingBy(Pair::getLeft, Collectors.counting()));

    for (Map.Entry<Pair<String, Option<HoodieRecordLocation>>, Long> e : partitionLocationCounts.entrySet()) {
      String partitionPath = e.getKey().getLeft();
      Long count = e.getValue();
      Option<HoodieRecordLocation> locOption = e.getKey().getRight();

      if (!partitionPathStatMap.containsKey(partitionPath)) {
        partitionPathStatMap.put(partitionPath, new WorkloadStat());
      }

      if (locOption.isPresent()) {
        // update
        partitionPathStatMap.get(partitionPath).addUpdates(locOption.get(), count);
        globalStat.addUpdates(locOption.get(), count);
      } else {
        // insert
        partitionPathStatMap.get(partitionPath).addInserts(count);
        globalStat.addInserts(count);
      }
    }
  }
}
