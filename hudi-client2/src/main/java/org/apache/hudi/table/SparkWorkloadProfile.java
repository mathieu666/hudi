package org.apache.hudi.table;

import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.Map;

public class SparkWorkloadProfile<T extends HoodieRecordPayload<T>> extends WorkloadProfile<HoodieWriteInput<JavaRDD<HoodieRecord<T>>>> {

  public SparkWorkloadProfile(HoodieWriteInput<JavaRDD<HoodieRecord<T>>> taggedRecords) {
    super(taggedRecords);
  }

  @Override
  public void buildProfile() {

    Map<Tuple2<String, Option<HoodieRecordLocation>>, Long> partitionLocationCounts = taggedRecords.getInputs()
        .mapToPair(record -> new Tuple2<>(
            new Tuple2<>(record.getPartitionPath(), Option.ofNullable(record.getCurrentLocation())), record))
        .countByKey();

    for (Map.Entry<Tuple2<String, Option<HoodieRecordLocation>>, Long> e : partitionLocationCounts.entrySet()) {
      String partitionPath = e.getKey()._1();
      Long count = e.getValue();
      Option<HoodieRecordLocation> locOption = e.getKey()._2();

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
