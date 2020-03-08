package org.apache.hudi.writer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;

public class HoodieBloomIndex {

  public static <T extends HoodieRecordPayload> DataStream<HoodieRecord<T>> tagLocation(DataStreamSource<HoodieRecord> records) {
    // Step 0: cache the input record RDD (ignore for Flink)

    // Step 1: Extract out thinner JavaPairRDD of (partitionPath, recordKey)
    DataStream<Tuple2<String, String>> partitionRecordKeyPairStream = records.map(item -> new Tuple2<>(item.getPartitionPath(), item.getRecordKey()));


  }

  private void lookupIndex() {

  }

}
