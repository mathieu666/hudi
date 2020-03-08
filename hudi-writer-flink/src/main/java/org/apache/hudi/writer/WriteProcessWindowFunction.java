package org.apache.hudi.writer;

import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;

public class WriteProcessWindowFunction extends
    ProcessWindowFunction<Tuple2<HoodieKey, HoodieRecordPayload>, Object, HoodieKey, TimeWindow> {

  @Override
  public void process(HoodieKey hoodieKey, Context context,
      Iterable<Tuple2<HoodieKey, HoodieRecordPayload>> recordsInWindow, Collector<Object> collector)
      throws Exception {

    //access key
    System.out.println(hoodieKey.toString());

    //access all buffered values
    recordsInWindow.forEach(item -> {
      System.out.println(item.f0);
      System.out.println(item.f1);
    });

    //access state
    KeyedStateStore globalState = context.globalState();
    KeyedStateStore windowState = context.windowState();

    index(env);

    write();

  }

  //-------------------------------------------------------
  //                    two parts
  //-------------------------------------------------------

  // part 1: Index Lookup to identify files to be changed
  private static void index(StreamExecutionEnvironment env, DataStreamSource<HoodieRecord> source) throws Exception {
    HoodieBloomIndex.tagLocation(source);

    // step 2 : Load the set of file names which we need check against
    loadFileNamesToCheck(env);

    // step 3 & 4 : Actual lookup after smart sizing of spark join parallelism, by joining RDDs in 1 & 2 above
    doLookup(env);

    // step 5 : Have a tagged RDD of recordKeys with locations
    tagLocations(env);
  }

  // part 2: Performing the actual writing of data
  private static void write(StreamExecutionEnvironment env) {
    //step 6 : Lazy join of incoming records against recordKey, location to provide a final set of
    // HoodieRecord which now contain the information about which file/partitionpath they are found at (or null if insert).
    // Then also profile the workload again to determine sizing of files
    joinWithTagLocationToGetInfor(env);

    //step 7 : Actual writing of data (update + insert + insert turned to updates to maintain file size)
    writeToSink(env);

  }

}
