package org.apache.hudi.writer;

import java.io.IOException;
import java.util.List;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;

public class WriteJob {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    //upsert
    upsert(env);

    env.execute("Hudi upsert via Flink");
  }

  private static void upsert(StreamExecutionEnvironment env) throws Exception {
    // step 1 : Triggers the input data read, converts to HoodieRecord object and then stops at obtaining a spread of input records to target partition paths
    DataStreamSource<HoodieRecord> source = ingestDataAndCovert(env);

    DataStream<Tuple2<HoodieKey, HoodieRecordPayload>> hoodieKVStream = source.map(record -> new Tuple2(record.getKey(), record.getData()));

    hoodieKVStream
        .keyBy(item -> item.f0)
        .timeWindow(Time.minutes(1))
        .process(new WriteProcessWindowFunction());
  }



  //------------------------------------------------------
  //                      steps
  //------------------------------------------------------

  private static DataStreamSource<HoodieRecord> ingestDataAndCovert(StreamExecutionEnvironment env) throws IOException {
    HoodieTestDataGenerator dataGen = new HoodieTestDataGenerator();
    List<HoodieRecord> records = dataGen.generateUpdates("001", 100);
    return env.fromCollection(records);
  }

  private static void loadFileNamesToCheck(StreamExecutionEnvironment env, DataStreamSource<HoodieRecord> source) {


  }

  private static void doLookup(StreamExecutionEnvironment env) {

  }

  private static void joinWithTagLocationToGetInfor(StreamExecutionEnvironment env) {

  }

  private static void writeToSink(StreamExecutionEnvironment env) {

  }

}
