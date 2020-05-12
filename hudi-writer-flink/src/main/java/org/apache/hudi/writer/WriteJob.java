package org.apache.hudi.writer;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hudi.HoodieEngineContext;
import org.apache.hudi.HoodieWriteClientV2;
import org.apache.hudi.WriteStatus;
import org.apache.hudi.common.HoodieWriteInput;
import org.apache.hudi.common.HoodieWriteOutput;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.writer.constant.Operation;
import org.apache.hudi.writer.context.HoodieFlinkEngineContext;
import org.apache.hudi.writer.exception.HoodieDeltaStreamerException;
import org.apache.hudi.writer.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class WriteJob {
  private static final Logger LOG = LoggerFactory.getLogger(WriteJob.class);

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    env.enableCheckpointing(cfg.checkpointInterval);
    env.getConfig().setGlobalJobParameters(cfg);

    HoodieWriteConfig writeConfig = getHoodieWriteConfig();
    HoodieEngineContext context = new HoodieFlinkEngineContext();
    HoodieWriteClientV2
        writeClient = new HoodieWriteFlinkClient(context, writeConfig);

    // read from source
    DataStream<HoodieRecord> source = env.addSource(new SourceReader());
    DataStream<HoodieRecord> records = source.keyBy(HoodieRecord::getPartitionPath);

    // filter dupes if needed
//    if (cfg.filterDupes) {
//      records = DataSourceUtils.dropDuplicates(env, records, writeClient.getEngineContext());
//    }

    // try to commit
    String instantTime = startCommit(writeClient);

    // write
    HoodieWriteOutput<DataStream<WriteStatus>> writeStatusDS;
    if (cfg.operation == Operation.INSERT) {
      writeStatusDS = writeClient.insert(new HoodieWriteInput<>(records), instantTime);
    } else if (cfg.operation == Operation.UPSERT) {
      writeStatusDS = writeClient.upsert(new HoodieWriteInput<>(records), instantTime);
    } else if (cfg.operation == Operation.BULK_INSERT) {
      writeStatusDS = writeClient.bulkInsert(new HoodieWriteInput<>(records), instantTime);
    } else {
      throw new HoodieDeltaStreamerException("Unknown operation :" + cfg.operation);
    }

    context.finalCommit(writeStatusDS, context);

    env.execute("Hudi upsert via Flink");
  }

  private static String startCommit(HoodieWriteClientV2 writeClient) {
    final int maxRetries = 2;
    int retryNum = 1;
    RuntimeException lastException = null;
    while (retryNum <= maxRetries) {
      try {
        return writeClient.startCommit();
      } catch (IllegalArgumentException ie) {
        lastException = ie;
        LOG.error("Got error trying to start a new commit. Retrying after sleeping for a sec", ie);
        retryNum++;
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // No-Op
        }
      }
    }
    throw lastException;
  }

  private static HoodieWriteConfig getHoodieWriteConfig() {
    return null;
  }

  public static class Config extends Configuration {

    @Parameter(names = {"--target-base-path"},
        description = "base path for the target hoodie table. "
            + "(Will be created if did not exist first time around. If exists, expected to be a hoodie table)",
        required = true)
    public String targetBasePath;

    @Parameter(names = {"--target-table"}, description = "name of the target table in Hive", required = true)
    public String targetTableName;

    @Parameter(names = {"--table-type"}, description = "Type of table. COPY_ON_WRITE (or) MERGE_ON_READ", required = true)
    public String tableType;

    @Parameter(names = {"--props"}, description = "path to properties file on localfs or dfs, with configurations for "
        + "hoodie client, schema provider, key generator and data source. For hoodie client props, sane defaults are "
        + "used, but recommend use to provide basic things like metrics endpoints, hive configs etc. For sources, refer"
        + "to individual classes, for supported properties.")
    public String propsFilePath =
        "file://" + System.getProperty("user.dir") + "/src/test/resources/delta-streamer-config/dfs-source.properties";

    @Parameter(names = {"--hoodie-conf"}, description = "Any configuration that can be set in the properties file "
        + "(using the CLI parameter \"--propsFilePath\") can also be passed command line using this parameter")
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--source-ordering-field"}, description = "Field within source record to decide how"
        + " to break ties between records with same key in input data. Default: 'ts' holding unix timestamp of record")
    public String sourceOrderingField = "ts";

    @Parameter(names = {"--payload-class"}, description = "subclass of HoodieRecordPayload, that works off "
        + "a GenericRecord. Implement your own, if you want to do something other than overwriting existing value")
    public String payloadClassName = OverwriteWithLatestAvroPayload.class.getName();

    @Parameter(names = {"--schemaprovider-class"}, description = "subclass of org.apache.hudi.utilities.schema"
        + ".SchemaProvider to attach schemas to input & target table data, built in options: "
        + "org.apache.hudi.utilities.schema.FilebasedSchemaProvider."
        + "Source (See org.apache.hudi.utilities.sources.Source) implementation can implement their own SchemaProvider."
        + " For Sources that return Dataset<Row>, the schema is obtained implicitly. "
        + "However, this CLI option allows overriding the schemaprovider returned by Source.")
    public String schemaProviderClassName = null;

    @Parameter(names = {"--transformer-class"},
        description = "subclass of org.apache.hudi.utilities.transform.Transformer"
            + ". Allows transforming raw source Dataset to a target Dataset (conforming to target schema) before "
            + "writing. Default : Not set. E:g - org.apache.hudi.utilities.transform.SqlQueryBasedTransformer (which "
            + "allows a SQL query templated to be passed as a transformation function)")
    public String transformerClassName = null;

    @Parameter(names = {"--source-limit"}, description = "Maximum amount of data to read from source. "
        + "Default: No limit For e.g: DFS-Source => max bytes to read, Kafka-Source => max events to read")
    public long sourceLimit = Long.MAX_VALUE;

    @Parameter(names = {"--op"}, description = "Takes one of these values : UPSERT (default), INSERT (use when input "
        + "is purely new data/inserts to gain speed)", converter = OperationConvertor.class)
    public Operation operation = Operation.UPSERT;

    @Parameter(names = {"--filter-dupes"},
        description = "Should duplicate records from source be dropped/filtered out before insert/bulk-insert")
    public Boolean filterDupes = false;

    @Parameter(names = {"--enable-hive-sync"}, description = "Enable syncing to hive")
    public Boolean enableHiveSync = false;

    @Parameter(names = {"--max-pending-compactions"},
        description = "Maximum number of outstanding inflight/requested compactions. Delta Sync will not happen unless"
            + "outstanding compactions is less than this number")
    public Integer maxPendingCompactions = 5;

    @Parameter(names = {"--continuous"}, description = "Delta Streamer runs in continuous mode running"
        + " source-fetch -> Transform -> Hudi Write in loop")
    public Boolean continuousMode = true;

    @Parameter(names = {"--min-sync-interval-seconds"},
        description = "the min sync interval of each sync in continuous mode")
    public Integer minSyncIntervalSeconds = 0;

    @Parameter(names = {"--commit-on-errors"}, description = "Commit even when some records failed to be written")
    public Boolean commitOnErrors = false;

    /**
     * Compaction is enabled for MoR table by default. This flag disables it
     */
    @Parameter(names = {"--disable-compaction"},
        description = "Compaction is enabled for MoR table by default. This flag disables it ")
    public Boolean forceDisableCompaction = false;

    /**
     * Resume Delta Streamer from this checkpoint.
     */
    @Parameter(names = {"--checkpoint"}, description = "Resume Delta Streamer from this checkpoint.")
    public String checkpoint = null;

    /**
     * FLink checkpoint interval.
     */
    @Parameter(names = {"--checkpoint-interval"}, description = "FLink checkpoint interval.")
    public Long checkpointInterval = 1000 * 60L;

    /**
     * InstantTime save path.
     */
    @Parameter(names = {"--instant-time-path"}, description = "InstantTime save path.")
    public String instantTimePath = propsFilePath;

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    public boolean isAsyncCompactionEnabled() {
      return continuousMode && !forceDisableCompaction
          && HoodieTableType.MERGE_ON_READ.equals(HoodieTableType.valueOf(tableType));
    }

    public boolean isInlineCompactionEnabled() {
      return !continuousMode && !forceDisableCompaction
          && HoodieTableType.MERGE_ON_READ.equals(HoodieTableType.valueOf(tableType));
    }
  }

  private static class OperationConvertor implements IStringConverter<Operation> {

    @Override
    public Operation convert(String value) throws ParameterException {
      return Operation.valueOf(value);
    }
  }
}
