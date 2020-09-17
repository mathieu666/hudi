package org.apache.hudi.writer.source;

import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.OverwriteWithLatestAvroPayload;
import org.apache.hudi.writer.keygen.SimpleKeyGenerator;

public class DataSourceWriteOptions {
  /**
   * The write operation, that this write should do.
   *
   * Default: upsert()
   */
  public static String OPERATION_OPT_KEY = "hoodie.datasource.write.operation";
  public static String BULK_INSERT_OPERATION_OPT_VAL = "bulk_insert";
  public static String INSERT_OPERATION_OPT_VAL = "insert";
  public static String UPSERT_OPERATION_OPT_VAL = "upsert";
  public static String DELETE_OPERATION_OPT_VAL = "delete";
  public static String DEFAULT_OPERATION_OPT_VAL = UPSERT_OPERATION_OPT_VAL;

  /**
   * The table type for the underlying data, for this write.
   * Note that this can't change across writes.
   *
   * Default: COPY_ON_WRITE
   */
  public static String TABLE_TYPE_OPT_KEY = "hoodie.datasource.write.table.type";
  public static String COW_TABLE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name();
  public static String MOR_TABLE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name();
  public static String DEFAULT_TABLE_TYPE_OPT_VAL = COW_TABLE_TYPE_OPT_VAL;

  @Deprecated
  public static String STORAGE_TYPE_OPT_KEY = "hoodie.datasource.write.storage.type";
  @Deprecated
  public static String COW_STORAGE_TYPE_OPT_VAL = HoodieTableType.COPY_ON_WRITE.name();
  @Deprecated
  public static String MOR_STORAGE_TYPE_OPT_VAL = HoodieTableType.MERGE_ON_READ.name();
  @Deprecated
  public static String DEFAULT_STORAGE_TYPE_OPT_VAL = COW_STORAGE_TYPE_OPT_VAL;


  /**
   * Hive table name, to register the table into.
   *
   * Default:  None (mandatory)
   */
  public static String TABLE_NAME_OPT_KEY = "hoodie.datasource.write.table.name";

  /**
   * Field used in preCombining before actual write. When two records have the same
   * key value, we will pick the one with the largest value for the precombine field,
   * determined by Object.compareTo(..)
   */
  public static String PRECOMBINE_FIELD_OPT_KEY = "hoodie.datasource.write.precombine.field";
  public static String DEFAULT_PRECOMBINE_FIELD_OPT_VAL = "ts";


  /**
   * Payload class used. Override this, if you like to roll your own merge logic, when upserting/inserting.
   * This will render any value set for `PRECOMBINE_FIELD_OPT_VAL` in-effective
   */
  public static String PAYLOAD_CLASS_OPT_KEY = "hoodie.datasource.write.payload.class";
  public static String DEFAULT_PAYLOAD_OPT_VAL = OverwriteWithLatestAvroPayload.class.getName();

  /**
   * Record key field. Value to be used as the `recordKey` component of `HoodieKey`. Actual value
   * will be obtained by invoking .topublic static String() on the field value. Nested fields can be specified using
   * the dot notation eg: `a.b.c`
   *
   */
  public static String RECORDKEY_FIELD_OPT_KEY = "hoodie.datasource.write.recordkey.field";
  public static String DEFAULT_RECORDKEY_FIELD_OPT_VAL = "uuid";

  /**
   * Partition path field. Value to be used at the `partitionPath` component of `HoodieKey`. Actual
   * value ontained by invoking .topublic static String()
   */
  public static String PARTITIONPATH_FIELD_OPT_KEY = "hoodie.datasource.write.partitionpath.field";
  public static String DEFAULT_PARTITIONPATH_FIELD_OPT_VAL = "partitionpath";

  /**
   * Flag to indicate whether to use Hive style partitioning.
   * If set true, the names of partition folders follow <partition_column_name>=<partition_value> format.
   * By default false (the names of partition folders are only partition values)
   */
  public static String HIVE_STYLE_PARTITIONING_OPT_KEY = "hoodie.datasource.write.hive_style_partitioning";
  public static String DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL = "false";

  /**
   * Key generator class, that implements will extract the key out of incoming record.
   *
   */
  public static String KEYGENERATOR_CLASS_OPT_KEY = "hoodie.datasource.write.keygenerator.class";
  public static String DEFAULT_KEYGENERATOR_CLASS_OPT_VAL = SimpleKeyGenerator.class.getName();

  /**
   * Option keys beginning with this prefix, are automatically added to the commit/deltacommit metadata.
   * This is useful to store checkpointing information, in a consistent way with the hoodie timeline
   */
  public static String COMMIT_METADATA_KEYPREFIX_OPT_KEY = "hoodie.datasource.write.commitmeta.key.prefix";
  public static String DEFAULT_COMMIT_METADATA_KEYPREFIX_OPT_VAL = "_";

  /**
   * Flag to indicate whether to drop duplicates upon insert.
   * By default insert will accept duplicates, to gain extra performance.
   */
  public static String INSERT_DROP_DUPS_OPT_KEY = "hoodie.datasource.write.insert.drop.duplicates";
  public static String DEFAULT_INSERT_DROP_DUPS_OPT_VAL = "false";

  /**
   * Flag to indicate how many times streaming job should retry for a failed microbatch.
   * By default 3
   */
  public static String STREAMING_RETRY_CNT_OPT_KEY = "hoodie.datasource.write.streaming.retry.count";
  public static String DEFAULT_STREAMING_RETRY_CNT_OPT_VAL = "3";

  /**
   * Flag to indicate how long (by millisecond) before a retry should issued for failed microbatch.
   * By default 2000 and it will be doubled by every retry
   */
  public static String STREAMING_RETRY_INTERVAL_MS_OPT_KEY = "hoodie.datasource.write.streaming.retry.interval.ms";
  public static String DEFAULT_STREAMING_RETRY_INTERVAL_MS_OPT_VAL = "2000";

  /**
   * Flag to indicate whether to ignore any non exception error (e.g. writestatus error)
   * within a streaming microbatch
   * By default true (in favor of streaming progressing over data integrity)
   */
  public static String STREAMING_IGNORE_FAILED_BATCH_OPT_KEY = "hoodie.datasource.write.streaming.ignore.failed.batch";
  public static String DEFAULT_STREAMING_IGNORE_FAILED_BATCH_OPT_VAL = "true";

  // HIVE SYNC SPECIFIC CONFIGS
  //NOTE: DO NOT USE uppercase for the keys as they are internally lower-cased. Using upper-cases causes
  // unexpected issues with config getting reset
  public static String HIVE_SYNC_ENABLED_OPT_KEY = "hoodie.datasource.hive_sync.enable";
  public static String HIVE_DATABASE_OPT_KEY = "hoodie.datasource.hive_sync.database";
  public static String HIVE_TABLE_OPT_KEY = "hoodie.datasource.hive_sync.table";
  public static String HIVE_USER_OPT_KEY = "hoodie.datasource.hive_sync.username";
  public static String HIVE_PASS_OPT_KEY = "hoodie.datasource.hive_sync.password";
  public static String HIVE_URL_OPT_KEY = "hoodie.datasource.hive_sync.jdbcurl";
  public static String HIVE_PARTITION_FIELDS_OPT_KEY = "hoodie.datasource.hive_sync.partition_fields";
  public static String HIVE_PARTITION_EXTRACTOR_CLASS_OPT_KEY = "hoodie.datasource.hive_sync.partition_extractor_class";
  public static String HIVE_USE_PRE_APACHE_INPUT_FORMAT_OPT_KEY = "hoodie.datasource.hive_sync.use_pre_apache_input_format";
  public static String HIVE_USE_JDBC_OPT_KEY = "hoodie.datasource.hive_sync.use_jdbc";

  // DEFAULT FOR HIVE SPECIFIC CONFIGS
  public static String DEFAULT_HIVE_SYNC_ENABLED_OPT_VAL = "false";
  public static String DEFAULT_HIVE_DATABASE_OPT_VAL = "default";
  public static String DEFAULT_HIVE_TABLE_OPT_VAL = "unknown";
  public static String DEFAULT_HIVE_USER_OPT_VAL = "hive";
  public static String DEFAULT_HIVE_PASS_OPT_VAL = "hive";
  public static String DEFAULT_HIVE_URL_OPT_VAL = "jdbc:hive2://localhost:10000";
  public static String DEFAULT_HIVE_PARTITION_FIELDS_OPT_VAL = "";
  public static String DEFAULT_HIVE_ASSUME_DATE_PARTITION_OPT_VAL = "false";
  public static String DEFAULT_USE_PRE_APACHE_INPUT_FORMAT_OPT_VAL = "false";
  public static String DEFAULT_HIVE_USE_JDBC_OPT_VAL = "true";
}
