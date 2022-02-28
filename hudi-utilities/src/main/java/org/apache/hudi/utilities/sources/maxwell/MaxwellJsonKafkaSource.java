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

package org.apache.hudi.utilities.sources.maxwell;

import com.alibaba.fastjson.JSONObject;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.utilities.deltastreamer.HoodieDeltaStreamerMetrics;
import org.apache.hudi.utilities.schema.SchemaProvider;
import org.apache.hudi.utilities.sources.JsonKafkaSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Locale;
import java.util.Objects;
import java.util.regex.Pattern;

public class MaxwellJsonKafkaSource extends JsonKafkaSource implements Serializable {

  private static final Logger LOG = LogManager.getLogger(MaxwellJsonKafkaSource.class);
  private TypedProperties prop;

  public MaxwellJsonKafkaSource(TypedProperties properties,
                                JavaSparkContext sparkContext,
                                SparkSession sparkSession,
                                SchemaProvider schemaProvider) {
    this(properties, sparkContext, sparkSession, schemaProvider, null);
  }

  public MaxwellJsonKafkaSource(TypedProperties properties,
                                JavaSparkContext sparkContext,
                                SparkSession sparkSession,
                                SchemaProvider schemaProvider,
                                HoodieDeltaStreamerMetrics metrics) {
    super(properties, sparkContext, sparkSession, schemaProvider, metrics);
    this.prop = properties;
  }

  @Override
  protected JavaRDD<String> transform(JavaRDD<ConsumerRecord<Object, Object>> consumerRecordJavaRDD) {
    String pattern = prop.getString("hoodie.deltastreamer.source.kafka.mysql.table.pattern");
    ValidationUtils.checkArgument(Objects.nonNull(pattern));

    return consumerRecordJavaRDD.filter(x -> Objects.nonNull(x.value()))
        .map(x -> {
          JSONObject inputObject = JSONObject.parseObject(x.value().toString());
          String tableName = inputObject.getString("table");
          final String type = inputObject.getString("type");

          if (Pattern.matches(pattern, tableName)) {
            // 暂时只支持解析 INSERT， UPDATE， DELETE
            if (MaxwellOperation.contain(type)) {
              LOG.info("Input data = " + inputObject.toJSONString().replace("\\", ""));
              return getData(type, inputObject);
            } else {
              LOG.warn("Unsupported operation type + " + type);
              return null;
            }
          } else {
            LOG.info("No target table " + tableName);
            return null;
          }
        }).filter(Objects::nonNull);
  }

  private String getData(String type, JSONObject inputObject) {
    final MaxwellOperation operation = MaxwellOperation.valueOf(type.toUpperCase(Locale.ROOT));
    // 是否使用kafka 时间
    boolean useKafkaTs = prop.getBoolean("hoodie.deltastreamer.source.kafka.use.kafka.ts", false);
    // 是否使用系统时间
    boolean useSystemTs = prop.getBoolean("hoodie.deltastreamer.source.kafka.use.system.ts", false);

    String data = inputObject.getString("data");
    final JSONObject result = JSONObject.parseObject(data);

    // 使用kafka时间戳优先级更高
    if (useSystemTs) {
      result.put("ts", System.currentTimeMillis());
    }
    if (useKafkaTs) {
      result.put("ts", inputObject.getLongValue("ts"));
    }

    // 有些数据insert时没有update_time字段，使用inset_time
    if (operation.equals(MaxwellOperation.INSERT)
        && StringUtils.isNullOrEmpty(result.getString("update_time"))
        && !StringUtils.isNullOrEmpty(result.getString("insert_time"))) {
      result.put("update_time", result.getString("insert_time"));
    }

    // 标记删除
    if (operation.equals(MaxwellOperation.DELETE)) {
      result.put("_hoodie_is_deleted", true);
      LOG.info("Record to delete " + result.toJSONString().replace("\\", ""));
    } else {
      result.put("_hoodie_is_deleted", false);
    }
    LOG.info("parsed record = " + result.toJSONString());
    return result.toJSONString().replace("\\", "");
  }
}
