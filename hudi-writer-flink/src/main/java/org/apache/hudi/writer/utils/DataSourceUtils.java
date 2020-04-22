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

package org.apache.hudi.writer.utils;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.writer.client.HoodieWriteClient;
import org.apache.hudi.writer.client.WriteStatus;
import org.apache.hudi.writer.config.HoodieWriteConfig;

import java.io.IOException;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Utilities used throughout the data source.
 */
public class DataSourceUtils {

  /**
   * Obtain value of the provided field as string, denoted by dot notation. e.g: a.b.c
   */
  public static String getNestedFieldValAsString(GenericRecord record, String fieldName, boolean returnNullIfNotFound) {
    Object obj = getNestedFieldVal(record, fieldName, returnNullIfNotFound);
    return (obj == null) ? null : obj.toString();
  }

  /**
   * Obtain value of the provided field, denoted by dot notation. e.g: a.b.c
   */
  public static Object getNestedFieldVal(GenericRecord record, String fieldName, boolean returnNullIfNotFound) {
    String[] parts = fieldName.split("\\.");
    GenericRecord valueNode = record;
    int i = 0;
    for (; i < parts.length; i++) {
      String part = parts[i];
      Object val = valueNode.get(part);
      if (val == null) {
        break;
      }

      // return, if last part of name
      if (i == parts.length - 1) {
        Schema fieldSchema = valueNode.getSchema().getField(part).schema();
        return convertValueForSpecificDataTypes(fieldSchema, val);
      } else {
        // VC: Need a test here
        if (!(val instanceof GenericRecord)) {
          throw new HoodieException("Cannot find a record at part value :" + part);
        }
        valueNode = (GenericRecord) val;
      }
    }

    if (returnNullIfNotFound) {
      return null;
    } else {
      throw new HoodieException(
          fieldName + "(Part -" + parts[i] + ") field not found in record. Acceptable fields were :"
              + valueNode.getSchema().getFields().stream().map(Field::name).collect(Collectors.toList()));
    }
  }

  /**
   * This method converts values for fields with certain Avro/Parquet data types that require special handling.
   * <p>
   * Logical Date Type is converted to actual Date value instead of Epoch Integer which is how it is
   * represented/stored in parquet.
   *
   * @param fieldSchema avro field schema
   * @param fieldValue  avro field value
   * @return field value either converted (for certain data types) or as it is.
   */
  private static Object convertValueForSpecificDataTypes(Schema fieldSchema, Object fieldValue) {
    if (fieldSchema == null) {
      return fieldValue;
    }

    if (isLogicalTypeDate(fieldSchema)) {
      return LocalDate.ofEpochDay(Long.parseLong(fieldValue.toString()));
    }
    return fieldValue;
  }

  /**
   * Given an Avro field schema checks whether the field is of Logical Date Type or not.
   *
   * @param fieldSchema avro field schema
   * @return boolean indicating whether fieldSchema is of Avro's Date Logical Type
   */
  private static boolean isLogicalTypeDate(Schema fieldSchema) {
    if (fieldSchema.getType() == Schema.Type.UNION) {
      return fieldSchema.getTypes().stream().anyMatch(schema -> schema.getLogicalType() == LogicalTypes.date());
    }
    return fieldSchema.getLogicalType() == LogicalTypes.date();
  }

  /**
   * Create a payload class via reflection, passing in an ordering/precombine value.
   */
  public static HoodieRecordPayload createPayload(String payloadClass, GenericRecord record, Comparable orderingVal)
      throws IOException {
    try {
      return (HoodieRecordPayload) ReflectionUtils.loadClass(payloadClass,
          new Class<?>[]{GenericRecord.class, Comparable.class}, record, orderingVal);
    } catch (Throwable e) {
      throw new IOException("Could not create payload for class: " + payloadClass, e);
    }
  }

  public static void checkRequiredProperties(TypedProperties props, List<String> checkPropNames) {
    checkPropNames.forEach(prop -> {
      if (!props.containsKey(prop)) {
        throw new HoodieNotSupportedException("Required property " + prop + " is missing");
      }
    });
  }

  public static HoodieWriteClient createHoodieClient(Configuration hadoopConf, String schemaStr, String basePath,
                                                     String tblName, Map<String, String> parameters) {
    //todo
    return null;
  }

  public static List<WriteStatus> doWriteOperation(HoodieWriteClient client, List<HoodieRecord> hoodieRecords,
                                                   String instantTime, String operation) {
    return null;
  }

  public static List<WriteStatus> doDeleteOperation(HoodieWriteClient client, List<HoodieKey> hoodieKeys,
                                                    String instantTime) {
    return null;
  }

  public static HoodieRecord createHoodieRecord(GenericRecord gr, Comparable orderingVal, HoodieKey hKey,
                                                String payloadClass) throws IOException {
    HoodieRecordPayload payload = DataSourceUtils.createPayload(payloadClass, gr, orderingVal);
    return new HoodieRecord<>(hKey, payload);
  }

  @SuppressWarnings("unchecked")
  public static List<HoodieRecord> dropDuplicates(Configuration hadoopConf, List<HoodieRecord> incomingHoodieRecords,
                                                  HoodieWriteConfig writeConfig) {
    return null;
  }

  @SuppressWarnings("unchecked")
  public static List<HoodieRecord> dropDuplicates(Configuration hadoopConf, List<HoodieRecord> incomingHoodieRecords,
                                                  Map<String, String> parameters) {
    HoodieWriteConfig writeConfig =
        HoodieWriteConfig.newBuilder().withPath(parameters.get("path")).withProps(parameters).build();
    return dropDuplicates(hadoopConf, incomingHoodieRecords, writeConfig);
  }
}
