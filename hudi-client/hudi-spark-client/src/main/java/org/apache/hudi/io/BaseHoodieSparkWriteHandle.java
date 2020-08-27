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

package org.apache.hudi.io;

import org.apache.avro.Schema;
import org.apache.hudi.client.TaskContextSupplier;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.BaseMarkerFiles;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.SparkMarkerFiles;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public abstract class BaseHoodieSparkWriteHandle<T extends HoodieRecordPayload> extends HoodieWriteHandle<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> {
  public BaseHoodieSparkWriteHandle(HoodieWriteConfig config,
                                    String instantTime,
                                    String partitionPath,
                                    String fileId,
                                    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> hoodieTable,
                                    TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, partitionPath, fileId, hoodieTable, taskContextSupplier);
  }

  public BaseHoodieSparkWriteHandle(HoodieWriteConfig config,
                                    String instantTime,
                                    String partitionPath,
                                    String fileId,
                                    HoodieTable<T, JavaRDD<HoodieRecord<T>>, JavaRDD<HoodieKey>, JavaRDD<WriteStatus>, JavaPairRDD<HoodieKey, Option<Pair<String, String>>>> hoodieTable,
                                    Pair<Schema, Schema> writerSchemaIncludingAndExcludingMetadataPair,
                                    TaskContextSupplier taskContextSupplier) {
    super(config, instantTime, partitionPath, fileId, hoodieTable,
        writerSchemaIncludingAndExcludingMetadataPair, taskContextSupplier);
  }

  @Override
  protected void createMarkerFile(String partitionPath, String dataFileName) {
    BaseMarkerFiles markerFiles = new SparkMarkerFiles(hoodieTable, instantTime);
    markerFiles.create(partitionPath, dataFileName, getIOType());
  }
}
