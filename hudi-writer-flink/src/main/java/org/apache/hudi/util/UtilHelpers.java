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

package org.apache.hudi.util;

import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.WriteJob;
import org.apache.hudi.common.config.DFSPropertiesConfiguration;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.index.AbstractHoodieIndex;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;

import static org.apache.hudi.source.HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA;


/**
 * Bunch of helper methods.
 */
public class UtilHelpers {
  private static final Logger LOG = LogManager.getLogger(UtilHelpers.class);

  /**
   * Read conig from files.
   */
  public static DFSPropertiesConfiguration readConfig(FileSystem fs, Path cfgPath, List<String> overriddenProps) {
    DFSPropertiesConfiguration conf;
    try {
      conf = new DFSPropertiesConfiguration(cfgPath.getFileSystem(fs.getConf()), cfgPath);
    } catch (Exception e) {
      conf = new DFSPropertiesConfiguration();
      LOG.warn("Unexpected error read props file at :" + cfgPath, e);
    }

    try {
      if (!overriddenProps.isEmpty()) {
        LOG.info("Adding overridden properties to file properties.");
        conf.addProperties(new BufferedReader(new StringReader(String.join("\n", overriddenProps))));
      }
    } catch (IOException ioe) {
      throw new HoodieIOException("Unexpected error adding config overrides", ioe);
    }

    return conf;
  }

  public static HoodieWriteConfig getHoodieClientConfig(WriteJob.Config cfg) {
    FileSystem fs = FSUtils.getFs(cfg.targetBasePath, getHadoopConf());
    HoodieWriteConfig.Builder builder =
        HoodieWriteConfig.newBuilder().withPath(cfg.targetBasePath).combineInput(cfg.filterDupes, true)
            .withCompactionConfig(HoodieCompactionConfig.newBuilder().withPayloadClass(cfg.payloadClassName)
                // Inline compaction is disabled for continuous mode. otherwise enabled for MOR
                .withInlineCompaction(cfg.isInlineCompactionEnabled()).build())
            .forTable(cfg.targetTableName)
            .withIndexConfig(HoodieIndexConfig.newBuilder().withIndexType(AbstractHoodieIndex.IndexType.HBASE).build())
            .withAutoCommit(false)
            .withProps(readConfig(fs, new Path(cfg.propsFilePath), cfg.configs)
                .getConfig());

    HoodieWriteConfig config = builder.build();
    config.setSchema(TRIP_EXAMPLE_SCHEMA);

    // Validate what deltastreamer assumes of write-config to be really safe
    Preconditions.checkArgument(config.isInlineCompaction() == cfg.isInlineCompactionEnabled());
    Preconditions.checkArgument(!config.shouldAutoCommit());
    Preconditions.checkArgument(config.shouldCombineBeforeInsert() == cfg.filterDupes);
    Preconditions.checkArgument(config.shouldCombineBeforeUpsert());

    return config;
  }

  public static Configuration getHadoopConf() {
    return new Configuration();
  }

}
