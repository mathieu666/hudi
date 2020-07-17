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

package org.apache.hudi.table.action.rollback;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieLogFormat;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock;
import org.apache.hudi.common.table.log.block.HoodieLogBlock;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieRollbackException;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SparkListingBasedRollbackHelper extends ListingBasedRollbackHelper {
  public SparkListingBasedRollbackHelper(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    super(metaClient, config);
  }

  @Override
  public List<HoodieRollbackStat> performRollback(HoodieEngineContext context, HoodieInstant instantToRollback, List<ListingBasedRollbackRequest> rollbackRequests) {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    SerializablePathFilter filter = (path) -> {
      if (path.toString().endsWith(this.metaClient.getTableConfig().getBaseFileFormat().getFileExtension())) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return instantToRollback.getTimestamp().equals(fileCommitTime);
      } else if (FSUtils.isLogFile(path)) {
        // Since the baseCommitTime is the only commit for new log files, it's okay here
        String fileCommitTime = FSUtils.getBaseCommitTimeFromLogPath(path);
        return instantToRollback.getTimestamp().equals(fileCommitTime);
      }
      return false;
    };

    int sparkPartitions = Math.max(Math.min(rollbackRequests.size(), config.getRollbackParallelism()), 1);
    jsc.setJobGroup(this.getClass().getSimpleName(), "Perform rollback actions");
    return jsc.parallelize(rollbackRequests, sparkPartitions).mapToPair(rollbackRequest -> {
      switch (rollbackRequest.getType()) {
        case DELETE_DATA_FILES_ONLY: {
          final Map<FileStatus, Boolean> filesToDeletedStatus = deleteCleanedFiles(metaClient, config, instantToRollback.getTimestamp(),
              rollbackRequest.getPartitionPath());
          return new Tuple2<>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withDeletedFileResults(filesToDeletedStatus).build());
        }
        case DELETE_DATA_AND_LOG_FILES: {
          final Map<FileStatus, Boolean> filesToDeletedStatus = deleteCleanedFiles(metaClient, config, rollbackRequest.getPartitionPath(), filter);
          return new Tuple2<>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withDeletedFileResults(filesToDeletedStatus).build());
        }
        case APPEND_ROLLBACK_BLOCK: {
          HoodieLogFormat.Writer writer = null;
          try {
            writer = HoodieLogFormat.newWriterBuilder()
                .onParentPath(FSUtils.getPartitionPath(metaClient.getBasePath(), rollbackRequest.getPartitionPath()))
                .withFileId(rollbackRequest.getFileId().get())
                .overBaseCommit(rollbackRequest.getLatestBaseInstant().get()).withFs(metaClient.getFs())
                .withFileExtension(HoodieLogFile.DELTA_EXTENSION).build();

            // generate metadata
            Map<HoodieLogBlock.HeaderMetadataType, String> header = generateHeader(instantToRollback.getTimestamp());
            // if update belongs to an existing log file
            writer = writer.appendBlock(new HoodieCommandBlock(header));
          } catch (IOException | InterruptedException io) {
            throw new HoodieRollbackException("Failed to rollback for instant " + instantToRollback, io);
          } finally {
            try {
              if (writer != null) {
                writer.close();
              }
            } catch (IOException io) {
              throw new HoodieIOException("Error appending rollback block..", io);
            }
          }

          // This step is intentionally done after writer is closed. Guarantees that
          // getFileStatus would reflect correct stats and FileNotFoundException is not thrown in
          // cloud-storage : HUDI-168
          Map<FileStatus, Long> filesToNumBlocksRollback = Collections.singletonMap(
              metaClient.getFs().getFileStatus(Objects.requireNonNull(writer).getLogFile().getPath()),
              1L
          );
          return new Tuple2<>(rollbackRequest.getPartitionPath(),
              HoodieRollbackStat.newBuilder().withPartitionPath(rollbackRequest.getPartitionPath())
                  .withRollbackBlockAppendResults(filesToNumBlocksRollback).build());
        }
        default:
          throw new IllegalStateException("Unknown Rollback action " + rollbackRequest);
      }
    }).reduceByKey(RollbackUtils::mergeRollbackStat).map(Tuple2::_2).collect();
  }
}
