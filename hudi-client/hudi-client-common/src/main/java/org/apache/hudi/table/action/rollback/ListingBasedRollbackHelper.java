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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieRollbackStat;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.block.HoodieCommandBlock.HoodieCommandBlockTypeEnum;
import org.apache.hudi.common.table.log.block.HoodieLogBlock.HeaderMetadataType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Performs Rollback of Hoodie Tables.
 */
public abstract class ListingBasedRollbackHelper implements Serializable {

  private static final Logger LOG = LogManager.getLogger(ListingBasedRollbackHelper.class);

  protected final HoodieTableMetaClient metaClient;
  protected final HoodieWriteConfig config;

  public ListingBasedRollbackHelper(HoodieTableMetaClient metaClient, HoodieWriteConfig config) {
    this.metaClient = metaClient;
    this.config = config;
  }

  /**
   * Performs all rollback actions that we have collected in parallel.
   */
  public abstract List<HoodieRollbackStat> performRollback(HoodieEngineContext context, HoodieInstant instantToRollback, List<ListingBasedRollbackRequest> rollbackRequests);

  /**
   * Common method used for cleaning out base files under a partition path during rollback of a set of commits.
   */
  protected Map<FileStatus, Boolean> deleteCleanedFiles(HoodieTableMetaClient metaClient, HoodieWriteConfig config,
                                                      String partitionPath, PathFilter filter) throws IOException {
    LOG.info("Cleaning path " + partitionPath);
    final Map<FileStatus, Boolean> results = new HashMap<>();
    FileSystem fs = metaClient.getFs();
    FileStatus[] toBeDeleted = fs.listStatus(FSUtils.getPartitionPath(config.getBasePath(), partitionPath), filter);
    for (FileStatus file : toBeDeleted) {
      boolean success = fs.delete(file.getPath(), false);
      results.put(file, success);
      LOG.info("Delete file " + file.getPath() + "\t" + success);
    }
    return results;
  }

  /**
   * Common method used for cleaning out base files under a partition path during rollback of a set of commits.
   */
  protected Map<FileStatus, Boolean> deleteCleanedFiles(HoodieTableMetaClient metaClient, HoodieWriteConfig config,
                                                      String commit, String partitionPath) throws IOException {
    final Map<FileStatus, Boolean> results = new HashMap<>();
    LOG.info("Cleaning path " + partitionPath);
    FileSystem fs = metaClient.getFs();
    String basefileExtension = metaClient.getTableConfig().getBaseFileFormat().getFileExtension();
    PathFilter filter = (path) -> {
      if (path.toString().contains(basefileExtension)) {
        String fileCommitTime = FSUtils.getCommitTime(path.getName());
        return commit.equals(fileCommitTime);
      }
      return false;
    };
    FileStatus[] toBeDeleted = fs.listStatus(FSUtils.getPartitionPath(config.getBasePath(), partitionPath), filter);
    for (FileStatus file : toBeDeleted) {
      boolean success = fs.delete(file.getPath(), false);
      results.put(file, success);
      LOG.info("Delete file " + file.getPath() + "\t" + success);
    }
    return results;
  }

  protected Map<HeaderMetadataType, String> generateHeader(String commit) {
    // generate metadata
    Map<HeaderMetadataType, String> header = new HashMap<>(3);
    header.put(HeaderMetadataType.INSTANT_TIME, metaClient.getActiveTimeline().lastInstant().get().getTimestamp());
    header.put(HeaderMetadataType.TARGET_INSTANT_TIME, commit);
    header.put(HeaderMetadataType.COMMAND_BLOCK_TYPE,
        String.valueOf(HoodieCommandBlockTypeEnum.ROLLBACK_PREVIOUS_BLOCK.ordinal()));
    return header;
  }

  public interface SerializablePathFilter extends PathFilter, Serializable {
  }
}
