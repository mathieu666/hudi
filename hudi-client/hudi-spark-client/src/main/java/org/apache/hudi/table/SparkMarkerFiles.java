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

package org.apache.hudi.table;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.IOType;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class SparkMarkerFiles extends BaseMarkerFiles {

  private static final Logger LOG = LogManager.getLogger(SparkMarkerFiles.class);

  public SparkMarkerFiles(HoodieTable table, String instantTime) {
    super(table, instantTime);
  }

  public SparkMarkerFiles(FileSystem fs, String basePath, String markerFolderPath, String instantTime) {
    super(fs, basePath, markerFolderPath, instantTime);
  }

  @Override
  public boolean deleteMarkerDir(HoodieEngineContext context, int parallelism) {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    try {
      if (fs.exists(markerDirPath)) {
        FileStatus[] fileStatuses = fs.listStatus(markerDirPath);
        List<String> markerDirSubPaths = Arrays.stream(fileStatuses)
            .map(fileStatus -> fileStatus.getPath().toString())
            .collect(Collectors.toList());

        if (markerDirSubPaths.size() > 0) {
          SerializableConfiguration conf = new SerializableConfiguration(fs.getConf());
          parallelism = Math.min(markerDirSubPaths.size(), parallelism);
          jsc.parallelize(markerDirSubPaths, parallelism).foreach(subPathStr -> {
            Path subPath = new Path(subPathStr);
            FileSystem fileSystem = subPath.getFileSystem(conf.get());
            fileSystem.delete(subPath, true);
          });
        }

        boolean result = fs.delete(markerDirPath, true);
        LOG.info("Removing marker directory at " + markerDirPath);
        return result;
      }
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
    return false;
  }

  @Override
  public Set<String> createdAndMergedDataPaths(HoodieEngineContext context, int parallelism) throws IOException {
    JavaSparkContext jsc = HoodieSparkEngineContext.getSparkContext(context);
    Set<String> dataFiles = new HashSet<>();

    FileStatus[] topLevelStatuses = fs.listStatus(markerDirPath);
    List<String> subDirectories = new ArrayList<>();
    for (FileStatus topLevelStatus: topLevelStatuses) {
      if (topLevelStatus.isFile()) {
        String pathStr = topLevelStatus.getPath().toString();
        if (pathStr.contains(HoodieTableMetaClient.MARKER_EXTN) && !pathStr.endsWith(IOType.APPEND.name())) {
          dataFiles.add(translateMarkerToDataPath(pathStr));
        }
      } else {
        subDirectories.add(topLevelStatus.getPath().toString());
      }
    }

    if (subDirectories.size() > 0) {
      parallelism = Math.min(subDirectories.size(), parallelism);
      SerializableConfiguration serializedConf = new SerializableConfiguration(fs.getConf());
      dataFiles.addAll(jsc.parallelize(subDirectories, parallelism).flatMap(directory -> {
        Path path = new Path(directory);
        FileSystem fileSystem = path.getFileSystem(serializedConf.get());
        RemoteIterator<LocatedFileStatus> itr = fileSystem.listFiles(path, true);
        List<String> result = new ArrayList<>();
        while (itr.hasNext()) {
          FileStatus status = itr.next();
          String pathStr = status.getPath().toString();
          if (pathStr.contains(HoodieTableMetaClient.MARKER_EXTN) && !pathStr.endsWith(IOType.APPEND.name())) {
            result.add(translateMarkerToDataPath(pathStr));
          }
        }
        return result.iterator();
      }).collect());
    }

    return dataFiles;
  }
}
