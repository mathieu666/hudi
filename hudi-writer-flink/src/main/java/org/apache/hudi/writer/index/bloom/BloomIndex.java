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

package org.apache.hudi.writer.index.bloom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordLocation;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIndexException;
import org.apache.hudi.exception.MetadataNotFoundException;
import org.apache.hudi.writer.client.WriteStatus;
import org.apache.hudi.writer.common.HoodieWriteInput;
import org.apache.hudi.writer.common.HoodieWriteKey;
import org.apache.hudi.writer.common.HoodieWriteOutput;
import org.apache.hudi.writer.config.HoodieWriteConfig;
import org.apache.hudi.writer.index.HoodieIndex;
import org.apache.hudi.writer.io.HoodieRangeInfoHandle;
import org.apache.hudi.writer.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.*;
import static org.apache.hudi.writer.index.HoodieIndexUtils.getLatestBaseFilesForAllPartitions;

public class BloomIndex <T extends HoodieRecordPayload> extends HoodieIndex<T> {

  private static final Logger LOG = LogManager.getLogger(BloomIndex.class);

  protected BloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  @Override
  public HoodieWriteKey<List<HoodieKey>> fetchRecordLocation(HoodieWriteKey<List<HoodieKey>> hoodieKeys, Configuration hadoopConf, HoodieTable<T> hoodieTable) {
    return null;
  }

  @Override
  public HoodieWriteInput<List<HoodieRecord<T>>> tagLocation(HoodieWriteInput<List<HoodieRecord<T>>> recordRDD, Configuration hadoopConf, HoodieTable<T> hoodieTable) throws HoodieIndexException {
    List<Tuple2<String, String>> partitionRecordKeyPair = recordRDD
        .getInputs()
        .stream()
        .map(x -> new Tuple2<>(x.getPartitionPath(), x.getRecordKey()))
        .collect(Collectors.toList());

    // Lookup indexes for all the partition/recordkey pair
    List<Tuple2<HoodieKey, HoodieRecordLocation>> keyFilenamePair =
        lookupIndex(partitionRecordKeyPair, hoodieTable);

    if (LOG.isDebugEnabled()) {
      long totalTaggedRecords = keyFilenamePair.size();
      LOG.debug("Number of update records (ones tagged with a fileID): " + totalTaggedRecords);
    }

    return tagLocationBacktoRecords(keyFilenamePair, recordRDD);
  }

  private List<Tuple2<HoodieKey, HoodieRecordLocation>> lookupIndex(List<Tuple2<String, String>> partitionRecordKeyPair, HoodieTable<T> hoodieTable) {
    // Obtain records per partition, in the incoming records
    Map<String, Long> recordsPerPartition = partitionRecordKeyPair.stream().collect(Collectors.groupingBy(Tuple2::_1, Collectors.counting()));
    List<String> affectedPartitionPathList = new ArrayList<>(recordsPerPartition.keySet());

    // Step 2: Load all involved files as <Partition, filename> pairs
    List<Tuple2<String, BloomIndexFileInfo>> fileInfoList =
        loadInvolvedFiles(affectedPartitionPathList, hoodieTable);

    final Map<String, List<BloomIndexFileInfo>> partitionToFileInfo =
        fileInfoList.stream().collect(groupingBy(Tuple2::_1, mapping(Tuple2::_2, toList())));

    // Step 3: Obtain a RDD, for each incoming record, that already exists, with the file id,
    // that contains it.
    Map<String, Long> comparisonsPerFileGroup =
        computeComparisonsPerFileGroup(recordsPerPartition, partitionToFileInfo, partitionRecordKeyPair);
    return findMatchingFilesForRecordKeys(partitionToFileInfo, partitionRecordKeyPair, hoodieTable,
        comparisonsPerFileGroup);
  }

  private List<Tuple2<HoodieKey, HoodieRecordLocation>> findMatchingFilesForRecordKeys(Map<String, List<BloomIndexFileInfo>> partitionToFileInfo, List<Tuple2<String, String>> partitionRecordKeyPair, HoodieTable<T> hoodieTable, Map<String, Long> comparisonsPerFileGroup) {
    List<Tuple2<String, HoodieKey>> fileComparisonsRDD =
        explodeRecordRDDWithFileComparisons(partitionToFileInfo, partitionRecordKeyPair);

      fileComparisonsRDD.stream().sorted((o1, o2) -> o1._1.compareTo(o2._1));

    return fileComparisonsRDD.mapPartitionsWithIndex(new HoodieBloomIndexCheckFunction(hoodieTable, config), true)
        .flatMap(List::stream).filter(lr -> lr.getMatchingRecordKeys().size() > 0)
        .flatMapToPair(lookupResult -> lookupResult.getMatchingRecordKeys().stream()
            .map(recordKey -> new Tuple2<>(new HoodieKey(recordKey, lookupResult.getPartitionPath()),
                new HoodieRecordLocation(lookupResult.getBaseInstantTime(), lookupResult.getFileId())))
            .collect(Collectors.toList()).iterator());
  }

  private Map<String, Long> computeComparisonsPerFileGroup(Map<String, Long> recordsPerPartition, Map<String, List<BloomIndexFileInfo>> partitionToFileInfo, List<Tuple2<String, String>> partitionRecordKeyPair) {
    Map<String, Long> fileToComparisons;
    if (config.getBloomIndexPruneByRanges()) {
      // we will just try exploding the input and then count to determine comparisons
      // FIX(vc): Only do sampling here and extrapolate?
      fileToComparisons = explodeRecordRDDWithFileComparisons(partitionToFileInfo, partitionRecordKeyPair)
          .stream()
          .collect(Collectors.groupingBy(Tuple2::_1, Collectors.counting()));
    } else {
      fileToComparisons = new HashMap<>();
      partitionToFileInfo.forEach((key, value) -> {
        for (BloomIndexFileInfo fileInfo : value) {
          // each file needs to be compared against all the records coming into the partition
          fileToComparisons.put(fileInfo.getFileId(), recordsPerPartition.get(key));
        }
      });
    }
    return fileToComparisons;
  }

  private List<Tuple2<String, HoodieKey>> explodeRecordRDDWithFileComparisons(Map<String, List<BloomIndexFileInfo>> partitionToFileInfo, List<Tuple2<String, String>> partitionRecordKeyPairs) {
    IndexFileFilter indexFileFilter =
        config.useBloomIndexTreebasedFilter() ? new IntervalTreeBasedIndexFileFilter(partitionToFileInfo)
            : new ListBasedIndexFileFilter(partitionToFileInfo);

    return partitionRecordKeyPairs.stream().map(partitionRecordKeyPair-> {
      String recordKey = partitionRecordKeyPair._2();
      String partitionPath = partitionRecordKeyPair._1();

      return indexFileFilter.getMatchingFilesAndPartition(partitionPath, recordKey).stream()
          .map(partitionFileIdPair -> new Tuple2<>(partitionFileIdPair.getRight(),
              new HoodieKey(recordKey, partitionPath)))
          .collect(Collectors.toList());
    }).flatMap(List::stream).collect(Collectors.toList());
  }

  private List<Tuple2<String, BloomIndexFileInfo>> loadInvolvedFiles(List<String> partitions, HoodieTable<T> hoodieTable) {
    // Obtain the latest data files from all the partitions.
    List<Pair<String, String>> partitionPathFileIDList = getLatestBaseFilesForAllPartitions(partitions, hoodieTable).stream()
        .map(pair -> Pair.of(pair.getKey(), pair.getValue().getFileId()))
        .collect(toList());
    if (config.getBloomIndexPruneByRanges()) {
      // also obtain file ranges, if range pruning is enabled
      return partitionPathFileIDList.stream().map(pf -> {
        try {
          HoodieRangeInfoHandle<T> rangeInfoHandle = new HoodieRangeInfoHandle<T>(config, hoodieTable, pf);
          String[] minMaxKeys = rangeInfoHandle.getMinMaxKeys();
          return new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue(), minMaxKeys[0], minMaxKeys[1]));
        } catch (MetadataNotFoundException me) {
          LOG.warn("Unable to find range metadata in file :" + pf);
          return new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue()));
        }
      }).collect(Collectors.toList());
    } else {
      return partitionPathFileIDList.stream()
          .map(pf -> new Tuple2<>(pf.getKey(), new BloomIndexFileInfo(pf.getValue()))).collect(toList());
    }
  }

  @Override
  public HoodieWriteOutput<List<WriteStatus>> updateLocation(HoodieWriteOutput<List<WriteStatus>> writeStatusRDD, Configuration hadoopConf, HoodieTable<T> hoodieTable) throws HoodieIndexException {
    return null;
  }

  @Override
  public boolean rollbackCommit(String commitTime) {
    // Nope, don't need to do anything.
    return true;
  }

  @Override
  public boolean isGlobal() {
    return false;
  }

  @Override
  public boolean canIndexLogFiles() {
    return false;
  }

  @Override
  public boolean isImplicitWithStorage() {
    return true;
  }
}
