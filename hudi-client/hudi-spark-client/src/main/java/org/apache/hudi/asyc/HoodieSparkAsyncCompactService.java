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

package org.apache.hudi.asyc;

import org.apache.hudi.client.AbstractHoodieWriteClient;
import org.apache.hudi.client.BaseCompactor;
import org.apache.hudi.client.HoodieSparkCompactor;
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.HoodieSparkEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

public class HoodieSparkAsyncCompactService extends BaseAsyncCompactService {

  private static final Logger LOG = LogManager.getLogger(HoodieSparkAsyncCompactService.class);

  private transient JavaSparkContext jssc;
  public HoodieSparkAsyncCompactService(HoodieEngineContext context, AbstractHoodieWriteClient client) {
    super(context, client);
    this.jssc = HoodieSparkEngineContext.getSparkContext(context);
  }

  public HoodieSparkAsyncCompactService(HoodieEngineContext context, AbstractHoodieWriteClient client, boolean runInDaemonMode) {
    super(context, client, runInDaemonMode);
    this.jssc = HoodieSparkEngineContext.getSparkContext(context);
  }

  @Override
  protected BaseCompactor createCompactor(AbstractHoodieWriteClient client) {
    return new HoodieSparkCompactor(client);
  }

  @Override
  protected Pair<CompletableFuture, ExecutorService> startService() {
    ExecutorService executor = Executors.newFixedThreadPool(maxConcurrentCompaction,
        r -> {
          Thread t = new Thread(r, "async_compact_thread");
          t.setDaemon(isRunInDaemonMode());
          return t;
        });
    return Pair.of(CompletableFuture.allOf(IntStream.range(0, maxConcurrentCompaction).mapToObj(i -> CompletableFuture.supplyAsync(() -> {
      try {
        // Set Compactor Pool Name for allowing users to prioritize compaction
        LOG.info("Setting Spark Pool name for compaction to " + COMPACT_POOL_NAME);
        jssc.setLocalProperty("spark.scheduler.pool", COMPACT_POOL_NAME);

        while (!isShutdownRequested()) {
          final HoodieInstant instant = fetchNextCompactionInstant();

          if (null != instant) {
            LOG.info("Starting Compaction for instant " + instant);
            compactor.compact(instant);
            LOG.info("Finished Compaction for instant " + instant);
          }
        }
        LOG.info("Compactor shutting down properly!!");
      } catch (InterruptedException ie) {
        LOG.warn("Compactor executor thread got interrupted exception. Stopping", ie);
      } catch (IOException e) {
        LOG.error("Compactor executor failed", e);
        throw new HoodieIOException(e.getMessage(), e);
      }
      return true;
    }, executor)).toArray(CompletableFuture[]::new)), executor);
  }
}
