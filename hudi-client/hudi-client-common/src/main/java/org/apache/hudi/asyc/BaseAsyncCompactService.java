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
import org.apache.hudi.common.HoodieEngineContext;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Async Compactor Service that runs in separate thread. Currently, only one compactor is allowed to run at any time.
 */
public abstract class BaseAsyncCompactService extends AbstractAsyncService {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(BaseAsyncCompactService.class);

  /**
   * This is the job pool used by async compaction.
   */
  public static final String COMPACT_POOL_NAME = "hoodiecompact";

  protected final int maxConcurrentCompaction;
  protected transient BaseCompactor compactor;
  private transient HoodieEngineContext context;
  private transient BlockingQueue<HoodieInstant> pendingCompactions = new LinkedBlockingQueue<>();
  private transient ReentrantLock queueLock = new ReentrantLock();
  private transient Condition consumed = queueLock.newCondition();

  public BaseAsyncCompactService(HoodieEngineContext context, AbstractHoodieWriteClient client) {
    this(context, client, false);
  }

  public BaseAsyncCompactService(HoodieEngineContext context, AbstractHoodieWriteClient client, boolean runInDaemonMode) {
    super(runInDaemonMode);
    this.context = context;
    this.compactor = createCompactor(client);
    this.maxConcurrentCompaction = 1;
  }

  protected abstract BaseCompactor createCompactor(AbstractHoodieWriteClient client);

  /**
   * Enqueues new Pending compaction.
   */
  public void enqueuePendingCompaction(HoodieInstant instant) {
    pendingCompactions.add(instant);
  }

  /**
   * Wait till outstanding pending compactions reduces to the passed in value.
   *
   * @param numPendingCompactions Maximum pending compactions allowed
   * @throws InterruptedException
   */
  public void waitTillPendingCompactionsReducesTo(int numPendingCompactions) throws InterruptedException {
    try {
      queueLock.lock();
      while (!isShutdown() && (pendingCompactions.size() > numPendingCompactions)) {
        consumed.await();
      }
    } finally {
      queueLock.unlock();
    }
  }

  /**
   * Fetch Next pending compaction if available.
   *
   * @return
   * @throws InterruptedException
   */
  protected HoodieInstant fetchNextCompactionInstant() throws InterruptedException {
    LOG.info("Compactor waiting for next instant for compaction upto 60 seconds");
    HoodieInstant instant = pendingCompactions.poll(10, TimeUnit.SECONDS);
    if (instant != null) {
      try {
        queueLock.lock();
        // Signal waiting thread
        consumed.signal();
      } finally {
        queueLock.unlock();
      }
    }
    return instant;
  }

  /**
   * Check whether compactor thread needs to be stopped.
   * @return
   */
  protected boolean shouldStopCompactor() {
    return false;
  }
}
