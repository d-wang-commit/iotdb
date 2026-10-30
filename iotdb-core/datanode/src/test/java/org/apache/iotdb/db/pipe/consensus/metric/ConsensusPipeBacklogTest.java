/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.pipe.consensus.metric;

import org.apache.iotdb.db.pipe.consensus.metric.ConsensusPipeBacklog.Kind;
import org.apache.iotdb.db.pipe.consensus.metric.ConsensusPipeBacklog.Token;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;

import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ConsensusPipeBacklogTest {

  private final List<ConsensusPipeBacklog> instances = new ArrayList<>();

  @After
  public void tearDown() {
    instances.forEach(ConsensusPipeBacklog::close);
  }

  @Test
  public void testHistoricalSupplyTransfersAlreadyCountedWork() {
    final ConsensusPipeBacklog backlog = newInstance();
    assertTrue(backlog.addPending(Kind.FILE, 10));
    assertEquals(ConsensusPipeBacklog.RECOVERING, backlog.snapshot().getState());
    backlog.markRealtimeReady();
    assertEquals(ConsensusPipeBacklog.RECOVERING, backlog.snapshot().getState());
    backlog.markHistoricalReady();
    assertEquals(ConsensusPipeBacklog.VALID, backlog.snapshot().getState());

    final Token first = backlog.takePendingToken(Kind.FILE);
    final Token second = backlog.takePendingToken(Kind.FILE);
    assertEquals(10, backlog.snapshot().getFileCount());
    second.finish();
    assertEquals(9, backlog.snapshot().getFileCount());
    backlog.discardPending(Kind.FILE, 8);
    assertEquals(1, backlog.snapshot().getFileCount());
    first.finish();
    first.finish();
    assertEquals(0, backlog.snapshot().getFileCount());
    assertEquals(ConsensusPipeBacklog.VALID, backlog.snapshot().getState());
  }

  @Test
  public void testReferencesAndRetryDoNotDuplicatePendingWork() {
    final ConsensusPipeBacklog backlog = readyInstance();
    final PipeDeleteDataNodeEvent event = new PipeDeleteDataNodeEvent(null, false);
    event.bindReplicationEventToken(backlog.newToken(Kind.DELETION));
    assertTrue(event.increaseReferenceCount("source"));
    assertTrue(event.increaseReferenceCount("sink"));
    assertTrue(event.increaseReferenceCount("retry"));
    assertEquals(1, backlog.snapshot().getDeletionCount());

    assertTrue(event.decreaseReferenceCount("source", false));
    assertTrue(event.decreaseReferenceCount("retry", false));
    assertEquals(1, backlog.snapshot().getDeletionCount());
    // A null committer key intentionally demonstrates independence from ordered progress commit.
    assertTrue(event.decreaseReferenceCount("sink", true));
    assertEquals(0, backlog.snapshot().getDeletionCount());
    assertFalse(event.decreaseReferenceCount("duplicate callback", true));
    assertEquals(0, backlog.snapshot().getDeletionCount());
    assertEquals(ConsensusPipeBacklog.VALID, backlog.snapshot().getState());
  }

  @Test
  public void testAssignerTransfersOnlyTheMatchingDirection() {
    final ConsensusPipeBacklog firstTarget = readyInstance();
    final ConsensusPipeBacklog secondTarget = readyInstance();
    final PipeRealtimeEvent raw =
        new PipeRealtimeEvent(new PipeDeleteDataNodeEvent(null, false), null, null);
    raw.addReplicationEventToken(11, firstTarget.newToken(Kind.DELETION));
    raw.addReplicationEventToken(22, secondTarget.newToken(Kind.DELETION));

    final PipeRealtimeEvent copy =
        raw.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
            "copy", 0, null, null, null, null, null, null, false, Long.MIN_VALUE, Long.MAX_VALUE);
    copy.bindReplicationEventToken(raw.takeReplicationEventToken(11));
    assertNull(raw.takeReplicationEventToken(11));
    assertTrue(copy.increaseReferenceCount("source"));
    raw.releaseUnassignedReplicationEventTokens();
    raw.releaseUnassignedReplicationEventTokens();
    assertEquals(1, firstTarget.snapshot().getDeletionCount());
    assertEquals(0, secondTarget.snapshot().getDeletionCount());
    assertEquals(ConsensusPipeBacklog.VALID, secondTarget.snapshot().getState());

    assertTrue(copy.decreaseReferenceCount("source", false));
    assertEquals(0, firstTarget.snapshot().getDeletionCount());
  }

  @Test
  public void testOldTaskCallbacksCannotDecrementReplacement() {
    final String name = "backlog-" + UUID.randomUUID();
    final ConsensusPipeBacklog old = create(name, 1);
    final Token oldToken = old.newToken(Kind.TABLET);
    final ConsensusPipeBacklog replacement = create(name, 2);
    replacement.markHistoricalReady();
    replacement.markRealtimeReady();
    final Token newToken = replacement.newToken(Kind.TABLET);

    oldToken.invalidate();
    oldToken.finish();
    old.close();
    assertSame(replacement, ConsensusPipeBacklog.get(name));
    assertNotEquals(old.getGeneration(), replacement.getGeneration());
    assertEquals(1, replacement.snapshot().getTabletCount());
    assertEquals(ConsensusPipeBacklog.VALID, replacement.snapshot().getState());
    newToken.finish();
    assertEquals(0, replacement.snapshot().getTabletCount());
  }

  @Test
  public void testAbnormalClearAndFailedResourceAcquisitionInvalidateSample() {
    final ConsensusPipeBacklog backlog = readyInstance();
    final PipeDeleteDataNodeEvent event = new PipeDeleteDataNodeEvent(null, false);
    event.bindReplicationEventToken(backlog.newToken(Kind.DELETION));
    assertTrue(event.increaseReferenceCount("source"));
    assertTrue(event.clearReferenceCount("shutdown"));
    assertEquals(0, backlog.snapshot().getDeletionCount());
    assertEquals(ConsensusPipeBacklog.ERROR, backlog.snapshot().getState());

    final ConsensusPipeBacklog failed = readyInstance();
    final PipeDeleteDataNodeEvent brokenResource =
        new PipeDeleteDataNodeEvent(null, false) {
          @Override
          public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
            return false;
          }
        };
    brokenResource.bindReplicationEventToken(failed.newToken(Kind.DELETION));
    assertFalse(brokenResource.increaseReferenceCount("source"));
    assertEquals(0, failed.snapshot().getDeletionCount());
    assertEquals(ConsensusPipeBacklog.ERROR, failed.snapshot().getState());
  }

  @Test
  public void testBindingAfterReleaseDoesNotLeakPendingItem() {
    final ConsensusPipeBacklog backlog = readyInstance();
    final PipeDeleteDataNodeEvent event = new PipeDeleteDataNodeEvent(null, false);
    assertTrue(event.clearReferenceCount("discard"));
    event.bindReplicationEventToken(backlog.newToken(Kind.FILE));
    assertEquals(0, backlog.snapshot().getFileCount());
    assertEquals(ConsensusPipeBacklog.ERROR, backlog.snapshot().getState());
  }

  @Test
  public void testFilePlaceholderTokenTransfersWithoutDoubleCounting() {
    final ConsensusPipeBacklog backlog = readyInstance();
    final Token unsealed = backlog.newToken(Kind.FILE);
    assertEquals(1, backlog.snapshot().getFileCount());
    final PipeDeleteDataNodeEvent event = new PipeDeleteDataNodeEvent(null, false);
    // The token is transferred to the sealed event; no second newToken call is made.
    event.bindReplicationEventToken(unsealed);
    assertTrue(event.increaseReferenceCount("file source"));
    assertTrue(event.increaseReferenceCount("file transfer"));
    assertEquals(1, backlog.snapshot().getFileCount());
    assertTrue(event.decreaseReferenceCount("file source", false));
    assertTrue(event.decreaseReferenceCount("file transfer", true));
    assertEquals(0, backlog.snapshot().getFileCount());
    assertEquals(ConsensusPipeBacklog.VALID, backlog.snapshot().getState());
  }

  @Test
  public void testOverflowAndUnreservedSupplyDoNotBecomeKnownZero() {
    final ConsensusPipeBacklog backlog = readyInstance();
    assertTrue(backlog.addPending(Kind.FILE, Long.MAX_VALUE));
    final Token overflow = backlog.newToken(Kind.FILE);
    overflow.finish();
    assertEquals(Long.MAX_VALUE, backlog.snapshot().getFileCount());
    assertEquals(ConsensusPipeBacklog.ERROR, backlog.snapshot().getState());

    final ConsensusPipeBacklog unreserved = readyInstance();
    unreserved.takePendingToken(Kind.TABLET).finish();
    assertEquals(0, unreserved.snapshot().getTabletCount());
    assertEquals(ConsensusPipeBacklog.ERROR, unreserved.snapshot().getState());
  }

  @Test
  public void testConcurrentCompletionsAndSnapshots() throws Exception {
    final ConsensusPipeBacklog backlog = readyInstance();
    final ExecutorService executor = Executors.newFixedThreadPool(4);
    final CountDownLatch start = new CountDownLatch(1);
    final List<Future<?>> futures = new ArrayList<>();
    try {
      for (int thread = 0; thread < 4; thread++) {
        futures.add(
            executor.submit(
                () -> {
                  start.await();
                  for (int item = 0; item < 1000; item++) {
                    final Token token = backlog.newToken(Kind.TABLET);
                    assertTrue(backlog.snapshot().getTabletCount() > 0);
                    token.finish();
                    token.finish();
                  }
                  return null;
                }));
      }
      start.countDown();
      for (final Future<?> future : futures) {
        future.get(30, TimeUnit.SECONDS);
      }
      assertEquals(0, backlog.snapshot().getTabletCount());
      assertEquals(ConsensusPipeBacklog.VALID, backlog.snapshot().getState());
    } finally {
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  private ConsensusPipeBacklog newInstance() {
    return create("backlog-" + UUID.randomUUID(), 1);
  }

  private ConsensusPipeBacklog readyInstance() {
    final ConsensusPipeBacklog backlog = newInstance();
    backlog.markRealtimeReady();
    backlog.markHistoricalReady();
    return backlog;
  }

  private ConsensusPipeBacklog create(final String name, final long sourceId) {
    final ConsensusPipeBacklog backlog = ConsensusPipeBacklog.create(name, 1, sourceId);
    instances.add(backlog);
    return backlog;
  }
}
