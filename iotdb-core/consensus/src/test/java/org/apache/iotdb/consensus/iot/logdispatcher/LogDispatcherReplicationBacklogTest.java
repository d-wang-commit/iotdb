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

package org.apache.iotdb.consensus.iot.logdispatcher;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.iot.IoTConsensusReplicationSnapshot;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class LogDispatcherReplicationBacklogTest {

  private static final DataRegionId REGION = new DataRegionId(7);

  @Test
  public void testEachTargetUsesCurrentAcknowledgementRatherThanFlushedCheckpoint()
      throws Exception {
    final LogDispatcher.LogDispatcherThread dn2 = thread(2, 115, 100);
    final LogDispatcher.LogDispatcherThread dn3 = thread(3, 110, 80);
    final LogDispatcher dispatcher = dispatcher(dn2, dn3);
    dispatcher.start();

    final IoTConsensusReplicationSnapshot snapshot =
        dispatcher.getReplicationBacklogSnapshot(120, true);

    assertTrue(snapshot.isValid());
    assertEquals(2, snapshot.getPendingRequests().size());
    assertEquals(Long.valueOf(5), snapshot.getPendingRequests().get(2));
    assertEquals(Long.valueOf(10), snapshot.getPendingRequests().get(3));
    verify(dn2, never()).getLastFlushedSyncIndex();
    verify(dn3, never()).getLastFlushedSyncIndex();

    // The same head includes queued, WAL-only and in-flight requests. An ACK alone reduces it.
    when(dn2.getCurrentSyncIndex()).thenReturn(120L);
    final IoTConsensusReplicationSnapshot advanced =
        dispatcher.getReplicationBacklogSnapshot(120, true);
    assertEquals(Long.valueOf(0), advanced.getPendingRequests().get(2));
    assertEquals(Long.valueOf(10), advanced.getPendingRequests().get(3));
    assertEquals(snapshot.getGeneration(), advanced.getGeneration());
    assertEquals(Long.valueOf(5), snapshot.getPendingRequests().get(2));
  }

  @Test
  public void testDifferentSourcesKeepIndependentSequenceSpaces() throws Exception {
    final LogDispatcher dn1 = dispatcher(thread(2, 115, 100), thread(3, 110, 100));
    final LogDispatcher dn2 = dispatcher(thread(1, 50, 40), thread(3, 70, 60));
    dn1.start();
    dn2.start();

    final Map<Integer, Long> fromDn1 =
        dn1.getReplicationBacklogSnapshot(120, true).getPendingRequests();
    final Map<Integer, Long> fromDn2 =
        dn2.getReplicationBacklogSnapshot(80, true).getPendingRequests();
    assertEquals(30, fromDn2.get(1).longValue());
    assertEquals(5, fromDn1.get(2).longValue());
    assertEquals(20, fromDn1.get(3) + fromDn2.get(3));
  }

  @Test
  public void testStartupInactiveAndStoppedStatesAreNotValidZeroes() throws Exception {
    final LogDispatcher dispatcher = dispatcher(thread(2, 120, 100));
    final IoTConsensusReplicationSnapshot recovering =
        dispatcher.getReplicationBacklogSnapshot(120, true);
    assertFalse(recovering.isValid());
    assertEquals(Long.valueOf(0), recovering.getPendingRequests().get(2));

    dispatcher.start();
    final IoTConsensusReplicationSnapshot running =
        dispatcher.getReplicationBacklogSnapshot(120, true);
    assertTrue(running.isValid());
    assertTrue(running.getGeneration() > recovering.getGeneration());
    assertFalse(dispatcher.getReplicationBacklogSnapshot(120, false).isValid());

    dispatcher.stop();
    final IoTConsensusReplicationSnapshot stopped =
        dispatcher.getReplicationBacklogSnapshot(120, true);
    assertFalse(stopped.isValid());
    assertTrue(stopped.getGeneration() > running.getGeneration());
  }

  @Test
  public void testInconsistentAcknowledgementInvalidatesTheWholeSource() throws Exception {
    final LogDispatcher.LogDispatcherThread dn3 = thread(3, 121, 100);
    final LogDispatcher dispatcher = dispatcher(thread(2, 115, 100), dn3);
    dispatcher.start();

    final IoTConsensusReplicationSnapshot ahead =
        dispatcher.getReplicationBacklogSnapshot(120, true);
    assertFalse(ahead.isValid());
    assertEquals(Long.valueOf(0), ahead.getPendingRequests().get(3));
    assertEquals(Long.valueOf(5), ahead.getPendingRequests().get(2));

    when(dn3.getCurrentSyncIndex()).thenReturn(-1L);
    assertFalse(dispatcher.getReplicationBacklogSnapshot(120, true).isValid());
    assertFalse(dispatcher.getReplicationBacklogSnapshot(-1, true).isValid());
  }

  @Test
  public void testLifecycleChangeDuringCollectionInvalidatesSnapshot() throws Exception {
    final LogDispatcher.LogDispatcherThread dn2 = thread(2, 115, 100);
    final LogDispatcher dispatcher = dispatcher(dn2);
    dispatcher.start();
    when(dn2.getCurrentSyncIndex())
        .thenAnswer(
            invocation -> {
              dispatcher.stop();
              return 115L;
            });

    assertFalse(dispatcher.getReplicationBacklogSnapshot(120, true).isValid());
  }

  @Test
  public void testSnapshotOwnsAnImmutableCopy() {
    final Map<Integer, Long> input = new HashMap<>();
    input.put(2, 5L);
    final IoTConsensusReplicationSnapshot snapshot =
        new IoTConsensusReplicationSnapshot(9, true, input);
    input.put(2, 99L);
    assertEquals(Long.valueOf(5), snapshot.getPendingRequests().get(2));
    try {
      snapshot.getPendingRequests().put(3, 1L);
      fail("Snapshot map must not expose mutable replication state");
    } catch (UnsupportedOperationException expected) {
      assertEquals(1, snapshot.getPendingRequests().size());
    }
  }

  private static LogDispatcher.LogDispatcherThread thread(
      final int target, final long current, final long flushed) throws Exception {
    final LogDispatcher.LogDispatcherThread thread = mock(LogDispatcher.LogDispatcherThread.class);
    when(thread.getPeer())
        .thenReturn(new Peer(REGION, target, new TEndPoint("127.0.0.1", 9000 + target)));
    when(thread.getCurrentSyncIndex()).thenReturn(current);
    when(thread.getLastFlushedSyncIndex()).thenReturn(flushed);
    // stop() invokes private worker cleanup directly, bypassing Mockito interception. Model an
    // already-finished worker with no queued requests so real cleanup does not wait or use WAL.
    setWorkerField(thread, "runFinished", new CountDownLatch(0));
    setWorkerField(thread, "pendingEntries", new LinkedBlockingQueue<>());
    setWorkerField(thread, "bufferedEntries", new LinkedList<>());
    setWorkerField(thread, "syncStatus", mock(SyncStatus.class));
    setWorkerField(thread, "logDispatcherThreadMetrics", mock(LogDispatcherThreadMetrics.class));
    return thread;
  }

  private static void setWorkerField(
      final LogDispatcher.LogDispatcherThread thread, final String name, final Object value)
      throws Exception {
    final Field field = LogDispatcher.LogDispatcherThread.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(thread, value);
  }

  @SuppressWarnings("unchecked")
  private static LogDispatcher dispatcher(final LogDispatcher.LogDispatcherThread... targets)
      throws Exception {
    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    final IStateMachine stateMachine = mock(IStateMachine.class);
    when(server.getStateMachine()).thenReturn(stateMachine);
    when(stateMachine.read(any()))
        .thenReturn(mock(DataSet.class, withSettings().extraInterfaces(ConsensusReqReader.class)));
    final Peer self = new Peer(REGION, 0, new TEndPoint("127.0.0.1", 9000));
    when(server.getThisNode()).thenReturn(self);
    when(server.getConfiguration()).thenReturn(Collections.singletonList(self));
    final LogDispatcher dispatcher = new LogDispatcher(server, null);

    // Exercise real snapshot/lifecycle methods without WAL readers, checkpoints or background jobs.
    final Field threads = LogDispatcher.class.getDeclaredField("threads");
    threads.setAccessible(true);
    ((List<LogDispatcher.LogDispatcherThread>) threads.get(dispatcher))
        .addAll(Arrays.asList(targets));
    final ExecutorService executor = mock(ExecutorService.class);
    when(executor.awaitTermination(anyLong(), any())).thenReturn(true);
    final Field executorField = LogDispatcher.class.getDeclaredField("executorService");
    executorField.setAccessible(true);
    executorField.set(dispatcher, executor);
    final Method publish = LogDispatcher.class.getDeclaredMethod("publishBacklogView");
    publish.setAccessible(true);
    publish.invoke(dispatcher);
    return dispatcher;
  }
}
