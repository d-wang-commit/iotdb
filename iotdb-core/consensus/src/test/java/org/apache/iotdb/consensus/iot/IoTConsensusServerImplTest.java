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

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserDataTransferAuditHandler;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.DeserializedBatchIndexedConsensusRequest;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.config.UserDataTransferAuditClassifier;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.i18n.ConsensusMessages;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.consensus.iot.logdispatcher.LogDispatcher;
import org.apache.iotdb.consensus.iot.thrift.TTriggerSnapshotLoadReq;
import org.apache.iotdb.consensus.iot.util.FakeConsensusReqReader;
import org.apache.iotdb.consensus.iot.util.TestEntry;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class IoTConsensusServerImplTest {

  private static final int WRITER_COUNT = 4;
  private static final int READER_COUNT = 4;
  private static final int PEERS_PER_WRITER = 1_000;

  @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testCorruptSourceFencePreventsRecovery() throws Exception {
    Path file = temporaryFolder.getRoot().toPath().resolve("migration.source");
    new MigrationSourceState(9, 1, 27).persist(file);
    byte[] record = Files.readAllBytes(file);
    record[record.length - 1] ^= 1;
    Files.write(file, record);
    assertThrows(IOException.class, () -> MigrationSourceState.load(file));
  }

  @Test
  public void testSourceRestartBeforeFreezeCannotSealShorterRecoveredWal() throws Exception {
    FakeConsensusReqReader reader = mock(FakeConsensusReqReader.class);
    when(reader.getCurrentSearchIndex()).thenReturn(1L);
    Peer self = new Peer(new DataRegionId(11), 10, new TEndPoint("127.0.0.1", 6667));
    Peer peer = new Peer(new DataRegionId(11), 9, new TEndPoint("127.0.0.1", 6668));
    ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor();
    try {
      IoTConsensusServerImpl source = newRetentionTestServer(self, reader, background);
      try {
        // Keep the target in the peer list while preventing outbound delivery of the tail.
        source.getLogDispatcher().stop();
        source.buildSyncLogChannel(peer, 1, false);
        source.prepareMigration(71, peer.getNodeId());
        assertTrue(source.hasPreparedMigration(71, peer.getNodeId()));
        assertFalse(Files.exists(temporaryFolder.getRoot().toPath().resolve("migration.source")));
        for (int value = 404; value <= 409; value++) {
          assertEquals(
              TSStatusCode.SUCCESS_STATUS.getStatusCode(),
              source.write(new TestEntry(value, self)).getCode());
        }
        assertEquals(7L, source.getSearchIndex());
      } finally {
        source.stop();
      }
      // The recovered WAL and the target contain only the baseline. The old preparation
      // cannot turn that coincidental agreement into a completed handoff.
      IoTConsensusServerImpl recovered = newRetentionTestServer(self, reader, background);
      try {
        recovered.getLogDispatcher().stop();
        recovered.buildSyncLogChannel(peer, 1, false);
        assertEquals(1L, recovered.getSearchIndex());
        assertFalse(recovered.hasPreparedMigration(71, peer.getNodeId()));
        assertThrows(
            ConsensusGroupModifyPeerException.class,
            () -> recovered.freezeForMigration(71, peer.getNodeId()));
        assertFalse(recovered.hasMigrationFence(71));
        assertTrue(recovered.isActive());
        recovered.rollbackMigration(71, peer.getNodeId());
        assertThrows(
            ConsensusGroupModifyPeerException.class,
            () -> recovered.prepareMigration(71, peer.getNodeId()));
        assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            recovered.write(new TestEntry(410, self)).getCode());
      } finally {
        recovered.stop();
      }
    } finally {
      background.shutdownNow();
    }
  }

  @Test
  public void testMigrationFenceDrainsThrottledWriterAndSurvivesRestart() throws Exception {
    FakeConsensusReqReader reader = mock(FakeConsensusReqReader.class);
    CountDownLatch writerReachedThrottle = new CountDownLatch(1);
    when(reader.getTotalSize())
        .thenAnswer(
            invocation -> {
              writerReachedThrottle.countDown();
              return Long.MAX_VALUE;
            });
    ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor();
    ExecutorService writers = Executors.newSingleThreadExecutor();
    Peer self = new Peer(new DataRegionId(1), 0, new TEndPoint("127.0.0.1", 6667));
    Peer peer = new Peer(new DataRegionId(1), 1, new TEndPoint("127.0.0.1", 6668));
    IoTConsensusServerImpl server = newRetentionTestServer(self, reader, background);
    try {
      server.getIndexObject().set(27);
      server.getLogDispatcher().stop();
      server.buildSyncLogChannel(peer, 27, false);
      server.prepareMigration(9, peer.getNodeId());
      assertFalse(server.hasMigrationFence(9));
      assertThrows(
          ConsensusGroupModifyPeerException.class,
          () -> server.freezeForMigration(10, peer.getNodeId()));
      assertThrows(
          ConsensusGroupModifyPeerException.class,
          () -> server.freezeForMigration(9, self.getNodeId()));
      Future<TSStatus> write = writers.submit(() -> server.write(new TestEntry(1, self)));
      assertTrue(writerReachedThrottle.await(5, TimeUnit.SECONDS));
      assertEquals(27, server.freezeForMigration(9, peer.getNodeId()));
      assertEquals(
          TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(),
          write.get(5, TimeUnit.SECONDS).getCode());
      assertTrue(server.getConfiguration().contains(peer));
      assertEquals(27, server.getSearchIndex());
    } finally {
      server.stop();
      writers.shutdownNow();
      background.shutdownNow();
    }

    ScheduledExecutorService afterRestart = Executors.newSingleThreadScheduledExecutor();
    IoTConsensusServerImpl recovered = newRetentionTestServer(self, reader, afterRestart);
    try {
      recovered.setActive(true);
      assertFalse(recovered.isActive());
      assertTrue(recovered.hasMigrationFence(9));
      assertTrue(recovered.hasPreparedMigration(9, peer.getNodeId()));
      assertFalse(recovered.hasMigrationFence(10));
      assertEquals(27, recovered.freezeForMigration(9, peer.getNodeId()));
      assertThrows(
          ConsensusGroupModifyPeerException.class,
          () -> recovered.prepareMigration(11, peer.getNodeId()));
      assertThrows(
          ConsensusGroupModifyPeerException.class,
          () -> recovered.rollbackMigration(11, peer.getNodeId()));
      // Reopening the original replica does not require its recovered index to reach the fence.
      assertTrue(recovered.getSearchIndex() < 27);
      recovered.rollbackMigration(9, peer.getNodeId());
      assertTrue(recovered.isActive());
      assertFalse(recovered.hasMigrationFence(9));
    } finally {
      recovered.stop();
      afterRestart.shutdownNow();
    }
  }

  @Test
  public void testRollbackRevokesFreezeAndDeleteAcrossRestarts() throws Exception {
    FakeConsensusReqReader reader = mock(FakeConsensusReqReader.class);
    when(reader.getCurrentSearchIndex()).thenReturn(27L);
    Peer self = new Peer(new DataRegionId(1), 0, new TEndPoint("127.0.0.1", 6667));
    Peer peer = new Peer(new DataRegionId(1), 1, new TEndPoint("127.0.0.1", 6668));
    ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor();
    try {
      IoTConsensusServerImpl source = newRetentionTestServer(self, reader, background);
      try {
        source.getLogDispatcher().stop();
        source.buildSyncLogChannel(peer, 27, false);
        source.prepareMigration(9, peer.getNodeId());
        assertEquals(27, source.freezeForMigration(9, peer.getNodeId()));
        assertFalse(source.isActive());
        assertTrue(source.isActiveForReplication());
        source.setActive(false);
        assertFalse(source.isActiveForReplication());
      } finally {
        source.stop();
      }
      IoTConsensusServerImpl recovered = newRetentionTestServer(self, reader, background);
      try {
        assertThrows(
            ConsensusGroupModifyPeerException.class,
            () -> recovered.rollbackMigration(9, self.getNodeId()));
        Path blockedTempFile =
            Files.createDirectory(
                temporaryFolder.getRoot().toPath().resolve("migration.source.tmp"));
        assertThrows(
            ConsensusGroupModifyPeerException.class,
            () -> recovered.rollbackMigration(9, peer.getNodeId()));
        assertTrue(recovered.hasMigrationFence(9));
        assertFalse(recovered.isActive());
        Files.delete(blockedTempFile);
        recovered.rollbackMigration(9, peer.getNodeId());
        assertTrue(recovered.isActive());
        assertFalse(recovered.hasMigrationFence(9));
        assertEquals(
            TSStatusCode.SUCCESS_STATUS.getStatusCode(),
            recovered.write(new TestEntry(1, self)).getCode());
      } finally {
        recovered.stop();
      }
      IoTConsensusServerImpl writable = newRetentionTestServer(self, reader, background);
      try {
        writable.rollbackMigration(9, peer.getNodeId());
        assertFalse(writable.hasPreparedMigration(9, peer.getNodeId()));
        assertFalse(writable.hasMigrationFence(9));
        assertThrows(
            ConsensusGroupModifyPeerException.class,
            () -> writable.freezeForMigration(9, peer.getNodeId()));
        assertThrows(
            ConsensusGroupModifyPeerException.class,
            () -> writable.prepareMigration(9, peer.getNodeId()));
        writable.prepareMigration(10, peer.getNodeId());
        assertThrows(
            ConsensusGroupModifyPeerException.class,
            () -> writable.rollbackMigration(9, peer.getNodeId()));
        assertTrue(writable.hasPreparedMigration(10, peer.getNodeId()));
      } finally {
        writable.stop();
      }
    } finally {
      background.shutdownNow();
    }
  }

  @Test
  public void testNewPeerImmediatelyProtectsWALAndCatchupTimesOut() throws Exception {
    FakeConsensusReqReader reader = mock(FakeConsensusReqReader.class);
    AtomicLong boundary = new AtomicLong();
    doAnswer(
            invocation -> {
              boundary.set(invocation.getArgument(0));
              return null;
            })
        .when(reader)
        .setSafelyDeletedSearchIndex(anyLong());
    ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor();
    Peer self = new Peer(new DataRegionId(1), 0, new TEndPoint("127.0.0.1", 6667));
    Peer peer = new Peer(new DataRegionId(1), 1, new TEndPoint("127.0.0.1", 6668));
    IoTConsensusServerImpl server = newRetentionTestServer(self, reader, background);
    try {
      server.getIndexObject().set(10);
      server.checkAndUpdateSafeDeletedSearchIndex();
      assertEquals(Long.MAX_VALUE, boundary.get());
      server.buildSyncLogChannel(peer, false);
      assertEquals(10, boundary.get());
      server.getLogDispatcher().waitUntilSyncLogCompleted(peer, 10, 100);
      assertThrows(
          ConsensusGroupModifyPeerException.class,
          () -> server.getLogDispatcher().waitUntilSyncLogCompleted(peer, 11, 1));
    } finally {
      server.stop();
      background.shutdownNow();
    }
  }

  @Test
  public void testMissingWALCannotProduceAnAcknowledgableBatch() throws Exception {
    FakeConsensusReqReader reader = mock(FakeConsensusReqReader.class);
    ConsensusReqReader.ReqIterator iterator = mock(ConsensusReqReader.ReqIterator.class);
    when(reader.getReqIterator(anyLong())).thenReturn(iterator);
    when(iterator.next()).thenReturn(new IndexedConsensusRequest(4, Collections.emptyList()));
    ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor();
    Peer self = new Peer(new DataRegionId(1), 0, new TEndPoint("127.0.0.1", 6667));
    Peer peer = new Peer(new DataRegionId(1), 1, new TEndPoint("127.0.0.1", 6668));
    IoTConsensusServerImpl server = newRetentionTestServer(self, reader, background);
    try {
      server.getIndexObject().set(4);
      server.buildSyncLogChannel(peer, 0, false);
      Field threadsField = LogDispatcher.class.getDeclaredField("threads");
      threadsField.setAccessible(true);
      @SuppressWarnings("unchecked")
      List<LogDispatcher.LogDispatcherThread> threads =
          (List<LogDispatcher.LogDispatcherThread>) threadsField.get(server.getLogDispatcher());
      LogDispatcher.LogDispatcherThread thread = threads.get(0);
      assertThrows(IllegalStateException.class, thread::getBatch);
      assertEquals(0, thread.getCurrentSyncIndex());
      // The real dispatcher loop must expose the failure to the add-peer waiter as well.
      thread.run();
      assertThrows(
          ConsensusGroupModifyPeerException.class,
          () -> server.getLogDispatcher().waitUntilSyncLogCompleted(peer, 4, 100));
    } finally {
      server.stop();
      background.shutdownNow();
    }
  }

  private IoTConsensusServerImpl newRetentionTestServer(
      Peer self, FakeConsensusReqReader reader, ScheduledExecutorService background)
      throws Exception {
    return new IoTConsensusServerImpl(
        temporaryFolder.getRoot().getAbsolutePath(),
        null,
        DirectoryStrategyType.SEQUENCE_STRATEGY,
        self,
        Collections.singletonList(self),
        new TestStateMachine() {
          @Override
          public synchronized DataSet read(IConsensusRequest request) {
            return reader;
          }
        },
        background,
        null,
        null,
        IoTConsensusConfig.newBuilder().build());
  }

  @Test
  public void testMigrationBaselineRequiresCompleteSnapshotAndSuccessfulLoad() throws Exception {
    ScheduledExecutorService background = Executors.newSingleThreadScheduledExecutor();
    Peer self = new Peer(new DataRegionId(1), 1, new TEndPoint("127.0.0.1", 6668));
    AtomicBoolean allowLoad = new AtomicBoolean();
    AtomicLong loads = new AtomicLong();
    IoTConsensusServerImpl server =
        new IoTConsensusServerImpl(
            temporaryFolder.getRoot().getAbsolutePath(),
            null,
            DirectoryStrategyType.SEQUENCE_STRATEGY,
            self,
            Collections.singletonList(self),
            new TestStateMachine() {
              @Override
              public boolean loadSnapshot(File dir) {
                loads.incrementAndGet();
                return allowLoad.get();
              }

              @Override
              public boolean flushForMigration() {
                return true;
              }
            },
            background,
            null,
            null,
            IoTConsensusConfig.newBuilder().build());
    try {
      server.setActive(false);
      String snapshot = "snapshot_1_test";
      TTriggerSnapshotLoadReq request =
          new TTriggerSnapshotLoadReq(self.getGroupId().convertToTConsensusGroupId(), snapshot)
              .setMigrationId(1)
              .setSourceNodeId(0)
              .setSnapshotIndex(5)
              .setSnapshotFileCount(1);
      assertFalse(server.loadMigrationSnapshot(request));
      assertEquals(0, loads.get());
      String path = snapshot + "/test.tsfile";
      server.receiveSnapshotFragment(snapshot, path, ByteBuffer.wrap(new byte[] {1, 2}), 0);
      server.receiveSnapshotFragment(snapshot, path, ByteBuffer.wrap(new byte[] {3}), 2);
      assertArrayEquals(
          new byte[] {1, 2, 3},
          Files.readAllBytes(temporaryFolder.getRoot().toPath().resolve(path)));
      request.setMigrationId(2);
      assertFalse(server.loadMigrationSnapshot(request));
      assertEquals(1, loads.get());
      assertFalse(server.confirmMigrationCatchUp(2, 0, 5));
      allowLoad.set(true);
      request.setMigrationId(3);
      assertTrue(server.loadMigrationSnapshot(request));
      server.setActive(true);
      assertTrue(server.loadMigrationSnapshot(request));
      assertEquals(2, loads.get());
      assertTrue(server.confirmMigrationCatchUp(3, 0, 5));
    } finally {
      server.stop();
      background.shutdownNow();
    }
  }

  @Test
  public void testSyncLogBecomesReadOnlyWhileWaiting() throws Exception {
    final ScheduledExecutorService backgroundTaskService =
        Executors.newSingleThreadScheduledExecutor();
    final AtomicBoolean readOnly = new AtomicBoolean();
    final TestStateMachine stateMachine =
        new TestStateMachine() {
          @Override
          public boolean isReadOnly() {
            return readOnly.get();
          }
        };
    final Peer thisNode = new Peer(new DataRegionId(1), 0, new TEndPoint("127.0.0.1", 6667));
    final IoTConsensusServerImpl server =
        new IoTConsensusServerImpl(
            temporaryFolder.getRoot().getAbsolutePath(),
            null,
            DirectoryStrategyType.SEQUENCE_STRATEGY,
            thisNode,
            Collections.singletonList(thisNode),
            stateMachine,
            backgroundTaskService,
            null,
            null,
            IoTConsensusConfig.newBuilder()
                .setReplication(
                    IoTConsensusConfig.Replication.newBuilder()
                        .setMaxWaitingTimeForWaitBatchInMs(100)
                        .build())
                .build());
    final DeserializedBatchIndexedConsensusRequest request =
        new DeserializedBatchIndexedConsensusRequest(1, 2, 2, 1, 2);
    for (int index = 1; index <= 2; index++) {
      request.add(
          new IndexedConsensusRequest(
              index, index, Collections.singletonList(new TestEntry(index, thisNode))));
    }
    final AtomicReference<TSStatus> status = new AtomicReference<>();
    final Thread syncLogThread = new Thread(() -> status.set(server.syncLog(1, request)));
    try {
      assertFalse(server.isReadOnly());
      syncLogThread.start();
      final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (syncLogThread.getState() != Thread.State.TIMED_WAITING
          && System.nanoTime() < deadline) {
        Thread.sleep(10);
      }
      assertEquals(Thread.State.TIMED_WAITING, syncLogThread.getState());
      readOnly.set(true);
      syncLogThread.join(TimeUnit.SECONDS.toMillis(15));
      assertFalse(syncLogThread.isAlive());
      assertEquals(2, status.get().getSubStatusSize());
      assertEquals(
          TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(),
          status.get().getSubStatus().get(0).getCode());
      assertEquals(
          ConsensusMessages.SYNC_LOG_SYSTEM_READ_ONLY,
          status.get().getSubStatus().get(0).getMessage());
      assertEquals(
          TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(),
          status.get().getSubStatus().get(1).getCode());
      assertTrue(stateMachine.getRequestSet().isEmpty());
      assertEquals(0, server.getEffectiveSafePhysicalTime(1));

      // The sender must be able to retry the complete batch after the replica becomes writable.
      readOnly.set(false);
      server.reloadConsensusConfig(
          IoTConsensusConfig.newBuilder()
              .setReplication(
                  IoTConsensusConfig.Replication.newBuilder()
                      .setMaxPendingBatchesNum(1)
                      .setMaxWaitingTimeForWaitBatchInMs(100)
                      .build())
              .build());
      final TSStatus retryStatus = server.syncLog(1, request);
      assertTrue(
          retryStatus.getSubStatus().stream()
              .allMatch(s -> s.getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()));
      assertEquals(2, stateMachine.getRequestSet().size());
    } finally {
      syncLogThread.interrupt();
      syncLogThread.join(TimeUnit.SECONDS.toMillis(5));
      backgroundTaskService.shutdownNow();
    }
  }

  @Test
  public void testSnapshotTransferAppliesGroupClassifier() {
    final DataRegionId groupId = new DataRegionId(1);
    final UserDataTransferAuditHandler enabledHandler = event -> {};
    final UserDataTransferAuditClassifier auditDatabaseClassifier =
        new UserDataTransferAuditClassifier() {
          @Override
          public boolean containsUserData(
              ConsensusGroupId ignoredGroupId, IConsensusRequest request) {
            return false;
          }

          @Override
          public boolean containsUserData(ConsensusGroupId ignoredGroupId) {
            return false;
          }
        };

    assertTrue(
        IoTConsensusServerImpl.shouldAuditSnapshotTransfer(
            enabledHandler, (ignoredGroupId, request) -> false, groupId));
    assertFalse(
        IoTConsensusServerImpl.shouldAuditSnapshotTransfer(
            enabledHandler, auditDatabaseClassifier, groupId));
    assertFalse(
        IoTConsensusServerImpl.shouldAuditSnapshotTransfer(
            UserDataTransferAuditHandler.NO_OP,
            UserDataTransferAuditClassifier.NO_USER_DATA,
            groupId));
  }

  /**
   * Verifies that configuration snapshots can be read while several writers concurrently add and
   * remove distinct peers. Every snapshot must remain duplicate-free and sorted, and the final
   * configuration must contain exactly the peers that were not removed.
   */
  @Test
  public void testConfigurationConcurrentReadAndWrite() throws Exception {
    final ScheduledExecutorService backgroundTaskService =
        Executors.newSingleThreadScheduledExecutor();
    final ExecutorService executorService =
        Executors.newFixedThreadPool(WRITER_COUNT + READER_COUNT);
    try {
      final Peer thisNode = new Peer(new DataRegionId(1), 0, new TEndPoint("127.0.0.1", 6667));
      final IoTConsensusServerImpl server =
          new IoTConsensusServerImpl(
              temporaryFolder.getRoot().getAbsolutePath(),
              null,
              DirectoryStrategyType.SEQUENCE_STRATEGY,
              thisNode,
              Collections.singletonList(thisNode),
              new TestStateMachine(),
              backgroundTaskService,
              null,
              null,
              IoTConsensusConfig.newBuilder().build());
      final Set<Peer> configuration = getConfiguration(server);
      assertTrue(configuration.spliterator().hasCharacteristics(Spliterator.CONCURRENT));

      final CountDownLatch startLatch = new CountDownLatch(1);
      final CountDownLatch writersDoneLatch = new CountDownLatch(WRITER_COUNT);
      final Set<Peer> expectedConfiguration = new HashSet<>();
      expectedConfiguration.add(thisNode);

      final Future<?>[] writerFutures = new Future<?>[WRITER_COUNT];
      for (int writerIndex = 0; writerIndex < WRITER_COUNT; writerIndex++) {
        final int currentWriterIndex = writerIndex;
        writerFutures[writerIndex] =
            executorService.submit(
                () -> {
                  startLatch.await();
                  try {
                    for (int peerIndex = 0; peerIndex < PEERS_PER_WRITER; peerIndex++) {
                      final Peer peer = createPeer(currentWriterIndex, peerIndex);
                      configuration.add(peer);
                      if ((peerIndex & 1) == 0) {
                        configuration.remove(peer);
                      }
                    }
                  } finally {
                    writersDoneLatch.countDown();
                  }
                  return null;
                });
        for (int peerIndex = 1; peerIndex < PEERS_PER_WRITER; peerIndex += 2) {
          expectedConfiguration.add(createPeer(writerIndex, peerIndex));
        }
      }

      final Future<?>[] readerFutures = new Future<?>[READER_COUNT];
      for (int readerIndex = 0; readerIndex < READER_COUNT; readerIndex++) {
        readerFutures[readerIndex] =
            executorService.submit(
                () -> {
                  startLatch.await();
                  do {
                    assertValidSnapshot(server.getConfiguration());
                  } while (writersDoneLatch.getCount() > 0);
                  assertValidSnapshot(server.getConfiguration());
                  return null;
                });
      }

      startLatch.countDown();
      for (Future<?> writerFuture : writerFutures) {
        writerFuture.get(30, TimeUnit.SECONDS);
      }
      for (Future<?> readerFuture : readerFutures) {
        readerFuture.get(30, TimeUnit.SECONDS);
      }

      assertEquals(expectedConfiguration, new HashSet<>(server.getConfiguration()));
    } finally {
      executorService.shutdownNow();
      backgroundTaskService.shutdownNow();
    }
  }

  @SuppressWarnings("unchecked")
  private static Set<Peer> getConfiguration(IoTConsensusServerImpl server) throws Exception {
    final Field configurationField = IoTConsensusServerImpl.class.getDeclaredField("configuration");
    configurationField.setAccessible(true);
    return (Set<Peer>) configurationField.get(server);
  }

  private static Peer createPeer(int writerIndex, int peerIndex) {
    final int nodeId = writerIndex * PEERS_PER_WRITER + peerIndex + 1;
    return new Peer(new DataRegionId(1), nodeId, new TEndPoint("127.0.0.1", 10_000 + nodeId));
  }

  private static void assertValidSnapshot(List<Peer> snapshot) {
    assertEquals(snapshot.size(), new HashSet<>(snapshot).size());
    for (int index = 1; index < snapshot.size(); index++) {
      assertTrue(snapshot.get(index - 1).compareTo(snapshot.get(index)) < 0);
    }
  }
}
