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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserDataTransferAuditHandler;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IndexedConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.iot.log.ConsensusReqReader;
import org.apache.iotdb.consensus.iot.util.TestEntry;
import org.apache.iotdb.consensus.iot.util.TestStateMachine;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Regression test for the snapshot-load-failure bug (a failed snapshot load on the AddPeer target
 * was silently swallowed, so the new peer was activated and the migration falsely reported
 * successful, losing data on the new replica).
 *
 * <p>It builds a real two-node IoTConsensus group, forces the target peer's {@link
 * org.apache.iotdb.consensus.IStateMachine#loadSnapshot} to fail, and verifies that {@code
 * addRemotePeer}:
 *
 * <ul>
 *   <li>actually reaches the snapshot-load step (so the failure is the one under test, not an
 *       earlier step),
 *   <li>fails with a {@link ConsensusException} instead of silently succeeding,
 *   <li>does not leave the target peer active with an incompletely-loaded snapshot.
 * </ul>
 */
public class AddPeerSnapshotLoadFailureTest {

  private final Logger logger = LoggerFactory.getLogger(AddPeerSnapshotLoadFailureTest.class);

  private final ConsensusGroupId gid = new DataRegionId(1);

  private final int basePort = 9200;

  private final List<Peer> peers =
      Arrays.asList(
          new Peer(gid, 1, new TEndPoint("127.0.0.1", basePort - 1)),
          new Peer(gid, 2, new TEndPoint("127.0.0.1", basePort)));

  private final List<File> peersStorage =
      Arrays.asList(
          new File("target" + File.separator + "snapshot-load-fail-1"),
          new File("target" + File.separator + "snapshot-load-fail-2"));

  private final List<List<String>> peersRecvSnapshotDirs =
      Arrays.asList(
          Arrays.asList(
              "target" + File.separator + "snapshot-load-fail-1-recv-1",
              "target" + File.separator + "snapshot-load-fail-1-recv-2"),
          Arrays.asList(
              "target" + File.separator + "snapshot-load-fail-2-recv-1",
              "target" + File.separator + "snapshot-load-fail-2-recv-2"));

  private final List<IoTConsensus> servers = new ArrayList<>();
  private final List<ControllableStateMachine> stateMachines = new ArrayList<>();
  private volatile UserDataTransferAuditHandler snapshotAudit = UserDataTransferAuditHandler.NO_OP;

  /**
   * A {@link TestStateMachine} that takes a real snapshot and whose snapshot load can be made to
   * fail on demand. When not forced to fail, {@link #loadSnapshot} mirrors the real {@code
   * SnapshotLoader} contract by reporting failure when the snapshot root does not exist — which is
   * exactly what happens for the receive folders that never got a fragment of a snapshot in a
   * multi-data-dir deployment.
   */
  private static class ControllableStateMachine extends TestStateMachine {
    private volatile boolean failLoadSnapshot = false;
    private volatile boolean loadSnapshotInvoked = false;
    private volatile boolean emptySnapshot = false;
    private boolean canFlushForMigration = true;
    private boolean includeSecondFile;
    private final CountDownLatch loading = new CountDownLatch(1);
    private CountDownLatch continueLoad;

    void setFailLoadSnapshot(boolean failLoadSnapshot) {
      this.failLoadSnapshot = failLoadSnapshot;
    }

    void setEmptySnapshot(boolean emptySnapshot) {
      this.emptySnapshot = emptySnapshot;
    }

    boolean isLoadSnapshotInvoked() {
      return loadSnapshotInvoked;
    }

    @Override
    public boolean loadSnapshot(File latestSnapshotRootDir) {
      loadSnapshotInvoked = true;
      loading.countDown();
      if (continueLoad != null) {
        try {
          if (!continueLoad.await(15, TimeUnit.SECONDS)) {
            return false;
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          return false;
        }
      }
      if (failLoadSnapshot) {
        return false;
      }
      // Mirror SnapshotLoader: a receive folder that never received a fragment of this snapshot has
      // no snapshot root, so loading from it must report failure.
      if (!latestSnapshotRootDir.exists()) {
        return false;
      }
      File data = new File(latestSnapshotRootDir, "snapshot.data");
      if (data.exists()) {
        try {
          ByteBuffer entries = ByteBuffer.wrap(Files.readAllBytes(data.toPath()));
          getRequestSet().clear();
          while (entries.hasRemaining()) {
            TestEntry entry = new TestEntry(entries.getInt(), Peer.deserialize(entries));
            write(
                new IndexedConsensusRequest(
                    ConsensusReqReader.DEFAULT_SEARCH_INDEX, Collections.singletonList(entry)));
          }
        } catch (IOException e) {
          return false;
        }
      }
      return true;
    }

    @Override
    public boolean flushForMigration() {
      return canFlushForMigration;
    }

    @Override
    public boolean takeSnapshot(File snapshotDir) {
      if (emptySnapshot) {
        // Mirror an empty region: the snapshot has no fragments to transmit. The receiver then
        // materializes no snapshot under any receive folder, which must still load successfully.
        return true;
      }
      // Use a second small file when the test needs to pause between snapshot transfers.
      try (OutputStream data =
          Files.newOutputStream(new File(snapshotDir, "snapshot.data").toPath())) {
        for (TestEntry entry : getData()) {
          ByteBuffer bytes = entry.serializeToByteBuffer();
          data.write(bytes.array(), bytes.arrayOffset() + bytes.position(), bytes.remaining());
        }
        if (includeSecondFile) {
          Files.write(new File(snapshotDir, "second").toPath(), new byte[] {1});
        }
        return true;
      } catch (IOException e) {
        return false;
      }
    }

    // TestStateMachine does not implement clearSnapshot (the IStateMachine default throws). The
    // AddPeer flow calls it in a finally block to clean up the local snapshot, so we provide a
    // no-op here; otherwise that cleanup would mask the ConsensusException we are asserting on.
    @Override
    public boolean clearSnapshot() {
      return true;
    }
  }

  @Before
  public void setUp() throws Exception {
    for (File file : peersStorage) {
      file.mkdirs();
      stateMachines.add(new ControllableStateMachine());
    }
    peersRecvSnapshotDirs.forEach(innerList -> innerList.forEach(dir -> new File(dir).mkdirs()));
    initServer();
  }

  @After
  public void tearDown() throws Exception {
    servers.parallelStream().forEach(IoTConsensus::stop);
    servers.clear();
    for (File file : peersStorage) {
      FileUtils.deleteFully(file);
    }
    peersRecvSnapshotDirs.forEach(
        innerList ->
            innerList.forEach(
                dir -> {
                  try {
                    FileUtils.deleteFully(new File(dir));
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                }));
  }

  private void initServer() throws IOException {
    Assume.assumeTrue(checkPortAvailable());
    try {
      for (int i = 0; i < peers.size(); i++) {
        servers.add(newServer(i));
      }
      for (int i = 0; i < peers.size(); i++) {
        servers.get(i).start();
      }
    } catch (IOException e) {
      if (e.getCause() instanceof StartupException) {
        // just succeed when can not bind socket
        logger.info("Can not start IoTConsensus because", e);
        Assume.assumeTrue(false);
      } else {
        logger.error("Failed because", e);
        Assert.fail("Failed because " + e.getMessage());
      }
    }
  }

  private IoTConsensus newServer(int node) {
    return (IoTConsensus)
        ConsensusFactory.getConsensusImpl(
                ConsensusFactory.IOT_CONSENSUS,
                ConsensusConfig.newBuilder()
                    .setThisNodeId(peers.get(node).getNodeId())
                    .setThisNode(peers.get(node).getEndpoint())
                    .setStorageDir(peersStorage.get(node).getAbsolutePath())
                    .setRecvSnapshotDirs(peersRecvSnapshotDirs.get(node))
                    .setUserDataTransferAuditHandler(event -> snapshotAudit.onAttempt(event))
                    .setUserDataTransferAuditClassifier((group, request) -> false)
                    .setConsensusGroupType(TConsensusGroupType.DataRegion)
                    .build(),
                groupId -> stateMachines.get(node))
            .orElseThrow(
                () ->
                    new IllegalArgumentException(
                        String.format(
                            ConsensusFactory.CONSTRUCT_FAILED_MSG,
                            ConsensusFactory.IOT_CONSENSUS)));
  }

  @Test
  public void singleReplicaAcceptsWritesBetweenSnapshotFragments() throws Exception {
    Peer source = peers.get(0);
    Peer target = peers.get(1);
    long migrationId = 71;
    IoTConsensus donor = servers.get(0);
    IoTConsensus receiver = servers.get(1);
    donor.createLocalPeer(gid, Collections.singletonList(source));
    receiver.createLocalPeer(gid, peers);
    TestEntry snapshotEntry = new TestEntry(1, source);
    TestEntry tail = new TestEntry(2, source);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), donor.write(gid, snapshotEntry).getCode());

    CountDownLatch fragmentSent = new CountDownLatch(1);
    CountDownLatch resumeTransfer = new CountDownLatch(1);
    snapshotAudit =
        event -> {
          fragmentSent.countDown();
          try {
            Assert.assertTrue(resumeTransfer.await(30, TimeUnit.SECONDS));
          } catch (InterruptedException e) {
            throw new AssertionError(e);
          }
        };
    stateMachines.get(0).includeSecondFile = true;
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> add =
          executor.submit(
              () -> {
                donor.addRemotePeerForMigration(gid, target, migrationId);
                return null;
              });
      Assert.assertTrue(fragmentSent.await(30, TimeUnit.SECONDS));
      // One fragment has arrived, the next has not been sent. The only original replica writes.
      Assert.assertFalse(stateMachines.get(1).isLoadSnapshotInvoked());
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), donor.write(gid, tail).getCode());
      resumeTransfer.countDown();
      add.get(30, TimeUnit.SECONDS);
      File receipt = new File(receiver.getImpl(gid).getStorageDir(), "migration.target");
      Assert.assertTrue(receipt.isFile());
      receiver.removeRemotePeerForMigration(gid, source, migrationId);
      Assert.assertTrue(receipt.isFile());
      Assert.assertTrue(receiver.isRegionMigrationConfirmed(gid, source.getNodeId(), migrationId));
      Assert.assertFalse(donor.getImpl(gid).isActive());
      donor.deleteLocalPeerForMigration(gid, migrationId, () -> RpcUtils.SUCCESS_STATUS);
      Assert.assertEquals(
          new HashSet<>(Arrays.asList(snapshotEntry, tail)), stateMachines.get(1).getData());
    } finally {
      resumeTransfer.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void failedTargetFlushCanBeRolledBackAfterTargetRestart() throws Exception {
    Peer source = peers.get(0);
    Peer target = peers.get(1);
    long migrationId = 26;
    IoTConsensus donor = servers.get(0);
    IoTConsensus receiver = servers.get(1);
    donor.createLocalPeer(gid, Collections.singletonList(source));
    receiver.createLocalPeer(gid, peers);
    donor.addRemotePeerForMigration(gid, target, migrationId);
    stateMachines.get(1).canFlushForMigration = false;
    Assert.assertThrows(
        ConsensusException.class,
        () -> receiver.removeRemotePeerForMigration(gid, source, migrationId));
    Assert.assertTrue(donor.getImpl(gid).hasMigrationFence(migrationId));

    receiver.stop();
    // The source can reopen even though the target is down.
    donor.rollbackRegionMigration(gid, target.getNodeId(), migrationId, true);
    TestEntry next = new TestEntry(51, source);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), donor.write(gid, next).getCode());
    IoTConsensus restarted = newServer(1);
    servers.set(1, restarted);
    restarted.recordCorrectPeerListBeforeStarting(Collections.singletonMap(gid, peers));
    restarted.start();
    Assert.assertFalse(restarted.isRegionMigrationConfirmed(gid, source.getNodeId(), migrationId));
    restarted.rollbackRegionMigration(gid, source.getNodeId(), migrationId, false);
    restarted.rollbackRegionMigration(gid, source.getNodeId(), migrationId, false);
    Assert.assertThrows(ConsensusException.class, () -> restarted.read(gid, next));
    Assert.assertEquals(
        TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(), restarted.write(gid, next).getCode());
    Assert.assertThrows(
        ConsensusException.class,
        () -> restarted.removeRemotePeerForMigration(gid, source, migrationId));
    Assert.assertThrows(
        ConsensusException.class,
        () -> donor.deleteLocalPeerForMigration(gid, migrationId, () -> RpcUtils.SUCCESS_STATUS));
    Assert.assertTrue(stateMachines.get(0).getData().contains(next));
    Assert.assertThrows(ConsensusException.class, () -> restarted.addRemotePeer(gid, source));
    restarted.resetPeerList(gid, Collections.singletonList(source));
  }

  @Test
  public void restartedTargetWithoutConfirmationKeepsSourceWritable() throws Exception {
    Peer source = peers.get(0);
    IoTConsensus donor = servers.get(0);
    donor.createLocalPeer(gid, Collections.singletonList(source));
    servers.get(1).createLocalPeer(gid, peers);
    long migrationId = 26;
    donor.addRemotePeerForMigration(gid, peers.get(1), migrationId);
    servers.get(1).stop();
    IoTConsensus restarted = newServer(1);
    servers.set(1, restarted);
    restarted.recordCorrectPeerListBeforeStarting(Collections.singletonMap(gid, peers));
    restarted.start();

    Assert.assertThrows(
        ConsensusException.class,
        () -> restarted.removeRemotePeerForMigration(gid, source, migrationId));
    Assert.assertFalse(donor.getImpl(gid).hasMigrationFence(migrationId));
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(),
        donor.write(gid, new TestEntry(45, source)).getCode());
    // CN withdrew the target while it was offline; no target-side cancellation was delivered.
    restarted.stop();
    donor.rollbackRegionMigration(gid, peers.get(1).getNodeId(), migrationId, true);
    IoTConsensus unlisted = newServer(1);
    servers.set(1, unlisted);
    unlisted.recordCorrectPeerListBeforeStarting(Collections.emptyMap());
    unlisted.start();
    Assert.assertNull(unlisted.getImpl(gid));
    unlisted.rollbackRegionMigration(gid, source.getNodeId(), migrationId, false);
    Assert.assertTrue(IoTConsensus.mustRetainMigrationPeer(peersStorage.get(1), gid));
    Assert.assertThrows(ConsensusException.class, () -> unlisted.createLocalPeer(gid, peers));
    unlisted.stop();
    IoTConsensus relisted = newServer(1);
    servers.set(1, relisted);
    relisted.recordCorrectPeerListBeforeStarting(Collections.singletonMap(gid, peers));
    relisted.start();
    Assert.assertFalse(relisted.getImpl(gid).isActive());
    Assert.assertThrows(ConsensusException.class, () -> relisted.createLocalPeer(gid, peers));
  }

  @Test
  public void verifiedMigrationIncludesWriteDuringTargetSnapshotLoad() throws Exception {
    Peer source = peers.get(0);
    Peer destination = peers.get(1);
    IoTConsensus donor = servers.get(0);
    IoTConsensus receiver = servers.get(1);
    donor.createLocalPeer(gid, peers.subList(0, 1));
    receiver.createLocalPeer(gid, peers);
    long migrationId = 24;
    ControllableStateMachine target = stateMachines.get(1);
    target.continueLoad = new CountDownLatch(1);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<?> add =
          executor.submit(
              () -> {
                donor.addRemotePeerForMigration(gid, destination, migrationId);
                return null;
              });
      Assert.assertTrue(target.loading.await(10, TimeUnit.SECONDS));
      TestEntry tail = new TestEntry(42, source);
      Assert.assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(), donor.write(gid, tail).getCode());
      target.continueLoad.countDown();
      add.get(30, TimeUnit.SECONDS);
      donor.addRemotePeerForMigration(gid, destination, migrationId);
      Assert.assertThrows(
          ConsensusException.class,
          () -> donor.addRemotePeerForMigration(gid, destination, migrationId + 1));

      receiver.removeRemotePeerForMigration(gid, source, migrationId);
      Assert.assertTrue(target.getData().contains(tail));
      donor.addRemotePeerForMigration(gid, destination, migrationId);
      Assert.assertEquals(
          TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(),
          donor.write(gid, new TestEntry(43, source)).getCode());
      receiver.removeRemotePeerForMigration(gid, source, migrationId);
      // A late Remove can confirm data, but it cannot cut the channel to the source while CN
      // is still deciding whether to use that confirmation.
      Assert.assertTrue(receiver.getImpl(gid).getConfiguration().contains(source));
      Assert.assertTrue(receiver.isRegionMigrationConfirmed(gid, source.getNodeId(), migrationId));
      donor.rollbackRegionMigration(gid, destination.getNodeId(), migrationId, true);
      receiver.rollbackRegionMigration(gid, source.getNodeId(), migrationId, false);
      Assert.assertFalse(receiver.isRegionMigrationConfirmed(gid, source.getNodeId(), migrationId));
      Assert.assertThrows(
          ConsensusException.class,
          () -> donor.deleteLocalPeerForMigration(gid, migrationId, () -> RpcUtils.SUCCESS_STATUS));
      Assert.assertTrue(donor.getImpl(gid).isActive());
    } finally {
      target.continueLoad.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void delayedDeleteCannotRemoveARecreatedPeer() throws Exception {
    IoTConsensus donor = servers.get(0);
    donor.createLocalPeer(gid, peers);
    IoTConsensusServerImpl old = donor.getImpl(gid);
    int target = peers.get(1).getNodeId();
    long migration = 71;
    old.prepareMigration(migration, target);
    old.freezeForMigration(migration, target);
    AtomicInteger removalNotifications = new AtomicInteger();
    Consumer<ConsensusGroupId> previous = IoTConsensus.onPeerRemoved;
    IoTConsensus.onPeerRemoved = group -> removalNotifications.incrementAndGet();
    FutureTask<TSStatus> delayed =
        new FutureTask<>(
            () ->
                donor.deleteLocalPeerForMigration(
                    gid,
                    migration,
                    () -> {
                      throw new AssertionError("Deleted the replacement's storage");
                    }));
    Thread deletingThread = new Thread(delayed);
    try {
      IoTConsensusServerImpl replacement;
      synchronized (old) {
        deletingThread.start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (deletingThread.getState() != Thread.State.BLOCKED && System.nanoTime() < deadline) {
          TimeUnit.MILLISECONDS.sleep(1);
        }
        Assert.assertEquals(Thread.State.BLOCKED, deletingThread.getState());
        donor.deleteLocalPeer(gid);
        donor.createLocalPeer(gid, peers);
        replacement = donor.getImpl(gid);
        Assert.assertNotSame(old, replacement);
        // The old object reads its fence by pathname. Make even that check match the new peer.
        replacement.prepareMigration(migration, target);
        replacement.freezeForMigration(migration, target);
      }
      ExecutionException rejected =
          Assert.assertThrows(ExecutionException.class, () -> delayed.get(10, TimeUnit.SECONDS));
      Assert.assertTrue(rejected.getCause() instanceof ConsensusException);
      Assert.assertSame(replacement, donor.getImpl(gid));
      Assert.assertTrue(replacement.hasMigrationFence(migration));
      Assert.assertEquals(1, removalNotifications.get());
    } finally {
      delayed.cancel(true);
      deletingThread.join(TimeUnit.SECONDS.toMillis(10));
      IoTConsensus.onPeerRemoved = previous;
    }
  }

  @Test
  public void failedSourceStorageCleanupBlocksReuseAcrossRestart() throws Exception {
    Peer source = peers.get(0);
    Peer target = peers.get(1);
    IoTConsensus donor = servers.get(0);
    donor.createLocalPeer(gid, Collections.singletonList(source));
    servers.get(1).createLocalPeer(gid, peers);
    donor.addRemotePeerForMigration(gid, target, 71);
    servers.get(1).removeRemotePeerForMigration(gid, source, 71);
    CountDownLatch deleting = new CountDownLatch(1);
    CountDownLatch finishDelete = new CountDownLatch(1);
    ExecutorService worker = Executors.newSingleThreadExecutor();
    try {
      Future<TSStatus> cleanup =
          worker.submit(
              () ->
                  donor.deleteLocalPeerForMigration(
                      gid,
                      71,
                      () -> {
                        deleting.countDown();
                        try {
                          Assert.assertTrue(finishDelete.await(10, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                          throw new AssertionError(e);
                        }
                        return new TSStatus(TSStatusCode.DELETE_REGION_ERROR.getStatusCode());
                      }));
      Assert.assertTrue(deleting.await(10, TimeUnit.SECONDS));
      Assert.assertThrows(
          ConsensusException.class, () -> donor.checkMigrationPeerBeforeCreate(gid));
      finishDelete.countDown();
      Assert.assertEquals(
          TSStatusCode.DELETE_REGION_ERROR.getStatusCode(),
          cleanup.get(10, TimeUnit.SECONDS).getCode());
    } finally {
      finishDelete.countDown();
      worker.shutdownNow();
    }
    Assert.assertNull(donor.getImpl(gid));
    Assert.assertThrows(ConsensusException.class, () -> donor.createLocalPeer(gid, peers));
    donor.stop();
    IoTConsensus restarted = newServer(0);
    servers.set(0, restarted);
    restarted.recordCorrectPeerListBeforeStarting(Collections.emptyMap());
    restarted.start();
    Assert.assertNull(restarted.getImpl(gid));
    Assert.assertThrows(ConsensusException.class, () -> restarted.createLocalPeer(gid, peers));
    Assert.assertTrue(
        new File(IoTConsensus.buildPeerDir(peersStorage.get(0), gid), "migration.source").isFile());
  }

  @Test
  public void ordinaryExtendAndReconstructNeedNoMigrationFiles() throws Exception {
    Peer source = peers.get(0);
    Peer target = peers.get(1);
    IoTConsensus donor = servers.get(0);
    IoTConsensus receiver = servers.get(1);
    donor.createLocalPeer(gid, Collections.singletonList(source));
    receiver.createLocalPeer(gid, peers);
    TestEntry row = new TestEntry(19, source);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), donor.write(gid, row).getCode());
    donor.addRemotePeer(gid, target);
    Assert.assertTrue(stateMachines.get(1).getData().contains(row));
    donor.removeRemotePeer(gid, target);
    receiver.deleteLocalPeer(gid);
    TestEntry duringReconstruct = new TestEntry(20, source);
    Assert.assertEquals(
        TSStatusCode.SUCCESS_STATUS.getStatusCode(), donor.write(gid, duringReconstruct).getCode());
    receiver.createLocalPeer(gid, peers);
    donor.addRemotePeer(gid, target);
    Assert.assertTrue(receiver.getImpl(gid).isActive());
    Assert.assertTrue(stateMachines.get(1).getData().contains(row));
    Assert.assertTrue(stateMachines.get(1).getData().contains(duringReconstruct));
    for (IoTConsensus node : servers) {
      Assert.assertFalse(new File(node.getImpl(gid).getStorageDir(), "migration.source").exists());
      Assert.assertFalse(new File(node.getImpl(gid).getStorageDir(), "migration.target").exists());
    }
  }

  @Test
  public void addRemotePeerMustFailWhenTargetSnapshotLoadFails() throws Exception {
    // node 0 is the sole initial member; node 1 will be added as a new peer. Mirroring the real
    // region-migration flow, the destination peer (node 1) is pre-created locally with the full
    // target peer list (IoTConsensus, unlike Ratis, requires a non-empty peer list here).
    servers.get(0).createLocalPeer(gid, peers.subList(0, 1));
    servers.get(1).createLocalPeer(gid, peers);

    // Put some data into the group so the snapshot transfer is meaningful.
    for (int i = 0; i < 10; i++) {
      servers.get(0).write(gid, new TestEntry(i, peers.get(0)));
    }

    // Force the target peer (node 1) to fail loading the transferred snapshot.
    stateMachines.get(1).setFailLoadSnapshot(true);

    // Before the fix, addRemotePeer swallowed the load failure and returned normally, leaving the
    // target peer active with incomplete data. It must now surface the failure.
    Assert.assertThrows(
        ConsensusException.class, () -> servers.get(0).addRemotePeer(gid, peers.get(1)));

    // The failure must be the snapshot load itself, i.e. the AddPeer flow actually reached the
    // load step on the target rather than aborting earlier.
    Assert.assertTrue(
        "Target peer's loadSnapshot was never invoked; the failure came from an earlier step",
        stateMachines.get(1).isLoadSnapshotInvoked());

    // The target peer must not be left active with an incompletely-loaded snapshot.
    Assert.assertFalse(
        "Target peer was activated despite a failed snapshot load",
        servers.get(1).getImpl(gid).isActive());
  }

  /**
   * A target peer configures one receive folder per local data dir, and snapshot fragments are
   * spread across those folders by the FolderManager, so a small snapshot only materializes under a
   * subset of them. A healthy transfer must therefore still succeed even though some receive
   * folders never received any fragment of the snapshot (regression for the multi-data-dir
   * false-failure reported on the load-failure-propagation PR).
   */
  @Test
  public void addRemotePeerSucceedsWhenSnapshotSpansSubsetOfRecvDirs() throws Exception {
    servers.get(0).createLocalPeer(gid, peers.subList(0, 1));
    servers.get(1).createLocalPeer(gid, peers);

    for (int i = 0; i < 10; i++) {
      servers.get(0).write(gid, new TestEntry(i, peers.get(0)));
    }

    // Do NOT force a load failure: this is a healthy transfer. The (small) snapshot lands in only a
    // subset of the target's receive folders, so the others legitimately have no snapshot root.
    // Before the fix, loadSnapshot() loaded from every receive folder and treated the missing ones
    // as failures, turning this healthy multi-data-dir transfer into a spurious failure.
    servers.get(0).addRemotePeer(gid, peers.get(1));

    Assert.assertTrue(
        "Target peer's loadSnapshot was never invoked",
        stateMachines.get(1).isLoadSnapshotInvoked());
    Assert.assertTrue(
        "Target peer was not activated after a successful snapshot load",
        servers.get(1).getImpl(gid).isActive());

    // Sanity-check that the test actually exercised a partial spread: at least one of the target's
    // receive folders must hold no snapshot at all, otherwise the skipped-folder path is untested.
    long emptyRecvFolders =
        peersRecvSnapshotDirs.get(1).stream()
            .map(dir -> new File(dir, IoTConsensusServerImpl.SNAPSHOT_DIR_NAME))
            .filter(recvFolder -> isEmptyOrMissing(recvFolder))
            .count();
    Assert.assertTrue(
        "Expected at least one receive folder without the snapshot, but every folder had it",
        emptyRecvFolders > 0);
  }

  private static boolean isEmptyOrMissing(File dir) {
    String[] children = dir.list();
    return children == null || children.length == 0;
  }

  /**
   * An empty region produces a snapshot with zero fragments, so nothing is transmitted and the
   * target materializes no snapshot under any receive folder. This must still be treated as a
   * successful (no-op) load — otherwise migrating an empty region (e.g. while removing a DataNode)
   * fails. Regression for the multi-data-dir AddPeer false-failure on empty regions.
   */
  @Test
  public void addRemotePeerSucceedsWhenSnapshotIsEmpty() throws Exception {
    servers.get(0).createLocalPeer(gid, peers.subList(0, 1));
    servers.get(1).createLocalPeer(gid, peers);

    // No data is written, and the source takes an empty snapshot: zero fragments are transmitted.
    stateMachines.get(0).setEmptySnapshot(true);

    // Before the fix, loadSnapshot() reported failure when no receive folder contained the
    // snapshot, so adding a peer for an empty region failed and DataNode removal timed out.
    servers.get(0).addRemotePeer(gid, peers.get(1));

    Assert.assertTrue(
        "Target peer was not activated after an empty snapshot load",
        servers.get(1).getImpl(gid).isActive());

    // Confirm the test really exercised the empty-snapshot path: no receive folder holds a
    // snapshot.
    long nonEmptyRecvFolders =
        peersRecvSnapshotDirs.get(1).stream()
            .map(dir -> new File(dir, IoTConsensusServerImpl.SNAPSHOT_DIR_NAME))
            .filter(recvFolder -> !isEmptyOrMissing(recvFolder))
            .count();
    Assert.assertEquals(
        "Expected no receive folder to contain a snapshot for an empty region",
        0,
        nonEmptyRecvFolders);
  }

  private boolean checkPortAvailable() {
    for (Peer peer : this.peers) {
      try (ServerSocket ignored = new ServerSocket(peer.getEndpoint().port)) {
        logger.info("check port {} success for node {}", peer.getEndpoint().port, peer.getNodeId());
      } catch (IOException e) {
        logger.error("check port {} failed for node {}", peer.getEndpoint().port, peer.getNodeId());
        return false;
      }
    }
    return true;
  }
}
