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
package org.apache.iotdb.db.protocol.thrift.impl;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
import org.apache.iotdb.common.rpc.thrift.TRegionMigrateFailedType;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.service.DataNode.DataNodeContext;
import org.apache.iotdb.db.service.RegionMigrateService;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.mpp.rpc.thrift.TMaintainPeerReq;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;
import org.apache.iotdb.rpc.TSStatusCode;

import com.sun.nio.file.ExtendedOpenOption;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataRegionDeletionRpcTest {
  private static final DataRegionId REGION_ID = new DataRegionId(918273);
  private static final TEndPoint ENDPOINT = new TEndPoint("127.0.0.1", 10760);

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private int originalNodeId;
  private IConsensus originalConsensus;
  private IConsensus consensus;
  private DataNodeInternalRPCServiceImpl service;
  private Path peerDirectory;

  @Before
  public void setUp() throws Exception {
    originalNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(1);
    originalConsensus = DataRegionConsensusImpl.getInstance();
    IStateMachine stateMachine = mock(IStateMachine.class);
    when(stateMachine.event()).thenReturn(mock(IStateMachine.EventApi.class));
    consensus =
        ConsensusFactory.getConsensusImpl(
                ConsensusFactory.SIMPLE_CONSENSUS,
                ConsensusConfig.newBuilder()
                    .setThisNodeId(1)
                    .setThisNode(ENDPOINT)
                    .setStorageDir(temporaryFolder.newFolder("consensus").getAbsolutePath())
                    .setConsensusGroupType(TConsensusGroupType.DataRegion)
                    .build(),
                groupId -> stateMachine)
            .get();
    consensus.start();
    DataRegionConsensusImpl.setInstance(consensus);
    peerDirectory = Paths.get(consensus.getRegionDirFromConsensusGroupId(REGION_ID));
    DataNodeContext context = mock(DataNodeContext.class);
    when(context.isAllConsensusStarted()).thenReturn(true);
    service = new DataNodeInternalRPCServiceImpl(context);
  }

  @After
  public void tearDown() throws Exception {
    try {
      if (consensus != null) {
        consensus.stop();
      }
      if (StorageEngine.isDataRegionDeletionPending(REGION_ID)) {
        assertStatus(
            TSStatusCode.SUCCESS_STATUS, StorageEngine.getInstance().deleteDataRegion(REGION_ID));
      }
    } finally {
      DataRegionConsensusImpl.setInstance(originalConsensus);
      IoTDBDescriptor.getInstance().getConfig().setDataNodeId(originalNodeId);
    }
  }

  @Test(timeout = 10000)
  public void testCompletedDeletionCanBeRetriedAfterReplyLoss() throws Exception {
    createPeer();
    assertStatus(TSStatusCode.SUCCESS_STATUS, deleteRegion());
    assertFalse(Files.exists(peerDirectory));

    // A caller that did not receive the first response sends the same request again.
    assertStatus(TSStatusCode.REGION_NOT_EXIST, deleteRegion());
    assertFalse(Files.exists(peerDirectory));
  }

  @Test(timeout = 10000)
  public void testMissingPeerStillCleansItsResidualDirectory() throws Exception {
    Files.createDirectories(peerDirectory);
    Files.write(peerDirectory.resolve("residual.bin"), new byte[] {1});
    Path unrelated = temporaryFolder.newFile("unrelated.bin").toPath();

    assertStatus(TSStatusCode.SUCCESS_STATUS, deleteRegion());
    assertFalse(Files.exists(peerDirectory));
    assertTrue(Files.exists(unrelated));
    assertStatus(TSStatusCode.REGION_NOT_EXIST, deleteRegion());
  }

  @Test(timeout = 10000)
  public void testPeerFailurePreservesDirectoryForRetry() throws Exception {
    createPeer();
    Path regionSystemDirectory =
        Paths.get(
            IoTDBDescriptor.getInstance().getConfig().getSystemDir(),
            "databases",
            "root.rpc_deletion",
            Integer.toString(REGION_ID.getId()));
    Files.createDirectories(regionSystemDirectory);
    IConsensus failingConsensus = mock(IConsensus.class, delegatesTo(consensus));
    doAnswer(
            invocation -> {
              assertTrue(StorageEngine.isDataRegionDeletionPending(REGION_ID));
              throw new ConsensusException("injected peer shutdown failure");
            })
        .when(failingConsensus)
        .deleteLocalPeer(REGION_ID);
    DataRegionConsensusImpl.setInstance(failingConsensus);

    assertStatus(TSStatusCode.DELETE_REGION_ERROR, deleteRegion());
    assertTrue(Files.exists(peerDirectory.resolve("peer.bin")));
    assertTrue(consensus.getAllConsensusGroupIds().contains(REGION_ID));
    assertTrue(StorageEngine.isDataRegionDeletionPending(REGION_ID));

    DataRegionConsensusImpl.setInstance(consensus);
    assertStatus(TSStatusCode.SUCCESS_STATUS, deleteRegion());
    assertFalse(Files.exists(peerDirectory));
    assertFalse(Files.exists(regionSystemDirectory));
    assertFalse(StorageEngine.isDataRegionDeletionPending(REGION_ID));
  }

  @Test(timeout = 10000)
  public void testLockedPeerDirectoryIsRetriedAfterPeerDisappears() throws Exception {
    Assume.assumeTrue(System.getProperty("os.name", "").startsWith("Windows"));
    createPeer();
    try (SeekableByteChannel lock =
        Files.newByteChannel(
            peerDirectory.resolve("peer.bin"),
            StandardOpenOption.READ,
            ExtendedOpenOption.NOSHARE_DELETE)) {
      assertTrue(lock.isOpen());
      assertStatus(TSStatusCode.DELETE_REGION_ERROR, deleteRegion());
      assertTrue(Files.exists(peerDirectory.resolve("peer.bin")));
      assertFalse(consensus.getAllConsensusGroupIds().contains(REGION_ID));
      assertStatus(TSStatusCode.DELETE_REGION_ERROR, deleteRegion());
    }

    assertStatus(TSStatusCode.SUCCESS_STATUS, deleteRegion());
    assertFalse(Files.exists(peerDirectory));
    assertStatus(TSStatusCode.REGION_NOT_EXIST, deleteRegion());
  }

  @Test(timeout = 10000)
  public void testConcurrentDeletionWaitsForPeerShutdown() throws Exception {
    createPeer();
    CountDownLatch peerShutdownStarted = new CountDownLatch(1);
    CountDownLatch releasePeerShutdown = new CountDownLatch(1);
    CountDownLatch secondRequestStarted = new CountDownLatch(1);
    CountDownLatch secondPeerCall = new CountDownLatch(1);
    AtomicInteger peerCalls = new AtomicInteger();
    IConsensus blockingConsensus = mock(IConsensus.class, delegatesTo(consensus));
    doAnswer(
            invocation -> {
              if (peerCalls.incrementAndGet() == 1) {
                peerShutdownStarted.countDown();
                assertTrue(releasePeerShutdown.await(5, TimeUnit.SECONDS));
              } else {
                secondPeerCall.countDown();
              }
              consensus.deleteLocalPeer(REGION_ID);
              return null;
            })
        .when(blockingConsensus)
        .deleteLocalPeer(REGION_ID);
    DataRegionConsensusImpl.setInstance(blockingConsensus);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<TSStatus> first = executor.submit(this::deleteRegion);
      assertTrue(peerShutdownStarted.await(5, TimeUnit.SECONDS));
      Future<TSStatus> second =
          executor.submit(
              () -> {
                secondRequestStarted.countDown();
                return deleteRegion();
              });
      assertTrue(secondRequestStarted.await(5, TimeUnit.SECONDS));
      assertFalse(secondPeerCall.await(200, TimeUnit.MILLISECONDS));
      assertTrue(Files.exists(peerDirectory));
      releasePeerShutdown.countDown();

      assertStatus(TSStatusCode.SUCCESS_STATUS, first.get(5, TimeUnit.SECONDS));
      assertStatus(TSStatusCode.REGION_NOT_EXIST, second.get(5, TimeUnit.SECONDS));
      assertEquals(2, peerCalls.get());
      assertFalse(Files.exists(peerDirectory));
    } finally {
      releasePeerShutdown.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test(timeout = 10000)
  public void testMigrationRetriesFailedStorageDeletionWithSameTaskId() throws Exception {
    createPeer();
    DataRegion region = mock(DataRegion.class);
    when(region.getDatabaseName()).thenReturn("root.rpc_deletion");
    when(region.getDataRegionIdString()).thenReturn(Integer.toString(REGION_ID.getId()));
    doThrow(new TsFileProcessorException("injected close failure"))
        .doNothing()
        .when(region)
        .syncDeleteDataFiles();
    StorageEngine.getInstance().setDataRegion(REGION_ID, region);
    TDataNodeLocation target =
        new TDataNodeLocation().setDataNodeId(1).setInternalEndPoint(ENDPOINT);
    TMaintainPeerReq req =
        new TMaintainPeerReq(REGION_ID.convertToTConsensusGroupId(), target, System.nanoTime());
    RegionMigrateService migration = RegionMigrateService.getInstance();
    migration.start();
    try {
      TRegionMigrateResult failed = submitMigrationAndWait(req);
      assertEquals(TRegionMaintainTaskStatus.FAIL, failed.getTaskStatus());
      assertStatus(TSStatusCode.DELETE_REGION_ERROR, failed.getMigrateResult());
      assertEquals(
          TRegionMigrateFailedType.DeleteRegionFailed, failed.getFailedNodeAndReason().get(target));
      assertFalse(Files.exists(peerDirectory));
      assertTrue(StorageEngine.isDataRegionDeletionPending(REGION_ID));

      TRegionMigrateResult retried = submitMigrationAndWait(req);
      assertEquals(TRegionMaintainTaskStatus.SUCCESS, retried.getTaskStatus());
      assertFalse(StorageEngine.isDataRegionDeletionPending(REGION_ID));
      // A new migration task also succeeds when both peer and storage are already gone.
      req.setTaskId(req.getTaskId() + 1);
      assertEquals(TRegionMaintainTaskStatus.SUCCESS, submitMigrationAndWait(req).getTaskStatus());
    } finally {
      migration.stop();
    }
  }

  private TRegionMigrateResult submitMigrationAndWait(TMaintainPeerReq req) throws Exception {
    assertStatus(TSStatusCode.SUCCESS_STATUS, service.deleteOldRegionPeer(req));
    TRegionMigrateResult result = service.getRegionMaintainResult(req.getTaskId());
    while (result.getTaskStatus() == TRegionMaintainTaskStatus.PROCESSING) {
      TimeUnit.MILLISECONDS.sleep(10);
      result = service.getRegionMaintainResult(req.getTaskId());
    }
    return result;
  }

  private void createPeer() throws Exception {
    consensus.createLocalPeer(
        REGION_ID, Collections.singletonList(new Peer(REGION_ID, 1, ENDPOINT)));
    Files.write(peerDirectory.resolve("peer.bin"), new byte[] {1});
  }

  private TSStatus deleteRegion() {
    return service.deleteRegion(REGION_ID.convertToTConsensusGroupId());
  }

  private static void assertStatus(TSStatusCode expected, TSStatus actual) {
    assertEquals(actual.toString(), expected.getStatusCode(), actual.getCode());
  }
}
