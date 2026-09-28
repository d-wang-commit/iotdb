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

package org.apache.iotdb.confignode.manager.load.cache;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateNodeStatusPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.cache.node.BaseNodeCache;
import org.apache.iotdb.confignode.manager.load.cache.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.manager.schema.ClusterSchemaManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoadCachePersistedNodeStatusTest {

  private static final int SELF_ID = ConfigNodeHeartbeatCache.CURRENT_NODE_ID;
  private static final int CONFIG_NODE_ID = SELF_ID + 100;
  private static final int DATA_NODE_ID = SELF_ID + 101;
  private static final int REMOVING_DATA_NODE_ID = SELF_ID + 102;

  private NodeInfo nodeInfo;
  private ConsensusManager consensusManager;
  private IManager configManager;
  private NodeManager nodeManager;
  private LoadCache loadCache;

  @Before
  public void setUp() throws Exception {
    nodeInfo = new NodeInfo();
    configManager = mock(IManager.class);
    nodeManager = mock(NodeManager.class);
    consensusManager = mock(ConsensusManager.class);
    ClusterSchemaManager schemaManager = mock(ClusterSchemaManager.class);

    when(configManager.getNodeManager()).thenReturn(nodeManager);
    when(configManager.getConsensusManager()).thenReturn(consensusManager);
    when(configManager.getClusterSchemaManager()).thenReturn(schemaManager);
    when(schemaManager.getDatabaseNames(null)).thenReturn(Collections.emptyList());
    when(nodeManager.getRegisteredConfigNodes())
        .thenReturn(
            Arrays.asList(
                new TConfigNodeLocation(
                    SELF_ID, new TEndPoint("127.0.0.1", 11000), new TEndPoint("127.0.0.1", 12000)),
                new TConfigNodeLocation(
                    CONFIG_NODE_ID,
                    new TEndPoint("127.0.0.1", 11001),
                    new TEndPoint("127.0.0.1", 12001))));
    when(nodeManager.getRegisteredDataNodes())
        .thenReturn(
            Arrays.asList(
                new TDataNodeConfiguration()
                    .setLocation(new TDataNodeLocation().setDataNodeId(DATA_NODE_ID)),
                new TDataNodeConfiguration()
                    .setLocation(new TDataNodeLocation().setDataNodeId(REMOVING_DATA_NODE_ID))));
    when(nodeManager.getRegisteredAINodes()).thenReturn(Collections.emptyList());
    for (TConfigNodeLocation node : nodeManager.getRegisteredConfigNodes()) {
      nodeInfo.applyConfigNode(new ApplyConfigNodePlan(node));
    }
    for (TDataNodeConfiguration node : nodeManager.getRegisteredDataNodes()) {
      nodeInfo.registerDataNode(new RegisterDataNodePlan(node));
    }
    when(nodeManager.getPersistedNodeStatus(anyInt()))
        .thenAnswer(invocation -> nodeInfo.getNodeStatus(invocation.getArgument(0)));
    when(consensusManager.write(any(UpdateNodeStatusPlan.class)))
        .thenAnswer(invocation -> nodeInfo.updateNodeStatus(invocation.getArgument(0)));
    loadCache = new LoadCache();
  }

  @Test
  public void testInitializationRestoresStickyStatusesAndKeepsLeaderRunning() throws Exception {
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(CONFIG_NODE_ID, NodeStatus.Stopped));
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(SELF_ID, NodeStatus.Stopped));

    loadCache.initHeartbeatCache(configManager);

    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(CONFIG_NODE_ID));
    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(SELF_ID));
    Assert.assertTrue(loadCache.getNodeHeartbeatUnreadyReasons().isEmpty());
    verify(nodeManager).getPersistedNodeStatus(CONFIG_NODE_ID);
    verify(nodeManager).getPersistedNodeStatus(DATA_NODE_ID);
    verify(nodeManager).getPersistedNodeStatus(REMOVING_DATA_NODE_ID);
    verify(nodeManager, never()).getPersistedNodeStatus(SELF_ID);

    loadCache.updateNodeStatistics(false);

    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(CONFIG_NODE_ID));
    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(SELF_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(SELF_ID));
  }

  @Test
  public void testRestoredStatusDoesNotCreateHeartbeatSamples() throws Exception {
    for (NodeStatus status : Arrays.asList(NodeStatus.Stopped, NodeStatus.Removing)) {
      for (BaseNodeCache cache :
          Arrays.asList(
              new DataNodeHeartbeatCache(DATA_NODE_ID),
              new ConfigNodeHeartbeatCache(CONFIG_NODE_ID))) {
        // A delayed callback may reach a newly inserted cache before initialization finishes.
        cache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Running));
        cache.initializeNodeStatus(status, (ignoredCache, ignoredStatus) -> success());
        Assert.assertFalse(cache.hasHeartbeatSample());
        cache.updateCurrentStatistics(false);
        Assert.assertEquals(status, cache.getNodeStatus());
        Assert.assertFalse(cache.hasHeartbeatSample());
      }
    }
  }

  @Test
  public void testLiveHeartbeatClearsStoppedStatusForBothNodeTypes() throws Exception {
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(CONFIG_NODE_ID, NodeStatus.Stopped));
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);

    loadCache.cacheConfigNodeHeartbeatSample(
        CONFIG_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    loadCache.updateNodeStatistics(false);

    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(CONFIG_NODE_ID));
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(CONFIG_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(DATA_NODE_ID));
  }

  @Test
  public void testFailedRevivalRetainsStoppedUntilPersistenceRetrySucceeds() throws Exception {
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    doReturn(failure())
        .doAnswer(invocation -> nodeInfo.updateNodeStatus(invocation.getArgument(0)))
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Running));
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));

    Assert.assertFalse(loadCache.updateNodeStatistics(false));
    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(DATA_NODE_ID));

    Assert.assertTrue(loadCache.updateNodeStatistics(false));
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(DATA_NODE_ID));
    verify(consensusManager, times(2))
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Running));
  }

  @Test
  public void testConflictingClearDoesNotPublishRunning() throws Exception {
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    // Change the applied marker after the caller has observed Stopped.
    doAnswer(
            invocation -> {
              nodeInfo.updateNodeStatus(
                  new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Removing));
              return nodeInfo.updateNodeStatus(invocation.getArgument(0));
            })
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Running));

    Assert.assertFalse(loadCache.updateNodeStatistics(false));
    Assert.assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
  }

  @Test
  public void testStoppedIsNotPublishedBeforePersistenceCompletes() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    loadCache.forceUpdateNodeCache(DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    CountDownLatch persistenceStarted = new CountDownLatch(1);
    CountDownLatch finishPersistence = new CountDownLatch(1);
    doAnswer(
            invocation -> {
              persistenceStarted.countDown();
              Assert.assertTrue(finishPersistence.await(5, TimeUnit.SECONDS));
              return failure();
            })
        .doAnswer(invocation -> nodeInfo.updateNodeStatus(invocation.getArgument(0)))
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    ExecutorService executor = Executors.newSingleThreadExecutor();
    try {
      Future<TSStatus> update =
          executor.submit(
              () ->
                  loadCache.forceUpdateNodeCache(
                      DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Stopped)));
      Assert.assertTrue(persistenceStarted.await(5, TimeUnit.SECONDS));
      Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
      Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(DATA_NODE_ID));
      finishPersistence.countDown();
      Assert.assertEquals(failure().getCode(), update.get(5, TimeUnit.SECONDS).getCode());
      Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));

      loadCache.updateNodeStatistics(false);
      Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
      Assert.assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(DATA_NODE_ID));
    } finally {
      finishPersistence.countDown();
      executor.shutdownNow();
    }
  }

  @Test
  public void testHeartbeatBeforeCacheInitializationCannotClearRestoredStoppedStatus()
      throws Exception {
    long oldSampleTimestamp = System.nanoTime() - 1;
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(oldSampleTimestamp, NodeStatus.Running));
    loadCache.updateNodeStatistics(false);

    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(DATA_NODE_ID));
    verify(consensusManager, never())
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Running));
  }

  @Test
  public void testForcingAnotherNodeDoesNotClearRemovingStatus() throws Exception {
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);
    loadCache.cacheDataNodeHeartbeatSample(
        REMOVING_DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));

    Assert.assertEquals(
        success().getCode(),
        loadCache
            .forceUpdateNodeCache(DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Stopped))
            .getCode());

    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(REMOVING_DATA_NODE_ID));
    verify(consensusManager, never())
        .write(
            argThat(
                plan ->
                    plan instanceof UpdateNodeStatusPlan
                        && ((UpdateNodeStatusPlan) plan).getNodeId() == REMOVING_DATA_NODE_ID));

    // An explicit rollback of that node still clears its durable Removing marker.
    Assert.assertEquals(
        success().getCode(),
        loadCache
            .forceUpdateNodeCache(
                REMOVING_DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running))
            .getCode());
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(REMOVING_DATA_NODE_ID));
  }

  @Test
  public void testExplicitRollbackRestoresOfflineAndRunningStatuses() throws Exception {
    for (NodeStatus rollbackStatus :
        Arrays.asList(NodeStatus.Unknown, NodeStatus.Stopped, NodeStatus.Running)) {
      nodeInfo.updateNodeStatus(
          new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
      loadCache.initHeartbeatCache(configManager);

      Assert.assertEquals(
          success().getCode(),
          loadCache.setNodeStatus(REMOVING_DATA_NODE_ID, rollbackStatus).getCode());
      Assert.assertEquals(rollbackStatus, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
      Assert.assertEquals(
          rollbackStatus == NodeStatus.Stopped ? NodeStatus.Stopped : null,
          nodeInfo.getNodeStatus(REMOVING_DATA_NODE_ID));

      // Ordinary statistics updates must preserve the successfully committed rollback.
      Assert.assertTrue(loadCache.updateNodeStatistics(false));
      Assert.assertEquals(rollbackStatus, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    }
  }

  @Test
  public void testFailedExplicitRollbackRetainsRemovingUntilRetried() throws Exception {
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);
    doReturn(failure())
        .doAnswer(invocation -> nodeInfo.updateNodeStatus(invocation.getArgument(0)))
        .when(consensusManager)
        .write(
            new UpdateNodeStatusPlan(
                REMOVING_DATA_NODE_ID, NodeStatus.Unknown, NodeStatus.Removing));

    Assert.assertEquals(
        failure().getCode(),
        loadCache.setNodeStatus(REMOVING_DATA_NODE_ID, NodeStatus.Unknown).getCode());
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(REMOVING_DATA_NODE_ID));

    Assert.assertEquals(
        success().getCode(),
        loadCache.setNodeStatus(REMOVING_DATA_NODE_ID, NodeStatus.Unknown).getCode());
    Assert.assertEquals(NodeStatus.Unknown, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(REMOVING_DATA_NODE_ID));
  }

  @Test
  public void testShutdownReportCannotOverrideRemovingStatus() throws Exception {
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(CONFIG_NODE_ID, NodeStatus.Removing));
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);

    for (int nodeId : Arrays.asList(CONFIG_NODE_ID, REMOVING_DATA_NODE_ID)) {
      Assert.assertEquals(
          success().getCode(),
          loadCache
              .forceUpdateNodeCache(nodeId, new NodeHeartbeatSample(NodeStatus.Stopped))
              .getCode());
      Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(nodeId));
      Assert.assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(nodeId));
      verify(consensusManager, never()).write(new UpdateNodeStatusPlan(nodeId, NodeStatus.Stopped));
    }
  }

  @Test
  public void testStatisticsFailureDoesNotPreventUpdatingOtherNodes() throws Exception {
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(CONFIG_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    doReturn(failure())
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Running));
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    loadCache.cacheConfigNodeHeartbeatSample(
        CONFIG_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));

    Assert.assertFalse(loadCache.updateNodeStatistics(false));

    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(CONFIG_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(CONFIG_NODE_ID));
    verify(consensusManager, never()).write(new UpdateNodeStatusPlan(SELF_ID, NodeStatus.Running));
    verify(consensusManager, never())
        .write(new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Unknown));
  }

  @Test
  public void testDiscardedCacheCannotClearNewRemovingMarker() throws Exception {
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    loadCache.initHeartbeatCache(configManager);
    Field field = LoadCache.class.getDeclaredField("nodeCacheMap");
    field.setAccessible(true);
    Map<Integer, BaseNodeCache> caches = (Map<Integer, BaseNodeCache>) field.get(loadCache);
    BaseNodeCache discarded = caches.get(DATA_NODE_ID);
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);
    Assert.assertEquals(failure().getCode(), discarded.setNodeStatus(NodeStatus.Running).getCode());
    Assert.assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(DATA_NODE_ID));
    verify(consensusManager, never())
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Running));
  }

  @Test
  public void testReadOnlyHeartbeatClearsStoppedButNotRemoving() throws Exception {
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    nodeInfo.updateNodeStatus(new UpdateNodeStatusPlan(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
    loadCache.initHeartbeatCache(configManager);
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.ReadOnly));
    loadCache.cacheDataNodeHeartbeatSample(
        REMOVING_DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.ReadOnly));
    Assert.assertTrue(loadCache.updateNodeStatistics(false));
    Assert.assertEquals(NodeStatus.ReadOnly, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(REMOVING_DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(REMOVING_DATA_NODE_ID));
  }

  @Test
  public void testAINodeDoesNotUseDurableStatusCallback() throws Exception {
    int aiNodeId = SELF_ID + 103;
    when(nodeManager.getRegisteredAINodes())
        .thenReturn(
            Collections.singletonList(
                new TAINodeConfiguration()
                    .setLocation(new TAINodeLocation().setAiNodeId(aiNodeId))));
    loadCache.initHeartbeatCache(configManager);
    Assert.assertEquals(
        success().getCode(), loadCache.setNodeStatus(aiNodeId, NodeStatus.Stopped).getCode());
    Assert.assertEquals(
        success().getCode(), loadCache.setNodeStatus(aiNodeId, NodeStatus.Running).getCode());
    verify(consensusManager, never())
        .write(
            argThat(
                plan ->
                    plan instanceof UpdateNodeStatusPlan
                        && ((UpdateNodeStatusPlan) plan).getNodeId() == aiNodeId));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(aiNodeId));
  }

  @Test
  public void testOlderHeartbeatCannotUndoShutdownReport() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    NodeHeartbeatSample delayedRunning = new NodeHeartbeatSample(NodeStatus.Running);
    Assert.assertEquals(
        success().getCode(),
        loadCache
            .forceUpdateNodeCache(DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Stopped))
            .getCode());
    loadCache.cacheDataNodeHeartbeatSample(DATA_NODE_ID, delayedRunning);
    loadCache.updateNodeStatistics(false);
    Assert.assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(DATA_NODE_ID));
  }

  @Test
  public void testBlockedNodeWriteDoesNotHoldOtherNodeCacheLock() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    CountDownLatch entered = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    doAnswer(
            i -> {
              entered.countDown();
              Assert.assertTrue(release.await(10, TimeUnit.SECONDS));
              return nodeInfo.updateNodeStatus(i.getArgument(0));
            })
        .when(consensusManager)
        .write(new UpdateNodeStatusPlan(DATA_NODE_ID, NodeStatus.Stopped));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<TSStatus> blocked =
          executor.submit(() -> loadCache.setNodeStatus(DATA_NODE_ID, NodeStatus.Stopped));
      Assert.assertTrue(entered.await(5, TimeUnit.SECONDS));
      Future<TSStatus> independent =
          executor.submit(
              () -> loadCache.setNodeStatus(REMOVING_DATA_NODE_ID, NodeStatus.Removing));
      Assert.assertEquals(success().getCode(), independent.get(5, TimeUnit.SECONDS).getCode());
      Assert.assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(REMOVING_DATA_NODE_ID));
      Assert.assertFalse(blocked.isDone());
      release.countDown();
      Assert.assertEquals(success().getCode(), blocked.get(5, TimeUnit.SECONDS).getCode());
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @Test
  public void testLateShutdownReportConvergesAfterRestartedNodeHeartbeat() throws Exception {
    loadCache.initHeartbeatCache(configManager);
    NodeHeartbeatSample oldProcessReport = new NodeHeartbeatSample(NodeStatus.Stopped);
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    loadCache.updateNodeStatistics(false);
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    // Shutdown reports carry no DataNode incarnation. Document the current temporary Stopped
    // result, then require a fresh heartbeat from the new process to durably clear that marker.
    Assert.assertEquals(
        success().getCode(),
        loadCache.forceUpdateNodeCache(DATA_NODE_ID, oldProcessReport).getCode());
    Assert.assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(DATA_NODE_ID));
    loadCache.cacheDataNodeHeartbeatSample(
        DATA_NODE_ID, new NodeHeartbeatSample(NodeStatus.Running));
    loadCache.updateNodeStatistics(false);
    Assert.assertEquals(NodeStatus.Running, loadCache.getNodeStatus(DATA_NODE_ID));
    Assert.assertFalse(nodeInfo.getPersistedNodeStatuses().containsKey(DATA_NODE_ID));
  }

  private static TSStatus failure() {
    return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
  }
}
