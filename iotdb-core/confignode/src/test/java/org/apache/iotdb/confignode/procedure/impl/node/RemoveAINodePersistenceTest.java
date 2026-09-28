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
package org.apache.iotdb.confignode.procedure.impl.node;

import org.apache.iotdb.common.rpc.thrift.TAINodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TAINodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.sync.SyncAINodeClient;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.client.sync.SyncAINodeClientPool;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RegisterAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.ainode.RemoveAINodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateNodeStatusPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.state.RemoveAINodeState;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Real removal procedure, cache and NodeInfo; failures are injected at RPC and consensus writes.
 */
public class RemoveAINodePersistenceTest {
  private static final int AI_NODE_ID = 100;
  private final TAINodeLocation location =
      new TAINodeLocation(AI_NODE_ID, new TEndPoint("127.0.0.1", 10810));

  private NodeInfo nodeInfo;
  private LoadCache cache;
  private ConfigManager manager;
  private ConfigNodeProcedureEnv env;
  private Field clientManagerField;
  private Object originalClientManager;
  private boolean failStatusWrite;
  private boolean failDeleteWrite;
  private boolean failStop;
  private int stopRequests;

  @Before
  public void setUp() throws Exception {
    nodeInfo = new NodeInfo();
    nodeInfo.registerAINode(
        new RegisterAINodePlan(new TAINodeConfiguration().setLocation(location)));
    manager = mock(ConfigManager.class, RETURNS_DEEP_STUBS);
    ConsensusManager consensus = mock(ConsensusManager.class);
    when(manager.getConsensusManager()).thenReturn(consensus);
    when(consensus.write(any(UpdateNodeStatusPlan.class)))
        .thenAnswer(
            i -> failStatusWrite ? failure() : nodeInfo.applyNodeStatusPlan(i.getArgument(0)));
    when(consensus.write(any(RemoveAINodePlan.class)))
        .thenAnswer(i -> failDeleteWrite ? failure() : nodeInfo.removeAINode(i.getArgument(0)));
    when(manager.getNodeManager()).thenReturn(new NodeManager(manager, nodeInfo));
    when(manager.getClusterSchemaManager().getDatabaseNames(null))
        .thenReturn(Collections.emptyList());
    cache = new LoadCache();
    cache.initHeartbeatCache(manager);
    assertEquals(
        success().getCode(),
        cache.trySetNodeStatus(AI_NODE_ID, NodeStatus.Running, false).getCode());
    LoadManager loadManager = mock(LoadManager.class);
    when(manager.getLoadManager()).thenReturn(loadManager);
    when(loadManager.trySetNodeStatus(anyInt(), any(), anyBoolean()))
        .thenAnswer(
            i -> cache.trySetNodeStatus(i.getArgument(0), i.getArgument(1), i.getArgument(2)));
    doAnswer(
            i -> {
              cache.removeNodeCache(i.getArgument(0));
              return null;
            })
        .when(loadManager)
        .removeNodeCache(anyInt());
    env = mock(ConfigNodeProcedureEnv.class);
    when(env.getConfigManager()).thenReturn(manager);

    IClientManager<TEndPoint, SyncAINodeClient> transport = mock(IClientManager.class);
    SyncAINodeClient client = mock(SyncAINodeClient.class);
    when(transport.borrowClient(location.getInternalEndPoint())).thenReturn(client);
    when(client.stopAINode())
        .thenAnswer(
            i -> {
              stopRequests++;
              assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(AI_NODE_ID));
              assertEquals(NodeStatus.Removing, cache.getNodeStatus(AI_NODE_ID));
              // The shutdown report from stopAINode must not replace Removing with Stopped.
              assertEquals(
                  success().getCode(),
                  cache.trySetNodeStatus(AI_NODE_ID, NodeStatus.Stopped, false).getCode());
              assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(AI_NODE_ID));
              return failStop ? failure() : success();
            });
    clientManagerField = SyncAINodeClientPool.class.getDeclaredField("clientManager");
    clientManagerField.setAccessible(true);
    originalClientManager = clientManagerField.get(SyncAINodeClientPool.getInstance());
    clientManagerField.set(SyncAINodeClientPool.getInstance(), transport);
  }

  @After
  public void tearDown() throws Exception {
    if (clientManagerField != null && originalClientManager != null) {
      clientManagerField.set(SyncAINodeClientPool.getInstance(), originalClientManager);
    }
  }

  @Test
  public void testFailedStatusWriteDoesNotStopNodeOrAdvanceRemoval() throws Exception {
    TestProcedure procedure = new TestProcedure(location);
    failStatusWrite = true;
    for (int attempt = 0; attempt < 2; attempt++) {
      procedure.step(env);
      assertFalse(procedure.isFailed());
      assertEquals(RemoveAINodeState.NODE_STOP, procedure.state());
      assertEquals(0, stopRequests);
      assertEquals(NodeStatus.Running, cache.getNodeStatus(AI_NODE_ID));
      assertNull(nodeInfo.getNodeStatus(AI_NODE_ID));
    }

    failStatusWrite = false;
    procedure.step(env);
    assertEquals(RemoveAINodeState.NODE_REMOVE, procedure.state());
    assertEquals(1, stopRequests);
    assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(AI_NODE_ID));
  }

  @Test
  public void testRepeatedStatusWriteFailuresReachRetryLimit() throws Exception {
    TestProcedure procedure = new TestProcedure(location);
    failStatusWrite = true;
    for (int attempt = 0; attempt < 10 && !procedure.isFailed(); attempt++) {
      procedure.step(env);
      assertEquals(RemoveAINodeState.NODE_STOP, procedure.state());
    }
    assertTrue(procedure.isFailed());
    assertEquals(0, stopRequests);
    assertEquals(NodeStatus.Running, cache.getNodeStatus(AI_NODE_ID));
    assertNull(nodeInfo.getNodeStatus(AI_NODE_ID));
    assertEquals(1, nodeInfo.getRegisteredAINodes().size());
  }

  @Test
  public void testRepeatedRemovalSkipsStoppingAlreadyRemovedNode() throws Exception {
    nodeInfo.removeAINode(new RemoveAINodePlan(location));
    cache.removeNodeCache(AI_NODE_ID);
    TestProcedure procedure = new TestProcedure(location);
    procedure.step(env);
    assertEquals(RemoveAINodeState.NODE_REMOVE, procedure.state());
    procedure.step(env);
    assertFalse(procedure.isFailed());
    assertEquals(0, stopRequests);
    assertTrue(nodeInfo.getRegisteredAINodes().isEmpty());
    assertFalse(cache.getNodeStatisticsSnapshot().containsKey(AI_NODE_ID));
  }

  @Test
  public void testOfflineRemovalResumesAfterLeaderChangeAndDeleteFailure() throws Exception {
    failStop = true;
    TestProcedure procedure = new TestProcedure(location);
    procedure.step(env);
    assertEquals(RemoveAINodeState.NODE_REMOVE, procedure.state());
    assertEquals(1, stopRequests);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    procedure.serialize(new DataOutputStream(output));
    ByteBuffer buffer = ByteBuffer.wrap(output.toByteArray());
    buffer.getShort(); // Procedure type precedes the serialized state.
    TestProcedure restored = new TestProcedure();
    restored.deserialize(buffer);
    cache.initHeartbeatCache(manager);
    assertEquals(NodeStatus.Removing, cache.getNodeStatus(AI_NODE_ID));

    failDeleteWrite = true;
    for (int attempt = 0; attempt < 2; attempt++) {
      restored.step(env);
      assertFalse(restored.isFailed());
      assertEquals(RemoveAINodeState.NODE_REMOVE, restored.state());
      assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(AI_NODE_ID));
      assertTrue(cache.getNodeStatisticsSnapshot().containsKey(AI_NODE_ID));
      assertEquals(1, nodeInfo.getRegisteredAINodes().size());
    }

    failDeleteWrite = false;
    // Simulate a committed deletion whose completion was not recorded in the procedure store.
    nodeInfo.removeAINode(new RemoveAINodePlan(location));
    restored.step(env);
    assertFalse(restored.isFailed());
    assertTrue(nodeInfo.getRegisteredAINodes().isEmpty());
    assertNull(nodeInfo.getNodeStatus(AI_NODE_ID));
    assertFalse(cache.getNodeStatisticsSnapshot().containsKey(AI_NODE_ID));
    assertEquals(1, stopRequests);
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static TSStatus failure() {
    return new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode());
  }

  private static class TestProcedure extends RemoveAINodeProcedure {
    private TestProcedure() {}

    private TestProcedure(TAINodeLocation location) {
      super(location);
    }

    private void step(ConfigNodeProcedureEnv env) throws InterruptedException {
      execute(env);
    }

    private RemoveAINodeState state() {
      return getCurrentState();
    }
  }
}
