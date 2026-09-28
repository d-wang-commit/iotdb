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

import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateNodeStatusPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.node.NodeManager;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LoadCacheNodeStatusTest {
  private static final int NODE_ID = 1;

  private NodeInfo nodeInfo;
  private LoadCache loadCache;
  private ConsensusManager consensusManager;

  @Before
  public void setUp() throws Exception {
    nodeInfo = new NodeInfo();
    nodeInfo.registerDataNode(
        new RegisterDataNodePlan(
            new TDataNodeConfiguration()
                .setLocation(new TDataNodeLocation().setDataNodeId(NODE_ID))));
    IManager manager = mock(IManager.class, RETURNS_DEEP_STUBS);
    consensusManager = mock(ConsensusManager.class);
    when(manager.getConsensusManager()).thenReturn(consensusManager);
    when(consensusManager.write(any(UpdateNodeStatusPlan.class)))
        .thenAnswer(invocation -> nodeInfo.updateNodeStatus(invocation.getArgument(0)));
    when(manager.getNodeManager()).thenReturn(new NodeManager(manager, nodeInfo));
    when(manager.getClusterSchemaManager().getDatabaseNames(null))
        .thenReturn(Collections.emptyList());
    loadCache = new LoadCache();
    loadCache.initHeartbeatCache(manager);
  }

  @Test
  public void testOnlyDurableTransitionsWriteConsensus() throws Exception {
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Running, false));
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Unknown, false));
    verify(consensusManager, never()).write(any());

    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(NODE_ID));
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    verify(consensusManager, times(1)).write(any());

    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Removing, true));
    assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(NODE_ID));
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Running, true));
    assertNull(nodeInfo.getNodeStatus(NODE_ID));
    verify(consensusManager, times(3)).write(any());
  }

  @Test
  public void testFailedWriteIsRetriedAndClearIsDurable() throws Exception {
    doReturn(new TSStatus(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()))
        .doAnswer(invocation -> nodeInfo.updateNodeStatus(invocation.getArgument(0)))
        .when(consensusManager)
        .write(any(UpdateNodeStatusPlan.class));
    assertFailure(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    assertNull(nodeInfo.getNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Unknown, loadCache.getNodeStatus(NODE_ID));
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(NODE_ID));

    doThrow(new ConsensusException("test consensus failure"))
        .doAnswer(invocation -> nodeInfo.updateNodeStatus(invocation.getArgument(0)))
        .when(consensusManager)
        .write(any(UpdateNodeStatusPlan.class));
    assertFailure(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Running, false));
    assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(NODE_ID));
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Running, false));
    assertNull(nodeInfo.getNodeStatus(NODE_ID));
  }

  private static void assertSuccess(TSStatus status) {
    assertEquals(TSStatusCode.SUCCESS_STATUS.getStatusCode(), status.getCode());
  }

  @Test
  public void testCommittedWriteWithLostResponseIsIdempotentOnRetry() throws Exception {
    doAnswer(
            i -> {
              nodeInfo.updateNodeStatus(i.getArgument(0));
              throw new ConsensusException("response lost after commit");
            })
        .when(consensusManager)
        .write(any(UpdateNodeStatusPlan.class));
    assertFailure(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Unknown, loadCache.getNodeStatus(NODE_ID));
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(NODE_ID));
    verify(consensusManager, times(1)).write(any());
  }

  @Test
  public void testOrdinaryUpdatesKeepRemovingWithoutQuorumButRollbackRequiresIt() throws Exception {
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Removing, true));
    clearInvocations(consensusManager);
    doThrow(new ConsensusException("quorum unavailable")).when(consensusManager).write(any());
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Removing, true));
    for (NodeStatus observed :
        new NodeStatus[] {NodeStatus.Unknown, NodeStatus.Stopped, NodeStatus.Running}) {
      assertSuccess(loadCache.updateNodeStatus(NODE_ID, observed, false));
      assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(NODE_ID));
    }
    verify(consensusManager, never()).write(any());
    assertFailure(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Stopped, true));
    assertFailure(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Running, true));
    assertEquals(NodeStatus.Removing, nodeInfo.getNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(NODE_ID));
  }

  @Test
  public void testConnectionFailureKeepsStoppedUntilExplicitReset() throws Exception {
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Stopped, false));
    clearInvocations(consensusManager);

    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Unknown, false));
    assertEquals(NodeStatus.Stopped, loadCache.getNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Stopped, nodeInfo.getNodeStatus(NODE_ID));
    verify(consensusManager, never()).write(any());

    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Unknown, true));
    assertEquals(NodeStatus.Unknown, loadCache.getNodeStatus(NODE_ID));
    assertNull(nodeInfo.getNodeStatus(NODE_ID));
    loadCache.updateNodeStatistics();
    assertEquals(NodeStatus.Unknown, loadCache.getNodeStatus(NODE_ID));
  }

  private static void assertFailure(TSStatus status) {
    assertEquals(TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void testCommittedClearWithLostResponseIsIdempotentOnRetry() throws Exception {
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Removing, true));
    clearInvocations(consensusManager);
    doAnswer(
            i -> {
              nodeInfo.updateNodeStatus(i.getArgument(0));
              throw new ConsensusException("clear response lost");
            })
        .when(consensusManager)
        .write(any(UpdateNodeStatusPlan.class));
    assertFailure(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Running, true));
    assertNull(nodeInfo.getNodeStatus(NODE_ID));
    assertEquals(NodeStatus.Removing, loadCache.getNodeStatus(NODE_ID));
    assertSuccess(loadCache.updateNodeStatus(NODE_ID, NodeStatus.Running, true));
    assertEquals(NodeStatus.Running, loadCache.getNodeStatus(NODE_ID));
    verify(consensusManager, times(1)).write(any());
  }
}
