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

package org.apache.iotdb.confignode.manager.partition;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.async.RegionDeletionRpcTestContext;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollSpecificRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainType;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.transport.TTransportException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PartitionManagerRegionMaintainTest {

  @Test
  public void testCreateRegionCompletedStatus() {
    assertTrue(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.CREATE, new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    assertTrue(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.CREATE,
            new TSStatus(TSStatusCode.REGION_ALREADY_EXISTS.getStatusCode())));
    assertFalse(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.CREATE,
            new TSStatus(TSStatusCode.REGION_NOT_EXIST.getStatusCode())));
    assertFalse(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.CREATE,
            new TSStatus(TSStatusCode.CREATE_REGION_ERROR.getStatusCode())));
  }

  @Test
  public void testDeleteRegionCompletedStatus() {
    assertTrue(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.DELETE, new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));
    assertTrue(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.DELETE,
            new TSStatus(TSStatusCode.REGION_NOT_EXIST.getStatusCode())));
    assertFalse(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.DELETE,
            new TSStatus(TSStatusCode.REGION_ALREADY_EXISTS.getStatusCode())));
    assertFalse(
        PartitionManager.isRegionMaintainTaskCompleted(
            RegionMaintainType.DELETE,
            new TSStatus(TSStatusCode.DELETE_REGION_ERROR.getStatusCode())));
  }

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private static final TConsensusGroupId REGION =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 7);
  private static final TDataNodeLocation NODE1 = node(1);
  private static final TDataNodeLocation NODE2 = node(2);

  @Test
  public void testPollPlanRemainsNonForwardableAfterSerialization() throws Exception {
    PollSpecificRegionMaintainTaskPlan plan =
        new PollSpecificRegionMaintainTaskPlan(Collections.singleton(REGION));
    ConfigPhysicalPlan recovered = ConfigPhysicalPlan.Factory.create(plan.serializeToByteBuffer());

    assertFalse(plan.isForwardable());
    assertTrue(recovered instanceof PollSpecificRegionMaintainTaskPlan);
    assertFalse(recovered.isForwardable());
    assertEquals(
        plan.getRegionIdSet(), ((PollSpecificRegionMaintainTaskPlan) recovered).getRegionIdSet());
  }

  @Test(timeout = 10000)
  public void testFailedDeletionSurvivesSnapshotAndRetries() throws Exception {
    PartitionInfo recovered = new PartitionInfo();
    RegionDeleteTask task = new RegionDeleteTask(NODE1, REGION);
    try (MaintenanceContext context = new MaintenanceContext(new PartitionInfo())) {
      context.offer(task);
      context.rpc.setResponder((endpoint, region) -> status(TSStatusCode.DELETE_REGION_ERROR));
      context.manager.maintainRegionReplicas();

      assertEquals(6, context.rpc.getRequests().size());
      assertEquals(Collections.singletonList(task), context.info.getRegionMaintainEntryList());
      verify(context.consensus, never()).write(any(PollSpecificRegionMaintainTaskPlan.class));
      File snapshot = temporaryFolder.newFolder();
      assertTrue(context.info.processTakeSnapshot(snapshot));
      recovered.processLoadSnapshot(snapshot);
      assertEquals(Collections.singletonList(task), recovered.getRegionMaintainEntryList());
    }

    try (MaintenanceContext context = new MaintenanceContext(recovered)) {
      context.rpc.setResponder((endpoint, region) -> status(TSStatusCode.REGION_NOT_EXIST));
      context.manager.maintainRegionReplicas();
      assertEquals(1, context.rpc.getRequests().size());
      assertTrue(recovered.getRegionMaintainEntryList().isEmpty());
      File snapshot = temporaryFolder.newFolder();
      assertTrue(recovered.processTakeSnapshot(snapshot));
      PartitionInfo afterCompletion = new PartitionInfo();
      afterCompletion.processLoadSnapshot(snapshot);
      assertTrue(afterCompletion.getRegionMaintainEntryList().isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void testFailedPollDoesNotAdvanceToNextReplica() throws Exception {
    assertPollFailureIsRetried(false);
  }

  @Test(timeout = 10000)
  public void testPollExceptionDoesNotAdvanceToNextReplica() throws Exception {
    assertPollFailureIsRetried(true);
  }

  private void assertPollFailureIsRetried(boolean throwException) throws Exception {
    try (MaintenanceContext context = new MaintenanceContext(new PartitionInfo())) {
      List<RegionMaintainTask> tasks =
          Arrays.asList(new RegionDeleteTask(NODE1, REGION), new RegionDeleteTask(NODE2, REGION));
      context.offer(tasks.toArray(new RegionMaintainTask[0]));
      AtomicInteger polls = new AtomicInteger();
      when(context.consensus.write(any(PollSpecificRegionMaintainTaskPlan.class)))
          .thenAnswer(
              invocation -> {
                if (polls.getAndIncrement() == 0) {
                  if (throwException) {
                    throw new ConsensusException("Simulated consensus failure");
                  }
                  return status(TSStatusCode.REDIRECTION_RECOMMEND);
                }
                return context.info.pollSpecificRegionMaintainTask(invocation.getArgument(0));
              });

      context.manager.maintainRegionReplicas();
      assertEquals(
          Collections.singletonList(NODE1.getInternalEndPoint()), context.requestedNodes());
      assertEquals(tasks, context.info.getRegionMaintainEntryList());

      context.manager.maintainRegionReplicas();
      assertEquals(
          Arrays.asList(
              NODE1.getInternalEndPoint(),
              NODE1.getInternalEndPoint(),
              NODE2.getInternalEndPoint()),
          context.requestedNodes());
      assertTrue(context.info.getRegionMaintainEntryList().isEmpty());
      assertEquals(3, polls.get());
    }
  }

  @Test(timeout = 10000)
  public void testCommittedPollWithLostResponseUsesPersistedQueueNextRound() throws Exception {
    try (MaintenanceContext context = new MaintenanceContext(new PartitionInfo())) {
      RegionDeleteTask nextReplica = new RegionDeleteTask(NODE2, REGION);
      context.offer(new RegionDeleteTask(NODE1, REGION), nextReplica);
      AtomicInteger polls = new AtomicInteger();
      when(context.consensus.write(any(PollSpecificRegionMaintainTaskPlan.class)))
          .thenAnswer(
              invocation -> {
                TSStatus result =
                    context.info.pollSpecificRegionMaintainTask(invocation.getArgument(0));
                if (polls.getAndIncrement() == 0) {
                  throw new ConsensusException("Simulated lost commit response");
                }
                return result;
              });

      context.manager.maintainRegionReplicas();
      assertEquals(
          Collections.singletonList(NODE1.getInternalEndPoint()), context.requestedNodes());
      assertEquals(
          Collections.singletonList(nextReplica), context.info.getRegionMaintainEntryList());
      context.manager.maintainRegionReplicas();
      assertEquals(
          Arrays.asList(NODE1.getInternalEndPoint(), NODE2.getInternalEndPoint()),
          context.requestedNodes());
      assertTrue(context.info.getRegionMaintainEntryList().isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void testTransportFailureKeepsTaskUntilLaterSuccess() throws Exception {
    try (MaintenanceContext context = new MaintenanceContext(new PartitionInfo())) {
      RegionDeleteTask task = new RegionDeleteTask(NODE1, REGION);
      context.offer(task);
      context.rpc.setResponder(
          (endpoint, region) -> {
            throw new TTransportException("Simulated unavailable DataNode");
          });
      context.manager.maintainRegionReplicas();
      assertEquals(6, context.rpc.getRequests().size());
      assertEquals(Collections.singletonList(task), context.info.getRegionMaintainEntryList());
      verify(context.consensus, never()).write(any(PollSpecificRegionMaintainTaskPlan.class));

      context.rpc.setResponder((endpoint, region) -> RpcUtils.SUCCESS_STATUS);
      context.manager.maintainRegionReplicas();
      assertEquals(7, context.rpc.getRequests().size());
      assertTrue(context.info.getRegionMaintainEntryList().isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void testOnlyCompletedRegionIsRemoved() throws Exception {
    try (MaintenanceContext context = new MaintenanceContext(new PartitionInfo())) {
      RegionDeleteTask failedTask = new RegionDeleteTask(NODE1, REGION);
      context.offer(
          failedTask,
          new RegionDeleteTask(NODE2, new TConsensusGroupId(TConsensusGroupType.DataRegion, 8)));
      context.rpc.setResponder(
          (endpoint, region) ->
              region.equals(REGION)
                  ? status(TSStatusCode.DELETE_REGION_ERROR)
                  : RpcUtils.SUCCESS_STATUS);
      context.manager.maintainRegionReplicas();
      assertEquals(7, context.rpc.getRequests().size());
      assertEquals(
          Collections.singletonList(failedTask), context.info.getRegionMaintainEntryList());

      context.rpc.setResponder((endpoint, region) -> RpcUtils.SUCCESS_STATUS);
      context.manager.maintainRegionReplicas();
      assertEquals(8, context.rpc.getRequests().size());
      assertTrue(context.info.getRegionMaintainEntryList().isEmpty());
    }
  }

  @Test(timeout = 10000)
  public void testFollowerDoesNotExecuteDeletion() throws Exception {
    try (MaintenanceContext context = new MaintenanceContext(new PartitionInfo())) {
      RegionDeleteTask task = new RegionDeleteTask(NODE1, REGION);
      context.offer(task);
      when(context.consensus.isLeader()).thenReturn(false);
      context.manager.maintainRegionReplicas();
      assertTrue(context.rpc.getRequests().isEmpty());
      assertEquals(Collections.singletonList(task), context.info.getRegionMaintainEntryList());
      verify(context.consensus, never()).write(any(PollSpecificRegionMaintainTaskPlan.class));
    }
  }

  @Test(timeout = 10000)
  public void testLeadershipLostDuringDeletionKeepsTasksForNextLeaderRound() throws Exception {
    try (MaintenanceContext context = new MaintenanceContext(new PartitionInfo())) {
      List<RegionMaintainTask> tasks =
          Arrays.asList(new RegionDeleteTask(NODE1, REGION), new RegionDeleteTask(NODE2, REGION));
      context.offer(tasks.toArray(new RegionMaintainTask[0]));
      context.rpc.setResponder(
          (endpoint, region) -> {
            when(context.consensus.isLeader()).thenReturn(false);
            return RpcUtils.SUCCESS_STATUS;
          });

      context.manager.maintainRegionReplicas();
      assertEquals(
          Collections.singletonList(NODE1.getInternalEndPoint()), context.requestedNodes());
      assertEquals(tasks, context.info.getRegionMaintainEntryList());
      verify(context.consensus, never()).write(any(PollSpecificRegionMaintainTaskPlan.class));

      when(context.consensus.isLeader()).thenReturn(true);
      context.rpc.setResponder(
          (endpoint, region) ->
              endpoint.equals(NODE1.getInternalEndPoint())
                  ? status(TSStatusCode.REGION_NOT_EXIST)
                  : RpcUtils.SUCCESS_STATUS);
      context.manager.maintainRegionReplicas();
      assertEquals(
          Arrays.asList(
              NODE1.getInternalEndPoint(),
              NODE1.getInternalEndPoint(),
              NODE2.getInternalEndPoint()),
          context.requestedNodes());
      assertTrue(context.info.getRegionMaintainEntryList().isEmpty());
    }
  }

  private static TSStatus status(TSStatusCode code) {
    return new TSStatus(code.getStatusCode());
  }

  private static TDataNodeLocation node(int nodeId) {
    TEndPoint endpoint = new TEndPoint("127.0.0.1", 9000 + nodeId);
    return new TDataNodeLocation(nodeId, endpoint, endpoint, endpoint, endpoint, endpoint);
  }

  private static class MaintenanceContext implements AutoCloseable {
    private final PartitionInfo info;
    private final ConsensusManager consensus = mock(ConsensusManager.class);
    private final PartitionManager manager;
    private final RegionDeletionRpcTestContext rpc;

    private MaintenanceContext(PartitionInfo info) throws Exception {
      this.info = info;
      IManager configManager = mock(IManager.class);
      when(configManager.getConsensusManager()).thenReturn(consensus);
      when(consensus.isLeader()).thenReturn(true);
      when(consensus.write(any(PollSpecificRegionMaintainTaskPlan.class)))
          .thenAnswer(invocation -> info.pollSpecificRegionMaintainTask(invocation.getArgument(0)));
      manager = new PartitionManager(configManager, info);
      rpc = new RegionDeletionRpcTestContext((endpoint, region) -> RpcUtils.SUCCESS_STATUS);
    }

    private void offer(RegionMaintainTask... tasks) {
      OfferRegionMaintainTasksPlan plan = new OfferRegionMaintainTasksPlan();
      for (RegionMaintainTask task : tasks) {
        plan.appendRegionMaintainTask(task);
      }
      assertEquals(
          TSStatusCode.SUCCESS_STATUS.getStatusCode(),
          info.offerRegionMaintainTasks(plan).getCode());
    }

    private List<TEndPoint> requestedNodes() {
      return rpc.getRequests().stream().map(request -> request.left).collect(Collectors.toList());
    }

    @Override
    public void close() throws Exception {
      try {
        rpc.close();
      } finally {
        manager.getRegionMaintainer().shutdownNow();
      }
    }
  }
}
