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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.DataOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;

public class RemoveRegionPeerProcedureTest {
  @Test
  public void serDeTest() throws Exception {
    RemoveRegionPeerProcedure procedure =
        new RemoveRegionPeerProcedure(
            new TConsensusGroupId(TConsensusGroupType.DataRegion, 10),
            new TDataNodeLocation(
                1,
                new TEndPoint("127.0.0.1", 0),
                new TEndPoint("127.0.0.1", 1),
                new TEndPoint("127.0.0.1", 2),
                new TEndPoint("127.0.0.1", 3),
                new TEndPoint("127.0.0.1", 4)),
            new TDataNodeLocation(
                5,
                new TEndPoint("127.0.0.1", 5),
                new TEndPoint("127.0.0.1", 6),
                new TEndPoint("127.0.0.1", 7),
                new TEndPoint("127.0.0.1", 8),
                new TEndPoint("127.0.0.1", 9)));
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      procedure.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
      Assert.assertEquals(procedure, ProcedureFactory.getInstance().create(buffer));
    }
  }

  @Test
  public void removePeerFailureMustRollbackAndNotDeleteOldRegionPeer() throws Exception {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 11);
    final TDataNodeLocation coordinator = location(2);
    final TDataNodeLocation target = location(1);
    final RegionMaintainHandler handler = Mockito.mock(RegionMaintainHandler.class);
    Mockito.when(handler.submitRemoveRegionPeerTask(anyLong(), any(), any(), any()))
        .thenReturn(success());
    Mockito.when(handler.waitTaskFinish(anyLong(), any(TDataNodeLocation.class)))
        .thenReturn(new TRegionMigrateResult(TRegionMaintainTaskStatus.FAIL));
    Mockito.when(handler.getRegionReplicaSet(regionId))
        .thenReturn(
            Optional.of(new TRegionReplicaSet(regionId, Arrays.asList(coordinator, target))));
    final Map<Integer, TSStatus> resetResponses = new HashMap<>();
    resetResponses.put(coordinator.getDataNodeId(), success());
    resetResponses.put(target.getDataNodeId(), success());
    Mockito.when(handler.resetPeerList(any(), anyList(), anyMap())).thenReturn(resetResponses);
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.getRegionMaintainHandler()).thenReturn(handler);

    final RemoveRegionPeerProcedure procedure =
        new RemoveRegionPeerProcedure(regionId, coordinator, target);
    procedure.executeFromState(env, RemoveRegionPeerState.REMOVE_REGION_PEER);

    Assert.assertTrue(procedure.isFailed());
    Assert.assertNotEquals(
        RemoveRegionPeerState.DELETE_OLD_REGION_PEER.ordinal(), readNextState(procedure));
    Mockito.verify(handler).resetPeerList(any(), anyList(), anyMap());
  }

  @Test
  public void processingWaitMustStayInRemovePeerState() throws Exception {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 11);
    final TDataNodeLocation coordinator = location(2);
    final TDataNodeLocation target = location(1);
    final RegionMaintainHandler handler = Mockito.mock(RegionMaintainHandler.class);
    Mockito.when(handler.submitRemoveRegionPeerTask(anyLong(), any(), any(), any()))
        .thenReturn(success());
    Mockito.when(handler.waitTaskFinish(anyLong(), any(TDataNodeLocation.class)))
        .thenReturn(new TRegionMigrateResult(TRegionMaintainTaskStatus.PROCESSING));
    final ConfigNodeProcedureEnv env = Mockito.mock(ConfigNodeProcedureEnv.class);
    Mockito.when(env.getRegionMaintainHandler()).thenReturn(handler);

    final RemoveRegionPeerProcedure procedure =
        new RemoveRegionPeerProcedure(regionId, coordinator, target);
    procedure.executeFromState(env, RemoveRegionPeerState.REMOVE_REGION_PEER);

    Assert.assertFalse(procedure.isFailed());
    Assert.assertEquals(
        RemoveRegionPeerState.REMOVE_REGION_PEER.ordinal(), readNextState(procedure));
  }

  private static int readNextState(RemoveRegionPeerProcedure procedure) throws Exception {
    final Field field = StateMachineProcedure.class.getDeclaredField("nextState");
    field.setAccessible(true);
    return field.getInt(procedure);
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static TDataNodeLocation location(int nodeId) {
    return new TDataNodeLocation(
        nodeId,
        new TEndPoint("127.0.0.1", 6667),
        new TEndPoint("127.0.0.1", 10730),
        new TEndPoint("127.0.0.1", 10740),
        new TEndPoint("127.0.0.1", 10760),
        new TEndPoint("127.0.0.1", 10750));
  }
}
