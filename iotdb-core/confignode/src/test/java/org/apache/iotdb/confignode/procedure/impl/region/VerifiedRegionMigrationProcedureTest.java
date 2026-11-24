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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.impl.StateMachineProcedure.Flow;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.state.RegionTransitionState;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class VerifiedRegionMigrationProcedureTest {
  private static final long MIGRATION_ID = 123;
  private final TConsensusGroupId region =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 11);
  private final TDataNodeLocation source = location(10);
  private final TDataNodeLocation target = location(9);
  private final ConfigNodeProcedureEnv env = mock(ConfigNodeProcedureEnv.class, RETURNS_DEEP_STUBS);
  private final RegionMaintainHandler handler = mock(RegionMaintainHandler.class);
  private final TSStatus success = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());

  @Before
  public void setUp() {
    when(env.getRegionMaintainHandler()).thenReturn(handler);
    when(handler.isIoTConsensusV1DataRegion(region)).thenReturn(true);
  }

  @Test
  public void plainAddSuccessCannotCompleteHandoffOrRollBackTarget() throws Exception {
    AddRegionPeerProcedure add = new AddRegionPeerProcedure(region, source, target, MIGRATION_ID);
    when(handler.submitAddRegionPeerTask(add.getProcId(), target, region, source, MIGRATION_ID))
        .thenReturn(success);
    when(handler.waitTaskFinish(anyLong(), any(), any()))
        .thenReturn(new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS));

    Assert.assertEquals(
        Flow.NO_MORE_STATE, add.executeFromState(env, AddRegionPeerState.DO_ADD_REGION_PEER));
    Assert.assertTrue(add.isFailed());
    verify(handler, never()).removeRegionLocation(any(), any());
    verify(handler, never()).resetPeerList(any(), any(), any());
  }

  @Test
  public void coordinatorRestartReplaysSameRemove() throws Exception {
    RemoveRegionPeerProcedure remove =
        new RemoveRegionPeerProcedure(region, target, source, MIGRATION_ID);
    when(handler.submitRemoveRegionPeerTask(
            remove.getProcId(), source, region, target, MIGRATION_ID))
        .thenReturn(success);
    when(handler.waitTaskFinish(anyLong(), any(), any()))
        .thenReturn(
            new TRegionMigrateResult(TRegionMaintainTaskStatus.TASK_NOT_EXIST),
            new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS)
                .setVerifiedMigrationId(MIGRATION_ID));

    Assert.assertEquals(
        Flow.HAS_MORE_STATE,
        remove.executeFromState(env, RemoveRegionPeerState.REMOVE_REGION_PEER));
    Assert.assertFalse(remove.isFailed());
    verify(handler, times(2))
        .submitRemoveRegionPeerTask(remove.getProcId(), source, region, target, MIGRATION_ID);
  }

  @Test
  public void wrongRemoveReceiptCannotStartDeletingSource() throws Exception {
    RemoveRegionPeerProcedure remove =
        new RemoveRegionPeerProcedure(region, target, source, MIGRATION_ID);
    when(handler.submitRemoveRegionPeerTask(
            remove.getProcId(), source, region, target, MIGRATION_ID))
        .thenReturn(success);
    when(handler.waitTaskFinish(anyLong(), any(), any()))
        .thenReturn(
            new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS)
                .setVerifiedMigrationId(MIGRATION_ID - 1));

    Assert.assertEquals(
        Flow.NO_MORE_STATE, remove.executeFromState(env, RemoveRegionPeerState.REMOVE_REGION_PEER));
    Assert.assertTrue(remove.isFailed());
    verify(handler, never()).submitDeleteOldRegionPeerTask(anyLong(), any(), any(), anyLong());
    verify(handler, never()).removeRegionLocation(any(), any());
  }

  @Test
  public void lostDeleteCannotFinalizeMetadata() throws Exception {
    RemoveRegionPeerProcedure remove =
        new RemoveRegionPeerProcedure(region, target, source, MIGRATION_ID);
    when(handler.submitDeleteOldRegionPeerTask(remove.getProcId(), source, region, MIGRATION_ID))
        .thenReturn(success);
    when(handler.waitTaskFinish(anyLong(), any(), any()))
        .thenReturn(new TRegionMigrateResult(TRegionMaintainTaskStatus.TASK_NOT_EXIST));

    Assert.assertEquals(
        Flow.NO_MORE_STATE,
        remove.executeFromState(env, RemoveRegionPeerState.DELETE_OLD_REGION_PEER));
    Assert.assertTrue(remove.isFailed());
    verify(handler, times(2))
        .submitDeleteOldRegionPeerTask(remove.getProcId(), source, region, MIGRATION_ID);
    verify(handler, never()).removeRegionLocation(any(), any());
  }

  @Test
  public void recoveredLegacyRemoveChildCannotDeleteSource() throws Exception {
    RemoveRegionPeerProcedure legacy =
        new RemoveRegionPeerProcedure(region, target, source) {
          {
            setParentProcId(MIGRATION_ID);
          }
        };
    RemoveRegionPeerProcedure recovered = (RemoveRegionPeerProcedure) roundTrip(legacy);
    Assert.assertNull(recovered.migrationId);
    ConcurrentHashMap<Long, Procedure<ConfigNodeProcedureEnv>> procedures =
        new ConcurrentHashMap<>();
    procedures.put(
        MIGRATION_ID, new RegionMigrateProcedure(region, source, target, source, target));
    when(env.getConfigManager().getProcedureManager().getExecutor().getProcedures())
        .thenReturn(procedures);

    Assert.assertEquals(
        Flow.NO_MORE_STATE,
        recovered.executeFromState(env, RemoveRegionPeerState.DELETE_OLD_REGION_PEER));
    Assert.assertTrue(recovered.isFailed());
    verify(handler, never()).submitDeleteOldRegionPeerTask(anyLong(), any(), any());
  }

  @Test
  public void migrationIdSurvivesRecovery() throws Exception {
    Assert.assertEquals(
        Long.valueOf(MIGRATION_ID),
        ((AddRegionPeerProcedure)
                roundTrip(new AddRegionPeerProcedure(region, source, target, MIGRATION_ID)))
            .migrationId);
    Assert.assertEquals(
        Long.valueOf(MIGRATION_ID),
        ((RemoveRegionPeerProcedure)
                roundTrip(new RemoveRegionPeerProcedure(region, target, source, MIGRATION_ID)))
            .migrationId);
  }

  @Test
  public void offlineSourceCannotMigrateFromAnotherDonor() throws Exception {
    RegionMigrateProcedure migrate =
        new RegionMigrateProcedure(region, source, target, location(8), target);
    when(handler.isDataNodeOffline(source.getDataNodeId())).thenReturn(true);

    Assert.assertEquals(
        Flow.NO_MORE_STATE, migrate.executeFromState(env, RegionTransitionState.ADD_REGION_PEER));
    Assert.assertTrue(migrate.isFailed());
  }

  private static Procedure<?> roundTrip(Procedure<?> procedure) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      procedure.serialize(out);
    }
    return ProcedureFactory.getInstance().create(ByteBuffer.wrap(bytes.toByteArray()));
  }

  private static TDataNodeLocation location(int id) {
    TEndPoint endpoint = new TEndPoint("127.0.0.1", 10000 + id);
    return new TDataNodeLocation(id, endpoint, endpoint, endpoint, endpoint, endpoint);
  }
}
