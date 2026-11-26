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
import org.apache.iotdb.commons.cluster.RegionStatus;
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
import org.mockito.InOrder;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentHashMap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
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
  private final ConcurrentHashMap<Long, Procedure<ConfigNodeProcedureEnv>> procedures =
      new ConcurrentHashMap<>();
  private final AddRegionPeerProcedure add =
      new AddRegionPeerProcedure(region, source, target) {
        {
          setParentProcId(MIGRATION_ID);
        }
      };
  private final TestRemoveProcedure remove = new TestRemoveProcedure();

  private class TestRemoveProcedure extends RemoveRegionPeerProcedure {
    TestRemoveProcedure() {
      super(VerifiedRegionMigrationProcedureTest.this.region, target, source);
      setParentProcId(MIGRATION_ID);
    }

    void compensate() throws Exception {
      rollback(env);
    }

    void executeStep(RemoveRegionPeerState expected) throws InterruptedException {
      Assert.assertEquals(expected, getCurrentState());
      execute(env);
    }
  }

  @Before
  public void setUp() throws Exception {
    when(env.getRegionMaintainHandler()).thenReturn(handler);
    when(handler.isIoTConsensusV1DataRegion(region)).thenReturn(true);
    when(env.getConfigManager().getProcedureManager().getExecutor().getProcedures())
        .thenReturn(procedures);
    RegionMigrateProcedure parent =
        new RegionMigrateProcedure(region, source, target, source, target);
    parent.setProcId(MIGRATION_ID);
    parent.executeFromState(env, RegionTransitionState.ADD_REGION_PEER);
    procedures.put(MIGRATION_ID, (RegionMigrateProcedure) roundTrip(parent));
    add.setProcId(124);
    remove.setProcId(125);
  }

  @Test
  public void plainAddSuccessCannotCompleteHandoffOrRollBackTarget() throws Exception {
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
  public void rejectedCreateDoesNotRegisterOrResetMigrationTarget() throws Exception {
    when(handler.createNewRegionPeer(region, target))
        .thenReturn(new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode()));
    Assert.assertEquals(
        Flow.NO_MORE_STATE, add.executeFromState(env, AddRegionPeerState.CREATE_NEW_REGION_PEER));
    Assert.assertTrue(add.isFailed());
    verify(handler, never()).addRegionLocation(any(), any());
    verify(handler, never()).forceUpdateRegionCache(any(), any(), any());
    verify(handler, never()).removeRegionLocation(any(), any());
    verify(handler, never()).resetPeerList(any(), any(), any());
  }

  @Test
  public void successfulCreateRegistersMigrationTarget() throws Exception {
    when(handler.createNewRegionPeer(region, target)).thenReturn(success);
    Assert.assertEquals(
        Flow.HAS_MORE_STATE, add.executeFromState(env, AddRegionPeerState.CREATE_NEW_REGION_PEER));
    InOrder order = inOrder(handler);
    order.verify(handler).createNewRegionPeer(region, target);
    order.verify(handler).addRegionLocation(region, target);
    order.verify(handler).forceUpdateRegionCache(region, target, RegionStatus.Adding);
  }

  @Test
  public void lostAddTaskIsResubmittedToSource() throws Exception {
    AddRegionPeerProcedure recovered = (AddRegionPeerProcedure) roundTrip(add);
    when(handler.submitAddRegionPeerTask(add.getProcId(), target, region, source, MIGRATION_ID))
        .thenReturn(success);
    when(handler.waitTaskFinish(anyLong(), any(), any()))
        .thenReturn(
            new TRegionMigrateResult(TRegionMaintainTaskStatus.TASK_NOT_EXIST),
            new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS)
                .setVerifiedMigrationId(MIGRATION_ID));

    Assert.assertEquals(
        Flow.HAS_MORE_STATE,
        recovered.executeFromState(env, AddRegionPeerState.DO_ADD_REGION_PEER));
    Assert.assertFalse(recovered.isFailed());
    verify(handler, times(2))
        .submitAddRegionPeerTask(add.getProcId(), target, region, source, MIGRATION_ID);
  }

  @Test
  public void coordinatorRestartReplaysSameRemove() throws Exception {
    RemoveRegionPeerProcedure recovered = (RemoveRegionPeerProcedure) roundTrip(remove);
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
        recovered.executeFromState(env, RemoveRegionPeerState.REMOVE_REGION_PEER));
    Assert.assertFalse(recovered.isFailed());
    verify(handler, times(2))
        .submitRemoveRegionPeerTask(remove.getProcId(), source, region, target, MIGRATION_ID);
  }

  @Test
  public void wrongRemoveReceiptCannotStartDeletingSource() throws Exception {
    when(handler.submitRemoveRegionPeerTask(
            remove.getProcId(), source, region, target, MIGRATION_ID))
        .thenReturn(success);
    when(handler.waitTaskFinish(anyLong(), any(), any()))
        .thenReturn(
            new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS)
                .setVerifiedMigrationId(MIGRATION_ID - 1));
    when(handler.isRegionMigrationConfirmed(region, source, target, MIGRATION_ID))
        .thenReturn(false);
    when(env.getConfigManager()
            .getPartitionManager()
            .isDataNodeContainsRegion(target.getDataNodeId(), region))
        .thenReturn(true);
    doThrow(new IOException())
        .when(handler)
        .rollbackRegionMigration(region, source, target, MIGRATION_ID, true);

    remove.executeStep(RemoveRegionPeerState.TRANSFER_REGION_LEADER);
    remove.executeStep(RemoveRegionPeerState.REMOVE_REGION_PEER);
    remove.executeStep(RemoveRegionPeerState.ROLLBACK_MIGRATION_TO_SOURCE);
    Assert.assertTrue(remove.isFailed());
    Assert.assertThrows(
        IOException.class,
        () -> remove.rollbackState(env, RemoveRegionPeerState.ROLLBACK_MIGRATION_TO_SOURCE));
    verify(handler).isRegionMigrationConfirmed(region, source, target, MIGRATION_ID);
    verify(handler, never()).submitDeleteOldRegionPeerTask(anyLong(), any(), any(), anyLong());
    verify(handler, never()).removeRegionLocation(any(), any());
    verify(handler, never()).forceUpdateRegionCache(region, source, RegionStatus.Running);
  }

  @Test
  public void lostDeleteAfterVerifiedRemoveFinalizesSourceMetadata() throws Exception {
    when(handler.submitRemoveRegionPeerTask(
            remove.getProcId(), source, region, target, MIGRATION_ID))
        .thenReturn(success);
    when(handler.waitTaskFinish(remove.getProcId(), target, null))
        .thenReturn(
            new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS)
                .setVerifiedMigrationId(MIGRATION_ID));
    when(handler.submitDeleteOldRegionPeerTask(remove.getProcId(), source, region, MIGRATION_ID))
        .thenReturn(success);
    when(handler.waitTaskFinish(remove.getProcId(), source, null))
        .thenReturn(new TRegionMigrateResult(TRegionMaintainTaskStatus.TASK_NOT_EXIST));

    remove.executeStep(RemoveRegionPeerState.TRANSFER_REGION_LEADER);
    Assert.assertFalse(remove.requiresDurableState(env));
    remove.executeStep(RemoveRegionPeerState.REMOVE_REGION_PEER);
    Assert.assertTrue(remove.requiresDurableState(env));
    remove.executeStep(RemoveRegionPeerState.DELETE_OLD_REGION_PEER);
    finishConfirmedRemoval();
    verify(handler, times(2))
        .submitDeleteOldRegionPeerTask(remove.getProcId(), source, region, MIGRATION_ID);
    verify(handler, never()).isRegionMigrationConfirmed(region, source, target, MIGRATION_ID);
  }

  @Test
  public void confirmedDestinationAfterRemoveFailureFinalizesWithoutDeletingSource()
      throws Exception {
    when(handler.submitRemoveRegionPeerTask(
            remove.getProcId(), source, region, target, MIGRATION_ID))
        .thenReturn(success);
    when(handler.waitTaskFinish(remove.getProcId(), target, null))
        .thenReturn(new TRegionMigrateResult(TRegionMaintainTaskStatus.FAIL));
    when(handler.isRegionMigrationConfirmed(region, source, target, MIGRATION_ID)).thenReturn(true);

    remove.executeStep(RemoveRegionPeerState.TRANSFER_REGION_LEADER);
    Assert.assertFalse(remove.requiresDurableState(env));
    remove.executeStep(RemoveRegionPeerState.REMOVE_REGION_PEER);
    Assert.assertTrue(remove.requiresDurableState(env));
    finishConfirmedRemoval();
    verify(handler, never()).submitDeleteOldRegionPeerTask(anyLong(), any(), any(), anyLong());
    verify(handler, never()).forceUpdateRegionCache(region, source, RegionStatus.Running);
  }

  @Test
  public void cancelledDestinationAndReopenedSourceRestoreSourceStatus() throws Exception {
    when(handler.submitRemoveRegionPeerTask(
            remove.getProcId(), source, region, target, MIGRATION_ID))
        .thenReturn(new TSStatus(TSStatusCode.MIGRATE_REGION_ERROR.getStatusCode()));
    when(handler.isRegionMigrationConfirmed(region, source, target, MIGRATION_ID))
        .thenReturn(false);
    when(env.getConfigManager()
            .getPartitionManager()
            .isDataNodeContainsRegion(target.getDataNodeId(), region))
        .thenReturn(true, false);
    doThrow(new IOException())
        .doNothing()
        .when(handler)
        .rollbackRegionMigration(region, target, source, MIGRATION_ID, false);
    when(handler.removeRegionLocation(region, target)).thenReturn(success);
    when(handler.isSucceed(success)).thenReturn(true);
    when(handler.resetPeerListAfterRemoveForIoTConsensusV1(region, target)).thenReturn(true);

    remove.executeStep(RemoveRegionPeerState.TRANSFER_REGION_LEADER);
    Assert.assertFalse(remove.requiresDurableState(env));
    remove.executeStep(RemoveRegionPeerState.REMOVE_REGION_PEER);
    Assert.assertTrue(remove.requiresDurableState(env));
    remove.executeStep(RemoveRegionPeerState.ROLLBACK_MIGRATION_TO_SOURCE);
    Assert.assertThrows(IOException.class, remove::compensate);
    Assert.assertTrue(remove.getRollbackRetryTimeout() > 0);
    InOrder order = inOrder(handler);
    order.verify(handler).rollbackRegionMigration(region, source, target, MIGRATION_ID, true);
    order.verify(handler).forceUpdateRegionCache(region, source, RegionStatus.Running);
    order.verify(handler).removeRegionLocation(region, target);
    order.verify(handler).resetPeerListAfterRemoveForIoTConsensusV1(region, target);

    // The target was offline while the source resumed serving. Recovery follows the same decision.
    ByteBuffer saved = serialize(remove);
    saved.getShort();
    TestRemoveProcedure recovered = new TestRemoveProcedure();
    recovered.deserialize(saved);
    recovered.compensate();
    verify(handler).isRegionMigrationConfirmed(region, source, target, MIGRATION_ID);
    verify(handler, times(2)).rollbackRegionMigration(region, target, source, MIGRATION_ID, false);
    verify(handler, never()).submitDeleteOldRegionPeerTask(anyLong(), any(), any(), anyLong());
    verify(handler, never()).removeRegionLocation(region, source);
  }

  private void finishConfirmedRemoval() throws InterruptedException {
    when(handler.removeRegionLocation(region, source)).thenReturn(success);
    when(handler.resetPeerListAfterRemoveForIoTConsensusV1(region, source)).thenReturn(true);
    remove.executeStep(RemoveRegionPeerState.DROP_CONSENSUS_PIPES);
    remove.executeStep(RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE);
    remove.executeStep(RemoveRegionPeerState.RESET_IOT_CONSENSUS_V1_PEER_LIST);
    Assert.assertFalse(remove.isFailed());
    verify(handler).removeRegionLocation(region, source);
  }

  @Test
  public void recoveredLegacyRemoveChildCannotDeleteSource() throws Exception {
    RemoveRegionPeerProcedure recovered = (RemoveRegionPeerProcedure) roundTrip(remove);
    procedures.put(
        MIGRATION_ID, new RegionMigrateProcedure(region, source, target, source, target));

    Assert.assertEquals(
        Flow.NO_MORE_STATE,
        recovered.executeFromState(env, RemoveRegionPeerState.DELETE_OLD_REGION_PEER));
    Assert.assertTrue(recovered.isFailed());
    verify(handler, never()).submitDeleteOldRegionPeerTask(anyLong(), any(), any());
  }

  @Test
  public void reconstructUsesOrdinaryRemove() throws Exception {
    procedures.put(MIGRATION_ID, new ReconstructRegionProcedure());
    when(handler.submitRemoveRegionPeerTask(remove.getProcId(), source, region, target))
        .thenReturn(success);
    when(handler.waitTaskFinish(remove.getProcId(), target))
        .thenReturn(new TRegionMigrateResult(TRegionMaintainTaskStatus.SUCCESS));
    Assert.assertEquals(
        Flow.HAS_MORE_STATE,
        remove.executeFromState(env, RemoveRegionPeerState.REMOVE_REGION_PEER));
    Assert.assertFalse(remove.isFailed());
    verify(handler).submitRemoveRegionPeerTask(remove.getProcId(), source, region, target);
    verify(handler, never()).submitRemoveRegionPeerTask(anyLong(), any(), any(), any(), anyLong());
    Assert.assertEquals(
        Flow.NO_MORE_STATE,
        remove.executeFromState(env, RemoveRegionPeerState.ROLLBACK_MIGRATION_TO_SOURCE));
    verify(handler, never()).isRegionMigrationConfirmed(region, source, target, MIGRATION_ID);
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
    return ProcedureFactory.getInstance().create(serialize(procedure));
  }

  private static ByteBuffer serialize(Procedure<?> procedure) throws Exception {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(bytes)) {
      procedure.serialize(out);
    }
    return ByteBuffer.wrap(bytes.toByteArray());
  }

  private static TDataNodeLocation location(int id) {
    TEndPoint endpoint = new TEndPoint("127.0.0.1", 10000 + id);
    return new TDataNodeLocation(id, endpoint, endpoint, endpoint, endpoint, endpoint);
  }
}
