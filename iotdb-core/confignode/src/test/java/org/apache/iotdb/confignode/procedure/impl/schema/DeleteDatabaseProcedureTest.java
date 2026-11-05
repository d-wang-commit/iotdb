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

package org.apache.iotdb.confignode.procedure.impl.schema;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.confignode.client.async.RegionDeletionRpcTestContext;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.consensus.ConsensusManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionDeleteTask;
import org.apache.iotdb.confignode.persistence.partition.maintainer.RegionMaintainTask;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.state.schema.DeleteDatabaseState;
import org.apache.iotdb.confignode.procedure.store.ProcedureFactory;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.utils.PublicBAOS;
import org.junit.Before;
import org.junit.Test;

import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DeleteDatabaseProcedureTest {

  private static final String DATABASE = "root.delete_database";

  private ConfigNodeProcedureEnv env;
  private ConsensusManager consensusManager;
  private LoadManager loadManager;
  private PartitionInfo partitionInfo;
  private List<TDataNodeLocation> dataNodes;
  private List<RegionMaintainTask> tasksAtMetadataDeletion;

  @Before
  public void setUp() {
    env = mock(ConfigNodeProcedureEnv.class);
    final ConfigManager configManager = mock(ConfigManager.class);
    consensusManager = mock(ConsensusManager.class);
    loadManager = mock(LoadManager.class);
    partitionInfo = new PartitionInfo();
    dataNodes =
        Arrays.asList(
            new TDataNodeLocation()
                .setDataNodeId(1)
                .setInternalEndPoint(new TEndPoint("127.0.0.1", 10730)),
            new TDataNodeLocation()
                .setDataNodeId(2)
                .setInternalEndPoint(new TEndPoint("127.0.0.1", 10731)));
    tasksAtMetadataDeletion = new ArrayList<>();
    when(env.getConfigManager()).thenReturn(configManager);
    when(configManager.getConsensusManager()).thenReturn(consensusManager);
    when(configManager.getLoadManager()).thenReturn(loadManager);
    when(env.deleteDatabaseConfig(DATABASE, false))
        .thenAnswer(
            invocation -> {
              tasksAtMetadataDeletion = partitionInfo.getRegionMaintainEntryList();
              return status(TSStatusCode.SUCCESS_STATUS);
            });
  }

  @Test
  public void serializeDeserializeTest() {

    PublicBAOS byteArrayOutputStream = new PublicBAOS();
    DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
    DeleteDatabaseProcedure p1 = new DeleteDatabaseProcedure(new TDatabaseSchema("root.sg"), false);

    try {
      p1.serialize(outputStream);
      ByteBuffer buffer =
          ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());

      DeleteDatabaseProcedure p2 =
          (DeleteDatabaseProcedure) ProcedureFactory.getInstance().create(buffer);
      assertEquals(p1, p2);

    } catch (Exception e) {
      fail();
    }
  }

  @Test(timeout = 10_000)
  public void testDataRegionOfferNonSuccessRetriedBeforeDeletingMetadata() throws Exception {
    assertRegionOfferRetried(TConsensusGroupType.DataRegion, false);
  }

  @Test(timeout = 10_000)
  public void testDataRegionOfferExceptionRetriedBeforeDeletingMetadata() throws Exception {
    assertRegionOfferRetried(TConsensusGroupType.DataRegion, true);
  }

  @Test(timeout = 10_000)
  public void testMetadataDeletionNonSuccessRetried() throws Exception {
    when(env.getAllReplicaSets(DATABASE)).thenReturn(Collections.emptyList());
    when(env.deleteDatabaseConfig(DATABASE, false))
        .thenReturn(status(TSStatusCode.SYSTEM_READ_ONLY), status(TSStatusCode.SUCCESS_STATUS));
    final TestingDeleteDatabaseProcedure procedure = new TestingDeleteDatabaseProcedure();

    procedure.executeDeletionRound(env);

    assertFalse(procedure.isFailed());
    assertEquals(DeleteDatabaseState.DELETE_DATABASE_SCHEMA, procedure.currentState());
    verify(env).deleteDatabaseConfig(DATABASE, false);

    procedure.executeDeletionRound(env);

    assertFalse(procedure.isFailed());
    verify(env, times(2)).deleteDatabaseConfig(DATABASE, false);
  }

  @Test(timeout = 10_000)
  public void testPersistentOfferFailureEndsAsFailureWithoutDeletingMetadata() throws Exception {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    when(env.getAllReplicaSets(DATABASE))
        .thenReturn(Collections.singletonList(new TRegionReplicaSet(regionId, dataNodes)));
    final AtomicInteger offers = new AtomicInteger();
    when(consensusManager.write(any(OfferRegionMaintainTasksPlan.class)))
        .thenAnswer(
            invocation -> {
              offers.incrementAndGet();
              return status(TSStatusCode.SYSTEM_READ_ONLY);
            });
    final TestingDeleteDatabaseProcedure procedure = new TestingDeleteDatabaseProcedure();

    for (int round = 0; round < 10 && !procedure.isFailed(); round++) {
      assertEquals(DeleteDatabaseState.DELETE_DATABASE_SCHEMA, procedure.currentState());
      procedure.executeDeletionRound(env);
      if (!procedure.isFailed()) {
        assertEquals(DeleteDatabaseState.DELETE_DATABASE_SCHEMA, procedure.currentState());
      }
    }

    assertTrue(procedure.isFailed());
    assertTrue(offers.get() > 1);
    assertTrue(offers.get() < 10);
    assertTrue(partitionInfo.getRegionMaintainEntryList().isEmpty());
    verify(env, never()).deleteDatabaseConfig(DATABASE, false);
    verify(loadManager, never()).clearDataPartitionPolicyTable(DATABASE);
  }

  @Test(timeout = 10_000)
  public void testSchemaRegionOfferNonSuccessRetriedBeforeDeletingMetadata() throws Exception {
    try (RegionDeletionRpcTestContext rpc =
        new RegionDeletionRpcTestContext(
            (endpoint, regionId) -> status(TSStatusCode.DELETE_REGION_ERROR))) {
      assertRegionOfferRetried(TConsensusGroupType.SchemaRegion, false);
      assertFalse(rpc.getRequests().isEmpty());
    }
  }

  @Test(timeout = 10_000)
  public void testSchemaRegionOfferExceptionRetriedBeforeDeletingMetadata() throws Exception {
    try (RegionDeletionRpcTestContext rpc =
        new RegionDeletionRpcTestContext(
            (endpoint, regionId) -> status(TSStatusCode.DELETE_REGION_ERROR))) {
      assertRegionOfferRetried(TConsensusGroupType.SchemaRegion, true);
      assertFalse(rpc.getRequests().isEmpty());
    }
  }

  @Test(timeout = 10_000)
  public void testSchemaRegionNotExistCompletesWithoutOfferingTasks() throws Exception {
    final TConsensusGroupId regionId = new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 1);
    when(env.getAllReplicaSets(DATABASE))
        .thenReturn(Collections.singletonList(new TRegionReplicaSet(regionId, dataNodes)));

    try (RegionDeletionRpcTestContext rpc =
        new RegionDeletionRpcTestContext(
            (endpoint, requestRegionId) -> status(TSStatusCode.REGION_NOT_EXIST))) {
      final TestingDeleteDatabaseProcedure procedure = new TestingDeleteDatabaseProcedure();
      procedure.executeDeletionRound(env);

      assertFalse(procedure.isFailed());
      assertEquals(dataNodes.size(), rpc.getRequests().size());
      assertTrue(rpc.getRequests().stream().allMatch(request -> regionId.equals(request.right)));
      assertTrue(partitionInfo.getRegionMaintainEntryList().isEmpty());
      verify(consensusManager, never()).write(any(OfferRegionMaintainTasksPlan.class));
      verify(env).deleteDatabaseConfig(DATABASE, false);
    }
  }

  @Test(timeout = 10_000)
  public void testSchemaOfferFailureKeepsPreviouslyOfferedDataTasks() throws Exception {
    final TConsensusGroupId dataRegionId = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    final TConsensusGroupId schemaRegionId =
        new TConsensusGroupId(TConsensusGroupType.SchemaRegion, 2);
    when(env.getAllReplicaSets(DATABASE))
        .thenReturn(
            Arrays.asList(
                new TRegionReplicaSet(dataRegionId, dataNodes),
                new TRegionReplicaSet(schemaRegionId, dataNodes)));
    final AtomicInteger schemaOffers = new AtomicInteger();
    when(consensusManager.write(any(OfferRegionMaintainTasksPlan.class)))
        .thenAnswer(
            invocation -> {
              final OfferRegionMaintainTasksPlan plan = invocation.getArgument(0);
              if (plan.getRegionMaintainTaskList().get(0).getRegionId().equals(schemaRegionId)
                  && schemaOffers.getAndIncrement() == 0) {
                return status(TSStatusCode.SYSTEM_READ_ONLY);
              }
              return partitionInfo.offerRegionMaintainTasks(plan);
            });

    try (RegionDeletionRpcTestContext rpc =
        new RegionDeletionRpcTestContext(
            (endpoint, regionId) -> status(TSStatusCode.DELETE_REGION_ERROR))) {
      final TestingDeleteDatabaseProcedure procedure = new TestingDeleteDatabaseProcedure();
      procedure.executeDeletionRound(env);

      assertRetryPending(procedure);
      assertTasksForRegion(dataRegionId, partitionInfo.getRegionMaintainEntryList());

      procedure.executeDeletionRound(env);

      assertFalse(procedure.isFailed());
      assertEquals(2, schemaOffers.get());
      // Retrying the state may offer data tasks again; every replica still needs a deletion task.
      for (TDataNodeLocation dataNode : dataNodes) {
        assertTrue(tasksAtMetadataDeletion.contains(new RegionDeleteTask(dataNode, dataRegionId)));
        assertTrue(
            tasksAtMetadataDeletion.contains(new RegionDeleteTask(dataNode, schemaRegionId)));
      }
      assertFalse(rpc.getRequests().isEmpty());
      verify(env).deleteDatabaseConfig(DATABASE, false);
    }
  }

  private void assertRegionOfferRetried(
      final TConsensusGroupType regionType, final boolean throwException) throws Exception {
    final TConsensusGroupId regionId = new TConsensusGroupId(regionType, 1);
    when(env.getAllReplicaSets(DATABASE))
        .thenReturn(Collections.singletonList(new TRegionReplicaSet(regionId, dataNodes)));
    final AtomicInteger offers = new AtomicInteger();
    when(consensusManager.write(any(OfferRegionMaintainTasksPlan.class)))
        .thenAnswer(
            invocation -> {
              if (offers.getAndIncrement() == 0) {
                if (throwException) {
                  throw new ConsensusException("injected offer failure");
                }
                return status(TSStatusCode.SYSTEM_READ_ONLY);
              }
              return partitionInfo.offerRegionMaintainTasks(invocation.getArgument(0));
            });

    final TestingDeleteDatabaseProcedure procedure = new TestingDeleteDatabaseProcedure();
    procedure.executeDeletionRound(env);

    assertRetryPending(procedure);
    assertTrue(partitionInfo.getRegionMaintainEntryList().isEmpty());

    procedure.executeDeletionRound(env);

    assertFalse(procedure.isFailed());
    assertEquals(2, offers.get());
    assertTasksForRegion(regionId, partitionInfo.getRegionMaintainEntryList());
    assertTasksForRegion(regionId, tasksAtMetadataDeletion);
    verify(env).deleteDatabaseConfig(DATABASE, false);
    verify(consensusManager, times(2)).write(any(OfferRegionMaintainTasksPlan.class));
  }

  private void assertRetryPending(final TestingDeleteDatabaseProcedure procedure) {
    assertFalse(procedure.isFailed());
    assertEquals(DeleteDatabaseState.DELETE_DATABASE_SCHEMA, procedure.currentState());
    verify(env, never()).deleteDatabaseConfig(DATABASE, false);
    verify(loadManager, never()).clearDataPartitionPolicyTable(DATABASE);
  }

  private void assertTasksForRegion(
      final TConsensusGroupId regionId, final List<RegionMaintainTask> tasks) {
    assertEquals(dataNodes.size(), tasks.size());
    for (TDataNodeLocation dataNode : dataNodes) {
      assertTrue(tasks.contains(new RegionDeleteTask(dataNode, regionId)));
    }
  }

  private static TSStatus status(final TSStatusCode statusCode) {
    return new TSStatus(statusCode.getStatusCode());
  }

  private static class TestingDeleteDatabaseProcedure extends DeleteDatabaseProcedure {

    private TestingDeleteDatabaseProcedure() {
      super(new TDatabaseSchema(DATABASE), false);
    }

    @Override
    protected DeleteDatabaseState getInitialState() {
      return DeleteDatabaseState.DELETE_DATABASE_SCHEMA;
    }

    private void executeDeletionRound(final ConfigNodeProcedureEnv env)
        throws InterruptedException {
      super.execute(env);
    }

    private DeleteDatabaseState currentState() {
      return getCurrentState();
    }
  }
}
