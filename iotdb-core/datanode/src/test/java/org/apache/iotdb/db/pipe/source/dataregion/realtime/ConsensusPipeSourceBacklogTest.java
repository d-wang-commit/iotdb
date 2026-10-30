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

package org.apache.iotdb.db.pipe.source.dataregion.realtime;

import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.metric.ConsensusPipeBacklog;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEvent;
import org.apache.iotdb.db.pipe.event.realtime.PipeRealtimeEventFactory;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.assigner.PipeDataRegionAssigner;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch.TsFileEpoch;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.pipe.api.event.Event;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsensusPipeSourceBacklogTest {

  @Test
  public void progressOnlyFilesAreNotReservedAndRestartedSourcesRetireOldFileSlots()
      throws Exception {
    final TsFileEpoch epoch = new TsFileEpoch(mock(TsFileResource.class));
    epoch.markLocalReplicationData();
    try (final TestSource source = new TestSource("backlog-epoch-restart", true)) {
      epoch.migrateState(source, state -> TsFileEpoch.State.USING_TABLET);
      assertNull(epoch.takeReplicationFile(source));
      assertEquals(0, source.backlog().snapshot().getFileCount());
      epoch.migrateState(source, state -> TsFileEpoch.State.USING_TSFILE);
      assertEquals(1, source.backlog().snapshot().getFileCount());
    }
    try (final TestSource replacement = new TestSource("backlog-epoch-restart", true)) {
      epoch.reserveReplicationFile(replacement);
      assertEquals(1, replacement.backlog().snapshot().getFileCount());
      final Field slotsField = TsFileEpoch.class.getDeclaredField("replicationFiles");
      slotsField.setAccessible(true);
      assertEquals(1, ((Map<?, ?>) slotsField.get(epoch)).size());
      epoch.takeReplicationFile(replacement).finish();
      assertEquals(0, replacement.backlog().snapshot().getFileCount());
    }
  }

  @Test
  public void batchCountsAnUnsealedLocalFileOnceAndTransfersItsOwnership() throws Exception {
    try (final TestSource source = new TestSource("backlog-batch", false);
        final PipeDataRegionAssigner assigner = new PipeDataRegionAssigner(Integer.MIN_VALUE)) {
      assigner.startAssignTo(source);
      final InsertNode localInsert = mock(InsertNode.class);
      when(localInsert.getProgressIndex()).thenReturn(localProgress());
      final TsFileResource resource = mock(TsFileResource.class);
      when(resource.getTsFilePath()).thenReturn("backlog-batch-local.tsfile");

      assigner.recordUnsealedReplicationFile(localInsert, resource);
      assigner.recordUnsealedReplicationFile(localInsert, resource);
      assertEquals(1, source.backlog().snapshot().getFileCount());
      assertEquals(0, source.backlog().snapshot().getTabletCount());

      final InsertNode remoteInsert = mock(InsertNode.class);
      when(remoteInsert.getProgressIndex())
          .thenReturn(new RecoverProgressIndex(Integer.MAX_VALUE, new SimpleProgressIndex(1, 1)));
      assigner.recordUnsealedReplicationFile(remoteInsert, resource);
      assertEquals(1, source.backlog().snapshot().getFileCount());

      final TsFileEpoch epoch = PipeRealtimeEventFactory.getOrCreateTsFileEpoch(resource);
      final ConsensusPipeBacklog.Token file = epoch.takeReplicationFile(source);
      assertNotNull(file);
      assertEquals(1, source.backlog().snapshot().getFileCount());
      assertNull(epoch.takeReplicationFile(source));
      file.finish();
      epoch.migrateState(source, state -> TsFileEpoch.State.USING_TSFILE);
      assertEquals(0, source.backlog().snapshot().getFileCount());
      assigner.stopAssignTo(source);
    }
  }

  @Test
  public void hybridRetainsTabletAndFallbackFileUntilEachReallyFinishes() throws Exception {
    try (final TestSource source = new TestSource("backlog-hybrid", true)) {
      final TsFileEpoch epoch = new TsFileEpoch(mock(TsFileResource.class));
      epoch.markLocalReplicationData();
      final ConsensusPipeBacklog.Token tablet =
          source.backlog().newToken(ConsensusPipeBacklog.Kind.TABLET);

      epoch.migrateState(source, state -> TsFileEpoch.State.USING_BOTH);
      epoch.migrateState(source, state -> TsFileEpoch.State.USING_BOTH);
      assertEquals(1, source.backlog().snapshot().getTabletCount());
      assertEquals(1, source.backlog().snapshot().getFileCount());

      tablet.finish();
      assertEquals(0, source.backlog().snapshot().getTabletCount());
      assertEquals(1, source.backlog().snapshot().getFileCount());
      final ConsensusPipeBacklog.Token file = epoch.takeReplicationFile(source);
      epoch.migrateState(source, state -> TsFileEpoch.State.USING_TSFILE);
      assertEquals(1, source.backlog().snapshot().getFileCount());
      file.finish();
      assertEquals(0, source.backlog().snapshot().getFileCount());
    }
  }

  @Test
  public void deletionIsCountedWhileAssignerIsBlockedAndIsNotCountedAgainOnDelivery()
      throws Exception {
    try (final TestSource source = new TestSource("backlog-before-assigner", true);
        final PipeDataRegionAssigner assigner = new PipeDataRegionAssigner(Integer.MIN_VALUE)) {
      assigner.startAssignTo(source);
      source.blockHeartbeat = true;
      assigner.publishToAssign(
          PipeRealtimeEventFactory.createRealtimeEvent(Integer.MIN_VALUE, false));
      try {
        assertTrue(source.heartbeatEntered.await(5, TimeUnit.SECONDS));
        final DeleteDataNode deletion =
            new DeleteDataNode(
                new PlanNodeId("delete"),
                Collections.singletonList(new MeasurementPath("root.backlog.**")),
                0,
                10);
        deletion.setProgressIndex(localProgress());
        assigner.publishDataEventToAssign(
            () -> PipeRealtimeEventFactory.createRealtimeEvent(deletion));
        assertEquals(1, source.backlog().snapshot().getDeletionCount());
        assertEquals(1, source.dataArrived.getCount());
      } finally {
        source.allowHeartbeat.countDown();
      }
      assertTrue(source.dataArrived.await(5, TimeUnit.SECONDS));
      assertEquals(1, source.backlog().snapshot().getDeletionCount());
      final PipeRealtimeEvent delivered = (PipeRealtimeEvent) source.supply();
      assertNotNull(delivered);
      delivered.decreaseReferenceCount(getClass().getName(), false);
      assertEquals(0, source.backlog().snapshot().getDeletionCount());
      assigner.stopAssignTo(source);
    }
  }

  private static RecoverProgressIndex localProgress() {
    return new RecoverProgressIndex(
        IoTDBDescriptor.getInstance().getConfig().getDataNodeId(), new SimpleProgressIndex(1, 1));
  }

  private static final class TestSource extends PipeRealtimeDataRegionSource {
    private final boolean stream;
    private boolean blockHeartbeat;
    private final CountDownLatch heartbeatEntered = new CountDownLatch(1);
    private final CountDownLatch allowHeartbeat = new CountDownLatch(1);
    private final CountDownLatch dataArrived = new CountDownLatch(1);

    private TestSource(final String name, final boolean stream) {
      pipeName = name;
      this.stream = stream;
      shouldExtractInsertion = true;
      shouldExtractDeletion = true;
      setConsensusPipeBacklog(ConsensusPipeBacklog.create(name, 1, getSourceId()));
    }

    private ConsensusPipeBacklog backlog() {
      return getConsensusPipeBacklog();
    }

    @Override
    public String getTaskID() {
      return pipeName;
    }

    @Override
    protected void doExtract(final PipeRealtimeEvent event) {
      if (event.getEvent() instanceof PipeHeartbeatEvent) {
        if (blockHeartbeat) {
          heartbeatEntered.countDown();
          try {
            assertTrue(allowHeartbeat.await(5, TimeUnit.SECONDS));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
          }
        }
        event.decreaseReferenceCount(getClass().getName(), false);
        return;
      }
      pendingQueue.offer(event);
      dataArrived.countDown();
    }

    @Override
    public Event supply() {
      return pendingQueue.directPoll();
    }

    @Override
    public boolean isNeedListenToTsFile() {
      return true;
    }

    @Override
    public boolean isNeedListenToInsertNode() {
      return stream;
    }
  }
}
