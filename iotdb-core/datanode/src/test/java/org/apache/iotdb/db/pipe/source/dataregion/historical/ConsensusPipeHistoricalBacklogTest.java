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

package org.apache.iotdb.db.pipe.source.dataregion.historical;

import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.pipe.datastructure.resource.PersistentResource;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.consensus.deletion.DeletionResource;
import org.apache.iotdb.db.pipe.consensus.metric.ConsensusPipeBacklog;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.DeleteDataNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ConsensusPipeHistoricalBacklogTest {

  @Test
  public void discoveryCountsOnlySelectedLocalWorkAndSupplyTransfersDeletionOwnership()
      throws Exception {
    try (final ConsensusPipeBacklog backlog =
        ConsensusPipeBacklog.create("historical-backlog", 1, 1)) {
      final PipeHistoricalDataRegionTsFileAndDeletionSource source =
          new PipeHistoricalDataRegionTsFileAndDeletionSource() {
            @Override
            protected boolean shouldAssignReplicateIndexForIoTV2(final EnrichedEvent event) {
              return false;
            }
          };
      source.setConsensusPipeBacklog(backlog);
      final RecoverProgressIndex localProgress =
          new RecoverProgressIndex(
              IoTDBDescriptor.getInstance().getConfig().getDataNodeId(),
              new SimpleProgressIndex(1, 1));
      final TsFileResource selectedLocal = mock(TsFileResource.class);
      when(selectedLocal.getMaxProgressIndex()).thenReturn(localProgress);
      final TsFileResource progressOnly = mock(TsFileResource.class);
      when(progressOnly.getMaxProgressIndex()).thenReturn(localProgress);
      final TsFileResource receivedFile = mock(TsFileResource.class);
      when(receivedFile.getMaxProgressIndex()).thenReturn(localProgress);
      when(receivedFile.isGeneratedByIoTConsensusV2()).thenReturn(true);
      final TsFileResource remoteOnly = mock(TsFileResource.class);
      when(remoteOnly.getMaxProgressIndex())
          .thenReturn(new RecoverProgressIndex(Integer.MAX_VALUE, new SimpleProgressIndex(1, 2)));
      final DeleteDataNode deleteNode =
          new DeleteDataNode(
              new PlanNodeId("delete"),
              Collections.singletonList(new MeasurementPath("root.backlog.**")),
              0,
              10);
      deleteNode.setProgressIndex(localProgress);
      final DeletionResource deletion = mock(DeletionResource.class);
      when(deletion.getProgressIndex()).thenReturn(localProgress);
      when(deletion.getDeleteDataNode()).thenReturn(deleteNode);
      final Map<TsFileResource, Set<String>> selected = new HashMap<>();
      selected.put(selectedLocal, Collections.emptySet());
      selected.put(receivedFile, Collections.emptySet());
      selected.put(remoteOnly, Collections.emptySet());
      setField(source, "pipeName", "historical-backlog");
      setField(source, "dataRegionId", Integer.MIN_VALUE);
      setField(source, "hasBeenStarted", true);
      setField(source, "filteredTsFileResources2TableNames", selected);
      setField(
          source,
          "pendingQueue",
          new ArrayDeque<PersistentResource>(
              Arrays.asList(deletion, selectedLocal, progressOnly, receivedFile, remoteOnly)));

      source.initializeHistoricalBacklog();
      assertEquals(1, backlog.snapshot().getFileCount());
      assertEquals(1, backlog.snapshot().getDeletionCount());
      assertEquals(ConsensusPipeBacklog.RECOVERING, backlog.snapshot().getState());
      backlog.markRealtimeReady();
      assertEquals(ConsensusPipeBacklog.VALID, backlog.snapshot().getState());
      assertEquals(5, source.getPendingQueueSize());

      final EnrichedEvent supplied = (EnrichedEvent) source.supply();
      assertNotNull(supplied);
      assertEquals(1, backlog.snapshot().getDeletionCount());
      assertEquals(4, source.getPendingQueueSize());
      supplied.decreaseReferenceCount(getClass().getName(), false);
      assertEquals(0, backlog.snapshot().getDeletionCount());
      assertEquals(1, backlog.snapshot().getFileCount());
      assertEquals(ConsensusPipeBacklog.VALID, backlog.snapshot().getState());
    }
  }

  @Test
  public void emptyHistoricalSourceBecomesReadyWithoutWaitingForSupply() throws Exception {
    try (final ConsensusPipeBacklog backlog = ConsensusPipeBacklog.create("empty-history", 1, 2)) {
      final PipeHistoricalDataRegionTsFileAndDeletionSource source =
          new PipeHistoricalDataRegionTsFileAndDeletionSource();
      source.setConsensusPipeBacklog(backlog);
      backlog.markRealtimeReady();
      assertEquals(ConsensusPipeBacklog.RECOVERING, backlog.snapshot().getState());
      source.start();
      assertEquals(ConsensusPipeBacklog.VALID, backlog.snapshot().getState());
    }
  }

  private static void setField(
      final PipeHistoricalDataRegionTsFileAndDeletionSource source,
      final String name,
      final Object value)
      throws ReflectiveOperationException {
    final Field field =
        PipeHistoricalDataRegionTsFileAndDeletionSource.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(source, value);
  }
}
