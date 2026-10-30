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

package org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.progress.CommitterKey;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.consensus.iotconsensusv2.thrift.TIoTConsensusV2TransferResp;
import org.apache.iotdb.db.pipe.consensus.metric.ConsensusPipeBacklog;
import org.apache.iotdb.db.pipe.consensus.metric.ConsensusPipeBacklog.Kind;
import org.apache.iotdb.db.pipe.consensus.metric.IoTConsensusV2SinkMetrics;
import org.apache.iotdb.db.pipe.event.common.PipeInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.deletion.PipeDeleteDataNodeEvent;
import org.apache.iotdb.db.pipe.event.common.tablet.PipeInsertNodeTabletInsertionEvent;
import org.apache.iotdb.db.pipe.event.common.tsfile.PipeTsFileInsertionEvent;
import org.apache.iotdb.db.pipe.sink.protocol.iotconsensusv2.handler.IoTConsensusV2DeleteEventHandler;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.AbstractDeleteDataNode;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertNode;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.rpc.TSStatusCode;

import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Queue;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class IoTConsensusV2SinkBacklogTest {

  private final ConsensusPipeBacklog backlog =
      ConsensusPipeBacklog.create("backlog-sink-" + UUID.randomUUID(), 1, 1);
  private final IoTConsensusV2SinkMetrics metrics = mock(IoTConsensusV2SinkMetrics.class);
  private final IoTConsensusV2AsyncSink sink = new IoTConsensusV2AsyncSink();

  @After
  public void close() {
    sink.clearRetryEventsReferenceCount();
    sink.clearTransferBufferReferenceCount();
    backlog.close();
  }

  @Test
  public void testRepeatedFilePreparationFailureCannotBecomeAValidZero() throws Exception {
    initializeSink();
    final File missingFile = mock(File.class);
    when(missingFile.getAbsolutePath()).thenReturn("missing-backlog-test.tsfile");
    final TsFileResource resource = mock(TsFileResource.class);
    when(resource.getTsFile()).thenReturn(missingFile);
    when(resource.isClosed()).thenReturn(true);
    final PipeTsFileInsertionEvent event =
        spy(new PipeTsFileInsertionEvent(false, "root.test", resource, false));
    // Isolate the sink's retry ownership from hardlinking. The real EnrichedEvent lifecycle runs.
    doReturn(true).when(event).internallyIncreaseResourceReferenceCount(anyString());
    doReturn(true).when(event).internallyDecreaseResourceReferenceCount(anyString());
    doNothing().when(event).trackResource();
    event.setCommitterKeyAndCommitId(new CommitterKey("backlog-sink-test", 1, 1, 0), 1);
    event.bindReplicationEventToken(backlog.newToken(Kind.FILE));
    try {
      // After an initial async failure, both the transfer buffer and retry own a reference.
      assertTrue(event.increaseReferenceCount("failed handler"));
      addToTransferBuffer(event);
      sink.addFailureEventToRetryQueue(event);
      assertSame(event, retryQueue().poll());

      final Method retry =
          IoTConsensusV2AsyncSink.class.getDeclaredMethod(
              "retryTransfer", PipeTsFileInsertionEvent.class);
      retry.setAccessible(true);
      retry.invoke(sink, event);
      assertFalse(event.isReleased());
      assertEquals(1, backlog.snapshot().getFileCount());
      assertEquals(ConsensusPipeBacklog.ERROR, backlog.snapshot().getState());
      assertSame(event, retryQueue().poll());

      retry.invoke(sink, event);
      // Preserve existing cleanup behavior, but its exhausted references are not proof of an ACK.
      assertTrue(event.isReleased());
      assertEquals(0, backlog.snapshot().getFileCount());
      assertEquals(ConsensusPipeBacklog.ERROR, backlog.snapshot().getState());
      assertTrue(retryQueue().isEmpty());
    } finally {
      event.clearReferenceCount("test cleanup");
      event.close();
    }
  }

  @Test
  public void testTabletSerializationFailureInvalidatesRetainedReferences() throws Exception {
    initializeSink();
    final InsertNode insertNode = mock(InsertNode.class);
    when(insertNode.getProgressIndex()).thenReturn(MinimumProgressIndex.INSTANCE);
    final IllegalStateException failure =
        new IllegalStateException("simulated serialization failure");
    when(insertNode.serializeToByteBuffer()).thenThrow(failure);
    final PipeInsertNodeTabletInsertionEvent event = new TestTabletEvent(insertNode);
    event.setCommitterKeyAndCommitId(new CommitterKey("backlog-sink-test", 1, 1, 0), 1);
    event.bindReplicationEventToken(backlog.newToken(Kind.TABLET));
    try {
      assertTrue(event.increaseReferenceCount("sink subtask"));
      addToTransferBuffer(event);
      final Method transfer =
          IoTConsensusV2AsyncSink.class.getDeclaredMethod(
              "transferInEventWithoutCheck", PipeInsertionEvent.class);
      transfer.setAccessible(true);
      try {
        transfer.invoke(sink, event);
        fail("The original serialization failure must propagate to the sink retry logic");
      } catch (InvocationTargetException expected) {
        assertSame(failure, expected.getCause());
      }
      // The existing handler reference is retained even though no handler was constructed.
      assertEquals(3, event.getReferenceCount());
      assertEquals(1, backlog.snapshot().getTabletCount());
      assertEquals(ConsensusPipeBacklog.ERROR, backlog.snapshot().getState());
    } finally {
      event.clearReferenceCount("test cleanup");
      event.close();
    }
  }

  @Test
  public void testDeletionSerializationFailureInvalidatesRetainedReferences() throws Exception {
    initializeSink();
    final AbstractDeleteDataNode deleteNode = mock(AbstractDeleteDataNode.class);
    when(deleteNode.getProgressIndex()).thenReturn(MinimumProgressIndex.INSTANCE);
    final IllegalStateException failure =
        new IllegalStateException("simulated serialization failure");
    when(deleteNode.serializeToByteBuffer()).thenThrow(failure);
    final PipeDeleteDataNodeEvent event = new PipeDeleteDataNodeEvent(deleteNode, false);
    event.setCommitterKeyAndCommitId(new CommitterKey("backlog-sink-test", 1, 1, 0), 1);
    event.bindReplicationEventToken(backlog.newToken(Kind.DELETION));
    assertTrue(event.increaseReferenceCount("sink subtask"));
    addToTransferBuffer(event);
    final Method transfer =
        IoTConsensusV2AsyncSink.class.getDeclaredMethod(
            "transferDeletion", PipeDeleteDataNodeEvent.class);
    transfer.setAccessible(true);
    try {
      transfer.invoke(sink, event);
      fail("The original serialization failure must propagate to the sink retry logic");
    } catch (InvocationTargetException expected) {
      assertSame(failure, expected.getCause());
    }
    assertEquals(3, event.getReferenceCount());
    assertEquals(1, backlog.snapshot().getDeletionCount());
    assertEquals(ConsensusPipeBacklog.ERROR, backlog.snapshot().getState());
  }

  @Test
  public void testNetworkFailureKeepsWorkValidUntilAcknowledged() throws Exception {
    initializeSink();
    final PipeDeleteDataNodeEvent event = new PipeDeleteDataNodeEvent(null, false);
    event.setCommitterKeyAndCommitId(new CommitterKey("backlog-sink-test", 1, 1, 0), 1);
    event.bindReplicationEventToken(backlog.newToken(Kind.DELETION));
    assertTrue(event.increaseReferenceCount("async handler"));
    addToTransferBuffer(event);
    final IoTConsensusV2DeleteEventHandler failedHandler =
        new IoTConsensusV2DeleteEventHandler(event, null, sink, metrics);
    failedHandler.onError(new IOException("simulated network failure"));

    assertEquals(1, backlog.snapshot().getDeletionCount());
    assertEquals(ConsensusPipeBacklog.VALID, backlog.snapshot().getState());
    assertSame(event, retryQueue().poll());
    assertTrue(event.increaseReferenceCount("retry handler"));
    assertTrue(event.decreaseReferenceCount("old retry ownership", false));
    final IoTConsensusV2DeleteEventHandler successfulHandler =
        new IoTConsensusV2DeleteEventHandler(event, null, sink, metrics);
    successfulHandler.onComplete(
        new TIoTConsensusV2TransferResp()
            .setStatus(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode())));

    assertTrue(event.isReleased());
    assertEquals(0, backlog.snapshot().getDeletionCount());
    assertEquals(ConsensusPipeBacklog.VALID, backlog.snapshot().getState());
  }

  private void initializeSink() throws Exception {
    backlog.markHistoricalReady();
    backlog.markRealtimeReady();
    final Field metricsField =
        IoTConsensusV2AsyncSink.class.getDeclaredField("iotConsensusV2SinkMetrics");
    metricsField.setAccessible(true);
    metricsField.set(sink, metrics);
  }

  private void addToTransferBuffer(final EnrichedEvent event) throws Exception {
    final Method add =
        IoTConsensusV2AsyncSink.class.getDeclaredMethod("addEvent2Buffer", EnrichedEvent.class);
    add.setAccessible(true);
    assertEquals(Boolean.TRUE, add.invoke(sink, event));
  }

  @SuppressWarnings("unchecked")
  private Queue<EnrichedEvent> retryQueue() throws Exception {
    final Field retryQueueField = IoTConsensusV2AsyncSink.class.getDeclaredField("retryEventQueue");
    retryQueueField.setAccessible(true);
    return (Queue<EnrichedEvent>) retryQueueField.get(sink);
  }

  private static final class TestTabletEvent extends PipeInsertNodeTabletInsertionEvent {
    private TestTabletEvent(final InsertNode insertNode) {
      super(false, "root.test", insertNode);
    }

    @Override
    protected void trackResource() {
      // Reference accounting remains real; this fixture needs no phantom-resource tracker.
    }
  }
}
