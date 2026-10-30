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

package org.apache.iotdb.db.pipe.event.realtime;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.commons.pipe.event.PipeReplicationEventToken;
import org.apache.iotdb.db.pipe.consensus.metric.ConsensusPipeBacklog.Token;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.epoch.TsFileEpoch;

import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@link PipeRealtimeEvent} is an event that decorates the {@link EnrichedEvent} with the
 * information of {@link TsFileEpoch} and schema info. It only exists in the realtime event
 * extractor.
 */
public class PipeRealtimeEvent extends EnrichedEvent {

  private final EnrichedEvent event;
  private final TsFileEpoch tsFileEpoch;

  private Map<IDeviceID, String[]> device2Measurements;

  // Only the raw assigner event owns these slots; each bound copy receives one token.
  private List<ReplicationTokenSlot> replicationTokenSlots;

  public PipeRealtimeEvent(
      final EnrichedEvent event,
      final TsFileEpoch tsFileEpoch,
      final Map<IDeviceID, String[]> device2Measurements) {
    this(
        event,
        tsFileEpoch,
        device2Measurements,
        null,
        null,
        null,
        null,
        null,
        null,
        true,
        Long.MIN_VALUE,
        Long.MAX_VALUE);
  }

  public PipeRealtimeEvent(
      final EnrichedEvent event,
      final TsFileEpoch tsFileEpoch,
      final Map<IDeviceID, String[]> device2Measurements,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final String userId,
      final String userName,
      final String cliHostname,
      final boolean skipIfNoPrivileges,
      final long startTime,
      final long endTime) {
    // PipeTaskMeta is used to report the progress of the event, the PipeRealtimeEvent
    // is only used in the realtime event extractor, which does not need to report the progress
    // of the event, so the pipeTaskMeta is always null.
    super(
        event != null ? event.getPipeName() : null,
        event != null ? event.getCreationTime() : 0,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        userId,
        userName,
        cliHostname,
        skipIfNoPrivileges,
        startTime,
        endTime);

    this.event = event;
    this.tsFileEpoch = tsFileEpoch;
    this.device2Measurements = device2Measurements;
  }

  public EnrichedEvent getEvent() {
    return event;
  }

  public TsFileEpoch getTsFileEpoch() {
    return tsFileEpoch;
  }

  public Map<IDeviceID, String[]> getSchemaInfo() {
    return device2Measurements;
  }

  public void gcSchemaInfo() {
    device2Measurements = null;
  }

  public synchronized void addReplicationEventToken(final long sourceId, final Token token) {
    if (token == null) {
      return;
    }
    if (replicationTokenSlots == null) {
      replicationTokenSlots = new ArrayList<>(2);
    }
    replicationTokenSlots.add(new ReplicationTokenSlot(sourceId, token));
  }

  public synchronized Token takeReplicationEventToken(final long sourceId) {
    if (replicationTokenSlots != null) {
      for (final ReplicationTokenSlot slot : replicationTokenSlots) {
        if (slot.sourceId == sourceId && slot.token != null) {
          final Token token = slot.token;
          slot.token = null;
          return token;
        }
      }
    }
    return null;
  }

  /** Finishes unmatched directions after assignment without retaining the raw event. */
  public synchronized void releaseUnassignedReplicationEventTokens() {
    if (replicationTokenSlots != null) {
      for (final ReplicationTokenSlot slot : replicationTokenSlots) {
        if (slot.token != null) {
          slot.token.finish();
        }
      }
      replicationTokenSlots = null;
    }
  }

  public synchronized void invalidateUnassignedReplicationEventTokens() {
    if (replicationTokenSlots != null) {
      for (final ReplicationTokenSlot slot : replicationTokenSlots) {
        if (slot.token != null) {
          slot.token.invalidate();
        }
      }
    }
  }

  @Override
  public void bindReplicationEventToken(final PipeReplicationEventToken token) {
    event.bindReplicationEventToken(token);
  }

  private static class ReplicationTokenSlot {

    private final long sourceId;
    private Token token;

    private ReplicationTokenSlot(final long sourceId, final Token token) {
      this.sourceId = sourceId;
      this.token = token;
    }
  }

  public boolean maySourceOnlyUseTablets(final PipeRealtimeDataRegionSource extractor) {
    final TsFileEpoch.State state = tsFileEpoch.getState(extractor);
    return state.equals(TsFileEpoch.State.EMPTY) || state.equals(TsFileEpoch.State.USING_TABLET);
  }

  @Override
  public boolean increaseReferenceCount(final String holderMessage) {
    // This method must be overridden, otherwise during the real-time data extraction stage, the
    // current PipeRealtimeEvent rather than the member variable EnrichedEvent will increase
    // the reference count, resulting in errors in the reference count of the EnrichedEvent
    // contained in this PipeRealtimeEvent during the processor and connector stages.
    return event.increaseReferenceCount(holderMessage);
  }

  @Override
  public boolean internallyIncreaseResourceReferenceCount(final String holderMessage) {
    return event.internallyIncreaseResourceReferenceCount(holderMessage);
  }

  @Override
  public boolean decreaseReferenceCount(final String holderMessage, final boolean shouldReport) {
    // This method must be overridden, otherwise during the real-time data extraction stage, the
    // current PipeRealtimeEvent rather than the member variable EnrichedEvent will decrease
    // the reference count, resulting in errors in the reference count of the EnrichedEvent
    // contained in this PipeRealtimeEvent during the processor and connector stages.
    return event.decreaseReferenceCount(holderMessage, shouldReport);
  }

  @Override
  public boolean clearReferenceCount(final String holderMessage) {
    // This method must be overridden, otherwise during the real-time data extraction stage, the
    // current PipeRealtimeEvent rather than the member variable EnrichedEvent will clear
    // the reference count, resulting in errors in the reference count of the EnrichedEvent
    // contained in this PipeRealtimeEvent during the processor and connector stages.
    return event.clearReferenceCount(holderMessage);
  }

  @Override
  public boolean internallyDecreaseResourceReferenceCount(final String holderMessage) {
    return event.internallyDecreaseResourceReferenceCount(holderMessage);
  }

  @Override
  public void bindProgressIndex(final ProgressIndex progressIndex) {
    event.bindProgressIndex(progressIndex);
  }

  @Override
  public ProgressIndex getProgressIndex() {
    return event.getProgressIndex();
  }

  @Override
  public void skipReportOnCommit() {
    event.skipReportOnCommit();
  }

  @Override
  public boolean isShouldReportOnCommit() {
    return event.isShouldReportOnCommit();
  }

  @Override
  public void skipParsingPattern() {
    event.skipParsingPattern();
  }

  @Override
  public void skipParsingTime() {
    event.skipParsingTime();
  }

  @Override
  public boolean shouldParseTime() {
    return event.shouldParseTime();
  }

  @Override
  public boolean shouldParsePattern() {
    return event.shouldParsePattern();
  }

  @Override
  public boolean mayEventTimeOverlappedWithTimeRange() {
    return event.mayEventTimeOverlappedWithTimeRange();
  }

  @Override
  public boolean mayEventPathsOverlappedWithPattern() {
    return event.mayEventPathsOverlappedWithPattern();
  }

  @Override
  public PipeRealtimeEvent shallowCopySelfAndBindPipeTaskMetaForProgressReport(
      final String pipeName,
      final long creationTime,
      final PipeTaskMeta pipeTaskMeta,
      final TreePattern treePattern,
      final TablePattern tablePattern,
      final String userId,
      final String userName,
      final String cliHostname,
      final boolean skipIfNoPrivileges,
      final long startTime,
      final long endTime) {
    return new PipeRealtimeEvent(
        event.shallowCopySelfAndBindPipeTaskMetaForProgressReport(
            pipeName,
            creationTime,
            pipeTaskMeta,
            treePattern,
            tablePattern,
            userId,
            userName,
            cliHostname,
            skipIfNoPrivileges,
            startTime,
            endTime),
        this.tsFileEpoch,
        // device2Measurements is not used anymore, so it is not copied.
        // If null is not passed, the field will not be GCed and may cause OOM.
        null,
        pipeTaskMeta,
        treePattern,
        tablePattern,
        userId,
        userName,
        cliHostname,
        skipIfNoPrivileges,
        startTime,
        endTime);
  }

  @Override
  public boolean isGeneratedByPipe() {
    return event.isGeneratedByPipe();
  }

  @Override
  public String toString() {
    return "PipeRealtimeEvent{"
        + "event="
        + event
        + ", tsFileEpoch="
        + tsFileEpoch
        + ", device2Measurements="
        + device2Measurements
        + '}';
  }
}
