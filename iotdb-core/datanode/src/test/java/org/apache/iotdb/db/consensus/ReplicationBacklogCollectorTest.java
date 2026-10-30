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

package org.apache.iotdb.db.consensus;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.consensus.SchemaRegionId;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusReplicationSnapshot;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.pipe.IoTConsensusV2;
import org.apache.iotdb.consensus.pipe.IoTConsensusV2ServerImpl;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.db.pipe.consensus.metric.ConsensusPipeBacklog;
import org.apache.iotdb.db.pipe.consensus.metric.ConsensusPipeBacklog.Kind;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatReq;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;
import org.apache.iotdb.mpp.rpc.thrift.TRegionReplicationBacklog;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogEdge;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogReport;

import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.After;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicationBacklogCollectorTest {

  private static final AtomicInteger REGION_IDS = new AtomicInteger(710000);
  private final DataRegionId region = new DataRegionId(REGION_IDS.incrementAndGet());
  private final ReplicationBacklogCollector collector = new ReplicationBacklogCollector();
  private final List<ConsensusPipeBacklog> owners = new ArrayList<>();

  @After
  public void closeOwners() {
    owners.forEach(ConsensusPipeBacklog::close);
  }

  @Test
  public void testV1ReportsEveryTargetAndSkipsSchemaRegions() {
    final IoTConsensus consensus = mock(IoTConsensus.class);
    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    when(consensus.getAllConsensusGroupIds())
        .thenReturn(Arrays.asList(region, new SchemaRegionId(region.getId())));
    when(consensus.getImpl(region)).thenReturn(server);
    when(server.getReplicationBacklogSnapshot())
        .thenReturn(new IoTConsensusReplicationSnapshot(91, true, Map.of(2, 5L, 3, 10L)));

    final TRegionReplicationBacklog group = onlyGroup(collector.collect(consensus));
    assertEquals(region.getId(), group.getRegionId());
    assertEquals(1, group.getProtocol());
    assertEquals(91, group.getGeneration());
    assertEquals(0, group.getState());
    assertEquals(2, group.getTargets().size());
    assertEquals(5, edge(group, 2).getRequestCount());
    assertEquals(10, edge(group, 3).getRequestCount());
    for (TReplicationBacklogEdge target : group.getTargets()) {
      assertEquals(91, target.getGeneration());
      assertEquals(0, target.getState());
      assertEquals(0, target.getTabletCount());
      assertEquals(0, target.getFileCount());
      assertEquals(0, target.getDeletionCount());
    }
  }

  @Test
  public void testMissingOrRecoveringV1ServerIsExplicitlyInvalid() {
    final IoTConsensus consensus = mock(IoTConsensus.class);
    when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.singletonList(region));
    final TRegionReplicationBacklog absent = onlyGroup(collector.collect(consensus));
    assertEquals(1, absent.getState());
    assertTrue(absent.getTargets().isEmpty());

    final IoTConsensusServerImpl server = mock(IoTConsensusServerImpl.class);
    when(consensus.getImpl(region)).thenReturn(server);
    when(server.getReplicationBacklogSnapshot())
        .thenReturn(new IoTConsensusReplicationSnapshot(92, false, Map.of(2, 0L, 3, 10L)));
    final TRegionReplicationBacklog recovering = onlyGroup(collector.collect(consensus));
    assertEquals(1, recovering.getState());
    assertEquals(1, edge(recovering, 2).getState());
    assertEquals(0, edge(recovering, 2).getRequestCount());
    assertEquals(1, edge(recovering, 3).getState());
    assertEquals(10, edge(recovering, 3).getRequestCount());
  }

  @Test
  public void testReplacedV1ServerCannotPublishAnOldValidSample() {
    final IoTConsensus consensus = mock(IoTConsensus.class);
    final IoTConsensusServerImpl oldServer = mock(IoTConsensusServerImpl.class);
    final IoTConsensusServerImpl replacement = mock(IoTConsensusServerImpl.class);
    when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.singletonList(region));
    when(consensus.getImpl(region)).thenReturn(oldServer, replacement);
    when(oldServer.getReplicationBacklogSnapshot())
        .thenReturn(new IoTConsensusReplicationSnapshot(93, true, Map.of(2, 0L)));

    final TRegionReplicationBacklog group = onlyGroup(collector.collect(consensus));
    assertEquals(1, group.getState());
    assertEquals(1, edge(group, 2).getState());
  }

  @Test
  public void testFailureInOneRegionDoesNotFabricateZeroOrDiscardOtherRegions() {
    final DataRegionId healthyRegion = new DataRegionId(REGION_IDS.incrementAndGet());
    final IoTConsensus consensus = mock(IoTConsensus.class);
    final IoTConsensusServerImpl failed = mock(IoTConsensusServerImpl.class);
    final IoTConsensusServerImpl healthy = mock(IoTConsensusServerImpl.class);
    when(consensus.getAllConsensusGroupIds()).thenReturn(Arrays.asList(region, healthyRegion));
    when(consensus.getImpl(region)).thenReturn(failed);
    when(consensus.getImpl(healthyRegion)).thenReturn(healthy);
    when(failed.getReplicationBacklogSnapshot()).thenThrow(new IllegalStateException());
    when(healthy.getReplicationBacklogSnapshot())
        .thenReturn(new IoTConsensusReplicationSnapshot(94, true, Map.of(2, 6L)));

    final TReplicationBacklogReport report = collector.collect(consensus);
    assertEquals(2, report.getGroups().size());
    final TRegionReplicationBacklog invalid = report.getGroups().get(0);
    assertEquals(region.getId(), invalid.getRegionId());
    assertEquals(3, invalid.getState());
    assertTrue(invalid.getTargets().isEmpty());
    final TRegionReplicationBacklog valid = report.getGroups().get(1);
    assertEquals(healthyRegion.getId(), valid.getRegionId());
    assertEquals(0, valid.getState());
    assertEquals(6, edge(valid, 2).getRequestCount());
  }

  @Test
  public void testV2MissingDirectionIsUnknownUntilItsCounterIsReady() {
    final IoTConsensusV2 consensus = v2Consensus();
    final ConsensusPipeBacklog toDn2 = owner(2);
    toDn2.addPending(Kind.TABLET, 4);
    toDn2.addPending(Kind.FILE, 2);
    toDn2.addPending(Kind.DELETION, 1);
    ready(toDn2);

    final TRegionReplicationBacklog incomplete = onlyGroup(collector.collect(consensus));
    assertEquals(2, incomplete.getProtocol());
    assertEquals(123, incomplete.getGeneration());
    assertEquals(1, incomplete.getState());
    assertEquals(2, incomplete.getTargets().size());
    final TReplicationBacklogEdge known = edge(incomplete, 2);
    assertEquals(0, known.getState());
    assertEquals(0, known.getRequestCount());
    assertEquals(4, known.getTabletCount());
    assertEquals(2, known.getFileCount());
    assertEquals(1, known.getDeletionCount());
    assertEquals(toDn2.getGeneration(), known.getGeneration());
    assertEquals(1, edge(incomplete, 3).getState());
    assertEquals(0, edge(incomplete, 3).getTabletCount());

    final ConsensusPipeBacklog toDn3 = owner(3);
    toDn3.markHistoricalReady();
    assertEquals(1, onlyGroup(collector.collect(consensus)).getState());
    toDn3.markRealtimeReady();
    final TRegionReplicationBacklog complete = onlyGroup(collector.collect(consensus));
    assertEquals(0, complete.getState());
    assertEquals(0, edge(complete, 3).getState());
    assertEquals(0, edge(complete, 3).getTabletCount());
    assertEquals(0, edge(complete, 3).getFileCount());
    assertEquals(0, edge(complete, 3).getDeletionCount());
  }

  @Test
  public void testV2CounterErrorAndTaskRebuildRemainVisible() {
    final IoTConsensusV2 consensus = v2Consensus();
    final ConsensusPipeBacklog oldOwner = owner(2);
    final ConsensusPipeBacklog.Token oldToken = oldOwner.newToken(Kind.TABLET);
    ready(oldOwner);
    ready(owner(3));
    oldOwner.invalidate();

    final TRegionReplicationBacklog invalid = onlyGroup(collector.collect(consensus));
    assertEquals(3, invalid.getState());
    assertEquals(3, edge(invalid, 2).getState());
    assertEquals(1, edge(invalid, 2).getTabletCount());

    final ConsensusPipeBacklog replacement = owner(2);
    replacement.addPending(Kind.FILE, 7);
    oldToken.finish();
    assertEquals(1, onlyGroup(collector.collect(consensus)).getState());
    ready(replacement);
    final TRegionReplicationBacklog rebuilt = onlyGroup(collector.collect(consensus));
    assertEquals(0, rebuilt.getState());
    assertNotEquals(oldOwner.getGeneration(), edge(rebuilt, 2).getGeneration());
    assertEquals(replacement.getGeneration(), edge(rebuilt, 2).getGeneration());
    assertEquals(0, edge(rebuilt, 2).getTabletCount());
    assertEquals(7, edge(rebuilt, 2).getFileCount());
  }

  @Test
  public void testSourceIdentitySequenceAndEnumerationFailure() {
    final IoTConsensus consensus = mock(IoTConsensus.class);
    when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.emptyList());
    final TReplicationBacklogReport first = collector.collect(consensus);
    when(consensus.getAllConsensusGroupIds()).thenThrow(new IllegalStateException());
    final TReplicationBacklogReport failed = collector.collect(consensus);
    final TReplicationBacklogReport newProcess =
        new ReplicationBacklogCollector().collect(consensus);

    assertFalse(first.getSourceInstanceId().isEmpty());
    assertEquals(first.getSourceInstanceId(), failed.getSourceInstanceId());
    assertEquals(first.getSampleSequence() + 1, failed.getSampleSequence());
    assertNotEquals(first.getSourceInstanceId(), newProcess.getSourceInstanceId());
    assertTrue(first.getSampleAgeNanos() >= 0);
    assertTrue(failed.getSampleAgeNanos() >= 0);
    assertTrue(failed.getGroups().isEmpty());

    final IConsensus unsupported = mock(IConsensus.class);
    assertTrue(collector.collect(unsupported).getGroups().isEmpty());
    verify(unsupported, never()).getAllConsensusGroupIds();
  }

  @Test
  public void testOptionalHeartbeatFieldsAndExplicitInvalidReportRoundTrip() throws Exception {
    final TDataNodeHeartbeatReq request =
        new TDataNodeHeartbeatReq()
            .setHeartbeatTimestamp(123)
            .setNeedJudgeLeader(false)
            .setNeedSamplingLoad(false)
            .setTimeSeriesQuotaRemain(100)
            .setLogicalClock(7);
    final TSerializer serializer = new TSerializer();
    final TDeserializer deserializer = new TDeserializer();
    final TDataNodeHeartbeatReq decodedRequest = new TDataNodeHeartbeatReq();
    deserializer.deserialize(decodedRequest, serializer.serialize(request));
    assertFalse(decodedRequest.isSetNeedReplicationBacklog());
    assertFalse(decodedRequest.isNeedReplicationBacklog());
    request.setNeedReplicationBacklog(true);
    deserializer.deserialize(decodedRequest, serializer.serialize(request));
    assertTrue(decodedRequest.isSetNeedReplicationBacklog());
    assertTrue(decodedRequest.isNeedReplicationBacklog());

    final TDataNodeHeartbeatResp response = new TDataNodeHeartbeatResp(123, "Running");
    final TDataNodeHeartbeatResp decodedResponse = new TDataNodeHeartbeatResp();
    deserializer.deserialize(decodedResponse, serializer.serialize(response));
    assertFalse(decodedResponse.isSetReplicationBacklogReport());

    final IoTConsensus consensus = mock(IoTConsensus.class);
    when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.singletonList(region));
    response.setReplicationBacklogReport(collector.collect(consensus));
    deserializer.deserialize(decodedResponse, serializer.serialize(response));
    assertTrue(decodedResponse.isSetReplicationBacklogReport());
    assertEquals(
        response.getReplicationBacklogReport(), decodedResponse.getReplicationBacklogReport());
    assertEquals(1, onlyGroup(decodedResponse.getReplicationBacklogReport()).getState());
  }

  private IoTConsensusV2 v2Consensus() {
    final IoTConsensusV2 consensus = mock(IoTConsensusV2.class);
    final IoTConsensusV2ServerImpl server = mock(IoTConsensusV2ServerImpl.class);
    final Peer self = new Peer(region, 1, new TEndPoint("127.0.0.1", 9001));
    when(consensus.getAllConsensusGroupIds()).thenReturn(Collections.singletonList(region));
    when(consensus.getImpl(region)).thenReturn(server);
    when(server.getThisNodePeer()).thenReturn(self);
    when(server.getPeers())
        .thenReturn(
            Arrays.asList(
                self,
                new Peer(region, 2, new TEndPoint("127.0.0.1", 9002)),
                new Peer(region, 3, new TEndPoint("127.0.0.1", 9003))));
    when(server.getReplicationBacklogGeneration()).thenReturn(123L);
    when(server.isReplicationBacklogReady()).thenReturn(true);
    return consensus;
  }

  private ConsensusPipeBacklog owner(final int target) {
    final ConsensusPipeBacklog owner =
        ConsensusPipeBacklog.create(new ConsensusPipeName(region, 1, target).toString(), 1, target);
    owners.add(owner);
    return owner;
  }

  private static void ready(final ConsensusPipeBacklog owner) {
    owner.markHistoricalReady();
    owner.markRealtimeReady();
  }

  private static TRegionReplicationBacklog onlyGroup(final TReplicationBacklogReport report) {
    assertEquals(1, report.getGroups().size());
    return report.getGroups().get(0);
  }

  private static TReplicationBacklogEdge edge(
      final TRegionReplicationBacklog group, final int target) {
    final TReplicationBacklogEdge result =
        group.getTargets().stream()
            .filter(edge -> edge.getTargetNodeId() == target)
            .findFirst()
            .orElse(null);
    assertNotNull(result);
    return result;
  }
}
