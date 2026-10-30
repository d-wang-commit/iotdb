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

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.iot.IoTConsensus;
import org.apache.iotdb.consensus.iot.IoTConsensusReplicationSnapshot;
import org.apache.iotdb.consensus.iot.IoTConsensusServerImpl;
import org.apache.iotdb.consensus.pipe.IoTConsensusV2;
import org.apache.iotdb.consensus.pipe.IoTConsensusV2ServerImpl;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.db.pipe.consensus.metric.ConsensusPipeBacklog;
import org.apache.iotdb.mpp.rpc.thrift.TRegionReplicationBacklog;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogEdge;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogReport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

/** Samples source-side replication counters without reading WAL, files, or remote replicas. */
public final class ReplicationBacklogCollector {
  private static final ReplicationBacklogCollector INSTANCE = new ReplicationBacklogCollector();
  private static final byte VALID = 0;
  private static final byte RECOVERING = 1;
  private static final byte ERROR = 3;
  private static final byte IOT_V1 = 1;
  private static final byte IOT_V2 = 2;
  private final String sourceInstanceId = UUID.randomUUID().toString();
  private final AtomicLong sampleSequence = new AtomicLong();

  public static ReplicationBacklogCollector getInstance() {
    return INSTANCE;
  }

  public TReplicationBacklogReport collect(IConsensus consensus) {
    final long start = System.nanoTime();
    final long sequence = sampleSequence.incrementAndGet();
    final List<TRegionReplicationBacklog> groups = new ArrayList<>();
    try {
      collectGroups(consensus, groups);
    } catch (RuntimeException e) {
      // A concurrently stopping consensus service may fail even while enumerating groups.
      // An absent group is unknown to the CN; never replace it with a successful zero.
      groups.clear();
    }
    return new TReplicationBacklogReport(
        sourceInstanceId, sequence, Math.max(0, System.nanoTime() - start), groups);
  }

  private void collectGroups(IConsensus consensus, List<TRegionReplicationBacklog> groups) {
    if (consensus instanceof IoTConsensus || consensus instanceof IoTConsensusV2) {
      for (ConsensusGroupId group : consensus.getAllConsensusGroupIds()) {
        if (!(group instanceof DataRegionId)) {
          continue;
        }
        final byte protocol = consensus instanceof IoTConsensus ? IOT_V1 : IOT_V2;
        try {
          groups.add(
              protocol == IOT_V1
                  ? collectV1((IoTConsensus) consensus, group)
                  : collectV2((IoTConsensusV2) consensus, group));
        } catch (RuntimeException e) {
          // Telemetry failure must not suppress the node's health heartbeat or fabricate a zero.
          groups.add(
              new TRegionReplicationBacklog(
                  group.getId(), protocol, 0, Collections.emptyList(), ERROR));
        }
      }
    }
  }

  private TRegionReplicationBacklog collectV1(IoTConsensus consensus, ConsensusGroupId group) {
    final IoTConsensusServerImpl impl = consensus.getImpl(group);
    if (impl == null) {
      return new TRegionReplicationBacklog(
          group.getId(), IOT_V1, 0, Collections.emptyList(), RECOVERING);
    }
    final IoTConsensusReplicationSnapshot snapshot = impl.getReplicationBacklogSnapshot();
    final List<TReplicationBacklogEdge> edges = new ArrayList<>();
    final byte state = snapshot.isValid() && impl == consensus.getImpl(group) ? VALID : RECOVERING;
    for (Map.Entry<Integer, Long> entry : snapshot.getPendingRequests().entrySet()) {
      edges.add(
          new TReplicationBacklogEdge(
              entry.getKey(), snapshot.getGeneration(), entry.getValue(), 0, 0, 0, state));
    }
    return new TRegionReplicationBacklog(
        group.getId(), IOT_V1, snapshot.getGeneration(), edges, state);
  }

  private TRegionReplicationBacklog collectV2(IoTConsensusV2 consensus, ConsensusGroupId group) {
    final IoTConsensusV2ServerImpl impl = consensus.getImpl(group);
    if (impl == null) {
      return new TRegionReplicationBacklog(
          group.getId(), IOT_V2, 0, Collections.emptyList(), RECOVERING);
    }
    final long generation = impl.getReplicationBacklogGeneration();
    final List<Peer> peers = new ArrayList<>(impl.getPeers());
    final int sourceId = impl.getThisNodePeer().getNodeId();
    final List<TReplicationBacklogEdge> edges = new ArrayList<>();
    byte state = impl.isReplicationBacklogReady() ? VALID : RECOVERING;
    for (Peer peer : peers) {
      if (peer.getNodeId() == sourceId) {
        continue;
      }
      final ConsensusPipeBacklog.Snapshot snapshot =
          ConsensusPipeBacklog.snapshot(
              new ConsensusPipeName(group, sourceId, peer.getNodeId()).toString());
      if (snapshot == null) {
        edges.add(new TReplicationBacklogEdge(peer.getNodeId(), 0, 0, 0, 0, 0, RECOVERING));
        state = RECOVERING;
      } else {
        edges.add(
            new TReplicationBacklogEdge(
                peer.getNodeId(),
                snapshot.getGeneration(),
                0,
                snapshot.getTabletCount(),
                snapshot.getFileCount(),
                snapshot.getDeletionCount(),
                snapshot.getState()));
        if (snapshot.getState() != VALID) {
          state = snapshot.getState();
        }
      }
    }
    if (impl != consensus.getImpl(group)
        || !impl.isReplicationBacklogReady()
        || !peers.equals(impl.getPeers())) {
      state = RECOVERING;
    }
    return new TRegionReplicationBacklog(group.getId(), IOT_V2, generation, edges, state);
  }
}
