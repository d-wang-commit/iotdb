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
package org.apache.iotdb.confignode.manager.load.cache;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.conf.ReplicationBacklogConfig;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.CostFlowSelectionLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TRegionReplicationBacklog;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogEdge;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogReport;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Assume;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Opt-in measurement, deliberately not named *Test. Run a fresh fork for each N/K pair with
 * -Diotdb.backlog.benchmark=true -Diotdb.backlog.benchmark.nodes=N
 * -Diotdb.backlog.benchmark.replicas_per_node=K. Suggested pairs: 100/100, 100/1000, 1000/100,
 * 1000/1000. Add -Diotdb.backlog.benchmark.cfs=false to measure telemetry separately. Heap deltas
 * are observed retained-heap differences after GC, not object-layout guarantees. Wire sizes contain
 * the actual Thrift report body, excluding outer heartbeat, RPC framing and transport compression.
 */
public class ReplicationBacklogScaleBenchmark {
  private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

  @Test
  public void measure() throws Exception {
    Assume.assumeTrue(Boolean.getBoolean("iotdb.backlog.benchmark"));
    int nodes = Integer.getInteger("iotdb.backlog.benchmark.nodes", 100);
    int replicas = Integer.getInteger("iotdb.backlog.benchmark.replicas_per_node", 100);
    byte protocol = Byte.parseByte(System.getProperty("iotdb.backlog.benchmark.protocol", "2"));
    Assume.assumeTrue(nodes >= 3 && replicas > 0 && (protocol == 1 || protocol == 2));
    Fixture fixture = new Fixture(nodes, replicas);
    long baseHeap = retainedHeap();
    ReplicationBacklogCache cache = new ReplicationBacklogCache();
    Properties properties = new Properties();
    properties.setProperty(ReplicationBacklogConfig.PREFIX + "mode", "ENFORCE");
    for (String type : Arrays.asList("request", "tablet", "file", "deletion")) {
      properties.setProperty(ReplicationBacklogConfig.PREFIX + type + "_gap", "10");
    }
    cache.configure(ReplicationBacklogConfig.load(properties, ReplicationBacklogConfig.defaults()));
    long topologyStart = System.nanoTime();
    cache.updateTopology(fixture.locations, fixture.nodes, fixture.regions, 0);
    long topologyNanos = System.nanoTime() - topologyStart;
    Measurement measurement = populate(cache, fixture, protocol);
    long cacheHeap = retainedHeap() - baseHeap;
    long snapshotStart = System.nanoTime();
    Map<TConsensusGroupId, Set<Integer>> candidates = cache.eligibleCandidates(20 * SECOND);
    long snapshotNanos = System.nanoTime() - snapshotStart;
    assertEquals(fixture.locations.size(), candidates.size());
    long snapshotHeap = retainedHeap() - baseHeap - cacheHeap;
    System.out.printf(
        Locale.ROOT,
        "BACKLOG_SCALE N=%d requested_K=%d groups=%d actual_edges=%d protocol=%d cache_heap_bytes=%d candidate_snapshot_heap_bytes=%d topology_ms=%.3f ingest_ms_per_round=%.3f evaluate_ms_per_round=%.3f candidate_snapshot_ms=%.3f binary_body_bytes_per_round=%d compact_body_bytes_per_round=%d binary_serialize_ms=%.3f compact_serialize_ms=%.3f%n",
        nodes,
        replicas,
        fixture.locations.size(),
        fixture.locations.size() * 6L,
        protocol,
        cacheHeap,
        snapshotHeap,
        millis(topologyNanos),
        millis(measurement.ingestNanos) / 4,
        millis(measurement.evaluateNanos) / 4,
        millis(snapshotNanos),
        measurement.binaryBytes,
        measurement.compactBytes,
        millis(measurement.binaryNanos),
        millis(measurement.compactNanos));
    if (Boolean.parseBoolean(System.getProperty("iotdb.backlog.benchmark.cfs", "true"))) {
      measureCfs(fixture, Collections.emptyMap(), "baseline");
      measureCfs(fixture, candidates, "filtered");
    }
    // Keep the measured cache reachable until every retained-heap measurement has finished.
    assertEquals(3, cache.getDiagnostics(0, 20 * SECOND).size());
  }

  private static Measurement populate(ReplicationBacklogCache cache, Fixture fixture, byte protocol)
      throws Exception {
    Measurement result = new Measurement();
    TSerializer binary = new TSerializer(new TBinaryProtocol.Factory());
    TSerializer compact = new TSerializer(new TCompactProtocol.Factory());
    for (int sequence = 1; sequence <= 4; sequence++) {
      long now = sequence * 5 * SECOND;
      for (Map.Entry<Integer, List<TConsensusGroupId>> entry : fixture.outgoing.entrySet()) {
        TReplicationBacklogReport report = fixture.report(entry.getKey(), sequence, protocol);
        if (sequence == 1) {
          long start = System.nanoTime();
          result.binaryBytes += binary.serialize(report).length;
          result.binaryNanos += System.nanoTime() - start;
          start = System.nanoTime();
          result.compactBytes += compact.serialize(report).length;
          result.compactNanos += System.nanoTime() - start;
        }
        long start = System.nanoTime();
        cache.cacheReport(entry.getKey(), report, now, cache.getEpoch(), now);
        result.ingestNanos += System.nanoTime() - start;
      }
      long start = System.nanoTime();
      cache.evaluate(now);
      result.evaluateNanos += System.nanoTime() - start;
    }
    return result;
  }

  private static void measureCfs(
      Fixture fixture, Map<TConsensusGroupId, Set<Integer>> candidates, String label) {
    CostFlowSelectionLeaderBalancer cfs = new CostFlowSelectionLeaderBalancer();
    long start = System.nanoTime();
    Map<TConsensusGroupId, Integer> result =
        cfs.generateOptimalLeaderDistribution(
            fixture.databases,
            fixture.locations,
            fixture.leaders,
            fixture.nodes,
            fixture.regions,
            candidates);
    long elapsed = System.nanoTime() - start;
    assertEquals(fixture.locations.size(), result.size());
    assertEquals(fixture.locations.size(), cfs.getMaximumFlow());
    if (!candidates.isEmpty()) {
      result.forEach(
          (group, leader) -> assertEquals(candidates.get(group).iterator().next(), leader));
    }
    System.out.printf(
        Locale.ROOT,
        "BACKLOG_CFS variant=%s groups=%d elapsed_ms=%.3f flow=%d cost=%d%n",
        label,
        fixture.locations.size(),
        millis(elapsed),
        cfs.getMaximumFlow(),
        cfs.getMinimumCost());
  }

  private static long retainedHeap() throws InterruptedException {
    for (int i = 0; i < 3; i++) {
      System.gc();
      Thread.sleep(200);
    }
    Runtime runtime = Runtime.getRuntime();
    return runtime.totalMemory() - runtime.freeMemory();
  }

  private static double millis(long nanos) {
    return nanos / 1_000_000.0;
  }

  private static final class Measurement {
    private long ingestNanos;
    private long evaluateNanos;
    private long binaryBytes;
    private long compactBytes;
    private long binaryNanos;
    private long compactNanos;
  }

  private static final class Fixture {
    private final Map<TConsensusGroupId, Set<Integer>> locations = new HashMap<>();
    private final Map<Integer, NodeStatistics> nodes = new HashMap<>();
    private final Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regions = new HashMap<>();
    private final Map<TConsensusGroupId, Integer> leaders = new HashMap<>();
    private final Map<Integer, List<TConsensusGroupId>> outgoing = new HashMap<>();
    private final Map<String, List<TConsensusGroupId>> databases;

    private Fixture(int nodeCount, int replicasPerNode) {
      int groupCount = Math.toIntExact((long) nodeCount * replicasPerNode / 3);
      for (int node = 0; node < nodeCount; node++) {
        nodes.put(node, new NodeStatistics(NodeStatus.Running));
        outgoing.put(node, new ArrayList<>());
      }
      List<TConsensusGroupId> ids = new ArrayList<>(groupCount);
      for (int group = 0; group < groupCount; group++) {
        TConsensusGroupId id = new TConsensusGroupId(TConsensusGroupType.DataRegion, group);
        Set<Integer> members = new HashSet<>();
        Map<Integer, RegionStatistics> health = new HashMap<>();
        for (int replica = 0; replica < 3; replica++) {
          int node = (int) (((long) group * 3 + replica) % nodeCount);
          members.add(node);
          health.put(node, new RegionStatistics(RegionStatus.Running));
          outgoing.get(node).add(id);
        }
        locations.put(id, members);
        regions.put(id, health);
        leaders.put(id, members.stream().mapToInt(Integer::intValue).min().getAsInt());
        ids.add(id);
      }
      databases = Collections.singletonMap("benchmark", ids);
    }

    private TReplicationBacklogReport report(int source, int sequence, byte protocol) {
      List<TRegionReplicationBacklog> samples = new ArrayList<>();
      for (TConsensusGroupId id : outgoing.get(source)) {
        List<TReplicationBacklogEdge> edges = new ArrayList<>(2);
        int[] members = locations.get(id).stream().mapToInt(Integer::intValue).sorted().toArray();
        for (int target = 0; target < members.length; target++) {
          if (members[target] == source) {
            continue;
          }
          long weight = target == 0 ? 30 : target == 1 ? 5 : 20;
          edges.add(
              new TReplicationBacklogEdge()
                  .setTargetNodeId(members[target])
                  .setGeneration(1)
                  .setState((byte) 0)
                  .setRequestCount(protocol == 1 ? weight : 0)
                  .setTabletCount(protocol == 2 ? weight * 100 : 0)
                  .setFileCount(protocol == 2 ? weight : 0)
                  .setDeletionCount(protocol == 2 ? weight : 0));
        }
        samples.add(
            new TRegionReplicationBacklog()
                .setRegionId(id.getId())
                .setProtocol(protocol)
                .setGeneration(1)
                .setState((byte) 0)
                .setTargets(edges));
      }
      return new TReplicationBacklogReport()
          .setSourceInstanceId("benchmark-" + source)
          .setSampleSequence(sequence)
          .setSampleAgeNanos(0)
          .setGroups(samples);
    }
  }
}
