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
package org.apache.iotdb.confignode.manager.load.balancer.router.leader;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.manager.load.cache.ReplicationBacklogCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TRegionReplicationBacklog;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogEdge;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogReport;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReplicationBacklogLeaderBalancerTest {
  @Test
  public void twoRecoveringNodesDoNotReceiveLeadersBecauseTheirPeersLostReports() {
    ReplicationBacklogCache cache = new ReplicationBacklogCache();
    Map<TConsensusGroupId, Set<Integer>> locations = new HashMap<>();
    Map<TConsensusGroupId, Integer> leaders = new HashMap<>();
    Map<Integer, NodeStatistics> nodes = new HashMap<>();
    Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regions = new HashMap<>();
    for (int node = 1; node <= 8; node++) {
      nodes.put(node, new NodeStatistics(NodeStatus.Running));
    }
    for (int index = 0; index < 128; index++) {
      TConsensusGroupId group = new TConsensusGroupId(TConsensusGroupType.DataRegion, index);
      Set<Integer> members = Set.of(index % 6 + 1, (index + 1) % 6 + 1, index % 2 + 7);
      locations.put(group, members);
      leaders.put(group, index % 6 + 1);
      Map<Integer, RegionStatistics> replicas = new HashMap<>();
      for (int member : members) {
        replicas.put(member, new RegionStatistics(RegionStatus.Running));
      }
      regions.put(group, replicas);
    }
    cache.updateTopology(locations, 0);
    cache.evaluate(
        CostFlowSelectionLeaderBalancer.getRunningCandidates(locations, nodes, regions), 0);
    for (int round = 1; round <= 5; round++) {
      reportRound(cache, locations, round, 8, 0);
    }
    nodes.put(7, new NodeStatistics(NodeStatus.Unknown));
    nodes.put(8, new NodeStatistics(NodeStatus.Unknown));
    cache.evaluate(
        CostFlowSelectionLeaderBalancer.getRunningCandidates(locations, nodes, regions),
        TimeUnit.SECONDS.toNanos(26));
    for (int round = 6; round <= 11; round++) {
      reportRound(cache, locations, round, 6, 100);
    }
    for (TConsensusGroupId group : locations.keySet()) {
      assertFalse(cache.getDiagnostics(group.getId(), 0).get(leaders.get(group)).isHeavy());
    }
    nodes.put(7, new NodeStatistics(NodeStatus.Running));
    nodes.put(8, new NodeStatistics(NodeStatus.Running));
    cache.evaluate(
        CostFlowSelectionLeaderBalancer.getRunningCandidates(locations, nodes, regions),
        TimeUnit.SECONDS.toNanos(56));

    CostFlowSelectionLeaderBalancer balancer = new CostFlowSelectionLeaderBalancer();
    Map<String, List<TConsensusGroupId>> databases =
        Collections.singletonMap("root.test", new ArrayList<>(locations.keySet()));
    Map<TConsensusGroupId, Integer> unrestricted =
        balancer.generateOptimalLeaderDistribution(databases, locations, leaders, nodes, regions);
    assertEquals(16, unrestricted.values().stream().filter(node -> node == 7).count());
    assertEquals(16, unrestricted.values().stream().filter(node -> node == 8).count());
    Map<TConsensusGroupId, Integer> filtered =
        balancer.generateOptimalLeaderDistributionWithBacklog(
            databases, locations, leaders, nodes, regions, cache.eligibleCandidates(0));
    assertEquals(128, filtered.size());
    assertTrue(filtered.values().stream().allMatch(node -> node <= 6));
    for (int round = 12; round <= 16; round++) {
      reportRound(cache, locations, round, 8, 100);
      for (Map.Entry<TConsensusGroupId, Set<Integer>> entry : locations.entrySet()) {
        Set<Integer> preferred = cache.eligibleCandidates(0).get(entry.getKey());
        assertEquals(2, preferred.size());
        assertTrue(preferred.stream().allMatch(node -> node <= 6));
      }
    }
  }

  private static void reportRound(
      ReplicationBacklogCache cache,
      Map<TConsensusGroupId, Set<Integer>> locations,
      int round,
      int reportingNodes,
      long recoveringBacklog) {
    long now = TimeUnit.SECONDS.toNanos(round * 5L);
    for (int source = 1; source <= reportingNodes; source++) {
      List<TRegionReplicationBacklog> groups = new ArrayList<>();
      for (Map.Entry<TConsensusGroupId, Set<Integer>> entry : locations.entrySet()) {
        if (!entry.getValue().contains(source)) {
          continue;
        }
        List<TReplicationBacklogEdge> edges = new ArrayList<>();
        for (int target : entry.getValue()) {
          if (target != source) {
            edges.add(
                new TReplicationBacklogEdge(
                    target, 1, target > 6 ? recoveringBacklog : 0, 0, 0, 0, (byte) 0));
          }
        }
        groups.add(
            new TRegionReplicationBacklog(entry.getKey().getId(), (byte) 1, 1, edges, (byte) 0));
      }
      cache.cacheReport(
          source,
          new TReplicationBacklogReport("boot-" + source, round, 0, groups),
          now,
          cache.getEpoch(),
          now);
    }
    cache.advanceSamplingRound(round);
  }

  @Test
  public void restrictsEdgesButPreservesTopologyAndUnfilteredCost() {
    TConsensusGroupId group = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    Set<Integer> members = new HashSet<>(Arrays.asList(1, 2, 3));
    Map<Integer, NodeStatistics> nodes = new HashMap<>();
    Map<Integer, RegionStatistics> regions = new HashMap<>();
    for (int node : members) {
      nodes.put(node, new NodeStatistics(NodeStatus.Running));
      regions.put(node, new RegionStatistics(RegionStatus.Running));
    }
    CostFlowSelectionLeaderBalancer balancer = new CostFlowSelectionLeaderBalancer();
    Map<TConsensusGroupId, Integer> original =
        balancer.generateOptimalLeaderDistribution(
            Collections.singletonMap("root.test", Collections.singletonList(group)),
            Collections.singletonMap(group, members),
            Collections.singletonMap(group, 1),
            nodes,
            Collections.singletonMap(group, regions));
    int originalCost = balancer.getMinimumCost();
    assertEquals(Integer.valueOf(1), original.get(group));
    Map<TConsensusGroupId, Integer> filtered =
        balancer.generateOptimalLeaderDistributionWithBacklog(
            Collections.singletonMap("root.test", Collections.singletonList(group)),
            Collections.singletonMap(group, members),
            Collections.singletonMap(group, 1),
            nodes,
            Collections.singletonMap(group, regions),
            Collections.singletonMap(group, Collections.singleton(2)));
    assertEquals(Integer.valueOf(2), filtered.get(group));
    assertEquals(3, members.size());
    assertEquals(originalCost + 1, balancer.getMinimumCost());
    assertEquals(
        original,
        balancer.generateOptimalLeaderDistributionWithBacklog(
            Collections.singletonMap("root.test", Collections.singletonList(group)),
            Collections.singletonMap(group, members),
            Collections.singletonMap(group, 1),
            nodes,
            Collections.singletonMap(group, regions),
            Collections.emptyMap()));
    assertEquals(originalCost, balancer.getMinimumCost());

    // A stale restriction must never remove the last actually available member.
    nodes.put(2, new NodeStatistics(NodeStatus.Unknown));
    assertEquals(
        Integer.valueOf(1),
        balancer
            .generateOptimalLeaderDistributionWithBacklog(
                Collections.singletonMap("root.test", Collections.singletonList(group)),
                Collections.singletonMap(group, members),
                Collections.singletonMap(group, 1),
                nodes,
                Collections.singletonMap(group, regions),
                Collections.singletonMap(group, Collections.singleton(2)))
            .get(group));
  }

  @Test
  public void runningCandidatesRequireMembershipAndBothHealthChecks() {
    TConsensusGroupId group = new TConsensusGroupId(TConsensusGroupType.DataRegion, 1);
    Set<Integer> members = Set.of(1, 2, 3, 4, 5);
    Map<Integer, NodeStatistics> nodes = new HashMap<>();
    Map<Integer, RegionStatistics> regions = new HashMap<>();
    for (int node : new int[] {1, 2, 3, 4, 6}) {
      nodes.put(node, new NodeStatistics(NodeStatus.Running));
      regions.put(node, new RegionStatistics(RegionStatus.Running));
    }
    nodes.put(2, new NodeStatistics(NodeStatus.Unknown));
    regions.put(3, new RegionStatistics(RegionStatus.Unknown));
    regions.remove(4);
    assertEquals(
        Collections.singletonMap(group, Set.of(1)),
        CostFlowSelectionLeaderBalancer.getRunningCandidates(
            Collections.singletonMap(group, members),
            nodes,
            Collections.singletonMap(group, regions)));

    // Neither a nonmember nor a non-Running preference can remove the actual Running candidate.
    CostFlowSelectionLeaderBalancer balancer = new CostFlowSelectionLeaderBalancer();
    assertEquals(
        Integer.valueOf(1),
        balancer
            .generateOptimalLeaderDistributionWithBacklog(
                Collections.singletonMap("root.test", Collections.singletonList(group)),
                Collections.singletonMap(group, members),
                Collections.singletonMap(group, 2),
                nodes,
                Collections.singletonMap(group, regions),
                Collections.singletonMap(group, Set.of(2, 3, 4, 5, 6)))
            .get(group));
    assertEquals(5, members.size());
  }
}
