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
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class ReplicationBacklogLeaderBalancerTest {
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
        balancer.generateOptimalLeaderDistribution(
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
        balancer.generateOptimalLeaderDistribution(
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
            .generateOptimalLeaderDistribution(
                Collections.singletonMap("root.test", Collections.singletonList(group)),
                Collections.singletonMap(group, members),
                Collections.singletonMap(group, 1),
                nodes,
                Collections.singletonMap(group, regions),
                Collections.singletonMap(group, Collections.singleton(2)))
            .get(group));
  }
}
