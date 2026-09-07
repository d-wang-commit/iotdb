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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.NodeType;
import org.apache.iotdb.confignode.manager.load.cache.node.ConfigNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.DataNodeHeartbeatCache;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatResp;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class NodeCacheTest {

  @Test
  public void updateStatisticsTest() {
    // Test DataNode heartbeat cache
    DataNodeHeartbeatCache dataNodeHeartbeatCache = new DataNodeHeartbeatCache(1);
    long currentTime = System.nanoTime();
    dataNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(currentTime, NodeStatus.Running));
    dataNodeHeartbeatCache.updateCurrentStatistics(false);
    Assert.assertEquals(NodeStatus.Running, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertEquals(0, dataNodeHeartbeatCache.getLoadScore());

    // Test ConfigNode heartbeat cache
    ConfigNodeHeartbeatCache configNodeHeartbeatCache = new ConfigNodeHeartbeatCache(2);
    currentTime = System.nanoTime();
    configNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(currentTime, NodeStatus.Running));
    configNodeHeartbeatCache.updateCurrentStatistics(false);
    Assert.assertEquals(NodeStatus.Running, configNodeHeartbeatCache.getNodeStatus());
    Assert.assertEquals(0, configNodeHeartbeatCache.getLoadScore());
  }

  @Test
  public void stoppedStatusStickyAndRevivalTest() {
    // Test DataNode heartbeat cache
    DataNodeHeartbeatCache dataNodeHeartbeatCache = new DataNodeHeartbeatCache(1);
    // A fresh Stopped report (shutdown hook) marks the node as Stopped without a status reason
    dataNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Stopped));
    dataNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Stopped, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertNull(dataNodeHeartbeatCache.getNodeStatusReason());
    Assert.assertEquals(Long.MAX_VALUE, dataNodeHeartbeatCache.getLoadScore());

    // A forced Unknown update (e.g. heartbeat connection failure) must not refresh Stopped
    dataNodeHeartbeatCache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Unknown));
    dataNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Stopped, dataNodeHeartbeatCache.getNodeStatus());

    // Periodic updates must not refresh Stopped to Unknown either
    dataNodeHeartbeatCache.updateCurrentStatistics(false);
    Assert.assertEquals(NodeStatus.Stopped, dataNodeHeartbeatCache.getNodeStatus());

    // A live heartbeat (e.g. the node restarted) revives the node
    dataNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Running));
    dataNodeHeartbeatCache.updateCurrentStatistics(false);
    Assert.assertEquals(NodeStatus.Running, dataNodeHeartbeatCache.getNodeStatus());

    // Removing has the highest priority: it refreshes Stopped
    dataNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Stopped));
    dataNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Stopped, dataNodeHeartbeatCache.getNodeStatus());
    dataNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Removing));
    dataNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Removing, dataNodeHeartbeatCache.getNodeStatus());
    // And the Stopped report must not refresh Removing
    dataNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Stopped));
    dataNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Removing, dataNodeHeartbeatCache.getNodeStatus());
    // A forced Unknown update (e.g. heartbeat connection failure) must not refresh Removing
    // either
    dataNodeHeartbeatCache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Unknown));
    dataNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Removing, dataNodeHeartbeatCache.getNodeStatus());
    // An explicit management status change (e.g. rollback to Running) still applies
    dataNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Running));
    dataNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Running, dataNodeHeartbeatCache.getNodeStatus());

    // Test ConfigNode heartbeat cache
    ConfigNodeHeartbeatCache configNodeHeartbeatCache = new ConfigNodeHeartbeatCache(2);
    configNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Stopped));
    configNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Stopped, configNodeHeartbeatCache.getNodeStatus());

    // A forced Unknown update (e.g. heartbeat connection failure) must not refresh Stopped
    configNodeHeartbeatCache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Unknown));
    configNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Stopped, configNodeHeartbeatCache.getNodeStatus());

    // Periodic updates must not refresh Stopped to Unknown either
    configNodeHeartbeatCache.updateCurrentStatistics(false);
    Assert.assertEquals(NodeStatus.Stopped, configNodeHeartbeatCache.getNodeStatus());

    // A live heartbeat revives the ConfigNode
    configNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Running));
    configNodeHeartbeatCache.updateCurrentStatistics(false);
    Assert.assertEquals(NodeStatus.Running, configNodeHeartbeatCache.getNodeStatus());

    // Removing has the highest priority: the Stopped report must not refresh it
    configNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Stopped));
    configNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Stopped, configNodeHeartbeatCache.getNodeStatus());
    configNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Removing));
    configNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Removing, configNodeHeartbeatCache.getNodeStatus());
    configNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Stopped));
    configNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Removing, configNodeHeartbeatCache.getNodeStatus());
    // Unlike the DataNode cache, the ConfigNode cache unconditionally keeps Removing against any
    // update (pre-existing guard), including forced ones
    configNodeHeartbeatCache.cacheHeartbeatSample(new NodeHeartbeatSample(NodeStatus.Unknown));
    configNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Removing, configNodeHeartbeatCache.getNodeStatus());
    configNodeHeartbeatCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime(), NodeStatus.Running));
    configNodeHeartbeatCache.updateCurrentStatistics(true);
    Assert.assertEquals(NodeStatus.Removing, configNodeHeartbeatCache.getNodeStatus());
  }

  @Test
  public void statusReasonPropagationTest() {
    DataNodeHeartbeatCache dataNodeHeartbeatCache = new DataNodeHeartbeatCache(1);

    // A heartbeat response carrying a status reason (e.g. ReadOnly + DiskFull from the DataNode)
    // publishes the reason into the node statistics.
    TDataNodeHeartbeatResp heartbeatResp =
        new TDataNodeHeartbeatResp()
            .setHeartbeatTimestamp(System.nanoTime())
            .setStatus(NodeStatus.ReadOnly.getStatus())
            .setStatusReason(NodeStatus.DISK_FULL);
    dataNodeHeartbeatCache.cacheHeartbeatSample(new NodeHeartbeatSample(heartbeatResp));
    dataNodeHeartbeatCache.updateCurrentStatistics(false);
    Assert.assertEquals(NodeStatus.ReadOnly, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertEquals(NodeStatus.DISK_FULL, dataNodeHeartbeatCache.getNodeStatusReason());

    // A heartbeat response without a status reason clears the previous reason.
    heartbeatResp =
        new TDataNodeHeartbeatResp()
            .setHeartbeatTimestamp(System.nanoTime())
            .setStatus(NodeStatus.Running.getStatus());
    dataNodeHeartbeatCache.cacheHeartbeatSample(new NodeHeartbeatSample(heartbeatResp));
    dataNodeHeartbeatCache.updateCurrentStatistics(false);
    Assert.assertEquals(NodeStatus.Running, dataNodeHeartbeatCache.getNodeStatus());
    Assert.assertNull(dataNodeHeartbeatCache.getNodeStatusReason());

    // An Unknown decided by the failure detector (a stale heartbeat) never carries a reason. The
    // stale sample is accepted because the fresh cache has an empty sliding window.
    DataNodeHeartbeatCache staleCache = new DataNodeHeartbeatCache(3);
    staleCache.cacheHeartbeatSample(
        new NodeHeartbeatSample(System.nanoTime() - 60_000_000_000L, NodeStatus.ReadOnly));
    staleCache.updateCurrentStatistics(false);
    Assert.assertEquals(NodeStatus.Unknown, staleCache.getNodeStatus());
    Assert.assertNull(staleCache.getNodeStatusReason());
  }

  @Test
  public void loadCacheSnapshotKeepsStatusAndReasonFromSameStatistics() {
    LoadCache loadCache = new LoadCache();
    loadCache.createNodeHeartbeatCache(NodeType.DataNode, 1);
    loadCache.cacheDataNodeHeartbeatSample(
        1,
        new NodeHeartbeatSample(
            new TDataNodeHeartbeatResp()
                .setHeartbeatTimestamp(System.nanoTime())
                .setStatus(NodeStatus.ReadOnly.getStatus())
                .setStatusReason(NodeStatus.MANUAL)));
    loadCache.updateNodeStatistics(false);

    // The snapshot used by SHOW CLUSTER reads status and reason from one statistics object.
    Map<Integer, NodeStatistics> snapshot = loadCache.getNodeStatisticsSnapshot();
    Assert.assertEquals(1, snapshot.size());
    Assert.assertEquals(NodeStatus.ReadOnly, snapshot.get(1).getStatus());
    Assert.assertEquals(NodeStatus.MANUAL, snapshot.get(1).getStatusReason());
    Assert.assertEquals(NodeStatus.MANUAL, loadCache.getNodeStatusReason(1));

    // A missing cache yields no reason and Unknown status, without a fake merged string.
    Assert.assertNull(loadCache.getNodeStatusReason(2));
    Assert.assertEquals(NodeStatus.Unknown, loadCache.getNodeStatus(2));
  }
}
