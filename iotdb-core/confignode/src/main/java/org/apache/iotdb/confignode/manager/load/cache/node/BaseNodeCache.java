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

package org.apache.iotdb.confignode.manager.load.cache.node;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.cache.AbstractLoadCache;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.function.BiFunction;

public abstract class BaseNodeCache extends AbstractLoadCache {

  protected final int nodeId;

  private BiFunction<BaseNodeCache, NodeStatus, TSStatus> nodeStatusPersister;

  protected BaseNodeCache(int nodeId) {
    super();
    this.nodeId = nodeId;
    currentStatistics.set(NodeStatistics.generateDefaultNodeStatistics());
  }

  public int getNodeId() {
    return nodeId;
  }

  public synchronized void initializeNodeStatus(
      NodeStatus persistedStatus,
      BiFunction<BaseNodeCache, NodeStatus, TSStatus> nodeStatusPersister) {
    this.nodeStatusPersister = nodeStatusPersister;
    slidingWindow.clear();
    if (persistedStatus != null) {
      currentStatistics.set(
          new NodeStatistics(System.nanoTime(), persistedStatus, null, Long.MAX_VALUE));
    }
  }

  public synchronized TSStatus updateNodeStatistics() {
    return updateCurrentStatistics(calculateCurrentStatistics(), false);
  }

  public synchronized TSStatus updateNodeStatus(NodeStatus status, boolean force) {
    NodeHeartbeatSample sample = new NodeHeartbeatSample(status);
    cacheHeartbeatSample(sample);
    return updateCurrentStatistics(
        new NodeStatistics(
            sample.getSampleLogicalTimestamp(),
            status,
            null,
            NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE),
        force);
  }

  private TSStatus updateCurrentStatistics(NodeStatistics statistics, boolean force) {
    NodeStatus status = NodeStatus.transition(getNodeStatus(), statistics.getStatus(), force);
    if (status != statistics.getStatus()) {
      statistics =
          new NodeStatistics(statistics.getStatisticsNanoTimestamp(), status, null, Long.MAX_VALUE);
    }
    if (nodeStatusPersister != null) {
      TSStatus result = nodeStatusPersister.apply(this, statistics.getStatus());
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result;
      }
    }
    currentStatistics.set(statistics);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  protected abstract NodeStatistics calculateCurrentStatistics();

  public long getLoadScore() {
    return ((NodeStatistics) currentStatistics.get()).getLoadScore();
  }

  public NodeStatus getNodeStatus() {
    return ((NodeStatistics) currentStatistics.get()).getStatus();
  }

  public String getNodeStatusWithReason() {
    NodeStatistics statistics = (NodeStatistics) currentStatistics.get();
    return statistics.getStatusReason() == null
        ? statistics.getStatus().getStatus()
        : statistics.getStatus().getStatus() + "(" + statistics.getStatusReason() + ")";
  }
}
