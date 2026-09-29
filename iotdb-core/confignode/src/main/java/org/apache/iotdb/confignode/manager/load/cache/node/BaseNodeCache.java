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
import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;
import org.apache.iotdb.confignode.manager.load.cache.AbstractLoadCache;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.function.BiFunction;

/**
 * NodeCache caches the NodeHeartbeatSamples of a Node. Update and cache the current statistics of
 * the Node based on the latest NodeHeartbeatSample.
 */
public abstract class BaseNodeCache extends AbstractLoadCache {

  protected final int nodeId;

  private BiFunction<BaseNodeCache, NodeStatus, TSStatus> nodeStatusUpdater;
  private long minimumSampleTimestamp = Long.MIN_VALUE;

  /** Constructor for NodeCache with default NodeStatistics. */
  protected BaseNodeCache(int nodeId) {
    super();
    this.nodeId = nodeId;
    this.currentStatistics.set(NodeStatistics.generateDefaultNodeStatistics());
  }

  public int getNodeId() {
    return nodeId;
  }

  /** Restore only the durable status; heartbeat history belongs to the current leader. */
  public synchronized void initializeNodeStatus(
      NodeStatus persistedStatus,
      BiFunction<BaseNodeCache, NodeStatus, TSStatus> nodeStatusUpdater) {
    this.nodeStatusUpdater = nodeStatusUpdater;
    this.minimumSampleTimestamp = System.nanoTime();
    slidingWindow.clear();
    if (persistedStatus != null) {
      currentStatistics.set(
          new NodeStatistics(minimumSampleTimestamp, persistedStatus, null, Long.MAX_VALUE));
    }
  }

  @Override
  public synchronized void cacheHeartbeatSample(AbstractHeartbeatSample sample) {
    // A response to an earlier leader term must not clear a restored Stopped status.
    if (sample.getSampleLogicalTimestamp() >= minimumSampleTimestamp) {
      super.cacheHeartbeatSample(sample);
    }
  }

  @Override
  public final void updateCurrentStatistics(boolean forceUpdate) {
    updateNodeStatistics(forceUpdate);
  }

  /** Publish a status change only after its durable marker has been committed. */
  public synchronized TSStatus updateNodeStatistics(boolean forceUpdate) {
    return publishNodeStatistics(calculateCurrentStatistics(forceUpdate));
  }

  /** Explicit procedure changes, including rollback to Unknown/Stopped, override Removing. */
  public synchronized TSStatus setNodeStatus(NodeStatus status) {
    NodeHeartbeatSample sample = new NodeHeartbeatSample(status);
    cacheHeartbeatSample(sample);
    return publishNodeStatistics(
        new NodeStatistics(
            sample.getSampleLogicalTimestamp(),
            status,
            null,
            NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE));
  }

  private TSStatus publishNodeStatistics(NodeStatistics statistics) {
    if (nodeStatusUpdater != null) {
      TSStatus result = nodeStatusUpdater.apply(this, statistics.getStatus());
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result;
      }
    }
    currentStatistics.set(statistics);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  protected abstract NodeStatistics calculateCurrentStatistics(boolean forceUpdate);

  /**
   * TODO: The loadScore of each Node will be changed to Double
   *
   * @return The latest load score of a node, the higher the score the higher the load
   */
  public long getLoadScore() {
    return ((NodeStatistics) currentStatistics.get()).getLoadScore();
  }

  /**
   * @return The current status of the Node.
   */
  public NodeStatus getNodeStatus() {
    return ((NodeStatistics) currentStatistics.get()).getStatus();
  }

  /**
   * @return The reason why lead to current NodeStatus.
   */
  public String getNodeStatusWithReason() {
    NodeStatistics statistics = (NodeStatistics) this.currentStatistics.get();
    return statistics.getStatusReason() == null
        ? statistics.getStatus().getStatus()
        : statistics.getStatus().getStatus() + "(" + statistics.getStatusReason() + ")";
  }
}
