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

  /** Initialize a new cache before adding it to LoadCache's nodeCacheMap. */
  public void initializeNodeStatus(
      NodeStatus persistedStatus,
      BiFunction<BaseNodeCache, NodeStatus, TSStatus> nodeStatusPersister) {
    this.nodeStatusPersister = nodeStatusPersister;
    if (persistedStatus != null) {
      currentStatistics.set(
          new NodeStatistics(System.nanoTime(), persistedStatus, null, Long.MAX_VALUE));
    }
  }

  public TSStatus updateNodeStatistics() {
    synchronized (slidingWindow) {
      return applyNodeStatistics(calculateCurrentStatistics(), false);
    }
  }

  /**
   * Try to set the requested status and record a sample for later statistics refreshes. Transition
   * rules may retain the previous status even when this method returns success.
   */
  public TSStatus trySetNodeStatus(NodeStatus status, boolean force) {
    synchronized (slidingWindow) {
      // SET_SYSTEM_STATUS classifies an explicitly requested ReadOnly as Manual on the DataNode.
      NodeHeartbeatSample sample =
          new NodeHeartbeatSample(status, status == NodeStatus.ReadOnly ? NodeStatus.MANUAL : null);
      cacheHeartbeatSample(sample);
      return applyNodeStatistics(
          new NodeStatistics(
              sample.getSampleLogicalTimestamp(),
              status,
              sample.getStatusReason(),
              NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE),
          force);
    }
  }

  private TSStatus applyNodeStatistics(NodeStatistics newStats, boolean force) {
    newStats = NodeStatistics.transition((NodeStatistics) currentStatistics.get(), newStats, force);
    if (nodeStatusPersister != null) {
      TSStatus result = nodeStatusPersister.apply(this, newStats.getStatus());
      if (result.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
        return result;
      }
    }
    currentStatistics.set(newStats);
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  /** Called with the slidingWindow lock held through calculation, persistence and publication. */
  protected abstract NodeStatistics calculateCurrentStatistics();

  public long getLoadScore() {
    return ((NodeStatistics) currentStatistics.get()).getLoadScore();
  }

  public NodeStatus getNodeStatus() {
    return ((NodeStatistics) currentStatistics.get()).getStatus();
  }

  /**
   * @return The reason why lead to current NodeStatus, null if there is none.
   */
  public String getNodeStatusReason() {
    return ((NodeStatistics) currentStatistics.get()).getStatusReason();
  }
}
