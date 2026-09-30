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

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.manager.load.cache.AbstractHeartbeatSample;

import java.util.Collections;
import java.util.List;

public class AINodeHeartbeatCache extends BaseNodeCache {

  public AINodeHeartbeatCache(int aiNodeId) {
    super(aiNodeId);
  }

  @Override
  protected NodeStatistics calculateCurrentStatistics() {
    NodeStatus status;
    String statusReason = null;
    long currentNanoTime = System.nanoTime();
    NodeHeartbeatSample lastSample = (NodeHeartbeatSample) getLastSample();
    List<AbstractHeartbeatSample> heartbeatHistory = Collections.unmodifiableList(slidingWindow);
    if (lastSample == null || !failureDetector.isAvailable(nodeId, heartbeatHistory)) {
      status = NodeStatus.Unknown;
    } else {
      status = lastSample.getStatus();
      statusReason = lastSample.getStatusReason();
    }
    return new NodeStatistics(
        currentNanoTime,
        status,
        statusReason,
        NodeStatus.isNormalStatus(status) ? 0 : Long.MAX_VALUE);
  }
}
