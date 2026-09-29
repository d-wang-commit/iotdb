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

package org.apache.iotdb.confignode.consensus.request.write.confignode;

import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlanType;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

/** Persist a sticky node status, or clear it when the node resumes a live status. */
public class UpdateNodeStatusPlan extends ConfigPhysicalPlan {

  private int nodeId;
  private NodeStatus status;
  private NodeStatus statusToClear = NodeStatus.Stopped;

  public UpdateNodeStatusPlan() {
    super(ConfigPhysicalPlanType.UpdateNodeStatus);
  }

  public UpdateNodeStatusPlan(int nodeId, NodeStatus status) {
    this(nodeId, status, NodeStatus.Stopped);
  }

  public UpdateNodeStatusPlan(int nodeId, NodeStatus status, NodeStatus statusToClear) {
    this();
    this.nodeId = nodeId;
    this.status = status;
    this.statusToClear = statusToClear;
  }

  public int getNodeId() {
    return nodeId;
  }

  public NodeStatus getStatus() {
    return status;
  }

  /** The durable marker to clear when status is neither Stopped nor Removing. */
  public NodeStatus getStatusToClear() {
    return statusToClear;
  }

  @Override
  protected void serializeImpl(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(getType().getPlanType(), stream);
    ReadWriteIOUtils.write(nodeId, stream);
    ReadWriteIOUtils.write(status.getStatus(), stream);
    ReadWriteIOUtils.write(statusToClear.getStatus(), stream);
  }

  @Override
  protected void deserializeImpl(ByteBuffer buffer) {
    nodeId = ReadWriteIOUtils.readInt(buffer);
    status = NodeStatus.parse(ReadWriteIOUtils.readString(buffer));
    statusToClear = NodeStatus.parse(ReadWriteIOUtils.readString(buffer));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UpdateNodeStatusPlan that = (UpdateNodeStatusPlan) o;
    return nodeId == that.nodeId && status == that.status && statusToClear == that.statusToClear;
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodeId, status, statusToClear);
  }
}
