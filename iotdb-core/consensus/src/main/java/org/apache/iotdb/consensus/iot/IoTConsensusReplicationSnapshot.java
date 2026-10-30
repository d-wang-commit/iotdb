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

package org.apache.iotdb.consensus.iot;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** An in-memory observation, never a replication or durability checkpoint. */
public final class IoTConsensusReplicationSnapshot {
  private final long generation;
  private final boolean valid;
  private final Map<Integer, Long> pendingRequests;

  public IoTConsensusReplicationSnapshot(
      long generation, boolean valid, Map<Integer, Long> pendingRequests) {
    this.generation = generation;
    this.valid = valid;
    this.pendingRequests = Collections.unmodifiableMap(new HashMap<>(pendingRequests));
  }

  public long getGeneration() {
    return generation;
  }

  public boolean isValid() {
    return valid;
  }

  public Map<Integer, Long> getPendingRequests() {
    return pendingRequests;
  }
}
