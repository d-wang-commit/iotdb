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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.common.Peer;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class IoTConsensusRemovePeerTest {

  @Test
  public void removeRemotePeerStillFencesAndWaitsWhenConfigurationMissesPeer() throws Exception {
    final ConsensusGroupId groupId = new DataRegionId(11);
    final Peer removedPeer = new Peer(groupId, 1, new TEndPoint("127.0.0.1", 10760));
    final IoTConsensusServerImpl impl = Mockito.mock(IoTConsensusServerImpl.class);
    Mockito.when(impl.getConfiguration()).thenReturn(Collections.emptyList());

    final IoTConsensus consensus = newSpy(groupId, impl, 1, new TEndPoint("127.0.0.1", 10760));

    consensus.removeRemotePeer(groupId, removedPeer);

    Mockito.verify(impl).inactivatePeer(removedPeer, true);
    Mockito.verify(impl).waitTargetPeerUntilSyncLogCompleted(removedPeer);
    Mockito.verify(impl).waitReleaseAllRegionRelatedResource(removedPeer);
  }

  @Test
  public void resetPeerListReactivatesTheLocalPeer() throws Exception {
    final ConsensusGroupId groupId = new DataRegionId(11);
    final TEndPoint localEndpoint = new TEndPoint("127.0.0.1", 10760);
    final Peer localPeer = new Peer(groupId, 1, localEndpoint);
    final IoTConsensusServerImpl impl = Mockito.mock(IoTConsensusServerImpl.class);
    Mockito.when(impl.getConfiguration()).thenReturn(Collections.singletonList(localPeer));

    final IoTConsensus consensus = newSpy(groupId, impl, 1, localEndpoint);

    consensus.resetPeerList(groupId, Collections.singletonList(localPeer));

    Mockito.verify(impl).setActive(true);
  }

  private static IoTConsensus newSpy(
      ConsensusGroupId groupId,
      IoTConsensusServerImpl impl,
      int localNodeId,
      TEndPoint localEndpoint)
      throws Exception {
    final IoTConsensus consensus = Mockito.mock(IoTConsensus.class, Mockito.CALLS_REAL_METHODS);
    final Map<ConsensusGroupId, IoTConsensusServerImpl> stateMachineMap = new ConcurrentHashMap<>();
    stateMachineMap.put(groupId, impl);
    setField(consensus, "stateMachineMap", stateMachineMap);
    setField(consensus, "logger", LoggerFactory.getLogger(IoTConsensus.class));
    setField(consensus, "thisNodeId", localNodeId);
    setField(consensus, "thisNode", localEndpoint);
    return consensus;
  }

  private static void setField(Object target, String name, Object value) throws Exception {
    final Field field = IoTConsensus.class.getDeclaredField(name);
    field.setAccessible(true);
    field.set(target, value);
  }
}
