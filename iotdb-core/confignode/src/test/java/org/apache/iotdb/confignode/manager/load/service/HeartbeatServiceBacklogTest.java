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
package org.apache.iotdb.confignode.manager.load.service;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.confignode.client.async.AsyncDataNodeHeartbeatClientPool;
import org.apache.iotdb.confignode.client.async.handlers.heartbeat.DataNodeHeartbeatHandler;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.ReplicationBacklogConfig;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.mpp.rpc.thrift.TDataNodeHeartbeatReq;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HeartbeatServiceBacklogTest {
  @Test
  public void asyncHeartbeatRetainsTheSamplingFlagOfEachDestination() throws Exception {
    ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
    ReplicationBacklogConfig oldConfig = conf.getReplicationBacklogConfig();
    String oldProtocol = conf.getDataRegionConsensusProtocolClass();
    IManager manager = mock(IManager.class, RETURNS_DEEP_STUBS);
    LoadManager loadManager = manager.getLoadManager();
    LoadCache loadCache = mock(LoadCache.class);
    when(loadCache.getCurrentRegionLocationMap(TConsensusGroupType.DataRegion))
        .thenReturn(Collections.emptyMap());
    when(loadCache.getCurrentDataNodeStatisticsMap()).thenReturn(Collections.emptyMap());
    when(loadCache.getCurrentRegionStatisticsMap(TConsensusGroupType.DataRegion))
        .thenReturn(Collections.emptyMap());
    when(manager.getNodeManager().getRegisteredConfigNodes()).thenReturn(Collections.emptyList());
    ReplicationBacklogService backlog = new ReplicationBacklogService(loadManager, loadCache);
    when(loadManager.getReplicationBacklogService()).thenReturn(backlog);
    ReplicationBacklogServiceTest.setField(
        backlog, "executor", mock(ScheduledExecutorService.class));
    AsyncDataNodeHeartbeatClientPool pool = AsyncDataNodeHeartbeatClientPool.getInstance();
    IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clients =
        mock(IClientManager.class);
    AsyncDataNodeInternalServiceClient client = mock(AsyncDataNodeInternalServiceClient.class);
    when(clients.borrowClient(any(TEndPoint.class))).thenReturn(client);
    List<TDataNodeHeartbeatReq> dispatched = new ArrayList<>();
    // Retain each actual argument, as an asynchronous serializer does, instead of copying in the
    // test.
    doAnswer(
            invocation -> {
              dispatched.add(invocation.getArgument(0));
              return null;
            })
        .when(client)
        .getDataNodeHeartBeat(
            any(TDataNodeHeartbeatReq.class), any(DataNodeHeartbeatHandler.class));
    Object originalClients = ReplicationBacklogServiceTest.setField(pool, "clientManager", clients);
    try {
      conf.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
      conf.setReplicationBacklogConfig(ReplicationBacklogConfig.defaults());
      backlog.start();
      HeartbeatService heartbeat = new HeartbeatService(manager, loadCache);
      TDataNodeHeartbeatReq shared =
          new TDataNodeHeartbeatReq().setHeartbeatTimestamp(System.nanoTime());
      List<TDataNodeConfiguration> nodes = new ArrayList<>();
      for (int node = 0; node < 2; node++) {
        nodes.add(
            new TDataNodeConfiguration()
                .setLocation(
                    new TDataNodeLocation()
                        .setDataNodeId(node)
                        .setInternalEndPoint(new TEndPoint("127.0.0.1", 19000 + node))));
      }
      Method ping =
          HeartbeatService.class.getDeclaredMethod(
              "pingRegisteredDataNodes", TDataNodeHeartbeatReq.class, List.class);
      ping.setAccessible(true);
      ping.invoke(heartbeat, shared, nodes);
      assertEquals(2, dispatched.size());
      assertNotSame(dispatched.get(0), dispatched.get(1));
      assertNotSame(shared, dispatched.get(0));
      assertTrue(dispatched.get(0).isNeedReplicationBacklog());
      assertFalse(dispatched.get(1).isNeedReplicationBacklog());
      assertEquals(shared.getHeartbeatTimestamp(), dispatched.get(0).getHeartbeatTimestamp());
    } finally {
      ReplicationBacklogServiceTest.setField(pool, "clientManager", originalClients);
      backlog.stop();
      conf.setReplicationBacklogConfig(oldConfig);
      conf.setDataRegionConsensusProtocolClass(oldProtocol);
    }
  }
}
