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

package org.apache.iotdb.confignode.client.async;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.async.AsyncDataNodeInternalServiceClient;
import org.apache.iotdb.commons.client.request.AsyncRequestManager;

import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.tsfile.utils.Pair;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Runs the real CN request dispatcher and callbacks against a simulated DataNode. */
public final class RegionDeletionRpcTestContext implements AutoCloseable {

  @FunctionalInterface
  public interface Responder {
    TSStatus respond(TEndPoint endpoint, TConsensusGroupId regionId) throws Exception;
  }

  private final CnToDnInternalServiceAsyncRequestManager requestManager =
      CnToDnInternalServiceAsyncRequestManager.getInstance();
  private final Field clientManagerField;
  private final Object originalClientManager;
  private final List<Pair<TEndPoint, TConsensusGroupId>> requests = new ArrayList<>();
  private Responder responder;

  @SuppressWarnings("unchecked")
  public RegionDeletionRpcTestContext(Responder responder) throws Exception {
    this.responder = responder;
    clientManagerField = AsyncRequestManager.class.getDeclaredField("clientManager");
    clientManagerField.setAccessible(true);
    originalClientManager = clientManagerField.get(requestManager);
    IClientManager<TEndPoint, AsyncDataNodeInternalServiceClient> clientManager =
        mock(IClientManager.class);
    when(clientManager.borrowClient(any(TEndPoint.class)))
        .thenAnswer(
            borrow -> {
              TEndPoint endpoint = borrow.getArgument(0);
              AsyncDataNodeInternalServiceClient client =
                  mock(AsyncDataNodeInternalServiceClient.class);
              doAnswer(
                      invocation -> {
                        TConsensusGroupId regionId = invocation.getArgument(0);
                        AsyncMethodCallback<TSStatus> callback = invocation.getArgument(1);
                        requests.add(new Pair<>(endpoint.deepCopy(), regionId.deepCopy()));
                        callback.onComplete(this.responder.respond(endpoint, regionId));
                        return null;
                      })
                  .when(client)
                  .deleteRegion(any(TConsensusGroupId.class), any(AsyncMethodCallback.class));
              return client;
            });
    clientManagerField.set(requestManager, clientManager);
  }

  public void setResponder(Responder responder) {
    this.responder = responder;
  }

  public List<Pair<TEndPoint, TConsensusGroupId>> getRequests() {
    return requests;
  }

  @Override
  public void close() throws IllegalAccessException {
    clientManagerField.set(requestManager, originalClientManager);
  }
}
