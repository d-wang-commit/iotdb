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

package org.apache.iotdb.confignode.client.async.handlers.rpc;

import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.client.request.AsyncRequestContext;
import org.apache.iotdb.commons.client.request.AsyncRequestManager;
import org.apache.iotdb.commons.client.request.AsyncRequestRPCHandler;
import org.apache.iotdb.confignode.client.async.CnToDnAsyncRequestType;
import org.apache.iotdb.confignode.client.async.handlers.DataNodeAsyncRequestContext;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.thrift.transport.TTransportException;
import org.junit.Test;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DataNodeTSStatusRPCHandlerTest {

  private static final int REQUEST_ID = 7;
  private static final TDataNodeLocation TARGET_DATA_NODE =
      new TDataNodeLocation()
          .setDataNodeId(2)
          .setInternalEndPoint(new TEndPoint("127.0.0.1", 10730));

  @Test(timeout = 10_000)
  public void successStopsRetrying() throws Exception {
    TSStatus response = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    RetryingRequestManager manager =
        new RetryingRequestManager(handler -> handler.onComplete(response));
    DataNodeAsyncRequestContext<Void, TSStatus> context =
        newContext(CnToDnAsyncRequestType.DELETE_REGION);

    manager.sendAsyncRequestWithRetry(context);

    assertEquals(1, manager.attempts);
    assertTrue(context.getRequestIndices().isEmpty());
    assertSame(response, context.getResponseMap().get(REQUEST_ID));
  }

  @Test(timeout = 10_000)
  public void deleteRegionNotExistStopsRetryingAndPreservesResponse() throws Exception {
    TSStatus response = new TSStatus(TSStatusCode.REGION_NOT_EXIST.getStatusCode());
    RetryingRequestManager manager =
        new RetryingRequestManager(handler -> handler.onComplete(response));
    DataNodeAsyncRequestContext<Void, TSStatus> context =
        newContext(CnToDnAsyncRequestType.DELETE_REGION);

    manager.sendAsyncRequestWithRetry(context);

    assertEquals(1, manager.attempts);
    assertTrue(context.getRequestIndices().isEmpty());
    assertSame(response, context.getResponseMap().get(REQUEST_ID));
    assertEquals(TSStatusCode.REGION_NOT_EXIST.getStatusCode(), response.getCode());
  }

  @Test(timeout = 10_000)
  public void deleteRegionErrorThenNotExistStopsAfterSecondAttempt() throws Exception {
    TSStatus error = new TSStatus(TSStatusCode.DELETE_REGION_ERROR.getStatusCode());
    TSStatus notExist = new TSStatus(TSStatusCode.REGION_NOT_EXIST.getStatusCode());
    AtomicInteger replies = new AtomicInteger();
    RetryingRequestManager manager =
        new RetryingRequestManager(
            handler -> handler.onComplete(replies.getAndIncrement() == 0 ? error : notExist));
    DataNodeAsyncRequestContext<Void, TSStatus> context =
        newContext(CnToDnAsyncRequestType.DELETE_REGION);

    manager.sendAsyncRequestWithRetry(context);

    assertEquals(2, manager.attempts);
    assertTrue(context.getRequestIndices().isEmpty());
    assertSame(notExist, context.getResponseMap().get(REQUEST_ID));
    assertEquals(TSStatusCode.REGION_NOT_EXIST.getStatusCode(), notExist.getCode());
  }

  @Test(timeout = 10_000)
  public void persistentDeleteRegionErrorExhaustsRetries() throws Exception {
    TSStatus response = new TSStatus(TSStatusCode.DELETE_REGION_ERROR.getStatusCode());
    RetryingRequestManager manager =
        new RetryingRequestManager(handler -> handler.onComplete(response));
    DataNodeAsyncRequestContext<Void, TSStatus> context =
        newContext(CnToDnAsyncRequestType.DELETE_REGION);

    manager.sendAsyncRequestWithRetry(context);

    assertEquals(6, manager.attempts);
    assertEquals(Collections.singletonList(REQUEST_ID), context.getRequestIndices());
    assertSame(response, context.getResponseMap().get(REQUEST_ID));
  }

  @Test(timeout = 10_000)
  public void nonDeleteRegionNotExistRemainsRetryable() throws Exception {
    TSStatus response = new TSStatus(TSStatusCode.REGION_NOT_EXIST.getStatusCode());
    RetryingRequestManager manager =
        new RetryingRequestManager(handler -> handler.onComplete(response));
    DataNodeAsyncRequestContext<Void, TSStatus> context = newContext(CnToDnAsyncRequestType.FLUSH);

    manager.sendAsyncRequestWithRetry(context);

    assertEquals(6, manager.attempts);
    assertEquals(Collections.singletonList(REQUEST_ID), context.getRequestIndices());
    assertSame(response, context.getResponseMap().get(REQUEST_ID));
  }

  @Test(timeout = 10_000)
  public void transportErrorThenSuccessRetries() throws Exception {
    TSStatus success = new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    AtomicInteger replies = new AtomicInteger();
    RetryingRequestManager manager =
        new RetryingRequestManager(
            handler -> {
              if (replies.getAndIncrement() == 0) {
                handler.onError(new TTransportException());
              } else {
                handler.onComplete(success);
              }
            });
    DataNodeAsyncRequestContext<Void, TSStatus> context =
        newContext(CnToDnAsyncRequestType.DELETE_REGION);

    manager.sendAsyncRequestWithRetry(context);

    assertEquals(2, manager.attempts);
    assertTrue(context.getRequestIndices().isEmpty());
    assertSame(success, context.getResponseMap().get(REQUEST_ID));
  }

  @Test(timeout = 10_000)
  public void persistentTransportErrorExhaustsRetries() throws Exception {
    RetryingRequestManager manager =
        new RetryingRequestManager(handler -> handler.onError(new TTransportException()));
    DataNodeAsyncRequestContext<Void, TSStatus> context =
        newContext(CnToDnAsyncRequestType.DELETE_REGION);

    manager.sendAsyncRequestWithRetry(context);

    assertEquals(6, manager.attempts);
    assertEquals(Collections.singletonList(REQUEST_ID), context.getRequestIndices());
    assertEquals(
        TSStatusCode.CAN_NOT_CONNECT_DATANODE.getStatusCode(),
        context.getResponseMap().get(REQUEST_ID).getCode());
  }

  private static DataNodeAsyncRequestContext<Void, TSStatus> newContext(
      CnToDnAsyncRequestType requestType) {
    return new DataNodeAsyncRequestContext<>(
        requestType, Collections.singletonMap(REQUEST_ID, TARGET_DATA_NODE));
  }

  private interface SimulatedDataNode {
    void respond(DataNodeTSStatusRPCHandler handler);
  }

  // Keep the real retry loop, dispatch, and handler factory; replace only the remote response.
  private static class RetryingRequestManager
      extends AsyncRequestManager<CnToDnAsyncRequestType, TDataNodeLocation, SimulatedDataNode> {

    private int attempts;

    private RetryingRequestManager(Consumer<DataNodeTSStatusRPCHandler> response) throws Exception {
      super(0);
      SimulatedDataNode client =
          handler -> {
            attempts++;
            response.accept(handler);
          };
      when(clientManager.borrowClient(any())).thenReturn(client);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void initClientManager(int selectorNumOfAsyncClientManager) {
      clientManager = mock(IClientManager.class);
    }

    @Override
    protected void initActionMapBuilder() {
      for (CnToDnAsyncRequestType requestType :
          new CnToDnAsyncRequestType[] {
            CnToDnAsyncRequestType.DELETE_REGION, CnToDnAsyncRequestType.FLUSH
          }) {
        actionMapBuilder.put(
            requestType,
            (request, client, handler) -> client.respond((DataNodeTSStatusRPCHandler) handler));
      }
    }

    @Override
    protected TEndPoint nodeLocationToEndPoint(TDataNodeLocation location) {
      return location.getInternalEndPoint();
    }

    @Override
    protected AsyncRequestRPCHandler<?, CnToDnAsyncRequestType, TDataNodeLocation> buildHandler(
        AsyncRequestContext<?, ?, CnToDnAsyncRequestType, TDataNodeLocation> context,
        int requestId,
        TDataNodeLocation targetNode) {
      return DataNodeAsyncRequestRPCHandler.buildHandler(context, requestId, targetNode);
    }
  }
}
