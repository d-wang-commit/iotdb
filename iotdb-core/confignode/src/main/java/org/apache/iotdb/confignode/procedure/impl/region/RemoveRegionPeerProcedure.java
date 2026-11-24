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

package org.apache.iotdb.confignode.procedure.impl.region;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TRegionMaintainTaskStatus;
import org.apache.iotdb.common.rpc.thrift.TRegionMigrateFailedType;
import org.apache.iotdb.common.rpc.thrift.TRegionReplicaSet;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.exception.runtime.ThriftSerDeException;
import org.apache.iotdb.commons.queryengine.utils.DateTimeUtils;
import org.apache.iotdb.commons.utils.CommonDateTimeUtils;
import org.apache.iotdb.commons.utils.ThriftCommonsSerDeUtils;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.confignode.procedure.store.ProcedureType;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.iotdb.commons.utils.KillPoint.KillPoint.setKillPoint;
import static org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState.DELETE_OLD_REGION_PEER;
import static org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState.DROP_CONSENSUS_PIPES;
import static org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE;
import static org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState.REMOVE_REGION_PEER;
import static org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState.RESET_IOT_CONSENSUS_V1_PEER_LIST;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

public class RemoveRegionPeerProcedure extends RegionOperationProcedure<RemoveRegionPeerState> {
  private static final Logger LOGGER = LoggerFactory.getLogger(RemoveRegionPeerProcedure.class);
  private static final int MAX_DELETE_OLD_REGION_PEER_RETRY = 3;
  private static final long DELETE_OLD_REGION_PEER_RETRY_INTERVAL_MS = 5_000;
  private static final int MAX_FINALIZATION_RETRY = 3;
  private static final long FINALIZATION_RETRY_INTERVAL_MS = 5_000;
  private static final int MAX_RESET_PEER_LIST_RETRY = 3;
  private static final long RESET_PEER_LIST_RETRY_INTERVAL_MS = 5_000;
  private TDataNodeLocation coordinator;
  private TDataNodeLocation targetDataNode;
  private transient int deleteOldRegionPeerAttempted = 0;

  public RemoveRegionPeerProcedure() {
    super();
  }

  public RemoveRegionPeerProcedure(
      TConsensusGroupId consensusGroupId,
      TDataNodeLocation coordinator,
      TDataNodeLocation targetDataNode) {
    super(consensusGroupId);
    this.coordinator = coordinator;
    this.targetDataNode = targetDataNode;
  }

  private void handleTransferLeader(RegionMaintainHandler handler)
      throws ProcedureException, InterruptedException {
    LOGGER.info(
        ProcedureMessages.PID_REMOVEREGION_STARTED_REGION_WILL_BE_REMOVED_FROM_DATANODE,
        getProcId(),
        regionId.getId(),
        targetDataNode.getDataNodeId());
    handler.forceUpdateRegionCache(regionId, targetDataNode, RegionStatus.Removing);
    handler.transferRegionLeader(regionId, targetDataNode, coordinator);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, RemoveRegionPeerState state)
      throws InterruptedException {
    if (regionId == null) {
      return Flow.NO_MORE_STATE;
    }
    TSStatus tsStatus;
    RegionMaintainHandler handler = env.getRegionMaintainHandler();
    try {
      switch (state) {
        case TRANSFER_REGION_LEADER:
          handleTransferLeader(handler);
          setKillPoint(state);
          setNextState(REMOVE_REGION_PEER);
          break;
        case REMOVE_REGION_PEER:
          handler.forceUpdateRegionCache(regionId, targetDataNode, RegionStatus.Removing);
          tsStatus =
              handler.submitRemoveRegionPeerTask(
                  this.getProcId(), targetDataNode, regionId, coordinator);
          setKillPoint(state);
          if (tsStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
            return retryRemovePeerStep(env, state, tsStatus, true);
          }
          final TRegionMigrateResult removeRegionPeerResult =
              handler.waitTaskFinish(this.getProcId(), coordinator);
          if (removeRegionPeerResult.getTaskStatus() == TRegionMaintainTaskStatus.SUCCESS) {
            setNextState(DELETE_OLD_REGION_PEER);
            break;
          }
          if (removeRegionPeerResult.getTaskStatus() == TRegionMaintainTaskStatus.PROCESSING) {
            // waitTaskFinish only returns PROCESSING when the wait was interrupted, i.e. this
            // ConfigNode is stopping or losing leadership. The task may still be running, so
            // persist the state and re-poll after recovery instead of deleting the source.
            return retryStep(state, null);
          }
          if (removeRegionPeerResult.getTaskStatus() == TRegionMaintainTaskStatus.TASK_NOT_EXIST
              || isDisconnectFailure(removeRegionPeerResult)) {
            // The coordinator restarted and lost its in-memory task table (or is unreachable).
            // Resubmit in the next round; the same taskId can run again on the new process and
            // re-run inactivate + the barrier.
            return retryRemovePeerStep(env, state, removeRegionPeerResult, true);
          }
          return retryRemovePeerStep(env, state, removeRegionPeerResult, false);
        case DELETE_OLD_REGION_PEER:
          handler.forceUpdateRegionCache(regionId, targetDataNode, RegionStatus.Removing);
          tsStatus =
              handler.submitDeleteOldRegionPeerTask(this.getProcId(), targetDataNode, regionId);
          setKillPoint(state);
          if (tsStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
            deleteOldRegionPeerAttempted++;
            if (deleteOldRegionPeerAttempted <= MAX_DELETE_OLD_REGION_PEER_RETRY) {
              LOGGER.warn(
                  ProcedureMessages
                      .PID_REMOVEREGION_DELETE_OLD_REGION_PEER_TASK_SUBMITTED_FAILED_ATTEMPT,
                  getProcId(),
                  deleteOldRegionPeerAttempted,
                  MAX_DELETE_OLD_REGION_PEER_RETRY + 1,
                  DELETE_OLD_REGION_PEER_RETRY_INTERVAL_MS,
                  regionId);
              Thread.sleep(DELETE_OLD_REGION_PEER_RETRY_INTERVAL_MS);
              setNextState(DELETE_OLD_REGION_PEER);
              return Flow.HAS_MORE_STATE;
            }
            LOGGER.warn(
                ProcedureMessages
                    .PID_REMOVEREGION_DELETE_OLD_REGION_PEER_TASK_SUBMITTED_FAILED_AFTER,
                getProcId(),
                deleteOldRegionPeerAttempted + 1,
                regionId);
            setNextState(DROP_CONSENSUS_PIPES);
            return Flow.HAS_MORE_STATE;
          }
          TRegionMigrateResult deleteOldRegionPeerResult =
              handler.waitTaskFinish(this.getProcId(), targetDataNode);
          if (deleteOldRegionPeerResult.getTaskStatus() != TRegionMaintainTaskStatus.SUCCESS) {
            deleteOldRegionPeerAttempted++;
            if (deleteOldRegionPeerAttempted <= MAX_DELETE_OLD_REGION_PEER_RETRY) {
              LOGGER.warn(
                  ProcedureMessages
                      .PID_REMOVEREGION_DELETE_OLD_REGION_PEER_EXECUTED_FAILED_ATTEMPT_WILL,
                  getProcId(),
                  deleteOldRegionPeerAttempted,
                  MAX_DELETE_OLD_REGION_PEER_RETRY + 1,
                  DELETE_OLD_REGION_PEER_RETRY_INTERVAL_MS,
                  regionId);
              Thread.sleep(DELETE_OLD_REGION_PEER_RETRY_INTERVAL_MS);
              setNextState(DELETE_OLD_REGION_PEER);
              return Flow.HAS_MORE_STATE;
            }
            LOGGER.warn(
                ProcedureMessages
                    .PID_REMOVEREGION_DELETE_OLD_REGION_PEER_EXECUTED_FAILED_AFTER_ATTEMPTS,
                getProcId(),
                deleteOldRegionPeerAttempted + 1,
                regionId);
            setNextState(DROP_CONSENSUS_PIPES);
            return Flow.HAS_MORE_STATE;
          }
          setNextState(DROP_CONSENSUS_PIPES);
          break;
        case DROP_CONSENSUS_PIPES:
          handler.dropConsensusPipesForRemovePeer(regionId, targetDataNode);
          setKillPoint(state);
          setNextState(REMOVE_REGION_LOCATION_CACHE);
          break;
        case REMOVE_REGION_LOCATION_CACHE:
          tsStatus = handler.removeRegionLocation(regionId, targetDataNode);
          if (tsStatus == null || tsStatus.getCode() != SUCCESS_STATUS.getStatusCode()) {
            return retryStep(
                state,
                new ProcedureException(
                    String.format(
                        ProcedureMessages
                            .EXCEPTION_FAILED_TO_FINALIZE_REGION_PEER_REMOVAL_FOR_ARG_AT_STATE_ARG_AFTER_ARG_ATTEMPTS_382EA168,
                        regionId,
                        state,
                        getCycles() + 1)));
          }
          if (!handler.isIoTConsensusV1DataRegion(regionId)) {
            return finishRemoval(state);
          }
          setKillPoint(state);
          setNextState(RESET_IOT_CONSENSUS_V1_PEER_LIST);
          break;
        case RESET_IOT_CONSENSUS_V1_PEER_LIST:
          if (!handler.resetPeerListAfterRemoveForIoTConsensusV1(regionId, targetDataNode)) {
            // The replica metadata is already committed. Peer list reconciliation is best effort.
            return retryStep(state, null);
          }
          return finishRemoval(state);
        default:
          throw new ProcedureException(ProcedureMessages.UNSUPPORTED_STATE + state.name());
      }
    } catch (Exception e) {
      LOGGER.error(ProcedureMessages.REMOVEREGIONPEER_STATE_FAILED, state, e);
      if (e instanceof InterruptedException) {
        // The wait was interrupted because this ConfigNode is stopping or losing leadership.
        // Keep the state for recovery instead of treating it as a remove-peer failure.
        Thread.currentThread().interrupt();
        if (state == REMOVE_REGION_PEER) {
          setNextState(REMOVE_REGION_PEER);
          return Flow.HAS_MORE_STATE;
        }
        return Flow.NO_MORE_STATE;
      }
      if (state == REMOVE_REGION_PEER) {
        // Any other error in the remove-peer phase must not fall through to the delete phase.
        rollbackAndFail(env, e);
        return Flow.NO_MORE_STATE;
      }
      if (state == REMOVE_REGION_LOCATION_CACHE) {
        return retryStep(state, new ProcedureException(e));
      }
      if (state == RESET_IOT_CONSENSUS_V1_PEER_LIST) {
        return retryStep(state, null);
      }
      return Flow.NO_MORE_STATE;
    }
    LOGGER.info(ProcedureMessages.PID_REMOVEREGION_STATE_SUCCESS, getProcId(), state);
    return Flow.HAS_MORE_STATE;
  }

  /**
   * Uses the shared retry budget for the remove-peer task. When a retry cannot help, or the budget
   * is exhausted, the source region data must not be deleted: the peer list of the region's current
   * replica set is reset so the source replica becomes writable again, and the procedure fails.
   */
  private Flow retryRemovePeerStep(
      ConfigNodeProcedureEnv env, RemoveRegionPeerState state, Object reason, boolean retryable)
      throws InterruptedException {
    if (!retryable) {
      rollbackAndFail(env, reason);
      return Flow.NO_MORE_STATE;
    }
    final Flow flow = retryStep(state, null);
    if (flow == Flow.NO_MORE_STATE) {
      rollbackAndFail(env, reason);
    }
    return flow;
  }

  private void rollbackAndFail(ConfigNodeProcedureEnv env, Object reason) {
    LOGGER.warn(
        ProcedureMessages
            .LOG_NO_MORE_RETRY_FOR_REMOVEPEER_ROLLING_BACK_REGION_ARG_AND_KEEPING_THE_SOURCE_DATA_REASON_ARG_8946EDE8,
        regionId,
        reason);
    rollbackPeerListOnAbort(env);
    setFailure(
        new ProcedureException(
            String.format(
                ProcedureMessages
                    .EXCEPTION_REMOVEPEER_FOR_REGION_ARG_FAILED_AND_THE_SOURCE_REGION_DATA_IS_KEPT_ARG_FDC2F5CF,
                regionId,
                reason)));
  }

  private static boolean isDisconnectFailure(TRegionMigrateResult result) {
    Map<TDataNodeLocation, TRegionMigrateFailedType> failedNodes = result.getFailedNodeAndReason();
    return failedNodes != null
        && failedNodes.values().stream()
            .anyMatch(failedType -> failedType == TRegionMigrateFailedType.Disconnect);
  }

  /**
   * Rolls the region back to the replica set currently recorded by ConfigNode. Resetting all
   * replicas (not only the source) also restores the sync channels and configuration entries that
   * notifyPeersToRemoveSyncLogChannel removed, and reactivates the local peer if a previous
   * remove-peer attempt fenced it.
   */
  private void rollbackPeerListOnAbort(ConfigNodeProcedureEnv env) {
    Map<Integer, TSStatus> responses = null;
    try {
      RegionMaintainHandler handler = env.getRegionMaintainHandler();
      Optional<TRegionReplicaSet> replicaSet = handler.getRegionReplicaSet(regionId);
      if (!replicaSet.isPresent()) {
        return;
      }
      List<TDataNodeLocation> correctLocations = replicaSet.get().getDataNodeLocations();
      Map<Integer, TDataNodeLocation> relatedLocations = new HashMap<>();
      correctLocations.forEach(
          location -> relatedLocations.put(location.getDataNodeId(), location));
      for (int retry = 0; retry < MAX_RESET_PEER_LIST_RETRY; retry++) {
        responses = handler.resetPeerList(regionId, correctLocations, relatedLocations);
        final TSStatus targetStatus =
            responses == null ? null : responses.get(targetDataNode.getDataNodeId());
        // Other replicas may be offline (e.g. a dead coordinator). A restarted DataNode rebuilds
        // its peer list from the CN partition table (DataNode.prepareToResetDataRegionPeerList ->
        // recordCorrectPeerListBeforeStarting -> resetPeerListImpl), so only the source replica
        // must be confirmed writable here; remaining failures are logged below.
        if (targetStatus != null && targetStatus.getCode() == SUCCESS_STATUS.getStatusCode()) {
          LOGGER.info(
              ProcedureMessages
                  .LOG_RESET_THE_PEER_LIST_FOR_REGION_ARG_AFTER_REMOVEPEER_FAILED_THE_SOURCE_REPLICA_IS_WRITABLE_AGAIN_FABA0DE9,
              regionId);
          handler.forceUpdateRegionCache(regionId, targetDataNode, RegionStatus.Running);
          final Map<Integer, TSStatus> unresolved = new HashMap<>();
          for (TDataNodeLocation location : correctLocations) {
            final TSStatus status = responses.get(location.getDataNodeId());
            if (status == null || status.getCode() != SUCCESS_STATUS.getStatusCode()) {
              unresolved.put(location.getDataNodeId(), status);
            }
          }
          if (!unresolved.isEmpty()) {
            LOGGER.warn(
                ProcedureMessages
                    .LOG_RESET_THE_PEER_LIST_FOR_REGION_ARG_AFTER_REMOVEPEER_FAILED_THE_SOURCE_REPLICA_IS_WRITABLE_AGAIN_BUT_SOME_REPLICAS_DID_NOT_RESET_ARG_3E704184,
                regionId,
                unresolved);
          }
          return;
        }
        Thread.sleep(RESET_PEER_LIST_RETRY_INTERVAL_MS);
      }
      LOGGER.warn(
          ProcedureMessages
              .LOG_FAILED_TO_RESET_THE_PEER_LIST_FOR_REGION_ARG_AFTER_REMOVEPEER_FAILED_THE_SOURCE_REPLICA_MAY_STAY_READ_ONLY_ARG_54DA709D,
          regionId,
          responses);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          ProcedureMessages
              .LOG_FAILED_TO_RESET_THE_PEER_LIST_FOR_REGION_ARG_AFTER_REMOVEPEER_FAILED_THE_SOURCE_REPLICA_MAY_STAY_READ_ONLY_ARG_54DA709D,
          regionId,
          e);
    } catch (Exception e) {
      LOGGER.warn(
          ProcedureMessages
              .LOG_FAILED_TO_RESET_THE_PEER_LIST_FOR_REGION_ARG_AFTER_REMOVEPEER_FAILED_THE_SOURCE_REPLICA_MAY_STAY_READ_ONLY_ARG_54DA709D,
          regionId,
          e);
    }
  }

  private Flow retryStep(
      RemoveRegionPeerState state, @Nullable ProcedureException failureOnExhaustion) {
    if (getCycles() >= MAX_FINALIZATION_RETRY) {
      if (failureOnExhaustion != null) {
        setFailure(failureOnExhaustion);
      }
      return Flow.NO_MORE_STATE;
    }
    setNextState(state);
    try {
      Thread.sleep(FINALIZATION_RETRY_INTERVAL_MS);
    } catch (InterruptedException e) {
      // Preserve the step for recovery when this ConfigNode stops or loses leadership.
      Thread.currentThread().interrupt();
    }
    return Flow.HAS_MORE_STATE;
  }

  private Flow finishRemoval(RemoveRegionPeerState state) {
    setKillPoint(state);
    LOGGER.info(ProcedureMessages.REMOVEREGIONPEER_STATE_SUCCESS, state);
    LOGGER.info(
        ProcedureMessages.PID_REMOVEREGION_SUCCESS_REGION_HAS_BEEN_REMOVED_FROM_DATANODE_PROCEDURE,
        getProcId(),
        regionId.getId(),
        targetDataNode.getDataNodeId(),
        CommonDateTimeUtils.convertMillisecondToDurationStr(
            System.currentTimeMillis() - getSubmittedTime()),
        DateTimeUtils.convertLongToDate(getSubmittedTime(), "ms"));
    return Flow.NO_MORE_STATE;
  }

  @Override
  protected void rollbackState(ConfigNodeProcedureEnv env, RemoveRegionPeerState state)
      throws IOException, InterruptedException, ProcedureException {}

  @Override
  protected RemoveRegionPeerState getState(int stateId) {
    return RemoveRegionPeerState.values()[stateId];
  }

  @Override
  protected int getStateId(RemoveRegionPeerState RemoveRegionPeerState) {
    return RemoveRegionPeerState.ordinal();
  }

  @Override
  protected RemoveRegionPeerState getInitialState() {
    return RemoveRegionPeerState.TRANSFER_REGION_LEADER;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    stream.writeShort(ProcedureType.REMOVE_REGION_PEER_PROCEDURE.getTypeCode());
    super.serialize(stream);
    ThriftCommonsSerDeUtils.serializeTConsensusGroupId(regionId, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(targetDataNode, stream);
    ThriftCommonsSerDeUtils.serializeTDataNodeLocation(coordinator, stream);
  }

  @Override
  public void deserialize(ByteBuffer byteBuffer) {
    super.deserialize(byteBuffer);
    try {
      regionId = ThriftCommonsSerDeUtils.deserializeTConsensusGroupId(byteBuffer);
      targetDataNode = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
      coordinator = ThriftCommonsSerDeUtils.deserializeTDataNodeLocation(byteBuffer);
    } catch (ThriftSerDeException e) {
      LOGGER.error(ProcedureMessages.ERROR_IN_DESERIALIZE, this.getClass(), e);
    }
  }

  public TDataNodeLocation getCoordinator() {
    return coordinator;
  }

  public TDataNodeLocation getTargetDataNode() {
    return targetDataNode;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof RemoveRegionPeerProcedure)) {
      return false;
    }
    RemoveRegionPeerProcedure procedure = (RemoveRegionPeerProcedure) obj;
    return this.regionId.equals(procedure.regionId)
        && this.targetDataNode.equals(procedure.targetDataNode)
        && this.coordinator.equals(procedure.coordinator);
  }

  @Override
  public int hashCode() {
    return Objects.hash(regionId, targetDataNode, coordinator);
  }
}
