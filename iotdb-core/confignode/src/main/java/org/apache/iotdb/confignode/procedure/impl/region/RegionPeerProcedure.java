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
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.utils.KillPoint.RegionMaintainKillPoints;
import org.apache.iotdb.confignode.i18n.ProcedureMessages;
import org.apache.iotdb.confignode.procedure.Procedure;
import org.apache.iotdb.confignode.procedure.env.ConfigNodeProcedureEnv;
import org.apache.iotdb.confignode.procedure.env.RegionMaintainHandler;
import org.apache.iotdb.confignode.procedure.exception.ProcedureException;
import org.apache.iotdb.mpp.rpc.thrift.TRegionMigrateResult;

import java.util.function.Supplier;

import static org.apache.iotdb.commons.utils.KillPoint.KillPoint.setKillPoint;
import static org.apache.iotdb.rpc.TSStatusCode.SUCCESS_STATUS;

abstract class RegionPeerProcedure<TState extends Enum<TState>>
    extends RegionOperationProcedure<TState> {
  RegionPeerProcedure() {}

  RegionPeerProcedure(TConsensusGroupId regionId) {
    super(regionId);
  }

  @Override
  protected Flow executeFromState(ConfigNodeProcedureEnv env, TState state)
      throws InterruptedException {
    if (regionId == null) {
      return Flow.NO_MORE_STATE;
    }
    if (!hasParent()) {
      return executePeerState(env, state, null);
    }
    Procedure<?> parent = getParentProcedure(env);
    if (isVerifiedMigration(parent)) {
      return executePeerState(env, state, getParentProcId());
    }
    // Reconstruct also uses these children. Only an unverified v1 migration must stop on recovery.
    if ((parent == null || parent instanceof RegionMigrateProcedure)
        && env.getRegionMaintainHandler().isIoTConsensusV1DataRegion(regionId)) {
      setFailure(
          new ProcedureException(
              String.format(
                  ProcedureMessages
                      .EXCEPTION_CANNOT_CONTINUE_LEGACY_MIGRATION_OF_ARG_WITHOUT_A_VERIFIED_HANDOFF_34FDA439,
                  regionId)));
      return Flow.NO_MORE_STATE;
    }
    return executePeerState(env, state, null);
  }

  protected final Procedure<?> getParentProcedure(ConfigNodeProcedureEnv env) {
    return env.getConfigManager()
        .getProcedureManager()
        .getExecutor()
        .getProcedures()
        .get(getParentProcId());
  }

  protected static boolean isVerifiedMigration(Procedure<?> parent) {
    return parent instanceof RegionMigrateProcedure
        && ((RegionMigrateProcedure) parent).usesVerifiedHandoff();
  }

  protected abstract Flow executePeerState(
      ConfigNodeProcedureEnv env, TState state, Long migrationId) throws InterruptedException;

  Flow runVerifiedTask(
      RegionMaintainHandler handler,
      TState state,
      TState nextState,
      TDataNodeLocation coordinator,
      Supplier<TSStatus> submit,
      RegionMaintainKillPoints pollingKillPoint)
      throws ProcedureException {
    for (int attempt = 0; ; attempt++) {
      TSStatus submission = submit.get();
      setKillPoint(state);
      if (submission.getCode() != SUCCESS_STATUS.getStatusCode()) {
        throw verificationFailure(state, submission);
      }
      TRegionMigrateResult result =
          handler.waitTaskFinish(getProcId(), coordinator, pollingKillPoint);
      switch (result.getTaskStatus()) {
        case TASK_NOT_EXIST:
          // A restarted coordinator lost its in-memory task table. Replay the same operation so
          // that the DN can recover its durable handoff, including its proof of completion.
          if (attempt == 0) {
            continue;
          }
          throw verificationFailure(state, result);
        case PROCESSING:
          setNextState(state);
          return Flow.HAS_MORE_STATE;
        case SUCCESS:
          if (result.isSetVerifiedMigrationId()
              && result.getVerifiedMigrationId() == getParentProcId()) {
            setNextState(nextState);
            return Flow.HAS_MORE_STATE;
          }
          // An older DN can return a plain success after ignoring the optional migration ID.
          throw verificationFailure(state, result);
        default:
          throw verificationFailure(state, result);
      }
    }
  }

  private ProcedureException verificationFailure(TState state, Object result) {
    return new ProcedureException(
        String.format(
            ProcedureMessages.EXCEPTION_MIGRATION_ARG_FOR_ARG_COULD_NOT_VERIFY_ARG_ARG_AAF21262,
            getParentProcId(),
            regionId,
            state,
            result));
  }
}
