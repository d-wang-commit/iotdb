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

package org.apache.iotdb.confignode.it.regionmigration.pass.daily.iotv1;

import org.apache.iotdb.commons.utils.KillPoint.KillNode;
import org.apache.iotdb.confignode.it.regionmigration.IoTDBRegionOperationReliabilityITFramework;
import org.apache.iotdb.confignode.procedure.state.AddRegionPeerState;
import org.apache.iotdb.confignode.procedure.state.RemoveRegionPeerState;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.DailyIT;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category({DailyIT.class})
@RunWith(IoTDBTestRunner.class)
public class IoTDBRegionMigrateClusterCrashIoTV1IT
    extends IoTDBRegionOperationReliabilityITFramework {

  @Override
  @Before
  public void setUp() throws Exception {
    super.setUp();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
  }

  private void restartDuringMigration(Enum<?> step, RecoveryExpectation recovery) throws Exception {
    generalTestWithAllOptions(
        2,
        3,
        3,
        3,
        buildSet(step),
        noKillPoints(),
        actionOfRestartCluster,
        false,
        KillNode.ALL_NODES,
        recovery);
  }

  @Test
  public void clusterCrash1() throws Exception {
    killClusterTest(buildSet(AddRegionPeerState.CREATE_NEW_REGION_PEER), true);
  }

  @Test
  public void clusterCrash2() throws Exception {
    restartDuringMigration(AddRegionPeerState.DO_ADD_REGION_PEER, RecoveryExpectation.ADD_FAILED);
  }

  @Test
  public void clusterCrash3() throws Exception {
    restartDuringMigration(
        AddRegionPeerState.UPDATE_REGION_LOCATION_CACHE, RecoveryExpectation.RETAIN_DESTINATION);
  }

  @Test
  public void clusterCrash4() throws Exception {
    restartDuringMigration(
        RemoveRegionPeerState.TRANSFER_REGION_LEADER, RecoveryExpectation.RETAIN_DESTINATION);
  }

  @Test
  public void clusterCrash6() throws Exception {
    restartDuringMigration(
        RemoveRegionPeerState.REMOVE_REGION_PEER, RecoveryExpectation.RESOLVE_REMOVE);
  }

  // TODO: @Yongzao Dan, reopen this CI after discussion with @HxpSerein
  //  @Test
  public void clusterCrash7() throws Exception {
    killClusterTest(buildSet(RemoveRegionPeerState.DELETE_OLD_REGION_PEER), true);
  }

  @Test
  public void clusterCrash8() throws Exception {
    killClusterTest(buildSet(RemoveRegionPeerState.REMOVE_REGION_LOCATION_CACHE), true);
  }
}
