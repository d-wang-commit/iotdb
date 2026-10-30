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
package org.apache.iotdb.confignode.manager.load.cache;

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.confignode.conf.ReplicationBacklogConfig;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;
import org.apache.iotdb.mpp.rpc.thrift.TRegionReplicationBacklog;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogEdge;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogReport;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReplicationBacklogCacheTest {
  private static final TConsensusGroupId GROUP =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 7);
  private static final Set<Integer> MEMBERS = new HashSet<>(Arrays.asList(1, 2, 3));
  private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

  private static ReplicationBacklogConfig config(String mode) throws Exception {
    Properties properties = new Properties();
    properties.setProperty(ReplicationBacklogConfig.PREFIX + "mode", mode);
    for (String type : Arrays.asList("request", "tablet", "file", "deletion")) {
      properties.setProperty(ReplicationBacklogConfig.PREFIX + type + "_gap", "10");
    }
    return ReplicationBacklogConfig.load(properties, ReplicationBacklogConfig.defaults());
  }

  private static Map<Integer, NodeStatistics> nodes(Set<Integer> members) {
    Map<Integer, NodeStatistics> result = new HashMap<>();
    members.forEach(id -> result.put(id, new NodeStatistics(NodeStatus.Running)));
    return result;
  }

  private static Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regions(
      Set<Integer> members) {
    Map<Integer, RegionStatistics> result = new HashMap<>();
    members.forEach(id -> result.put(id, new RegionStatistics(RegionStatus.Running)));
    return Collections.singletonMap(GROUP, result);
  }

  private static ReplicationBacklogCache cache(String mode) throws Exception {
    ReplicationBacklogCache cache = new ReplicationBacklogCache();
    cache.configure(config(mode));
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), nodes(MEMBERS), regions(MEMBERS), 0);
    return cache;
  }

  private static TReplicationBacklogEdge edge(
      int target, long requests, long tablets, long files, long deletes) {
    return new TReplicationBacklogEdge()
        .setTargetNodeId(target)
        .setGeneration(1)
        .setRequestCount(requests)
        .setTabletCount(tablets)
        .setFileCount(files)
        .setDeletionCount(deletes)
        .setState((byte) 0);
  }

  private static TReplicationBacklogReport report(
      int source, long sequence, byte protocol, TReplicationBacklogEdge... edges) {
    return new TReplicationBacklogReport()
        .setSourceInstanceId("boot-" + source)
        .setSampleSequence(sequence)
        .setSampleAgeNanos(0)
        .setGroups(
            Collections.singletonList(
                new TRegionReplicationBacklog()
                    .setRegionId(7)
                    .setProtocol(protocol)
                    .setGeneration(1)
                    .setState((byte) 0)
                    .setTargets(Arrays.asList(edges))));
  }

  private static TReplicationBacklogReport v1(int source, long sequence) {
    if (source == 1) {
      return report(source, sequence, (byte) 1, edge(2, 5, 0, 0, 0), edge(3, 10, 0, 0, 0));
    }
    if (source == 2) {
      return report(source, sequence, (byte) 1, edge(1, 30, 0, 0, 0), edge(3, 10, 0, 0, 0));
    }
    return report(source, sequence, (byte) 1, edge(1, 0, 0, 0, 0), edge(2, 0, 0, 0, 0));
  }

  private static void round(ReplicationBacklogCache cache, int seq) {
    long time = seq * 5 * SECOND;
    for (int source : MEMBERS) {
      cache.cacheReport(source, v1(source, seq), time, cache.getEpoch(), time);
    }
    cache.evaluate(time);
  }

  @Test
  public void recoveredLightReplicaWaitsForThreeNewCompleteRounds() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    recover(cache, 2);
    assertEquals(Set.of(1, 3), cache.eligibleCandidates(22 * SECOND).get(GROUP));
    assertTrue(cache.getDiagnostics(7, 22 * SECOND).get(2).isAdmissionPending());
    assertFalse(cache.getDiagnostics(7, 22 * SECOND).get(1).isAdmissionPending());
    ReplicationBacklogCache.SelectionSnapshot pending = cache.captureSelection(22 * SECOND);
    assertTrue(cache.isSelectionValid(pending, 22 * SECOND));
    for (int seq = 5; seq <= 6; seq++) {
      round(cache, seq);
      // Neither repeated evaluations nor duplicate reports provide another confirmation.
      for (int i = 0; i < 10; i++) {
        round(cache, seq);
      }
      assertEquals(Set.of(1, 3), cache.eligibleCandidates(seq * 5 * SECOND).get(GROUP));
    }
    round(cache, 7);
    assertFalse(cache.getDiagnostics(7, 35 * SECOND).get(2).isAdmissionPending());
    assertEquals(Set.of(2), cache.eligibleCandidates(35 * SECOND).get(GROUP));
    assertFalse(cache.isSelectionValid(pending, 35 * SECOND));
  }

  @Test
  public void confirmingHeavyRecoveryDoesNotAdmitIt() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    recover(cache, 1);
    assertEquals(Set.of(2, 3), cache.eligibleCandidates(22 * SECOND).get(GROUP));
    for (int seq = 5; seq <= 7; seq++) {
      round(cache, seq);
    }
    assertFalse(cache.getDiagnostics(7, 35 * SECOND).get(1).isAdmissionPending());
    assertTrue(cache.getDiagnostics(7, 35 * SECOND).get(1).isHeavy());
    assertEquals(Set.of(2), cache.eligibleCandidates(35 * SECOND).get(GROUP));
  }

  @Test
  public void missingOrExpiredTelemetryDoesNotOpenRecoveryGate() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    recover(cache, 2);
    round(cache, 5);
    // One invalid source breaks confirmation; it must not readmit the recovering replica.
    cache.cacheReport(3, null, 26 * SECOND, cache.getEpoch(), 26 * SECOND);
    cache.evaluate(26 * SECOND);
    assertEquals(Set.of(1, 3), cache.eligibleCandidates(26 * SECOND).get(GROUP));
    ReplicationBacklogCache.SelectionSnapshot pending = cache.captureSelection(26 * SECOND);
    assertTrue(cache.isSelectionValid(pending, 26 * SECOND));
    cache.evaluate(60 * SECOND);
    assertEquals(Set.of(1, 3), cache.eligibleCandidates(60 * SECOND).get(GROUP));
    assertTrue(cache.isSelectionValid(pending, 60 * SECOND));
    // Source reinitialization establishes a baseline, then three new rounds are required.
    for (int seq = 13; seq <= 15; seq++) {
      round(cache, seq);
      assertTrue(cache.getDiagnostics(7, seq * 5 * SECOND).get(2).isAdmissionPending());
    }
    round(cache, 16);
    assertFalse(cache.getDiagnostics(7, 80 * SECOND).get(2).isAdmissionPending());
  }

  @Test
  public void delayedPreRecoverySamplesCannotConfirmAdmission() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    recover(cache, 2);
    for (int seq = 5; seq <= 7; seq++) {
      long now = seq * 5 * SECOND;
      for (int source : MEMBERS) {
        cache.cacheReport(
            source,
            v1(source, seq).setSampleAgeNanos(now - 20 * SECOND),
            now,
            cache.getEpoch(),
            now);
      }
      cache.evaluate(now);
      assertTrue(cache.getDiagnostics(7, now).get(2).isAdmissionPending());
      assertEquals(Set.of(1, 3), cache.eligibleCandidates(now).get(GROUP));
    }
    for (int seq = 8; seq <= 10; seq++) {
      round(cache, seq);
      assertEquals(seq < 10, cache.getDiagnostics(7, seq * 5 * SECOND).get(2).isAdmissionPending());
    }
  }

  @Test
  public void restartIdentityClosesAdmissionWithoutAnOfflineHeartbeat() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    TReplicationBacklogReport restarted = v1(2, 1).setSourceInstanceId("restarted");
    cache.cacheReport(2, restarted, 25 * SECOND, cache.getEpoch(), 25 * SECOND);
    assertTrue(cache.getDiagnostics(7, 25 * SECOND).get(2).isAdmissionPending());
    assertEquals(Set.of(1, 3), cache.eligibleCandidates(25 * SECOND).get(GROUP));
    // A delayed response from the previous process must not reopen or replace the new instance.
    cache.cacheReport(2, v1(2, 100), 24 * SECOND, cache.getEpoch(), 26 * SECOND);
    assertEquals(Set.of(1, 3), cache.eligibleCandidates(26 * SECOND).get(GROUP));
    for (int seq = 6; seq <= 8; seq++) {
      long now = seq * 5 * SECOND;
      for (int source : MEMBERS) {
        TReplicationBacklogReport sample = v1(source, seq);
        if (source == 2) {
          sample.setSourceInstanceId("restarted");
        }
        cache.cacheReport(source, sample, now, cache.getEpoch(), now);
      }
      cache.evaluate(now);
      assertEquals(seq < 8, cache.getDiagnostics(7, now).get(2).isAdmissionPending());
    }
  }

  @Test
  public void v2PipeRecreationPreservesHealthyAdmissionDuringPeerRecovery() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int seq = 1; seq <= 7; seq++) {
      if (seq == 5) {
        recover(cache, 2);
      }
      long now = seq * 5 * SECOND;
      for (int source : MEMBERS) {
        ArrayList<TReplicationBacklogEdge> edges = new ArrayList<>();
        for (int target : MEMBERS) {
          if (source != target) {
            edges.add(
                edge(target, 0, 0, 0, 0)
                    .setGeneration(source == 1 && target == 2 && seq >= 5 ? 2 : 1));
          }
        }
        cache.cacheReport(
            source,
            report(source, seq, (byte) 2, edges.toArray(new TReplicationBacklogEdge[0])),
            now,
            cache.getEpoch(),
            now);
      }
      cache.evaluate(now);
      if (seq == 5 || seq == 6) {
        assertFalse(cache.getDiagnostics(7, now).get(1).isAdmissionPending());
        assertTrue(cache.getDiagnostics(7, now).get(2).isAdmissionPending());
        assertEquals(Set.of(1, 3), cache.eligibleCandidates(now).get(GROUP));
      }
    }
    assertFalse(cache.getDiagnostics(7, 35 * SECOND).get(2).isAdmissionPending());
    assertTrue(cache.eligibleCandidates(35 * SECOND).isEmpty());
  }

  @Test
  public void regionRecoveryOnRunningNodeClosesAdmission() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    Map<TConsensusGroupId, Map<Integer, RegionStatistics>> health = regions(MEMBERS);
    health.get(GROUP).put(2, new RegionStatistics(RegionStatus.Unknown));
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), nodes(MEMBERS), health, 21 * SECOND);
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), nodes(MEMBERS), regions(MEMBERS), 22 * SECOND);
    assertEquals(Set.of(1, 3), cache.eligibleCandidates(22 * SECOND).get(GROUP));
    assertTrue(cache.getDiagnostics(7, 22 * SECOND).get(2).isAdmissionPending());
  }

  @Test
  public void newLocalRegionGenerationClosesAdmissionWithoutHealthChange() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    TReplicationBacklogReport sample = v1(2, 5);
    sample.getGroups().get(0).setGeneration(2);
    cache.cacheReport(2, sample, 25 * SECOND, cache.getEpoch(), 25 * SECOND);
    assertEquals(Set.of(1, 3), cache.eligibleCandidates(25 * SECOND).get(GROUP));
    assertTrue(cache.getDiagnostics(7, 25 * SECOND).get(2).isAdmissionPending());
  }

  @Test
  public void membershipChangeKeepsExistingAdmissionButGatesNewReplica() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    Set<Integer> members = Set.of(1, 2, 4);
    cache.updateTopology(
        Collections.singletonMap(GROUP, members), nodes(members), regions(members), 21 * SECOND);
    assertEquals(Set.of(1, 2), cache.eligibleCandidates(21 * SECOND).get(GROUP));
    assertTrue(cache.getDiagnostics(7, 21 * SECOND).get(4).isAdmissionPending());
    assertFalse(cache.getDiagnostics(7, 21 * SECOND).get(1).isAdmissionPending());
  }

  @Test
  public void availabilityFallbackRequiresNoConfirmedAvailableReplica() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    recover(cache, 2);
    Map<Integer, NodeStatistics> health = nodes(MEMBERS);
    health.put(1, new NodeStatistics(NodeStatus.Unknown));
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), health, regions(MEMBERS), 23 * SECOND);
    assertEquals(Set.of(3), cache.eligibleCandidates(23 * SECOND).get(GROUP));
    health.put(3, new NodeStatistics(NodeStatus.Unknown));
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), health, regions(MEMBERS), 24 * SECOND);
    assertTrue(cache.eligibleCandidates(24 * SECOND).isEmpty());
    assertTrue(cache.getDiagnostics(7, 24 * SECOND).get(2).isAdmissionPending());
    // Bootstrap likewise cannot wait for an already confirmed replica that does not exist yet.
    assertTrue(cache("ENFORCE").eligibleCandidates(0).isEmpty());
  }

  @Test
  public void observeAndOffDoNotEnforceRecoveryGate() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    recover(cache, 2);
    cache.configure(config("OBSERVE"));
    assertTrue(cache.getDiagnostics(7, 22 * SECOND).get(2).isAdmissionPending());
    assertTrue(cache.eligibleCandidates(22 * SECOND).isEmpty());
    cache.configure(config("ENFORCE"));
    assertEquals(Set.of(1, 3), cache.eligibleCandidates(22 * SECOND).get(GROUP));
    cache.configure(config("OFF"));
    assertTrue(cache.eligibleCandidates(22 * SECOND).isEmpty());
  }

  private static ReplicationBacklogCache warmedCache() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int seq = 1; seq <= 4; seq++) {
      round(cache, seq);
    }
    return cache;
  }

  private static void recover(ReplicationBacklogCache cache, int node) {
    Map<Integer, NodeStatistics> health = nodes(MEMBERS);
    health.put(node, new NodeStatistics(NodeStatus.Unknown));
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), health, regions(MEMBERS), 21 * SECOND);
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), nodes(MEMBERS), regions(MEMBERS), 22 * SECOND);
  }

  @Test
  public void multiSourceIncomingAggregationAndObserveMode() throws Exception {
    ReplicationBacklogCache cache = cache("OBSERVE");
    for (int i = 1; i <= 4; i++) {
      round(cache, i);
    }
    Map<Integer, ReplicationBacklogCache.ReplicaDiagnostic> diagnostics =
        cache.getDiagnostics(7, 20 * SECOND);
    assertTrue(diagnostics.get(1).isKnown());
    assertEquals(30, diagnostics.get(1).getCounts()[0], 0);
    assertEquals(5, diagnostics.get(2).getCounts()[0], 0);
    assertEquals(20, diagnostics.get(3).getCounts()[0], 0);
    assertTrue(diagnostics.get(1).isHeavy());
    assertFalse(diagnostics.get(2).isHeavy());
    assertTrue(diagnostics.get(3).isHeavy());
    assertTrue(cache.eligibleCandidates(20 * SECOND).isEmpty());
    cache.configure(config("ENFORCE"));
    assertEquals(Collections.singleton(2), cache.eligibleCandidates(20 * SECOND).get(GROUP));
  }

  @Test
  public void repeatedTicksAreNotNewObservations() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    round(cache, 1);
    round(cache, 2);
    for (int i = 0; i < 20; i++) {
      cache.evaluate(11 * SECOND);
    }
    assertTrue(cache.eligibleCandidates(11 * SECOND).isEmpty());
    round(cache, 3);
    assertTrue(cache.eligibleCandidates(15 * SECOND).isEmpty());
    round(cache, 4);
    assertEquals(Collections.singleton(2), cache.eligibleCandidates(20 * SECOND).get(GROUP));
  }

  @Test
  public void missingAndDuplicateSamplesHoldButNeverRefreshExpiry() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int i = 1; i <= 4; i++) {
      round(cache, i);
    }
    for (int sequence = 5; sequence <= 9; sequence++) {
      long now = sequence * 5 * SECOND;
      cache.cacheReport(1, v1(1, sequence), now, cache.getEpoch(), now);
      cache.cacheReport(2, v1(2, sequence), now, cache.getEpoch(), now);
      // source 3 repeatedly returns the same cached snapshot. This is not fresh evidence.
      cache.cacheReport(3, v1(3, 4), now, cache.getEpoch(), now);
      cache.evaluate(now);
      assertEquals(Collections.singleton(2), cache.eligibleCandidates(now).get(GROUP));
    }
    cache.evaluate(50 * SECOND);
    assertTrue(cache.eligibleCandidates(50 * SECOND).isEmpty());
    assertFalse(cache.getDiagnostics(7, 50 * SECOND).get(1).isKnown());
  }

  @Test
  public void v2ClassificationKeepsIncomparableReplicas() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int seq = 1; seq <= 4; seq++) {
      long now = seq * 5 * SECOND;
      cache.cacheReport(
          1,
          report(1, seq, (byte) 2, edge(2, 0, 1000, 5, 20), edge(3, 0, 500, 8, 20)),
          now,
          cache.getEpoch(),
          now);
      cache.cacheReport(
          2,
          report(2, seq, (byte) 2, edge(1, 0, 5000, 20, 100), edge(3, 0, 0, 0, 0)),
          now,
          cache.getEpoch(),
          now);
      cache.cacheReport(
          3,
          report(3, seq, (byte) 2, edge(1, 0, 0, 0, 0), edge(2, 0, 0, 0, 0)),
          now,
          cache.getEpoch(),
          now);
      cache.evaluate(now);
    }
    assertEquals(
        new HashSet<>(Arrays.asList(2, 3)), cache.eligibleCandidates(20 * SECOND).get(GROUP));
  }

  @Test
  public void allHighAndEqualKeepsAllCandidates() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int seq = 1; seq <= 5; seq++) {
      long now = seq * 5 * SECOND;
      for (int source : MEMBERS) {
        ArrayList<TReplicationBacklogEdge> edges = new ArrayList<>();
        MEMBERS.stream()
            .filter(target -> target != source)
            .forEach(target -> edges.add(edge(target, 1000000, 0, 0, 0)));
        cache.cacheReport(
            source,
            report(source, seq, (byte) 1, edges.toArray(new TReplicationBacklogEdge[0])),
            now,
            cache.getEpoch(),
            now);
      }
      cache.evaluate(now);
    }
    assertTrue(cache.eligibleCandidates(25 * SECOND).isEmpty());
    assertEquals(2000000, cache.getDiagnostics(7, 25 * SECOND).get(1).getCounts()[0], 0);
  }

  @Test
  public void invalidCountersAndMembershipAreUnknownNotZero() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int i = 1; i <= 4; i++) {
      round(cache, i);
    }
    TReplicationBacklogReport corrupt = v1(3, 5);
    corrupt.getGroups().get(0).getTargets().get(0).setRequestCount(-1);
    cache.cacheReport(3, corrupt, 25 * SECOND, cache.getEpoch(), 25 * SECOND);
    cache.evaluate(25 * SECOND);
    assertTrue(cache.eligibleCandidates(25 * SECOND).isEmpty());
    assertFalse(cache.getDiagnostics(7, 25 * SECOND).get(1).isKnown());
    TReplicationBacklogReport wrongMember = v1(3, 6);
    wrongMember.getGroups().get(0).getTargets().get(0).setTargetNodeId(99);
    cache.cacheReport(3, wrongMember, 30 * SECOND, cache.getEpoch(), 30 * SECOND);
    assertFalse(cache.getDiagnostics(7, 30 * SECOND).get(1).isKnown());
  }

  @Test
  public void serviceEpochAndOutOfOrderBootResponsesAreRejected() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int i = 1; i <= 4; i++) {
      round(cache, i);
    }
    long oldEpoch = cache.getEpoch();
    cache.reset();
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), nodes(MEMBERS), regions(MEMBERS), 21 * SECOND);
    cache.cacheReport(1, v1(1, 100), 22 * SECOND, oldEpoch, 22 * SECOND);
    assertFalse(cache.getDiagnostics(7, 22 * SECOND).get(2).isKnown());
    TReplicationBacklogReport reboot = v1(1, 1).setSourceInstanceId("new-boot");
    cache.cacheReport(1, reboot, 25 * SECOND, cache.getEpoch(), 25 * SECOND);
    TReplicationBacklogReport lateOldBoot = v1(1, 100);
    lateOldBoot.getGroups().get(0).getTargets().get(0).setRequestCount(9999);
    cache.cacheReport(1, lateOldBoot, 24 * SECOND, cache.getEpoch(), 26 * SECOND);
    assertEquals(5, cache.getDiagnostics(7, 26 * SECOND).get(2).getCounts()[0], 0);
  }

  @Test
  public void topologyAndAvailabilityChangesWithdrawExclusions() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int i = 1; i <= 4; i++) {
      round(cache, i);
    }
    Map<Integer, NodeStatistics> health = nodes(MEMBERS);
    health.put(2, new NodeStatistics(NodeStatus.Unknown));
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), health, regions(MEMBERS), 21 * SECOND);
    assertTrue(cache.eligibleCandidates(21 * SECOND).isEmpty());
    Set<Integer> newMembers = new HashSet<>(Arrays.asList(1, 2, 4));
    cache.updateTopology(
        Collections.singletonMap(GROUP, newMembers),
        nodes(newMembers),
        regions(newMembers),
        22 * SECOND);
    assertFalse(cache.getDiagnostics(7, 22 * SECOND).containsKey(3));
    assertFalse(cache.getDiagnostics(7, 22 * SECOND).get(4).isKnown());
    cache.updateTopology(
        Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap(), 23 * SECOND);
    assertTrue(cache.getDiagnostics(7, 23 * SECOND).isEmpty());
  }

  @Test
  public void elapsedSampleAgeIsNotRefreshedAtReceipt() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    TReplicationBacklogReport report = v1(1, 1).setSampleAgeNanos(29 * SECOND);
    cache.cacheReport(1, report, SECOND, cache.getEpoch(), 3 * SECOND);
    assertFalse(cache.getDiagnostics(7, 3 * SECOND).get(2).isKnown());
  }

  @Test
  public void ewmaUpdatesOnlyOnUniqueSamplesAndResetsForNewPipeGeneration() throws Exception {
    ReplicationBacklogCache cache = cache("OBSERVE");
    TReplicationBacklogReport report = v1(1, 1);
    report.getGroups().get(0).getTargets().get(0).setRequestCount(100);
    cache.cacheReport(1, report, SECOND, cache.getEpoch(), SECOND);
    TReplicationBacklogReport spike = v1(1, 2);
    spike.getGroups().get(0).getTargets().get(0).setRequestCount(1000);
    cache.cacheReport(1, spike, 2 * SECOND, cache.getEpoch(), 2 * SECOND);
    assertEquals(550, cache.getDiagnostics(7, 2 * SECOND).get(2).getSmoothedCounts()[0], 0);
    cache.cacheReport(1, spike, 3 * SECOND, cache.getEpoch(), 3 * SECOND);
    assertEquals(550, cache.getDiagnostics(7, 3 * SECOND).get(2).getSmoothedCounts()[0], 0);
    TReplicationBacklogReport restarted = v1(1, 3);
    restarted.getGroups().get(0).getTargets().get(0).setGeneration(2);
    cache.cacheReport(1, restarted, 4 * SECOND, cache.getEpoch(), 4 * SECOND);
    assertEquals(5, cache.getDiagnostics(7, 4 * SECOND).get(2).getSmoothedCounts()[0], 0);
  }

  @Test
  public void disablingFilteringNeverRestoresOldDecisions() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int i = 1; i <= 4; i++) {
      round(cache, i);
    }
    cache.configure(config("OFF"));
    assertTrue(cache.eligibleCandidates(20 * SECOND).isEmpty());
    assertTrue(cache.getDiagnostics(7, 20 * SECOND).isEmpty());
    cache.configure(config("ENFORCE"));
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), nodes(MEMBERS), regions(MEMBERS), 20 * SECOND);
    assertTrue(cache.eligibleCandidates(20 * SECOND).isEmpty());
    assertFalse(cache.getDiagnostics(7, 20 * SECOND).get(1).isKnown());
  }

  @Test
  public void topologyAndUnchangedSamplesDoNotRequestAnotherBalance() throws Exception {
    ReplicationBacklogCache cache = cache("OBSERVE");
    assertFalse(cache.consumeDirty());
    for (int i = 1; i <= 4; i++) {
      round(cache, i);
    }
    assertTrue(cache.consumeDirty());
    round(cache, 5);
    assertFalse(cache.consumeDirty());
    cache.configure(config("ENFORCE"));
    assertTrue(cache.consumeDirty());
    assertEquals(Collections.singleton(2), cache.eligibleCandidates(25 * SECOND).get(GROUP));
    cache.configure(config("OFF"));
    assertTrue(cache.consumeDirty());
    assertTrue(cache.eligibleCandidates(25 * SECOND).isEmpty());
  }

  @Test
  public void malformedEnvelopeInvalidatesSourceButAnOlderCallbackCannot() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int i = 1; i <= 4; i++) {
      round(cache, i);
    }
    TReplicationBacklogReport malformed = v1(1, 5).setSampleAgeNanos(-1);
    cache.cacheReport(1, malformed, 19 * SECOND, cache.getEpoch(), 21 * SECOND);
    assertEquals(Collections.singleton(2), cache.eligibleCandidates(21 * SECOND).get(GROUP));
    cache.cacheReport(1, malformed, 25 * SECOND, cache.getEpoch(), 25 * SECOND);
    assertTrue(cache.eligibleCandidates(25 * SECOND).isEmpty());
    assertFalse(cache.getDiagnostics(7, 25 * SECOND).get(2).isKnown());
  }

  @Test
  public void hysteresisKeepsIntermediateDifferenceThenRequiresThreeReleaseRounds()
      throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    Properties parameters = new Properties();
    parameters.setProperty(ReplicationBacklogConfig.PREFIX + "ewma_alpha", "1");
    cache.configure(ReplicationBacklogConfig.load(parameters, config("ENFORCE")));
    cache.updateTopology(
        Collections.singletonMap(GROUP, MEMBERS), nodes(MEMBERS), regions(MEMBERS), 0);
    for (int i = 1; i <= 4; i++) {
      round(cache, i);
    }
    for (int sequence = 5; sequence <= 10; sequence++) {
      long now = sequence * 5 * SECOND;
      for (int source : MEMBERS) {
        TReplicationBacklogReport sample = v1(source, sequence);
        if (source == 2) {
          sample.getGroups().get(0).getTargets().get(0).setRequestCount(sequence <= 7 ? 12 : 8);
        }
        cache.cacheReport(source, sample, now, cache.getEpoch(), now);
      }
      cache.evaluate(now);
      if (sequence < 10) {
        assertEquals(Collections.singleton(2), cache.eligibleCandidates(now).get(GROUP));
      } else {
        assertEquals(new HashSet<>(Arrays.asList(1, 2)), cache.eligibleCandidates(now).get(GROUP));
      }
    }
  }

  @Test
  public void solverSnapshotUsesCurrentFreshnessWithoutExpiringOnOrdinarySamples()
      throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE");
    for (int sequence = 1; sequence <= 4; sequence++) {
      round(cache, sequence);
    }
    ReplicationBacklogCache.SelectionSnapshot snapshot = cache.captureSelection(20 * SECOND);
    assertEquals(Set.of(2), snapshot.getCandidates().get(GROUP));
    assertTrue(cache.isSelectionValid(snapshot, 21 * SECOND));
    assertFalse(cache.isSelectionValid(snapshot, 50 * SECOND));
    for (int sequence = 5; sequence <= 12; sequence++) {
      long now = sequence * 5 * SECOND;
      for (int source : MEMBERS) {
        TReplicationBacklogReport sample = v1(source, sequence);
        if (source == 1) {
          sample.getGroups().get(0).getTargets().get(0).setRequestCount(sequence % 2 == 0 ? 6 : 5);
        }
        cache.cacheReport(source, sample, now, cache.getEpoch(), now);
      }
      cache.evaluate(now);
      assertTrue(cache.isSelectionValid(snapshot, now));
    }
  }

  @Test
  public void evenAnEmptySolverConstraintIsBoundToItsConfigurationAndLeadership() throws Exception {
    ReplicationBacklogCache cache = cache("OFF");
    ReplicationBacklogCache.SelectionSnapshot snapshot = cache.captureSelection(0);
    assertTrue(snapshot.getCandidates().isEmpty());
    assertTrue(cache.isSelectionValid(snapshot, 0));
    cache.configure(config("ENFORCE"));
    assertFalse(cache.isSelectionValid(snapshot, 0));
    snapshot = cache.captureSelection(0);
    cache.reset();
    assertFalse(cache.isSelectionValid(snapshot, 0));
  }
}
