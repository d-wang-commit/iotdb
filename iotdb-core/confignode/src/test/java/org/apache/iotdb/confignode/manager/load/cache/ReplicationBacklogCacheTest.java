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
import org.apache.iotdb.confignode.conf.ReplicationBacklogConfig;
import org.apache.iotdb.confignode.manager.load.cache.ReplicationBacklogCache.ReplicaDiagnostic;
import org.apache.iotdb.mpp.rpc.thrift.TRegionReplicationBacklog;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogEdge;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogReport;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReplicationBacklogCacheTest {
  private static final TConsensusGroupId GROUP =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 7);
  private static final Set<Integer> MEMBERS = Set.of(1, 2, 3);
  private static final Map<TConsensusGroupId, Set<Integer>> RUNNING =
      Collections.singletonMap(GROUP, MEMBERS);
  private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
  private static final long[] BACKLOG = {30, 5, 20};

  private static ReplicationBacklogConfig config(String mode, int rounds) throws Exception {
    Properties properties = new Properties();
    properties.setProperty(ReplicationBacklogConfig.PREFIX + "mode", mode);
    properties.setProperty(
        ReplicationBacklogConfig.PREFIX + "confirmation_rounds", Integer.toString(rounds));
    for (String type : Arrays.asList("request", "tablet", "file", "deletion")) {
      properties.setProperty(ReplicationBacklogConfig.PREFIX + type + "_gap", "10");
    }
    return ReplicationBacklogConfig.load(properties, ReplicationBacklogConfig.defaults());
  }

  private static ReplicationBacklogCache cache(String mode, int rounds) throws Exception {
    ReplicationBacklogCache cache = new ReplicationBacklogCache();
    cache.configure(config(mode, rounds));
    cache.updateTopology(RUNNING, 0);
    cache.evaluate(RUNNING, 0);
    return cache;
  }

  private static ReplicationBacklogCache cache() throws Exception {
    return cache("ENFORCE", 3);
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

  private static TReplicationBacklogReport v1(int source, int sequence, long[] incoming) {
    List<TReplicationBacklogEdge> edges = new ArrayList<>();
    for (int target = 1; target <= 3; target++) {
      if (target != source) {
        // Put the target's load on one incoming edge; the other edge explicitly reports zero.
        int producer = target == 1 ? 2 : 1;
        edges.add(edge(target, source == producer ? incoming[target - 1] : 0, 0, 0, 0));
      }
    }
    return report(source, sequence, (byte) 1, edges.toArray(new TReplicationBacklogEdge[0]));
  }

  private static void send(
      ReplicationBacklogCache cache, int source, TReplicationBacklogReport report, long now) {
    cache.cacheReport(source, report, now, cache.getEpoch(), now);
  }

  private static void sample(
      ReplicationBacklogCache cache,
      int sequence,
      long[] incoming,
      Map<TConsensusGroupId, Set<Integer>> running) {
    long now = sequence * 5 * SECOND;
    for (int source = 1; source <= 3; source++) {
      send(cache, source, v1(source, sequence, incoming), now);
    }
    cache.evaluate(running, now);
    cache.advanceSamplingRound(sequence);
    cache.evaluate(running, now);
  }

  private static void sample(ReplicationBacklogCache cache, int sequence) {
    sample(cache, sequence, BACKLOG, RUNNING);
  }

  private static ReplicationBacklogCache warmedCache() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 5; sequence++) {
      sample(cache, sequence);
    }
    return cache;
  }

  private static ReplicaDiagnostic diagnostic(ReplicationBacklogCache cache, int target) {
    return cache.getDiagnostics(7, 0).get(target);
  }

  @Test
  public void missingSourceContributesItsLastValuesForExactlyMRounds() throws Exception {
    for (int rounds : new int[] {1, 3}) {
      ReplicationBacklogCache cache = cache("ENFORCE", rounds);
      send(cache, 1, report(1, 1, (byte) 1, edge(2, 5, 0, 0, 0), edge(3, 10, 0, 0, 0)), SECOND);
      send(cache, 2, report(2, 1, (byte) 1, edge(1, 30, 0, 0, 0), edge(3, 10, 0, 0, 0)), SECOND);
      send(cache, 3, v1(3, 1, BACKLOG), SECOND);
      cache.advanceSamplingRound(1);
      for (int missing = 1; missing <= rounds + 1; missing++) {
        int round = missing + 1;
        long now = round * 5 * SECOND;
        send(cache, 1, report(1, round, (byte) 1, edge(2, 50, 0, 0, 0), edge(3, 4, 0, 0, 0)), now);
        send(cache, 3, v1(3, round, BACKLOG), now);
        cache.advanceSamplingRound(round);
        assertEquals(missing <= rounds ? 30 : 0, diagnostic(cache, 1).getCounts()[0], 0);
        assertEquals(50, diagnostic(cache, 2).getCounts()[0], 0);
        assertEquals(missing <= rounds ? 14 : 4, diagnostic(cache, 3).getCounts()[0], 0);
        double[] ewma = diagnostic(cache, 3).getSmoothedCounts();
        for (int call = 0; call < 10; call++) {
          cache.advanceSamplingRound(round);
          cache.evaluate(RUNNING, now);
          cache.captureSelection(now);
        }
        assertArrayEquals(ewma, diagnostic(cache, 3).getSmoothedCounts(), 0);
      }
      // Dropping an input does not erase the EWMA's history.
      assertEquals(15, diagnostic(cache, 1).getSmoothedCounts()[0], 0);
    }
  }

  @Test
  public void onlyANewValidSampleRenewsTheReuseWindow() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE", 1);
    sample(cache, 1);
    cache.advanceSamplingRound(2);
    send(cache, 2, v1(2, 2, new long[] {40, 5, 20}), 15 * SECOND);
    cache.advanceSamplingRound(3);
    assertEquals(40, diagnostic(cache, 1).getCounts()[0], 0);
    send(cache, 2, v1(2, 2, new long[] {999, 5, 20}), 20 * SECOND);
    cache.advanceSamplingRound(4);
    assertEquals(40, diagnostic(cache, 1).getCounts()[0], 0);
    TReplicationBacklogReport invalid = v1(2, 3, BACKLOG);
    invalid.getGroups().get(0).setState((byte) 3);
    send(cache, 2, invalid, 25 * SECOND);
    cache.advanceSamplingRound(5);
    assertEquals(0, diagnostic(cache, 1).getCounts()[0], 0);
    send(cache, 2, v1(2, 4, new long[] {7, 5, 20}), 30 * SECOND);
    cache.advanceSamplingRound(6);
    assertEquals(7, diagnostic(cache, 1).getCounts()[0], 0);
  }

  @Test
  public void heldInputsConfirmLightAndMissingSourceHasNoHeavyPenalty() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int round = 1; round <= 3; round++) {
      sample(cache, round, new long[] {5, 5, 5}, RUNNING);
    }
    for (int round = 4; round <= 9; round++) {
      long now = round * 5 * SECOND;
      send(cache, 1, v1(1, round, new long[] {5, 5, 5}), now);
      send(cache, 3, v1(3, round, new long[] {5, 5, 5}), now);
      cache.advanceSamplingRound(round);
      if (round >= 5) {
        for (int target : MEMBERS) {
          assertFalse(diagnostic(cache, target).isHeavy());
        }
      }
    }
  }

  @Test
  public void v2ReusesAndExpiresEachCategoryWithoutClearingOtherSources() throws Exception {
    ReplicationBacklogCache cache = cache();
    send(cache, 1, report(1, 1, (byte) 2, edge(2, 0, 5, 1, 2), edge(3, 0, 100, 2, 1)), SECOND);
    send(cache, 2, report(2, 1, (byte) 2, edge(1, 0, 30, 1, 2), edge(3, 0, 40, 3, 2)), SECOND);
    send(cache, 3, report(3, 1, (byte) 2, edge(1, 0, 0, 0, 0), edge(2, 0, 0, 0, 0)), SECOND);
    cache.advanceSamplingRound(1);
    for (int round = 2; round <= 5; round++) {
      long now = round * 5 * SECOND;
      send(cache, 1, report(1, round, (byte) 2, edge(2, 0, 5, 1, 2), edge(3, 0, 100, 2, 1)), now);
      send(cache, 3, report(3, round, (byte) 2, edge(1, 0, 0, 0, 0), edge(2, 0, 0, 0, 0)), now);
      cache.advanceSamplingRound(round);
      assertArrayEquals(
          round <= 4 ? new double[] {0, 140, 5, 3} : new double[] {0, 100, 2, 1},
          diagnostic(cache, 3).getCounts(),
          0);
      assertArrayEquals(new double[] {0, 5, 1, 2}, diagnostic(cache, 2).getCounts(), 0);
    }
  }

  @Test
  public void threeWarmupSamplesAndThreeLightVotesRequireFiveRoundObservations() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 5; sequence++) {
      sample(cache, sequence);
      assertEquals(Math.min(sequence, 3), diagnostic(cache, 2).getWarmupSamples());
      assertEquals(Math.max(0, sequence - 2), diagnostic(cache, 2).getLightStreak());
      assertEquals(sequence < 5, diagnostic(cache, 2).isHeavy());
      if (sequence < 5) {
        assertTrue(cache.eligibleCandidates(0).isEmpty());
      }
    }
    assertEquals(Set.of(2), cache.eligibleCandidates(0).get(GROUP));
    assertTrue(diagnostic(cache, 1).isHeavy());
    assertTrue(diagnostic(cache, 1).isInstantHeavy());
    assertFalse(diagnostic(cache, 2).isInstantHeavy());
    assertTrue(diagnostic(cache, 3).isHeavy());
  }

  @Test
  public void oneLightVoteStillRequiresThreeWarmupSamples() throws Exception {
    ReplicationBacklogCache cache = cache("ENFORCE", 1);
    sample(cache, 1);
    sample(cache, 2);
    assertTrue(cache.eligibleCandidates(0).isEmpty());
    sample(cache, 3);
    assertEquals(Set.of(2), cache.eligibleCandidates(0).get(GROUP));
  }

  @Test
  public void reportsOnlyUpdateInputsUntilTheNextSamplingRound() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 5; sequence++) {
      for (int source = 1; source <= 3; source++) {
        send(cache, source, v1(source, sequence, BACKLOG), sequence * 5 * SECOND);
      }
    }
    assertTrue(cache.eligibleCandidates(0).isEmpty());
    assertEquals(0, diagnostic(cache, 2).getWarmupSamples());
    cache.advanceSamplingRound(1);
    assertEquals(1, diagnostic(cache, 2).getWarmupSamples());
    assertEquals(5, diagnostic(cache, 2).getCounts()[0], 0);
    assertEquals(0, diagnostic(cache, 2).getLightStreak());
  }

  @Test
  public void anIntermediateHeavyObservationBreaksLightConfirmationWithoutEvaluation()
      throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 4; sequence++) {
      sample(cache, sequence, new long[] {10, 10, 10}, RUNNING);
    }
    assertEquals(2, diagnostic(cache, 1).getLightStreak());
    // The first new observation is H; the next two return to similar loads. No evaluate() occurs
    // between them, so a version difference alone could incorrectly count all three as light votes.
    for (int sequence = 5; sequence <= 7; sequence++) {
      long[] incoming = sequence == 5 ? new long[] {1000, 10, 10} : new long[] {0, 1000, 1000};
      for (int source = 1; source <= 3; source++) {
        send(cache, source, v1(source, sequence, incoming), sequence * 5 * SECOND);
      }
      cache.advanceSamplingRound(sequence);
    }
    assertTrue(diagnostic(cache, 1).isHeavy());
    assertEquals(2, diagnostic(cache, 1).getLightStreak());
    sample(cache, 8, new long[] {0, 1000, 1000}, RUNNING);
    assertFalse(diagnostic(cache, 1).isHeavy());
  }

  @Test
  public void repeatedReportsAndQueriesCannotAdvanceWarmupOrConfirmation() throws Exception {
    ReplicationBacklogCache cache = cache();
    sample(cache, 1);
    for (int i = 0; i < 20; i++) {
      for (int source = 1; source <= 3; source++) {
        send(cache, source, v1(source, 1, BACKLOG), 5 * SECOND + i + 1);
      }
      cache.evaluate(RUNNING, 6 * SECOND);
      cache.captureSelection(6 * SECOND);
      cache.advanceSamplingRound(1);
    }
    assertEquals(1, diagnostic(cache, 2).getWarmupSamples());
    assertEquals(0, diagnostic(cache, 2).getLightStreak());
    sample(cache, 2);
    sample(cache, 3);
    for (int i = 0; i < 20; i++) {
      cache.evaluate(RUNNING, 16 * SECOND);
      cache.eligibleCandidates(16 * SECOND);
      cache.captureSelection(16 * SECOND);
    }
    assertEquals(1, diagnostic(cache, 2).getLightStreak());
    assertTrue(diagnostic(cache, 2).isHeavy());
    sample(cache, 4);
    sample(cache, 5);
    assertFalse(diagnostic(cache, 2).isHeavy());
  }

  @Test
  public void missingInputsExpireWithoutForcingLightReplicasHeavy() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    double ewma = diagnostic(cache, 2).getSmoothedCounts()[0];
    for (int round = 6; round <= 8; round++) {
      cache.advanceSamplingRound(round);
      for (int i = 0; i < 10; i++) {
        cache.evaluate(RUNNING, 100 * SECOND);
        cache.captureSelection(100 * SECOND);
        cache.advanceSamplingRound(round);
      }
      assertEquals(round - 5, diagnostic(cache, 2).getMissingRounds());
      assertFalse(diagnostic(cache, 2).isHeavy());
      assertEquals(ewma, diagnostic(cache, 2).getSmoothedCounts()[0], 0);
    }
    assertEquals(Set.of(2), cache.eligibleCandidates(0).get(GROUP));
    cache.advanceSamplingRound(9);
    assertEquals(0, diagnostic(cache, 2).getCounts()[0], 0);
    assertEquals(ewma / 2, diagnostic(cache, 2).getSmoothedCounts()[0], 0);
    assertEquals(3, diagnostic(cache, 2).getLightStreak());
    assertTrue(diagnostic(cache, 2).isKnown());
    for (int round = 10; round <= 12; round++) {
      cache.advanceSamplingRound(round);
    }
    for (int target : MEMBERS) {
      assertFalse(diagnostic(cache, target).isHeavy());
    }
    assertTrue(cache.eligibleCandidates(0).isEmpty());
    sample(cache, 13);
    assertEquals(3, diagnostic(cache, 2).getWarmupSamples());
    assertFalse(diagnostic(cache, 2).isHeavy());
  }

  @Test
  public void heldInputsSupplyOneLightVotePerSamplingRound() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 3; sequence++) {
      sample(cache, sequence);
    }
    cache.advanceSamplingRound(4);
    cache.advanceSamplingRound(5);
    cache.evaluate(RUNNING, 25 * SECOND);
    assertEquals(3, diagnostic(cache, 2).getLightStreak());
    assertFalse(diagnostic(cache, 2).isHeavy());
    sample(cache, 6);
    assertEquals(3, diagnostic(cache, 2).getLightStreak());
    sample(cache, 7);
    assertFalse(diagnostic(cache, 2).isHeavy());
  }

  @Test
  public void samplingWithoutAnyHistoryCannotBootstrapAnObservation() throws Exception {
    ReplicationBacklogCache cache = cache();
    cache.advanceSamplingRound(10);
    assertEquals(0, diagnostic(cache, 1).getMissingRounds());
    cache.advanceSamplingRound(10);
    cache.advanceSamplingRound(9);
    assertEquals(0, diagnostic(cache, 1).getMissingRounds());
    cache.advanceSamplingRound(11);
    assertEquals(0, diagnostic(cache, 1).getWarmupSamples());
    assertFalse(diagnostic(cache, 1).isKnown());
    assertTrue(diagnostic(cache, 1).isHeavy());
  }

  @Test
  public void targetObservationsNeedOnlyItsIncomingSources() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 5; sequence++) {
      long now = sequence * 5 * SECOND;
      // dn2 never reports its output edges; its own incoming load remains fully observable.
      send(cache, 1, v1(1, sequence, BACKLOG), now);
      send(cache, 3, v1(3, sequence, BACKLOG), now);
      cache.evaluate(RUNNING, now);
      cache.advanceSamplingRound(sequence);
    }
    assertEquals(Set.of(1, 2), cache.eligibleCandidates(0).get(GROUP));
    assertTrue(diagnostic(cache, 1).isKnown());
    assertTrue(diagnostic(cache, 3).isKnown());
    assertEquals(5, diagnostic(cache, 2).getCounts()[0], 0);
  }

  @Test
  public void alternatingMissingSourcesReuseTheirInputsOncePerRound() throws Exception {
    ReplicationBacklogCache cache = cache();
    send(cache, 1, v1(1, 1, BACKLOG), 5 * SECOND);
    send(cache, 3, v1(3, 1, BACKLOG), 5 * SECOND);
    cache.advanceSamplingRound(1);
    assertEquals(1, diagnostic(cache, 2).getWarmupSamples());
    send(cache, 1, v1(1, 2, BACKLOG), 10 * SECOND);
    cache.advanceSamplingRound(2);
    assertEquals(2, diagnostic(cache, 2).getWarmupSamples());
    send(cache, 1, v1(1, 3, BACKLOG), 15 * SECOND);
    cache.advanceSamplingRound(3);
    assertEquals(3, diagnostic(cache, 2).getWarmupSamples());
    send(cache, 3, v1(3, 2, BACKLOG), 20 * SECOND);
    cache.advanceSamplingRound(4);
    assertEquals(2, diagnostic(cache, 2).getLightStreak());
    send(cache, 3, v1(3, 3, BACKLOG), 25 * SECOND);
    cache.advanceSamplingRound(5);
    assertEquals(3, diagnostic(cache, 2).getLightStreak());
    assertFalse(diagnostic(cache, 2).isHeavy());
  }

  @Test
  public void absentInputsContributeZeroWithoutBlockingKnownSources() throws Exception {
    ReplicationBacklogCache cache = cache();
    long[] incoming = {1000, 1000, 1000};
    for (int sequence = 1; sequence <= 5; sequence++) {
      long now = sequence * 5 * SECOND;
      send(cache, 1, v1(1, sequence, incoming), now);
      send(cache, 3, v1(3, sequence, incoming), now);
      cache.evaluate(RUNNING, now);
      cache.advanceSamplingRound(sequence);
    }
    assertFalse(diagnostic(cache, 1).isHeavy());
    assertTrue(diagnostic(cache, 2).isHeavy());
    assertEquals(Set.of(1), cache.eligibleCandidates(0).get(GROUP));
  }

  @Test
  public void nearbyCandidatesBecomeLightEvenWhenBothWereStableHeavy() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    Map<TConsensusGroupId, Set<Integer>> running = Collections.singletonMap(GROUP, Set.of(1, 3));
    cache.evaluate(running, 26 * SECOND);
    long[] nearby = {30, 0, 29};
    for (int sequence = 6; sequence <= 8; sequence++) {
      sample(cache, sequence, nearby, running);
      assertFalse(diagnostic(cache, 1).isInstantHeavy());
      assertFalse(diagnostic(cache, 3).isInstantHeavy());
      assertEquals(sequence < 8, diagnostic(cache, 1).isHeavy());
      assertEquals(sequence < 8, diagnostic(cache, 3).isHeavy());
    }
    assertTrue(cache.eligibleCandidates(0).isEmpty());
  }

  @Test
  public void highButSimilarLoadsKeepAllCandidates() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 5; sequence++) {
      sample(cache, sequence, new long[] {2000000, 2000001, 2000002}, RUNNING);
    }
    for (int target : MEMBERS) {
      assertFalse(diagnostic(cache, target).isHeavy());
    }
    assertTrue(cache.eligibleCandidates(0).isEmpty());
  }

  @Test
  public void expiringAnAlreadyZeroSourceDoesNotChangeTheOtherLoads() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    for (int sequence = 6; sequence <= 11; sequence++) {
      long now = sequence * 5 * SECOND;
      send(cache, 1, v1(1, sequence, BACKLOG), now);
      send(cache, 2, v1(2, sequence, BACKLOG), now);
      cache.advanceSamplingRound(sequence);
      cache.evaluate(RUNNING, now);
    }
    assertEquals(5, diagnostic(cache, 2).getSmoothedCounts()[0], 0);
    assertFalse(diagnostic(cache, 2).isHeavy());
    assertTrue(diagnostic(cache, 3).isHeavy());
    assertEquals(Set.of(2), cache.eligibleCandidates(0).get(GROUP));
  }

  @Test
  public void recoveryPreservesOtherReplicasStableLightState() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 5; sequence++) {
      sample(cache, sequence, new long[] {10, 10, 10}, RUNNING);
    }
    cache.evaluate(Collections.singletonMap(GROUP, Set.of(1, 3)), 26 * SECOND);
    cache.evaluate(RUNNING, 27 * SECOND);
    assertFalse(diagnostic(cache, 1).isHeavy());
    assertFalse(diagnostic(cache, 3).isHeavy());
    assertTrue(diagnostic(cache, 2).isHeavy());
    assertEquals(Set.of(1, 3), cache.eligibleCandidates(0).get(GROUP));
  }

  @Test
  public void significantNewLoadTurnsLightToHeavyImmediately() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 5; sequence++) {
      sample(cache, sequence, new long[] {10, 10, 10}, RUNNING);
    }
    sample(cache, 6, new long[] {1000, 10, 10}, RUNNING);
    assertTrue(diagnostic(cache, 1).isHeavy());
    assertTrue(diagnostic(cache, 1).isInstantHeavy());
    assertEquals(0, diagnostic(cache, 1).getLightStreak());
    assertEquals(Set.of(2, 3), cache.eligibleCandidates(0).get(GROUP));
  }

  @Test
  public void initializationAndRecoveryUseTheSameWarmupAndLightConfirmation() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    cache.evaluate(Collections.singletonMap(GROUP, Set.of(1, 3)), 26 * SECOND);
    cache.evaluate(RUNNING, 27 * SECOND);
    assertTrue(diagnostic(cache, 2).isHeavy());
    assertEquals(0, diagnostic(cache, 2).getWarmupSamples());
    assertEquals(0, diagnostic(cache, 2).getSmoothedCounts()[0], 0);
    assertEquals(3, diagnostic(cache, 1).getWarmupSamples());
    // No L remains, so all Running H replicas are allowed during recovery.
    assertTrue(cache.eligibleCandidates(0).isEmpty());
    for (int sequence = 6; sequence <= 10; sequence++) {
      sample(cache, sequence);
      assertEquals(sequence < 10, diagnostic(cache, 2).isHeavy());
    }
    assertEquals(Set.of(2), cache.eligibleCandidates(0).get(GROUP));
  }

  @Test
  public void returningRunningResetsImmediatelyWithoutAnyNewReport() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    cache.evaluate(Collections.singletonMap(GROUP, Set.of(1, 3)), 26 * SECOND);
    cache.evaluate(RUNNING, 100 * SECOND);
    assertTrue(diagnostic(cache, 2).isHeavy());
    assertEquals(0, diagnostic(cache, 2).getWarmupSamples());
    for (int i = 0; i < 10; i++) {
      cache.evaluate(RUNNING, 101 * SECOND);
    }
    assertTrue(diagnostic(cache, 2).isHeavy());
  }

  @Test
  public void delayedPreRecoverySamplesCannotWarmTheRecoveredTarget() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    cache.evaluate(Collections.singletonMap(GROUP, Set.of(1, 3)), 26 * SECOND);
    cache.evaluate(RUNNING, 27 * SECOND);
    for (int source = 1; source <= 3; source++) {
      send(cache, source, v1(source, 6, BACKLOG).setSampleAgeNanos(5 * SECOND), 30 * SECOND);
    }
    cache.evaluate(RUNNING, 30 * SECOND);
    cache.advanceSamplingRound(6);
    assertEquals(0, diagnostic(cache, 2).getWarmupSamples());
    sample(cache, 7);
    assertEquals(1, diagnostic(cache, 2).getWarmupSamples());
  }

  @Test
  public void restartIdentityAndLocalRegionGenerationResetOnlyTheRecoveredReplica()
      throws Exception {
    for (boolean reboot : new boolean[] {true, false}) {
      ReplicationBacklogCache cache = warmedCache();
      TReplicationBacklogReport changed = v1(2, 6, BACKLOG);
      if (reboot) {
        changed.setSourceInstanceId("new-boot-2");
      } else {
        changed.getGroups().get(0).setGeneration(2);
      }
      send(cache, 2, changed, 30 * SECOND);
      cache.evaluate(RUNNING, 30 * SECOND);
      assertTrue(diagnostic(cache, 2).isHeavy());
      assertEquals(0, diagnostic(cache, 2).getWarmupSamples());
      assertEquals(3, diagnostic(cache, 1).getWarmupSamples());
      assertEquals(3, diagnostic(cache, 3).getWarmupSamples());
    }
  }

  @Test
  public void recoveringNewLocalGenerationResetsLightBeforeTheFirstValidReport() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    TReplicationBacklogReport recovering = v1(2, 6, BACKLOG);
    recovering.getGroups().get(0).setGeneration(2).setState((byte) 1).unsetTargets();
    send(cache, 2, recovering, 30 * SECOND);
    assertTrue(diagnostic(cache, 2).isHeavy());
    assertEquals(0, diagnostic(cache, 2).getWarmupSamples());
    assertEquals(3, diagnostic(cache, 1).getWarmupSamples());
    assertEquals(3, diagnostic(cache, 3).getWarmupSamples());
    // The recovered target warms up; the others continue with zero for the replaced source.
    send(cache, 1, v1(1, 6, BACKLOG), 31 * SECOND);
    send(cache, 3, v1(3, 6, BACKLOG), 31 * SECOND);
    cache.advanceSamplingRound(6);
    assertEquals(1, diagnostic(cache, 2).getWarmupSamples());
    assertEquals(0, diagnostic(cache, 1).getCounts()[0], 0);
  }

  @Test
  public void recoveringExistingRegionAndErrorGenerationZeroPreserveTheEstimate() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    TReplicationBacklogReport recovering = v1(2, 6, BACKLOG);
    recovering.getGroups().get(0).setState((byte) 1).unsetTargets();
    send(cache, 2, recovering, 30 * SECOND);
    assertFalse(diagnostic(cache, 2).isHeavy());
    assertEquals(3, diagnostic(cache, 2).getWarmupSamples());
    TReplicationBacklogReport error = v1(2, 7, BACKLOG);
    error.getGroups().get(0).setGeneration(0).setState((byte) 2).unsetTargets();
    send(cache, 2, error, 35 * SECOND);
    assertFalse(diagnostic(cache, 2).isHeavy());
    assertEquals(5, diagnostic(cache, 2).getSmoothedCounts()[0], 0);
  }

  @Test
  public void newPipeGenerationResetsOnlyItsDestination() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 5; sequence++) {
      sample(cache, sequence, new long[] {10, 10, 10}, RUNNING);
    }
    TReplicationBacklogReport changed = v1(1, 6, new long[] {10, 10, 10});
    changed.getGroups().get(0).getTargets().get(0).setGeneration(2);
    send(cache, 1, changed, 30 * SECOND);
    cache.evaluate(RUNNING, 30 * SECOND);
    assertTrue(diagnostic(cache, 2).isHeavy());
    assertEquals(0, diagnostic(cache, 2).getWarmupSamples());
    assertFalse(diagnostic(cache, 1).isHeavy());
    assertFalse(diagnostic(cache, 3).isHeavy());
    assertEquals(Set.of(1, 3), cache.eligibleCandidates(0).get(GROUP));
  }

  @Test
  public void v2ClassificationKeepsIncomparableReplicas() throws Exception {
    ReplicationBacklogCache cache = cache();
    for (int sequence = 1; sequence <= 5; sequence++) {
      long now = sequence * 5 * SECOND;
      send(
          cache,
          1,
          report(1, sequence, (byte) 2, edge(2, 0, 1000, 5, 20), edge(3, 0, 500, 8, 20)),
          now);
      send(
          cache,
          2,
          report(2, sequence, (byte) 2, edge(1, 0, 5000, 20, 100), edge(3, 0, 0, 0, 0)),
          now);
      send(cache, 3, report(3, sequence, (byte) 2, edge(1, 0, 0, 0, 0), edge(2, 0, 0, 0, 0)), now);
      cache.evaluate(RUNNING, now);
      cache.advanceSamplingRound(sequence);
    }
    assertEquals(Set.of(2, 3), cache.eligibleCandidates(0).get(GROUP));
    assertTrue(diagnostic(cache, 1).isInstantHeavy());
    assertFalse(diagnostic(cache, 2).isInstantHeavy());
    assertFalse(diagnostic(cache, 3).isInstantHeavy());
  }

  @Test
  public void invalidEnvelopeEdgesAndDuplicateGroupsHoldPreviousEstimate() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    List<TReplicationBacklogReport> invalid = new ArrayList<>();
    invalid.add(null);
    TReplicationBacklogReport negative = v1(3, 6, BACKLOG);
    negative.getGroups().get(0).getTargets().get(0).setRequestCount(-1);
    invalid.add(negative);
    TReplicationBacklogReport missingCounter = v1(3, 7, BACKLOG);
    missingCounter.getGroups().get(0).getTargets().get(0).unsetFileCount();
    invalid.add(missingCounter);
    TReplicationBacklogReport wrongMember = v1(3, 8, BACKLOG);
    wrongMember.getGroups().get(0).getTargets().get(0).setTargetNodeId(99);
    invalid.add(wrongMember);
    TReplicationBacklogReport duplicate = v1(3, 9, BACKLOG);
    duplicate.setGroups(Arrays.asList(duplicate.getGroups().get(0), duplicate.getGroups().get(0)));
    invalid.add(duplicate);
    long now = 30 * SECOND;
    for (TReplicationBacklogReport bad : invalid) {
      send(cache, 3, bad, now++);
      cache.evaluate(RUNNING, now);
      assertFalse(diagnostic(cache, 2).isHeavy());
      assertEquals(5, diagnostic(cache, 2).getSmoothedCounts()[0], 0);
    }
    cache.advanceSamplingRound(6);
    cache.advanceSamplingRound(7);
    cache.advanceSamplingRound(8);
    assertFalse(diagnostic(cache, 2).isHeavy());
    assertEquals(5, diagnostic(cache, 2).getCounts()[0], 0);
    cache.advanceSamplingRound(9);
    assertEquals(0, diagnostic(cache, 2).getCounts()[0], 0);
  }

  @Test
  public void elapsedReportAgeIsRejectedButStoredEwmaDoesNotExpireByTime() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    TReplicationBacklogReport late =
        v1(1, 6, new long[] {30, 9999, 20}).setSampleAgeNanos(29 * SECOND);
    cache.cacheReport(1, late, 30 * SECOND, cache.getEpoch(), 32 * SECOND);
    send(cache, 3, v1(3, 6, BACKLOG), 32 * SECOND);
    cache.evaluate(RUNNING, 1000 * SECOND);
    assertEquals(5, diagnostic(cache, 2).getSmoothedCounts()[0], 0);
    assertFalse(diagnostic(cache, 2).isHeavy());
  }

  @Test
  public void ewmaUsesOneAggregatePerSamplingRound() throws Exception {
    ReplicationBacklogCache cache = cache("OBSERVE", 3);
    sample(cache, 1, new long[] {30, 100, 20}, RUNNING);
    sample(cache, 2, new long[] {30, 1000, 20}, RUNNING);
    assertEquals(550, diagnostic(cache, 2).getSmoothedCounts()[0], 0);
    for (int source = 1; source <= 3; source++) {
      send(cache, source, v1(source, 2, new long[] {30, 9999, 20}), 11 * SECOND);
    }
    cache.evaluate(RUNNING, 11 * SECOND);
    assertEquals(550, diagnostic(cache, 2).getSmoothedCounts()[0], 0);
  }

  @Test
  public void serviceEpochAndOutOfOrderBootResponsesAreRejected() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    long oldEpoch = cache.getEpoch();
    cache.reset();
    cache.updateTopology(RUNNING, 26 * SECOND);
    cache.evaluate(RUNNING, 26 * SECOND);
    cache.cacheReport(1, v1(1, 100, BACKLOG), 27 * SECOND, oldEpoch, 27 * SECOND);
    assertFalse(diagnostic(cache, 2).isKnown());
    send(cache, 1, v1(1, 1, BACKLOG).setSourceInstanceId("new-boot"), 30 * SECOND);
    cache.cacheReport(
        1, v1(1, 100, new long[] {30, 9999, 20}), 29 * SECOND, cache.getEpoch(), 31 * SECOND);
    send(cache, 3, v1(3, 1, BACKLOG), 31 * SECOND);
    cache.advanceSamplingRound(1);
    assertEquals(5, diagnostic(cache, 2).getCounts()[0], 0);
  }

  @Test
  public void topologyChangeDropsOldEdgesAndInvalidatesSolverSnapshot() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    ReplicationBacklogCache.SelectionSnapshot snapshot = cache.captureSelection(0);
    Map<TConsensusGroupId, Set<Integer>> next = Collections.singletonMap(GROUP, Set.of(1, 2, 4));
    cache.updateTopology(next, 26 * SECOND);
    cache.evaluate(next, 26 * SECOND);
    assertFalse(cache.isSelectionValid(snapshot, 26 * SECOND));
    assertFalse(cache.getDiagnostics(7, 0).containsKey(3));
    assertFalse(diagnostic(cache, 4).isKnown());
    assertTrue(diagnostic(cache, 4).isHeavy());
    cache.cacheReport(1, v1(1, 6, BACKLOG), 25 * SECOND, cache.getEpoch(), 27 * SECOND);
    assertFalse(diagnostic(cache, 2).isKnown());
    cache.updateTopology(Collections.emptyMap(), 28 * SECOND);
    assertTrue(cache.getDiagnostics(7, 0).isEmpty());
  }

  @Test
  public void observeAndOffNeverConstrainCandidates() throws Exception {
    ReplicationBacklogCache cache = cache("OBSERVE", 3);
    for (int sequence = 1; sequence <= 5; sequence++) {
      sample(cache, sequence);
    }
    assertTrue(diagnostic(cache, 1).isHeavy());
    assertFalse(diagnostic(cache, 2).isHeavy());
    assertTrue(cache.eligibleCandidates(0).isEmpty());
    cache.configure(config("ENFORCE", 3));
    assertEquals(Set.of(2), cache.eligibleCandidates(0).get(GROUP));
    cache.configure(config("OFF", 3));
    assertTrue(cache.eligibleCandidates(0).isEmpty());
    cache.configure(config("ENFORCE", 3));
    cache.updateTopology(RUNNING, 26 * SECOND);
    cache.evaluate(RUNNING, 26 * SECOND);
    assertTrue(diagnostic(cache, 2).isHeavy());
    assertEquals(0, diagnostic(cache, 2).getWarmupSamples());
  }

  @Test
  public void unchangedSamplesAndRepeatedQueriesDoNotRequestAnotherBalance() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    assertTrue(cache.consumeDirty());
    sample(cache, 6);
    cache.updateTopology(RUNNING, 31 * SECOND);
    cache.evaluate(RUNNING, 31 * SECOND);
    assertFalse(cache.consumeDirty());
    assertEquals(
        2,
        cache.sampleDiagnostics(1, 0).get(7).values().stream()
            .filter(ReplicaDiagnostic::isHeavy)
            .count());
    assertTrue(cache.sampleDiagnostics(0, 0).isEmpty());
  }

  @Test
  public void solverSnapshotTracksCandidatesRatherThanReportAge() throws Exception {
    ReplicationBacklogCache cache = warmedCache();
    ReplicationBacklogCache.SelectionSnapshot snapshot = cache.captureSelection(25 * SECOND);
    sample(cache, 6);
    assertTrue(cache.isSelectionValid(snapshot, 1000 * SECOND));
    cache.advanceSamplingRound(7);
    cache.advanceSamplingRound(8);
    assertTrue(cache.isSelectionValid(snapshot, 1000 * SECOND));
    cache.advanceSamplingRound(9);
    assertTrue(cache.isSelectionValid(snapshot, 1000 * SECOND));
    cache.advanceSamplingRound(10);
    cache.advanceSamplingRound(11);
    assertTrue(cache.isSelectionValid(snapshot, 1000 * SECOND));
    cache.advanceSamplingRound(12);
    assertFalse(cache.isSelectionValid(snapshot, 1000 * SECOND));
  }

  @Test
  public void evenAnEmptySnapshotIsBoundToConfigurationLeadershipAndRunningCandidates()
      throws Exception {
    ReplicationBacklogCache cache = cache();
    ReplicationBacklogCache.SelectionSnapshot snapshot = cache.captureSelection(0);
    cache.evaluate(Collections.singletonMap(GROUP, Set.of(1, 3)), SECOND);
    assertFalse(cache.isSelectionValid(snapshot, SECOND));
    snapshot = cache.captureSelection(SECOND);
    cache.configure(config("OBSERVE", 3));
    assertFalse(cache.isSelectionValid(snapshot, SECOND));
    snapshot = cache.captureSelection(SECOND);
    cache.reset();
    assertFalse(cache.isSelectionValid(snapshot, SECOND));
  }
}
