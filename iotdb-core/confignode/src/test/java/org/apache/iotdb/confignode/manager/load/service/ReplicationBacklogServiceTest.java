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

import org.apache.iotdb.common.rpc.thrift.TConsensusGroupId;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.commons.cluster.NodeStatus;
import org.apache.iotdb.commons.cluster.RegionStatus;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.ReplicationBacklogConfig;
import org.apache.iotdb.confignode.manager.IManager;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.balancer.RouteBalancer;
import org.apache.iotdb.confignode.manager.load.balancer.router.leader.CostFlowSelectionLeaderBalancer;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.ReplicationBacklogCache.SelectionSnapshot;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.mpp.rpc.thrift.TRegionReplicationBacklog;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogEdge;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogReport;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplicationBacklogServiceTest {
  private static final TConsensusGroupId GROUP =
      new TConsensusGroupId(TConsensusGroupType.DataRegion, 7);
  private final ConfigNodeConfig conf = ConfigNodeDescriptor.getInstance().getConf();
  private ReplicationBacklogConfig oldConfig;
  private String oldProtocol;
  private ReplicationBacklogService service;
  private RouteBalancer routeBalancer;
  private ScheduledFuture<?> scheduled;
  private LoadCache loadCache;
  private LoadManager loadManager;

  @Before
  public void setUp() throws Exception {
    oldConfig = conf.getReplicationBacklogConfig();
    oldProtocol = conf.getDataRegionConsensusProtocolClass();
    conf.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS);
    Properties properties = new Properties();
    properties.setProperty(ReplicationBacklogConfig.PREFIX + "mode", "OBSERVE");
    for (String type : new String[] {"request", "tablet", "file", "deletion"}) {
      properties.setProperty(ReplicationBacklogConfig.PREFIX + type + "_gap", "10");
    }
    conf.setReplicationBacklogConfig(
        ReplicationBacklogConfig.load(properties, ReplicationBacklogConfig.defaults()));
    loadCache = mock(LoadCache.class);
    Map<Integer, NodeStatistics> nodes = new HashMap<>();
    Map<Integer, RegionStatistics> regions = new HashMap<>();
    for (int node : new int[] {1, 2, 3}) {
      nodes.put(node, new NodeStatistics(NodeStatus.Running));
      regions.put(node, new RegionStatistics(RegionStatus.Running));
    }
    when(loadCache.getCurrentRegionLocationMap(TConsensusGroupType.DataRegion))
        .thenReturn(Collections.singletonMap(GROUP, Set.of(1, 2, 3)));
    when(loadCache.getCurrentDataNodeStatisticsMap()).thenReturn(nodes);
    when(loadCache.getCurrentRegionStatisticsMap(TConsensusGroupType.DataRegion))
        .thenReturn(Collections.singletonMap(GROUP, regions));
    loadManager = mock(LoadManager.class);
    routeBalancer = mock(RouteBalancer.class);
    when(loadManager.getRouteBalancer()).thenReturn(routeBalancer);
    service = new ReplicationBacklogService(loadManager, loadCache);
    when(loadManager.getLoadCache()).thenReturn(loadCache);
    when(loadManager.getReplicationBacklogService()).thenReturn(service);
    scheduled = mock(ScheduledFuture.class);
    ScheduledExecutorService executor =
        mock(
            ScheduledExecutorService.class,
            invocation ->
                ScheduledFuture.class.isAssignableFrom(invocation.getMethod().getReturnType())
                    ? scheduled
                    : RETURNS_DEFAULTS.answer(invocation));
    setField(service, "executor", executor);
    ExecutorService directBalanceExecutor = mock(ExecutorService.class);
    doAnswer(
            invocation -> {
              invocation.<Runnable>getArgument(0).run();
              return null;
            })
        .when(directBalanceExecutor)
        .execute(any(Runnable.class));
    setField(service, "balanceExecutor", directBalanceExecutor);
  }

  @After
  public void tearDown() {
    if (service != null) {
      service.stop();
    }
    conf.setReplicationBacklogConfig(oldConfig);
    conf.setDataRegionConsensusProtocolClass(oldProtocol);
  }

  @Test
  public void stoppingAndRestartingRejectsOldCallbacksAndCancelsSchedule() throws Exception {
    service.start();
    long oldEpoch = service.getEpoch();
    cache(1, 1, oldEpoch);
    cache(3, 1, oldEpoch);
    tick(false);
    assertTrue(service.getDiagnostics(7).get(2).isKnown());
    service.stop();
    verify(scheduled).cancel(false);
    assertFalse(service.shouldSample(0, System.nanoTime()));
    assertTrue(service.getDiagnostics(7).isEmpty());
    service.start();
    assertTrue(service.getEpoch() > oldEpoch);
    cache(3, 1, service.getEpoch());
    cache(1, 100, oldEpoch);
    assertFalse(service.getDiagnostics(7).get(2).isKnown());
    assertEquals(0, service.getDiagnostics(7).get(2).getCounts()[0], 0);
    cache(1, 1, service.getEpoch());
    tick(false);
    assertTrue(service.getDiagnostics(7).get(2).isKnown());
    assertEquals(5, service.getDiagnostics(7).get(2).getCounts()[0], 0);
  }

  @Test
  public void observeEnforceAndOffApplyAndWithdrawOnlyConfirmedConstraints() throws Exception {
    service.start();
    for (int sequence = 1; sequence <= 5; sequence++) {
      round(sequence);
    }
    assertTrue(service.getDiagnostics(7).get(1).isHeavy());
    assertTrue(service.getEligibleCandidates().isEmpty());
    verify(routeBalancer, never()).balanceDataRegionLeadersForReplicationBacklog();
    configure("mode", "ENFORCE");
    assertEquals(Set.of(2), service.getEligibleCandidates().get(GROUP));
    round(6);
    verify(routeBalancer).balanceDataRegionLeadersForReplicationBacklog();
    invokeTick();
    verify(routeBalancer).balanceDataRegionLeadersForReplicationBacklog();
    configure("mode", "OBSERVE");
    assertTrue(service.getEligibleCandidates().isEmpty());
    assertTrue(service.getDiagnostics(7).get(1).isHeavy());
    round(7);
    verify(routeBalancer, times(2)).balanceDataRegionLeadersForReplicationBacklog();
    configure("mode", "ENFORCE");
    assertEquals(Set.of(2), service.getEligibleCandidates().get(GROUP));
    round(8);
    verify(routeBalancer, times(3)).balanceDataRegionLeadersForReplicationBacklog();
    configure("mode", "OFF");
    assertTrue(service.getEligibleCandidates().isEmpty());
    assertFalse(service.shouldSample(0, System.nanoTime()));
    tick(true);
    verify(routeBalancer, times(4)).balanceDataRegionLeadersForReplicationBacklog();
    configure("mode", "ENFORCE");
    assertTrue(service.getEligibleCandidates().isEmpty());
    assertFalse(service.getDiagnostics(7).get(2).isKnown());
    tick(true);
    verify(routeBalancer, times(4)).balanceDataRegionLeadersForReplicationBacklog();
    for (int sequence = 9; sequence <= 13; sequence++) {
      round(sequence);
    }
    assertEquals(Set.of(2), service.getEligibleCandidates().get(GROUP));
    verify(routeBalancer, times(5)).balanceDataRegionLeadersForReplicationBacklog();
  }

  @Test
  public void shorteningSampleIntervalDiscardsTheOldFutureDeadline() throws Exception {
    configure("ttl_ms", "120000");
    configure("sample_interval_ms", "60000");
    service.start();
    long now = System.nanoTime();
    assertTrue(service.shouldSample(0, now));
    assertFalse(service.shouldSample(0, now + TimeUnit.MILLISECONDS.toNanos(1)));
    assertFalse(service.shouldSample(1, now));
    long epoch = service.getEpoch();
    configure("sample_interval_ms", "5000");
    long afterReload = now + TimeUnit.MILLISECONDS.toNanos(2);
    assertTrue(service.shouldSample(0, afterReload));
    assertTrue(service.getEpoch() > epoch);
    assertFalse(service.shouldSample(0, afterReload));
    assertFalse(service.shouldSample(1, afterReload));
    assertTrue(service.shouldSample(1, afterReload + TimeUnit.SECONDS.toNanos(5)));
  }

  @Test
  public void repeatedSelectionsDoNotAdvanceWarmupOrLowConfirmation() throws Exception {
    configure("mode", "ENFORCE");
    service.start();
    for (int sequence = 1; sequence <= 3; sequence++) {
      round(sequence);
    }
    for (int call = 0; call < 10; call++) {
      assertTrue(service.getEligibleCandidates().isEmpty());
      SelectionSnapshot snapshot = captureSelection();
      assertTrue(service.validateSelection(snapshot));
      invokeTick();
    }
    assertTrue(service.getDiagnostics(7).get(2).isHeavy());
    round(4);
    assertTrue(service.getDiagnostics(7).get(2).isHeavy());
    round(5);
    assertFalse(service.getDiagnostics(7).get(2).isHeavy());
    assertEquals(Set.of(2), service.getEligibleCandidates().get(GROUP));
  }

  @Test
  public void missingInputsExpireBySamplingRoundsWithoutAHeavyPenalty() throws Exception {
    prepareFilteredSelection();
    double[] previous = service.getDiagnostics(7).get(2).getSmoothedCounts();
    for (int round = 0; round < 3; round++) {
      tick(false);
      for (int call = 0; call < 10; call++) {
        assertEquals(Set.of(2), service.getEligibleCandidates().get(GROUP));
        SelectionSnapshot snapshot = captureSelection();
        assertTrue(service.validateSelection(snapshot));
        invokeTick();
      }
    }
    assertEquals(previous[0], service.getDiagnostics(7).get(2).getSmoothedCounts()[0], 0);
    tick(false);
    assertFalse(service.getDiagnostics(7).get(2).isHeavy());
    assertEquals(0, service.getDiagnostics(7).get(2).getCounts()[0], 0);
    assertEquals(previous[0] / 2, service.getDiagnostics(7).get(2).getSmoothedCounts()[0], 0);
    round(6);
    assertEquals(previous[0], service.getDiagnostics(7).get(2).getCounts()[0], 0);
    assertEquals(0, service.getDiagnostics(7).get(2).getMissingRounds());
    assertEquals(Set.of(2), service.getEligibleCandidates().get(GROUP));
  }

  @Test
  public void missingRoundsDuringSolveDiscardTransfersAndScheduleRecomputation() throws Exception {
    prepareFilteredSelection();
    setField(service, "nextBalance", System.nanoTime() + TimeUnit.MINUTES.toNanos(1));
    runSelection(
        () -> {
          for (int round = 0; round < 7; round++) {
            tick(false);
          }
        });
    verify(loadManager, never()).forceUpdateConsensusGroupCache(anyMap());
    assertTrue((boolean) getField(service, "pendingBalance"));
    assertTrue(service.getEligibleCandidates().isEmpty());
    tick(true);
    assertFalse((boolean) getField(service, "pendingBalance"));
    verify(routeBalancer, times(2)).balanceDataRegionLeadersForReplicationBacklog();
  }

  @Test
  public void blockedBalanceDoesNotBlockSamplingAndCoalescesFurtherChanges() throws Exception {
    configure("sample_interval_ms", "50");
    configure("balance_interval_ms", "50");
    prepareFilteredSelection();
    clearInvocations(routeBalancer);
    ExecutorService balancing = Executors.newSingleThreadExecutor();
    ScheduledExecutorService sampling = Executors.newSingleThreadScheduledExecutor();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch completed = new CountDownLatch(1);
    CountDownLatch missed = new CountDownLatch(1);
    AtomicInteger solves = new AtomicInteger();
    AtomicBoolean accepted = new AtomicBoolean(true);
    AtomicReference<Throwable> failure = new AtomicReference<>();
    doAnswer(
            invocation -> {
              solves.incrementAndGet();
              SelectionSnapshot snapshot = captureSelection();
              started.countDown();
              try {
                if (!release.await(10, TimeUnit.SECONDS)) {
                  throw new AssertionError("Timed out waiting to finish the blocked solve");
                }
                accepted.set(service.validateSelection(snapshot));
              } catch (Throwable e) {
                failure.set(e);
              } finally {
                completed.countDown();
              }
              return null;
            })
        .when(routeBalancer)
        .balanceDataRegionLeadersForReplicationBacklog();
    ScheduledFuture<?> samplingTask = null;
    try {
      setField(service, "balanceExecutor", balancing);
      setField(service, "pendingBalance", true);
      setField(service, "nextBalance", System.nanoTime() - 1);
      // This single scheduler invokes the production tick. It must continue while the separate
      // balancing executor is blocked, rather than relying on recursive calls inside the solve.
      samplingTask =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              sampling,
              () -> {
                try {
                  invokeTick();
                  if (!started.await(5, TimeUnit.SECONDS)) {
                    throw new AssertionError("The balance task did not start");
                  }
                  if (service.getDiagnostics(7).values().stream().noneMatch(d -> d.isHeavy())) {
                    missed.countDown();
                  }
                } catch (Throwable e) {
                  failure.set(e);
                }
              },
              0,
              10,
              TimeUnit.MILLISECONDS);
      assertTrue(started.await(5, TimeUnit.SECONDS));
      assertTrue(missed.await(5, TimeUnit.SECONDS));
      assertEquals(1, solves.get());
      assertFalse(service.getDiagnostics(7).get(2).isHeavy());
      assertTrue(service.getEligibleCandidates().isEmpty());
      assertTrue((boolean) getField(service, "pendingBalance"));
      samplingTask.cancel(false);
      sampling.submit(() -> {}).get(5, TimeUnit.SECONDS);
      release.countDown();
      assertTrue(completed.await(5, TimeUnit.SECONDS));
      balancing.submit(() -> {}).get(5, TimeUnit.SECONDS);
      assertFalse(accepted.get());
      assertFalse((boolean) getField(service, "balanceInProgress"));
      if (failure.get() != null) {
        throw new AssertionError(failure.get());
      }
    } finally {
      if (samplingTask != null) {
        samplingTask.cancel(false);
      }
      release.countDown();
      sampling.shutdownNow();
      balancing.shutdownNow();
      assertTrue(sampling.awaitTermination(5, TimeUnit.SECONDS));
      assertTrue(balancing.awaitTermination(5, TimeUnit.SECONDS));
    }
  }

  @Test
  public void disablingEnforcementDuringSolveDiscardsTransfers() throws Exception {
    prepareFilteredSelection();
    runSelection(() -> configure("mode", "OFF"));
    verify(loadManager, never()).forceUpdateConsensusGroupCache(anyMap());
    assertTrue((boolean) getField(service, "pendingBalance"));
    assertTrue(service.getEligibleCandidates().isEmpty());
  }

  @Test
  public void changingThresholdDuringSolveDiscardsTransfers() throws Exception {
    prepareFilteredSelection();
    runSelection(() -> configure("request_gap", "20"));
    verify(loadManager, never()).forceUpdateConsensusGroupCache(anyMap());
    assertTrue((boolean) getField(service, "pendingBalance"));
  }

  @Test
  public void stoppingAndRestartingDuringSolveDiscardsTransfers() throws Exception {
    prepareFilteredSelection();
    runSelection(
        () -> {
          service.stop();
          service.start();
        });
    verify(loadManager, never()).forceUpdateConsensusGroupCache(anyMap());
    assertTrue((boolean) getField(service, "pendingBalance"));
  }

  @Test
  public void losingCandidateHealthDuringSolveDiscardsTransfers() throws Exception {
    prepareFilteredSelection();
    runSelection(
        () ->
            loadCache
                .getCurrentDataNodeStatisticsMap()
                .put(2, new NodeStatistics(NodeStatus.Unknown)));
    verify(loadManager, never()).forceUpdateConsensusGroupCache(anyMap());
    assertTrue((boolean) getField(service, "pendingBalance"));
  }

  @Test
  public void freshReportsDuringSolveKeepTheConfirmedSelectionApplicable() throws Exception {
    prepareFilteredSelection();
    runSelection(
        () -> {
          for (int source : new int[] {1, 2, 3}) {
            cache(source, 6, service.getEpoch());
          }
        });
    verify(loadManager)
        .forceUpdateConsensusGroupCache(
            argThat(updates -> updates.size() == 1 && updates.get(GROUP).getLeaderId() == 2));
    assertFalse((boolean) getField(service, "pendingBalance"));
  }

  private void prepareFilteredSelection() throws Exception {
    configure("mode", "ENFORCE");
    service.start();
    for (int sequence = 1; sequence <= 5; sequence++) {
      round(sequence);
    }
    assertEquals(Set.of(2), service.getEligibleCandidates().get(GROUP));
  }

  @Test
  public void recoveryResetsToHeavyBeforeTheImmediateSolveAndRepeatsWarmup() throws Exception {
    prepareFilteredSelection();
    loadCache.getCurrentDataNodeStatisticsMap().put(2, new NodeStatistics(NodeStatus.Unknown));
    service.getEligibleCandidates();
    loadCache.getCurrentDataNodeStatisticsMap().put(2, new NodeStatistics(NodeStatus.Running));
    // All replicas are now H, so availability takes priority and CFS may use any Running member.
    assertTrue(service.getEligibleCandidates().isEmpty());
    assertTrue(service.getDiagnostics(7).get(2).isHeavy());
    runSelection(Set.of(1, 2, 3), 2, () -> {});
    for (int seq = 6; seq <= 9; seq++) {
      round(seq);
      assertTrue(service.getDiagnostics(7).get(2).isHeavy());
    }
    round(10);
    assertEquals(Set.of(2), service.getEligibleCandidates().get(GROUP));
    runSelection(() -> {});
    verify(loadManager)
        .forceUpdateConsensusGroupCache(
            argThat(updates -> updates.size() == 1 && updates.get(GROUP).getLeaderId() == 2));
  }

  private void runSelection(CheckedAction duringSolve) throws Exception {
    runSelection(Set.of(2), 1, duringSolve);
  }

  private SelectionSnapshot captureSelection() {
    return service.captureSelection(
        loadCache.getCurrentRegionLocationMap(TConsensusGroupType.DataRegion),
        loadCache.getCurrentDataNodeStatisticsMap(),
        loadCache.getCurrentRegionStatisticsMap(TConsensusGroupType.DataRegion));
  }

  private void runSelection(Set<Integer> expectedCandidates, int leader, CheckedAction duringSolve)
      throws Exception {
    IManager manager = mock(IManager.class);
    when(manager.getLoadManager()).thenReturn(loadManager);
    when(loadCache.getCurrentDatabaseRegionGroupMap(TConsensusGroupType.DataRegion))
        .thenReturn(Collections.singletonMap("root.test", Collections.singletonList(GROUP)));
    when(loadCache.getRegionLeaderMap(TConsensusGroupType.DataRegion))
        .thenReturn(Collections.singletonMap(GROUP, leader));
    RouteBalancer actualRouteBalancer = new RouteBalancer(manager);
    Object context = getField(actualRouteBalancer, "dataRegionLeaderBalanceContext");
    setField(context, "consensusProtocolClass", ConsensusFactory.IOT_CONSENSUS);
    AtomicBoolean solved = new AtomicBoolean();
    setField(
        context,
        "leaderBalancer",
        new CostFlowSelectionLeaderBalancer() {
          @Override
          public Map<TConsensusGroupId, Integer> generateOptimalLeaderDistributionWithBacklog(
              Map<String, List<TConsensusGroupId>> databases,
              Map<TConsensusGroupId, Set<Integer>> locations,
              Map<TConsensusGroupId, Integer> leaders,
              Map<Integer, NodeStatistics> nodes,
              Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regions,
              Map<TConsensusGroupId, Set<Integer>> candidates) {
            assertEquals(
                expectedCandidates,
                candidates.getOrDefault(
                    GROUP,
                    CostFlowSelectionLeaderBalancer.getRunningCandidates(locations, nodes, regions)
                        .get(GROUP)));
            Map<TConsensusGroupId, Integer> result =
                super.generateOptimalLeaderDistributionWithBacklog(
                    databases, locations, leaders, nodes, regions, candidates);
            assertTrue(expectedCandidates.contains(result.get(GROUP)));
            solved.set(true);
            try {
              duringSolve.run();
            } catch (Exception e) {
              throw new AssertionError(e);
            }
            return result;
          }
        });
    Method method =
        RouteBalancer.class.getDeclaredMethod("balanceRegionLeadersInLock", context.getClass());
    method.setAccessible(true);
    method.invoke(actualRouteBalancer, context);
    assertTrue(solved.get());
  }

  private interface CheckedAction {
    void run() throws Exception;
  }

  private static Object getField(Object target, String name) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    return field.get(target);
  }

  private void configure(String key, String value) throws Exception {
    Properties properties = new Properties();
    properties.setProperty(ReplicationBacklogConfig.PREFIX + key, value);
    conf.setReplicationBacklogConfig(
        ReplicationBacklogConfig.load(properties, conf.getReplicationBacklogConfig()));
  }

  private void round(int sequence) throws Exception {
    for (int source : new int[] {1, 2, 3}) {
      cache(source, sequence, service.getEpoch());
    }
    tick(true);
  }

  private void cache(int source, int sequence, long epoch) {
    ArrayList<TReplicationBacklogEdge> edges = new ArrayList<>();
    for (int target : new int[] {1, 2, 3}) {
      if (target == source) {
        continue;
      }
      long count = source == 1 ? (target == 2 ? 5 : 10) : source == 2 ? (target == 1 ? 30 : 10) : 0;
      edges.add(
          new TReplicationBacklogEdge()
              .setTargetNodeId(target)
              .setGeneration(1)
              .setState((byte) 0)
              .setRequestCount(count)
              .setTabletCount(0)
              .setFileCount(0)
              .setDeletionCount(0));
    }
    TReplicationBacklogReport report =
        new TReplicationBacklogReport()
            .setSourceInstanceId("boot-" + source)
            .setSampleSequence(sequence)
            .setSampleAgeNanos(0)
            .setGroups(
                Collections.singletonList(
                    new TRegionReplicationBacklog()
                        .setRegionId(7)
                        .setProtocol((byte) 1)
                        .setGeneration(1)
                        .setState((byte) 0)
                        .setTargets(edges)));
    service.cacheReport(source, report, System.nanoTime(), epoch);
  }

  private void tick(boolean balanceDue) throws Exception {
    setField(service, "nextEvaluation", System.nanoTime() - 1);
    if (balanceDue) {
      setField(service, "nextBalance", System.nanoTime() - 1);
    }
    invokeTick();
  }

  private void invokeTick() throws Exception {
    Method method = ReplicationBacklogService.class.getDeclaredMethod("tick");
    method.setAccessible(true);
    method.invoke(service);
  }

  static Object setField(Object target, String name, Object value) throws Exception {
    Field field = target.getClass().getDeclaredField(name);
    field.setAccessible(true);
    Object old = field.get(target);
    field.set(target, value);
    return old;
  }
}
