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
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.conf.ReplicationBacklogConfig;
import org.apache.iotdb.confignode.conf.ReplicationBacklogConfig.Mode;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.manager.load.LoadManager;
import org.apache.iotdb.confignode.manager.load.cache.LoadCache;
import org.apache.iotdb.confignode.manager.load.cache.ReplicationBacklogCache;
import org.apache.iotdb.confignode.manager.load.cache.ReplicationBacklogCache.SelectionSnapshot;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogReport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Collects low-frequency telemetry independently of node/region health-change events. */
public final class ReplicationBacklogService {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReplicationBacklogService.class);
  private final LoadManager loadManager;
  private final LoadCache loadCache;
  private final ReplicationBacklogCache cache = new ReplicationBacklogCache();
  private final Map<Integer, Long> nextSample = new HashMap<>();
  private final ScheduledExecutorService executor =
      IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
          ThreadName.CONFIG_NODE_LOAD_STATISTIC.getName());
  private Future<?> future;
  private volatile boolean running;
  private long nextEvaluation;
  private long nextBalance;
  private long nextDiagnostic;
  private boolean pendingBalance;
  private Mode lastMode = Mode.OBSERVE;
  private ReplicationBacklogConfig lastConfiguration;

  public ReplicationBacklogService(LoadManager loadManager, LoadCache loadCache) {
    this.loadManager = loadManager;
    this.loadCache = loadCache;
  }

  public synchronized void start() {
    if (running) {
      return;
    }
    cache.reset();
    lastConfiguration = configuration();
    cache.configure(lastConfiguration);
    nextSample.clear();
    nextEvaluation = System.nanoTime();
    nextBalance = nextEvaluation;
    nextDiagnostic = nextEvaluation + TimeUnit.MINUTES.toNanos(1);
    pendingBalance = false;
    lastMode = lastConfiguration.getMode();
    refreshTopology(nextEvaluation);
    running = true;
    future =
        ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
            executor, this::tick, 0, 1, TimeUnit.SECONDS);
  }

  public synchronized void stop() {
    running = false;
    if (future != null) {
      future.cancel(false);
      future = null;
    }
    cache.reset();
    nextSample.clear();
  }

  public synchronized boolean shouldSample(int nodeId, long now) {
    if (!running || !supportedProtocol()) {
      return false;
    }
    ReplicationBacklogConfig conf = synchronizeConfiguration(now);
    if (conf.getMode() == Mode.OFF) {
      return false;
    }
    Long next = nextSample.get(nodeId);
    if (next == null) {
      next =
          now
              + TimeUnit.MILLISECONDS.toNanos(
                  Math.floorMod(nodeId * 2654435761L, conf.getSampleIntervalMs()));
      nextSample.put(nodeId, next);
    }
    if (now - next < 0) {
      return false;
    }
    nextSample.put(nodeId, now + TimeUnit.MILLISECONDS.toNanos(conf.getSampleIntervalMs()));
    return true;
  }

  public long getEpoch() {
    return cache.getEpoch();
  }

  public void cacheReport(
      int nodeId, TReplicationBacklogReport report, long requestTime, long epoch) {
    if (running) {
      cache.cacheReport(nodeId, report, requestTime, epoch, System.nanoTime());
    }
  }

  private void tick() {
    boolean balance;
    long epoch;
    synchronized (this) {
      if (!running) {
        return;
      }
      long now = System.nanoTime();
      ReplicationBacklogConfig conf = synchronizeConfiguration(now);
      if (conf.getMode() != Mode.OFF && supportedProtocol() && now - nextEvaluation >= 0) {
        refreshTopology(now);
        cache.evaluate(now);
        nextEvaluation = now + TimeUnit.MILLISECONDS.toNanos(conf.getSampleIntervalMs());
      }
      if (conf.getMode() != Mode.OFF && supportedProtocol() && now - nextDiagnostic >= 0) {
        logDiagnostics(conf.getMode(), now);
        nextDiagnostic = now + TimeUnit.MINUTES.toNanos(1);
      }
      boolean changed = cache.consumeDirty();
      pendingBalance |= changed && (conf.getMode() == Mode.ENFORCE || lastMode == Mode.ENFORCE);
      lastMode = conf.getMode();
      balance = pendingBalance && now - nextBalance >= 0 && supportedProtocol();
      if (balance) {
        pendingBalance = false;
        nextBalance = now + TimeUnit.MILLISECONDS.toNanos(conf.getBalanceIntervalMs());
      }
      epoch = cache.getEpoch();
    }
    if (balance && running && epoch == cache.getEpoch()) {
      loadManager.getRouteBalancer().balanceDataRegionLeadersForReplicationBacklog();
    }
  }

  private void logDiagnostics(Mode mode, long now) {
    if (!LOGGER.isInfoEnabled()) {
      return;
    }
    cache
        .sampleDiagnostics(3, now)
        .forEach(
            (region, replicas) ->
                replicas.forEach(
                    (node, diagnostic) ->
                        LOGGER.info(
                            ConfigNodeMessages
                                .LOG_REPLICATIONBACKLOG_REGION_ARG_REPLICA_ARG_KNOWN_ARG_AVAILABLE_ARG_PROPOSEDEXCLUDED_ARG_RAW_REQUEST_TABLET_FILE_DELETION_ARG_EWMA_ARG_MODE_ARG_819B0890,
                            region,
                            node,
                            diagnostic.isKnown(),
                            diagnostic.isAvailable(),
                            diagnostic.isHeavy(),
                            Arrays.toString(diagnostic.getCounts()),
                            Arrays.toString(diagnostic.getSmoothedCounts()),
                            mode)));
  }

  /**
   * Recheck current health before a solver snapshot, so a previous exclusion cannot block failover.
   */
  public synchronized Map<TConsensusGroupId, Set<Integer>> getEligibleCandidates() {
    if (!running || !supportedProtocol()) {
      return Collections.emptyMap();
    }
    long now = System.nanoTime();
    if (synchronizeConfiguration(now).getMode() == Mode.OFF) {
      return Collections.emptyMap();
    }
    refreshTopology(now);
    return cache.eligibleCandidates(now);
  }

  /** Capture the same membership/health inputs that the caller is about to pass to CFS. */
  public synchronized SelectionSnapshot captureSelection(
      Map<TConsensusGroupId, Set<Integer>> locations,
      Map<Integer, NodeStatistics> nodes,
      Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regions) {
    long now = System.nanoTime();
    if (running && supportedProtocol() && synchronizeConfiguration(now).getMode() != Mode.OFF) {
      cache.updateTopology(locations, nodes, regions, now);
    }
    return cache.captureSelection(now);
  }

  /**
   * A solve can outlive a report, a policy or this CN leadership. Revalidate once immediately
   * before applying its result, without allocating another full candidate map or retrying in CFS.
   */
  public synchronized boolean validateSelection(SelectionSnapshot snapshot) {
    if (!running || !supportedProtocol()) {
      return false;
    }
    long now = System.nanoTime();
    if (synchronizeConfiguration(now).getMode() != Mode.OFF) {
      refreshTopology(now);
      cache.evaluate(now);
    }
    boolean valid = cache.isSelectionValid(snapshot, now);
    if (!valid) {
      pendingBalance = true;
      nextEvaluation = now;
    }
    return valid;
  }

  private ReplicationBacklogConfig synchronizeConfiguration(long now) {
    ReplicationBacklogConfig conf = configuration();
    if (conf != lastConfiguration) {
      cache.configure(conf);
      if (lastConfiguration == null
          || !conf.sameSettings(lastConfiguration)
          || conf.getMode() == Mode.OFF
          || lastConfiguration.getMode() == Mode.OFF) {
        nextSample.clear();
        nextEvaluation = now;
        nextBalance = now + TimeUnit.MILLISECONDS.toNanos(conf.getBalanceIntervalMs());
      }
      lastConfiguration = conf;
    }
    return conf;
  }

  public Map<Integer, ReplicationBacklogCache.ReplicaDiagnostic> getDiagnostics(int regionId) {
    return cache.getDiagnostics(regionId, System.nanoTime());
  }

  private void refreshTopology(long now) {
    cache.updateTopology(
        loadCache.getCurrentRegionLocationMap(TConsensusGroupType.DataRegion),
        loadCache.getCurrentDataNodeStatisticsMap(),
        loadCache.getCurrentRegionStatisticsMap(TConsensusGroupType.DataRegion),
        now);
  }

  private static ReplicationBacklogConfig configuration() {
    return ConfigNodeDescriptor.getInstance().getConf().getReplicationBacklogConfig();
  }

  private static boolean supportedProtocol() {
    String protocol =
        ConfigNodeDescriptor.getInstance().getConf().getDataRegionConsensusProtocolClass();
    return ConsensusFactory.IOT_CONSENSUS.equals(protocol)
        || ConsensusFactory.IOT_CONSENSUS_V2.equals(protocol);
  }
}
