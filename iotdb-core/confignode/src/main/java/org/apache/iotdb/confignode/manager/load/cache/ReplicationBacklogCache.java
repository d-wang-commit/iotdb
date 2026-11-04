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
import org.apache.iotdb.confignode.conf.ReplicationBacklogConfig.Mode;
import org.apache.iotdb.mpp.rpc.thrift.TRegionReplicationBacklog;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogEdge;
import org.apache.iotdb.mpp.rpc.thrift.TReplicationBacklogReport;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Leader-local incoming replication load. Availability belongs to CFS, which supplies the
 * candidates to compare. Each sampling round reuses missing inputs for at most M rounds before
 * treating their contribution as zero. Arrays contain actual members/edges, never a cluster-wide
 * DN-by-DN matrix.
 */
public final class ReplicationBacklogCache {
  private static final int DIMENSIONS = 4;
  private static final int WARMUP_OBSERVATIONS = 3;

  private final Map<Integer, Group> groups = new HashMap<>();
  private final Map<Integer, Reporter> reporters = new HashMap<>();
  private ReplicationBacklogConfig config = ReplicationBacklogConfig.defaults();
  private long epoch;
  private long topologyRevision;
  private long candidateRevision;
  private long samplingRound = Long.MIN_VALUE;
  private boolean dirty;
  private int diagnosticCursor;

  public synchronized long reset() {
    dirty |= groups.values().stream().anyMatch(group -> !group.selected.isEmpty());
    groups.clear();
    reporters.clear();
    samplingRound = Long.MIN_VALUE;
    diagnosticCursor = 0;
    return ++epoch;
  }

  public synchronized long getEpoch() {
    return epoch;
  }

  public synchronized void configure(ReplicationBacklogConfig next) {
    if (next == config) {
      return;
    }
    if (!next.sameSettings(config) || next.getMode() == Mode.OFF || config.getMode() == Mode.OFF) {
      reset();
    }
    if (next.getMode() != config.getMode()) {
      dirty |= groups.values().stream().anyMatch(group -> !group.selected.isEmpty());
    }
    config = next;
  }

  /**
   * Refresh only authoritative membership; the cache does not interpret node or Region statuses.
   */
  public synchronized void updateTopology(
      Map<TConsensusGroupId, Set<Integer>> locations, long now) {
    Set<Integer> retained = new HashSet<>();
    Set<Integer> retainedNodes = new HashSet<>();
    locations.forEach(
        (id, members) -> {
          if (id.getType() != TConsensusGroupType.DataRegion) {
            return;
          }
          retained.add(id.getId());
          retainedNodes.addAll(members);
          int[] sorted = members.stream().mapToInt(Integer::intValue).sorted().toArray();
          Group group = groups.get(id.getId());
          if (group == null || !Arrays.equals(group.members, sorted)) {
            dirty |= group != null && !group.selected.isEmpty();
            groups.put(id.getId(), new Group(sorted, now));
            topologyRevision++;
          }
        });
    groups
        .entrySet()
        .removeIf(
            entry -> {
              if (retained.contains(entry.getKey())) {
                return false;
              }
              dirty |= !entry.getValue().selected.isEmpty();
              topologyRevision++;
              return true;
            });
    reporters.keySet().retainAll(retainedNodes);
  }

  public synchronized void cacheReport(
      int sourceNodeId,
      TReplicationBacklogReport report,
      long requestTime,
      long callbackEpoch,
      long now) {
    if (callbackEpoch != epoch || config.getMode() == Mode.OFF || now - requestTime < 0) {
      return;
    }
    Reporter reporter = reporters.computeIfAbsent(sourceNodeId, key -> new Reporter());
    if (requestTime <= reporter.requestTime) {
      return;
    }
    reporter.requestTime = requestTime;
    if (report == null
        || !report.isSetSourceInstanceId()
        || report.getSourceInstanceId().isEmpty()
        || !report.isSetSampleSequence()
        || !report.isSetSampleAgeNanos()
        || report.getSampleAgeNanos() < 0
        || report.getSampleSequence() < 0
        || !report.isSetGroups()) {
      return;
    }
    if (reporter.initialized
        && reporter.instance.equals(report.getSourceInstanceId())
        && report.getSampleSequence() <= reporter.sequence) {
      return;
    }
    long ttl = TimeUnit.MILLISECONDS.toNanos(config.getTtlMs());
    long transit = now - requestTime;
    long age = report.getSampleAgeNanos();
    if (age >= ttl || transit >= ttl - age) {
      return;
    }
    if (reporter.initialized && !reporter.instance.equals(report.getSourceInstanceId())) {
      for (Group group : groups.values()) {
        int source = Arrays.binarySearch(group.members, sourceNodeId);
        if (source >= 0) {
          group.sources[source] = null;
          resetTarget(group, source, now);
          updateSelection(group);
        }
      }
    }
    reporter.instance = report.getSourceInstanceId();
    reporter.sequence = report.getSampleSequence();
    reporter.initialized = true;
    long sampledAt = requestTime - age;
    Set<Integer> seen = new HashSet<>();
    Set<Integer> duplicated = new HashSet<>();
    for (TRegionReplicationBacklog sample : report.getGroups()) {
      if (sample != null && sample.isSetRegionId() && !seen.add(sample.getRegionId())) {
        duplicated.add(sample.getRegionId());
      }
    }
    for (TRegionReplicationBacklog sample : report.getGroups()) {
      if (sample == null || !sample.isSetRegionId() || duplicated.contains(sample.getRegionId())) {
        continue;
      }
      Group group = groups.get(sample.getRegionId());
      if (group == null || requestTime < group.createdAt) {
        continue;
      }
      int source = Arrays.binarySearch(group.members, sourceNodeId);
      if (source >= 0) {
        acceptSource(group, source, sample, sampledAt, now);
      }
    }
  }

  private void acceptSource(
      Group group, int sourceIndex, TRegionReplicationBacklog sample, long sampledAt, long now) {
    if (!sample.isSetProtocol()
        || (sample.getProtocol() != 1 && sample.getProtocol() != 2)
        || !sample.isSetGeneration()
        || !sample.isSetState()) {
      return;
    }
    Source old = group.sources[sourceIndex];
    // A RECOVERING report can already identify a new local Region. A recovering outgoing Pipe
    // alone does not restart its source replica, and ERROR reports may carry generation zero.
    if (sample.getState() == 1
        && sample.getGeneration() != 0
        && old != null
        && (old.generation != sample.getGeneration() || old.protocol != sample.getProtocol())) {
      group.sources[sourceIndex] = null;
      resetTarget(group, sourceIndex, now);
      updateSelection(group);
    }
    if (sample.getState() != 0
        || !sample.isSetTargets()
        || sample.getTargetsSize() != group.members.length - 1) {
      return;
    }
    int edgeCount = group.members.length - 1;
    long[] raw = new long[edgeCount * DIMENSIONS];
    long[] generations = new long[edgeCount];
    boolean[] seen = new boolean[group.members.length];
    for (TReplicationBacklogEdge edge : sample.getTargets()) {
      int target =
          edge == null || !edge.isSetTargetNodeId()
              ? -1
              : Arrays.binarySearch(group.members, edge.getTargetNodeId());
      if (target < 0
          || target == sourceIndex
          || seen[target]
          || !edge.isSetGeneration()
          || !edge.isSetState()
          || edge.getState() != 0
          || !edge.isSetRequestCount()
          || !edge.isSetTabletCount()
          || !edge.isSetFileCount()
          || !edge.isSetDeletionCount()
          || edge.getRequestCount() < 0
          || edge.getTabletCount() < 0
          || edge.getFileCount() < 0
          || edge.getDeletionCount() < 0
          || (sample.getProtocol() == 1
              && (edge.getTabletCount() != 0
                  || edge.getFileCount() != 0
                  || edge.getDeletionCount() != 0))
          || (sample.getProtocol() == 2 && edge.getRequestCount() != 0)) {
        return;
      }
      seen[target] = true;
      int edgeIndex = target < sourceIndex ? target : target - 1;
      generations[edgeIndex] = edge.getGeneration();
      int offset = edgeIndex * DIMENSIONS;
      raw[offset] = edge.getRequestCount();
      raw[offset + 1] = edge.getTabletCount();
      raw[offset + 2] = edge.getFileCount();
      raw[offset + 3] = edge.getDeletionCount();
    }
    if (old != null) {
      if (old.generation != sample.getGeneration() || old.protocol != sample.getProtocol()) {
        resetTarget(group, sourceIndex, now);
      } else {
        for (int target = 0; target < group.members.length; target++) {
          if (target != sourceIndex) {
            int edge = target < sourceIndex ? target : target - 1;
            if (old.edgeGenerations[edge] != generations[edge]) {
              resetTarget(group, target, now);
            }
          }
        }
      }
    }
    group.sources[sourceIndex] =
        new Source(sample.getProtocol(), sample.getGeneration(), generations, raw, sampledAt);
    updateSelection(group);
  }

  /** Aggregate fresh, held and expired inputs once per sampling round, independently per source. */
  private void updateObservations(Group group) {
    if (group.members.length < 2) {
      return;
    }
    for (int targetIndex = 0; targetIndex < group.members.length; targetIndex++) {
      Target target = group.targets[targetIndex];
      boolean compatible = true;
      byte protocol = 0;
      int missingRounds = 0;
      for (int sourceIndex = 0; sourceIndex < group.members.length; sourceIndex++) {
        if (sourceIndex == targetIndex) {
          continue;
        }
        Source source = group.sources[sourceIndex];
        missingRounds =
            Math.max(
                missingRounds,
                source == null ? config.getConfirmationRounds() + 1 : source.missingRounds);
        if (!usable(source, target)) {
          continue;
        }
        if (protocol != 0 && protocol != source.protocol) {
          compatible = false;
          break;
        }
        protocol = source.protocol;
      }
      if (!compatible) {
        continue;
      }
      if (protocol == 0) {
        protocol = target.protocol;
      }
      if (protocol == 0) {
        // Initialization/recovery still needs a post-reset incoming sample to identify its units.
        continue;
      }
      if (target.observations > 0 && target.protocol != protocol) {
        resetTarget(group, targetIndex, target.since);
        // Protocol changes cannot combine old and new dimensions in one EWMA.
      }
      Arrays.fill(target.raw, 0);
      for (int sourceIndex = 0; sourceIndex < group.members.length; sourceIndex++) {
        if (sourceIndex == targetIndex) {
          continue;
        }
        Source source = group.sources[sourceIndex];
        if (!usable(source, target)) {
          continue;
        }
        int offset = (targetIndex < sourceIndex ? targetIndex : targetIndex - 1) * DIMENSIONS;
        for (int dimension = 0; dimension < DIMENSIONS; dimension++) {
          target.raw[dimension] += source.raw[offset + dimension];
        }
      }
      for (int dimension = 0; dimension < DIMENSIONS; dimension++) {
        target.smoothed[dimension] =
            target.observations == 0
                ? target.raw[dimension]
                : config.getAlpha() * target.raw[dimension]
                    + (1 - config.getAlpha()) * target.smoothed[dimension];
      }
      target.protocol = protocol;
      target.observations = Math.min(WARMUP_OBSERVATIONS, target.observations + 1);
      target.version++;
      target.missingRounds = missingRounds;
    }
  }

  private boolean usable(Source source, Target target) {
    return source != null
        && source.sampledAt >= target.since
        && source.missingRounds <= config.getConfirmationRounds();
  }

  /**
   * Only the sampling scheduler calls this method. Reports update inputs; this method advances
   * missing counters, EWMA and light confirmation at most once per distinct round.
   */
  public synchronized void advanceSamplingRound(long roundId) {
    if (roundId <= samplingRound) {
      return;
    }
    samplingRound = roundId;
    for (Group group : groups.values()) {
      for (Source source : group.sources) {
        if (source == null) {
          continue;
        }
        if (!source.updated) {
          source.missingRounds =
              Math.min(config.getConfirmationRounds() + 1, source.missingRounds + 1);
        }
        source.updated = false;
      }
      updateObservations(group);
      classify(group);
      updateSelection(group);
    }
  }

  /** Compare only CFS candidates; the first mature EWMA is also the first possible light vote. */
  public synchronized void evaluate(
      Map<TConsensusGroupId, Set<Integer>> runningCandidates, long now) {
    for (Map.Entry<Integer, Group> entry : groups.entrySet()) {
      Group group = entry.getValue();
      Set<Integer> running =
          runningCandidates.getOrDefault(
              new TConsensusGroupId(TConsensusGroupType.DataRegion, entry.getKey()),
              Collections.emptySet());
      boolean changed = false;
      for (int i = 0; i < group.members.length; i++) {
        boolean available = running.contains(group.members[i]);
        if (group.available[i] != available) {
          changed = true;
          if (group.runningKnown && available) {
            resetTarget(group, i, now);
          }
        }
        group.available[i] = available;
      }
      if (changed) {
        topologyRevision++;
      }
      group.runningKnown = true;
      classify(group);
      updateSelection(group);
    }
  }

  private void classify(Group group) {
    for (int i = 0; i < group.members.length; i++) {
      Target target = group.targets[i];
      if (!group.available[i] || !comparable(target) || !config.hasThresholds(target.protocol)) {
        continue;
      }
      boolean heavy = false;
      for (int j = 0; j < group.members.length; j++) {
        Target other = group.targets[j];
        if (i != j
            && group.available[j]
            && comparable(other)
            && target.protocol == other.protocol) {
          heavy |= dominates(target.smoothed, other.smoothed, target.protocol);
        }
      }
      if (heavy) {
        target.heavy = true;
        target.lightStreak = 0;
      } else if (target.classifiedVersion != target.version) {
        target.lightStreak = Math.min(config.getConfirmationRounds(), target.lightStreak + 1);
        if (target.lightStreak >= config.getConfirmationRounds()) {
          target.heavy = false;
        }
      }
      target.instantHeavy = heavy;
      target.classifiedVersion = target.version;
    }
  }

  private boolean comparable(Target target) {
    return target.observations >= WARMUP_OBSERVATIONS;
  }

  private boolean dominates(double[] heavy, double[] light, byte protocol) {
    boolean significant = false;
    int first = protocol == 1 ? 0 : 1;
    int end = protocol == 1 ? 1 : DIMENSIONS;
    for (int dimension = first; dimension < end; dimension++) {
      if (heavy[dimension] < light[dimension]) {
        return false;
      }
      significant |=
          heavy[dimension] - light[dimension]
              >= Math.max(
                  config.getAbsoluteGap(dimension),
                  config.getRelativeGap() * Math.max(light[dimension], 1));
    }
    return significant;
  }

  /** Initialization and all recovery paths use the same conservative state. */
  private void resetTarget(Group group, int index, long now) {
    Target target = group.targets[index];
    Arrays.fill(target.raw, 0);
    Arrays.fill(target.smoothed, 0);
    target.since = now;
    target.protocol = 0;
    target.observations = 0;
    target.version = 0;
    target.classifiedVersion = 0;
    target.missingRounds = 0;
    target.lightStreak = 0;
    target.heavy = true;
    target.instantHeavy = false;
  }

  /** Empty means no constraint: either all candidates are H, or every candidate is L. */
  private void updateSelection(Group group) {
    Set<Integer> light = new HashSet<>();
    int available = 0;
    for (int i = 0; i < group.members.length; i++) {
      if (group.available[i]) {
        available++;
        if (!group.targets[i].heavy) {
          light.add(group.members[i]);
        }
      }
    }
    Set<Integer> selected = light.size() == available ? Collections.emptySet() : light;
    if (!selected.equals(group.selected)) {
      group.selected = selected;
      dirty = true;
      candidateRevision++;
    }
  }

  /**
   * Only constrained groups are returned. Absence means use all CFS-provided Running candidates.
   */
  public synchronized Map<TConsensusGroupId, Set<Integer>> eligibleCandidates(long now) {
    if (config.getMode() != Mode.ENFORCE) {
      return Collections.emptyMap();
    }
    Map<TConsensusGroupId, Set<Integer>> result = new HashMap<>();
    groups.forEach(
        (id, group) -> {
          if (!group.selected.isEmpty()) {
            result.put(
                new TConsensusGroupId(TConsensusGroupType.DataRegion, id),
                new HashSet<>(group.selected));
          }
        });
    return result;
  }

  public synchronized SelectionSnapshot captureSelection(long now) {
    return new SelectionSnapshot(
        epoch, topologyRevision, candidateRevision, config, eligibleCandidates(now));
  }

  public synchronized boolean isSelectionValid(SelectionSnapshot snapshot, long now) {
    return snapshot.epoch == epoch
        && snapshot.config == config
        && snapshot.topologyRevision == topologyRevision
        && (config.getMode() != Mode.ENFORCE || snapshot.candidateRevision == candidateRevision);
  }

  public static final class SelectionSnapshot {
    private final long epoch;
    private final long topologyRevision;
    private final long candidateRevision;
    private final ReplicationBacklogConfig config;
    private final Map<TConsensusGroupId, Set<Integer>> candidates;

    private SelectionSnapshot(
        long epoch,
        long topologyRevision,
        long candidateRevision,
        ReplicationBacklogConfig config,
        Map<TConsensusGroupId, Set<Integer>> candidates) {
      this.epoch = epoch;
      this.topologyRevision = topologyRevision;
      this.candidateRevision = candidateRevision;
      this.config = config;
      this.candidates = candidates;
    }

    public Map<TConsensusGroupId, Set<Integer>> getCandidates() {
      return candidates;
    }
  }

  public synchronized boolean consumeDirty() {
    boolean changed = dirty;
    dirty = false;
    return changed;
  }

  /** Rotate a bounded sample for operator logs without registering per-region metric objects. */
  public synchronized Map<Integer, Map<Integer, ReplicaDiagnostic>> sampleDiagnostics(
      int limit, long now) {
    Map<Integer, Map<Integer, ReplicaDiagnostic>> result = new HashMap<>();
    if (groups.isEmpty() || limit <= 0) {
      return result;
    }
    int start = diagnosticCursor % groups.size();
    int offset = 0;
    for (int regionId : groups.keySet()) {
      if (offset++ < start) {
        continue;
      }
      result.put(regionId, getDiagnostics(regionId, now));
      if (result.size() >= limit) {
        break;
      }
    }
    diagnosticCursor = (start + result.size()) % groups.size();
    return result;
  }

  public synchronized Map<Integer, ReplicaDiagnostic> getDiagnostics(int regionId, long now) {
    Group group = groups.get(regionId);
    if (group == null) {
      return Collections.emptyMap();
    }
    Map<Integer, ReplicaDiagnostic> result = new HashMap<>();
    for (int i = 0; i < group.members.length; i++) {
      Target target = group.targets[i];
      result.put(
          group.members[i],
          new ReplicaDiagnostic(
              target.observations > 0,
              group.available[i],
              target.heavy,
              target.instantHeavy,
              target.observations,
              target.lightStreak,
              target.missingRounds,
              target.raw.clone(),
              target.smoothed.clone()));
    }
    return result;
  }

  public static final class ReplicaDiagnostic {
    private final boolean known;
    private final boolean available;
    private final boolean heavy;
    private final boolean instantHeavy;
    private final int warmupSamples;
    private final int lightStreak;
    private final int missingRounds;
    private final double[] counts;
    private final double[] smoothedCounts;

    private ReplicaDiagnostic(
        boolean known,
        boolean available,
        boolean heavy,
        boolean instantHeavy,
        int warmupSamples,
        int lightStreak,
        int missingRounds,
        double[] counts,
        double[] smoothedCounts) {
      this.known = known;
      this.available = available;
      this.heavy = heavy;
      this.instantHeavy = instantHeavy;
      this.warmupSamples = warmupSamples;
      this.lightStreak = lightStreak;
      this.missingRounds = missingRounds;
      this.counts = counts;
      this.smoothedCounts = smoothedCounts;
    }

    public boolean isKnown() {
      return known;
    }

    public boolean isAvailable() {
      return available;
    }

    public boolean isHeavy() {
      return heavy;
    }

    public boolean isInstantHeavy() {
      return instantHeavy;
    }

    public int getWarmupSamples() {
      return warmupSamples;
    }

    public int getLightStreak() {
      return lightStreak;
    }

    public int getMissingRounds() {
      return missingRounds;
    }

    public double[] getCounts() {
      return counts.clone();
    }

    public double[] getSmoothedCounts() {
      return smoothedCounts.clone();
    }
  }

  private static final class Reporter {
    private boolean initialized;
    private String instance;
    private long requestTime = Long.MIN_VALUE;
    private long sequence;
  }

  private static final class Group {
    private final int[] members;
    private final Source[] sources;
    private final Target[] targets;
    private final boolean[] available;
    private final long createdAt;
    private boolean runningKnown;
    private Set<Integer> selected = Collections.emptySet();

    private Group(int[] members, long now) {
      this.members = members;
      sources = new Source[members.length];
      targets = new Target[members.length];
      available = new boolean[members.length];
      for (int i = 0; i < members.length; i++) {
        targets[i] = new Target(now);
      }
      createdAt = now;
    }
  }

  private static final class Target {
    private final double[] raw = new double[DIMENSIONS];
    private final double[] smoothed = new double[DIMENSIONS];
    private long since;
    private byte protocol;
    private int observations;
    private long version;
    private long classifiedVersion;
    private int missingRounds;
    private int lightStreak;
    private boolean heavy = true;
    private boolean instantHeavy;

    private Target(long now) {
      since = now;
    }
  }

  private static final class Source {
    private final byte protocol;
    private final long generation;
    private final long[] edgeGenerations;
    private final long[] raw;
    private final long sampledAt;
    private boolean updated = true;
    private int missingRounds;

    private Source(
        byte protocol, long generation, long[] edgeGenerations, long[] raw, long sampledAt) {
      this.protocol = protocol;
      this.generation = generation;
      this.edgeGenerations = edgeGenerations;
      this.raw = raw;
      this.sampledAt = sampledAt;
    }
  }
}
