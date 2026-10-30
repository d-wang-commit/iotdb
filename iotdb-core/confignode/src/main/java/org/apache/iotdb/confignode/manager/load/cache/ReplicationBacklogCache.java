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
import org.apache.iotdb.confignode.conf.ReplicationBacklogConfig.Mode;
import org.apache.iotdb.confignode.manager.load.cache.node.NodeStatistics;
import org.apache.iotdb.confignode.manager.load.cache.region.RegionStatistics;
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
 * Leader-local telemetry. Missing samples are held until their original expiry; neither a duplicate
 * response nor a scheduler tick can supply additional confirmation evidence. Arrays contain only
 * actual members/edges, never a cluster-wide DN-by-DN matrix.
 */
public final class ReplicationBacklogCache {
  private static final int DIMENSIONS = 4;
  private final Map<Integer, Group> groups = new HashMap<>();
  private final Map<Integer, Reporter> reporters = new HashMap<>();
  private ReplicationBacklogConfig config = ReplicationBacklogConfig.defaults();
  private long epoch;
  private long topologyRevision;
  private long candidateRevision;
  private boolean dirty;
  private int diagnosticCursor;

  public synchronized long reset() {
    dirty |= groups.values().stream().anyMatch(group -> hasExclusion(group));
    groups.clear();
    reporters.clear();
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
      dirty |= groups.values().stream().anyMatch(group -> hasExclusion(group));
    }
    config = next;
  }

  /** Refresh authoritative membership and availability; load exclusion never modifies either. */
  public synchronized void updateTopology(
      Map<TConsensusGroupId, Set<Integer>> locations,
      Map<Integer, NodeStatistics> nodes,
      Map<TConsensusGroupId, Map<Integer, RegionStatistics>> regions,
      long now) {
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
            if (group != null) {
              clearDecision(group);
            }
            group = new Group(sorted, now, group);
            groups.put(id.getId(), group);
            topologyRevision++;
          }
          Map<Integer, RegionStatistics> region = regions.getOrDefault(id, Collections.emptyMap());
          boolean changed = false;
          for (int i = 0; i < sorted.length; i++) {
            NodeStatistics node = nodes.get(sorted[i]);
            RegionStatistics replica = region.get(sorted[i]);
            boolean available =
                node != null
                    && node.getStatus() == NodeStatus.Running
                    && replica != null
                    && replica.getRegionStatus() == RegionStatus.Running;
            if (group.available[i] != available) {
              changed = true;
              resetAdmission(group, i, now);
            }
            group.available[i] = available;
          }
          if (changed) {
            topologyRevision++;
            clearDecision(group);
          }
        });
    groups
        .entrySet()
        .removeIf(
            entry -> {
              if (retained.contains(entry.getKey())) {
                return false;
              }
              dirty |= hasExclusion(entry.getValue());
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
    Reporter reporter = reporters.get(sourceNodeId);
    if (reporter != null && requestTime <= reporter.requestTime) {
      return;
    }
    if (reporter == null) {
      reporter = new Reporter();
      reporters.put(sourceNodeId, reporter);
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
      invalidateSource(sourceNodeId);
      return;
    }
    if (reporter.initialized
        && reporter.instance.equals(report.getSourceInstanceId())
        && report.getSampleSequence() <= reporter.sequence) {
      return;
    }
    if (reporter.initialized && !reporter.instance.equals(report.getSourceInstanceId())) {
      invalidateSource(sourceNodeId);
      for (Group group : groups.values()) {
        int index = Arrays.binarySearch(group.members, sourceNodeId);
        if (index >= 0) {
          resetAdmission(group, index, now);
        }
      }
    }
    reporter.instance = report.getSourceInstanceId();
    reporter.sequence = report.getSampleSequence();
    reporter.initialized = true;
    long ttl = TimeUnit.MILLISECONDS.toNanos(config.getTtlMs());
    long transit = now - requestTime;
    long age = report.getSampleAgeNanos();
    boolean expired = age >= ttl || transit >= ttl - age;
    long sampledAt = expired ? now - ttl : requestTime - age;
    Set<Integer> seen = new HashSet<>();
    for (TRegionReplicationBacklog sample : report.getGroups()) {
      if (sample == null || !sample.isSetRegionId()) {
        continue;
      }
      Group group = groups.get(sample.getRegionId());
      if (group == null || requestTime < group.createdAt) {
        continue;
      }
      int source = Arrays.binarySearch(group.members, sourceNodeId);
      if (source < 0) {
        continue;
      }
      if (!seen.add(sample.getRegionId())) {
        group.sources[source] = null;
        clearDecision(group);
        continue;
      }
      acceptSource(group, source, sample, report.getSampleSequence(), sampledAt, expired, now);
    }
  }

  private void invalidateSource(int sourceNodeId) {
    for (Group group : groups.values()) {
      int index = Arrays.binarySearch(group.members, sourceNodeId);
      if (index >= 0) {
        group.sources[index] = null;
        clearDecision(group);
      }
    }
  }

  private void acceptSource(
      Group group,
      int sourceIndex,
      TRegionReplicationBacklog sample,
      long sequence,
      long sampledAt,
      boolean expired,
      long now) {
    if (!sample.isSetProtocol()
        || (sample.getProtocol() != 1 && sample.getProtocol() != 2)
        || !sample.isSetGeneration()
        || !sample.isSetState()
        || sample.getState() != 0
        || !sample.isSetTargets()
        || sample.getTargetsSize() != group.members.length - 1
        || expired) {
      group.sources[sourceIndex] = null;
      clearDecision(group);
      return;
    }
    Source old = group.sources[sourceIndex];
    if (old != null && !fresh(old, now)) {
      old = null;
      clearDecision(group);
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
        group.sources[sourceIndex] = null;
        clearDecision(group);
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
    if (old == null
        || old.generation != sample.getGeneration()
        || old.protocol != sample.getProtocol()
        || !Arrays.equals(old.edgeGenerations, generations)) {
      // A new local Region incarnation needs admission again. Recreating only an outgoing Pipe
      // resets load evidence, but does not mean the healthy source replica has restarted.
      if (old != null
          && (old.generation != sample.getGeneration() || old.protocol != sample.getProtocol())) {
        resetAdmission(group, sourceIndex, now);
      }
      clearDecision(group);
      old = new Source(sample.getProtocol(), sample.getGeneration(), generations, raw.length);
      for (int i = 0; i < raw.length; i++) {
        old.smoothed[i] = raw[i];
      }
      group.sources[sourceIndex] = old;
    } else {
      for (int i = 0; i < raw.length; i++) {
        old.smoothed[i] = config.getAlpha() * raw[i] + (1 - config.getAlpha()) * old.smoothed[i];
      }
    }
    old.raw = raw;
    old.sequence = sequence;
    old.sampledAt = sampledAt;
  }

  /** Evaluate once per complete set of genuinely new source observations, never once per tick. */
  public synchronized void evaluate(long now) {
    for (Group group : groups.values()) {
      boolean valid = group.members.length > 1;
      boolean complete = true;
      byte protocol = 0;
      for (Source source : group.sources) {
        if (source == null || !fresh(source, now)) {
          valid = false;
          break;
        }
        if (protocol != 0 && protocol != source.protocol) {
          valid = false;
          break;
        }
        protocol = source.protocol;
        complete &= source.sequence != source.observedSequence;
      }
      if (!valid) {
        clearDecision(group);
        continue;
      }
      group.valid = true;
      if (!complete) {
        continue;
      }
      for (Source source : group.sources) {
        source.observedSequence = source.sequence;
      }
      if (!config.hasThresholds(protocol)) {
        clearDecision(group);
        group.valid = true;
        continue;
      }
      double[][] incoming = incoming(group, true);
      for (int i = 0; i < group.members.length; i++) {
        if (!group.available[i]) {
          group.heavy[i] = false;
          group.streak[i] = 0;
          continue;
        }
        if (!group.confirmed[i]) {
          boolean afterRecovery = true;
          for (Source source : group.sources) {
            afterRecovery &= source.sampledAt >= group.admissionSince[i];
          }
          if (!afterRecovery) {
            group.streak[i] = 0;
            group.admissionStreak[i] = 0;
            continue;
          }
        }
        boolean enter = false;
        boolean retain = false;
        for (int j = 0; j < group.members.length; j++) {
          if (i == j || !group.available[j]) {
            continue;
          }
          enter |= dominates(incoming[i], incoming[j], protocol, 1);
          retain |= dominates(incoming[i], incoming[j], protocol, 0.5);
        }
        boolean change = group.heavy[i] ? !retain : enter;
        group.streak[i] = change ? group.streak[i] + 1 : 0;
        if (group.streak[i] >= config.getConfirmationRounds()) {
          group.heavy[i] = !group.heavy[i];
          group.streak[i] = 0;
          dirty = true;
          candidateRevision++;
        }
        // A recovered replica is neither light nor heavy until fresh post-recovery observations
        // confirm its classification. Missing/duplicate samples cannot open this admission gate.
        if (!group.confirmed[i]) {
          group.admissionStreak[i] = !enter ? group.admissionStreak[i] + 1 : 0;
          if (group.heavy[i] || group.admissionStreak[i] >= config.getConfirmationRounds()) {
            group.confirmed[i] = true;
            group.admissionStreak[i] = 0;
            dirty = true;
            candidateRevision++;
          }
        }
      }
      boolean anyEligible = false;
      for (int i = 0; i < group.members.length; i++) {
        anyEligible |= group.available[i] && !group.heavy[i];
      }
      if (!anyEligible) {
        clearDecision(group);
        group.valid = true;
      }
    }
  }

  private boolean dominates(double[] heavy, double[] light, byte protocol, double scale) {
    boolean significant = false;
    int first = protocol == 1 ? 0 : 1;
    int end = protocol == 1 ? 1 : DIMENSIONS;
    for (int k = first; k < end; k++) {
      if (heavy[k] < light[k]) {
        return false;
      }
      significant |=
          heavy[k] - light[k]
              >= Math.max(
                  config.getAbsoluteGap(k) * scale,
                  config.getRelativeGap() * scale * Math.max(light[k], 1));
    }
    return significant;
  }

  private boolean fresh(Source source, long now) {
    long age = now - source.sampledAt;
    return age >= 0 && age < TimeUnit.MILLISECONDS.toNanos(config.getTtlMs());
  }

  private void clearDecision(Group group) {
    if (hasHeavyReplica(group)) {
      dirty = true;
      candidateRevision++;
    }
    Arrays.fill(group.heavy, false);
    Arrays.fill(group.streak, 0);
    Arrays.fill(group.admissionStreak, 0);
    group.valid = false;
    for (Source source : group.sources) {
      if (source != null) {
        source.observedSequence = source.sequence;
      }
    }
  }

  private static boolean hasExclusion(Group group) {
    boolean confirmed = false;
    boolean pending = false;
    for (int i = 0; i < group.members.length; i++) {
      confirmed |= group.available[i] && group.confirmed[i];
      pending |= group.available[i] && !group.confirmed[i];
    }
    return hasHeavyReplica(group) || (confirmed && pending);
  }

  private static boolean hasHeavyReplica(Group group) {
    for (boolean heavy : group.heavy) {
      if (heavy) {
        return true;
      }
    }
    return false;
  }

  private void resetAdmission(Group group, int index, long now) {
    if (group.confirmed[index]) {
      dirty = true;
      candidateRevision++;
    }
    group.confirmed[index] = false;
    group.admissionStreak[index] = 0;
    group.admissionSince[index] = now;
  }

  private boolean hasFreshDecision(Group group, long now) {
    return group.valid && Arrays.stream(group.sources).allMatch(s -> s != null && fresh(s, now));
  }

  /** Only constrained groups are returned. Absence means use the original availability checks. */
  public synchronized Map<TConsensusGroupId, Set<Integer>> eligibleCandidates(long now) {
    if (config.getMode() != Mode.ENFORCE) {
      return Collections.emptyMap();
    }
    Map<TConsensusGroupId, Set<Integer>> result = new HashMap<>();
    for (Map.Entry<Integer, Group> entry : groups.entrySet()) {
      Group group = entry.getValue();
      boolean freshDecision = hasFreshDecision(group, now);
      Set<Integer> retained = new HashSet<>();
      Set<Integer> confirmed = new HashSet<>();
      int available = 0;
      for (int i = 0; i < group.members.length; i++) {
        if (!group.available[i]) {
          continue;
        }
        available++;
        if (group.confirmed[i]) {
          confirmed.add(group.members[i]);
          if (!freshDecision || !group.heavy[i]) {
            retained.add(group.members[i]);
          }
        }
      }
      // Prefer even a heavy confirmed replica over an unconfirmed recovery. Only when none is
      // available (including cluster bootstrap) may availability override the admission gate.
      if (retained.isEmpty()) {
        retained = confirmed;
      }
      if (!retained.isEmpty() && retained.size() < available) {
        result.put(new TConsensusGroupId(TConsensusGroupType.DataRegion, entry.getKey()), retained);
      }
    }
    return result;
  }

  /** One solver input; its map is reused during final validation, never rebuilt. */
  public synchronized SelectionSnapshot captureSelection(long now) {
    Map<TConsensusGroupId, Set<Integer>> candidates = eligibleCandidates(now);
    Set<Integer> loadConstrainedGroups = new HashSet<>();
    candidates.forEach(
        (id, retained) -> {
          Group group = groups.get(id.getId());
          if (hasFreshDecision(group, now)) {
            for (int i = 0; i < group.members.length; i++) {
              if (group.available[i]
                  && group.confirmed[i]
                  && group.heavy[i]
                  && !retained.contains(group.members[i])) {
                loadConstrainedGroups.add(id.getId());
              }
            }
          }
        });
    return new SelectionSnapshot(
        epoch, topologyRevision, candidateRevision, config, candidates, loadConstrainedGroups);
  }

  /**
   * Check current source freshness, so a long solve can remain valid when reports keep arriving.
   */
  public synchronized boolean isSelectionValid(SelectionSnapshot snapshot, long now) {
    if (snapshot.epoch != epoch
        || snapshot.config != config
        || snapshot.topologyRevision != topologyRevision
        || (config.getMode() == Mode.ENFORCE && snapshot.candidateRevision != candidateRevision)) {
      return false;
    }
    // Admission-only constraints remain valid while recovery samples are missing. Only exclusions
    // based on measured load require fresh telemetry at the end of a potentially long solve.
    for (int region : snapshot.loadConstrainedGroups) {
      Group group = groups.get(region);
      if (group == null || !hasFreshDecision(group, now)) {
        return false;
      }
    }
    return true;
  }

  public static final class SelectionSnapshot {
    private final long epoch;
    private final long topologyRevision;
    private final long candidateRevision;
    private final ReplicationBacklogConfig config;
    private final Map<TConsensusGroupId, Set<Integer>> candidates;
    private final Set<Integer> loadConstrainedGroups;

    private SelectionSnapshot(
        long epoch,
        long topologyRevision,
        long candidateRevision,
        ReplicationBacklogConfig config,
        Map<TConsensusGroupId, Set<Integer>> candidates,
        Set<Integer> loadConstrainedGroups) {
      this.epoch = epoch;
      this.topologyRevision = topologyRevision;
      this.candidateRevision = candidateRevision;
      this.config = config;
      this.candidates = candidates;
      this.loadConstrainedGroups = loadConstrainedGroups;
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

  /** On-demand bounded diagnostics; normal heartbeat processing does not copy the whole cache. */
  public synchronized Map<Integer, ReplicaDiagnostic> getDiagnostics(int regionId, long now) {
    Group group = groups.get(regionId);
    if (group == null) {
      return Collections.emptyMap();
    }
    Map<Integer, ReplicaDiagnostic> result = new HashMap<>();
    double[][] smooth = incoming(group, true);
    double[][] raw = incoming(group, false);
    boolean complete =
        group.valid && Arrays.stream(group.sources).allMatch(s -> s != null && fresh(s, now));
    for (int i = 0; i < group.members.length; i++) {
      boolean known = true;
      for (int s = 0; s < group.members.length; s++) {
        if (s != i && (group.sources[s] == null || !fresh(group.sources[s], now))) {
          known = false;
        }
      }
      result.put(
          group.members[i],
          new ReplicaDiagnostic(
              known,
              group.available[i],
              !group.confirmed[i],
              complete && group.heavy[i],
              raw[i],
              smooth[i]));
    }
    return result;
  }

  private static double[][] incoming(Group group, boolean smoothed) {
    double[][] result = new double[group.members.length][DIMENSIONS];
    for (int s = 0; s < group.members.length; s++) {
      Source source = group.sources[s];
      if (source == null) {
        continue;
      }
      for (int t = 0; t < group.members.length; t++) {
        if (t == s) {
          continue;
        }
        int offset = (t < s ? t : t - 1) * DIMENSIONS;
        for (int k = 0; k < DIMENSIONS; k++) {
          result[t][k] += smoothed ? source.smoothed[offset + k] : source.raw[offset + k];
        }
      }
    }
    return result;
  }

  public static final class ReplicaDiagnostic {
    private final boolean known;
    private final boolean available;
    private final boolean admissionPending;
    private final boolean heavy;
    private final double[] counts;
    private final double[] smoothedCounts;

    private ReplicaDiagnostic(
        boolean known,
        boolean available,
        boolean admissionPending,
        boolean heavy,
        double[] counts,
        double[] smoothedCounts) {
      this.known = known;
      this.available = available;
      this.admissionPending = admissionPending;
      this.heavy = heavy;
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

    public boolean isAdmissionPending() {
      return admissionPending;
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
    private long requestTime;
    private long sequence;
  }

  private static final class Group {
    private final int[] members;
    private final Source[] sources;
    private final boolean[] available;
    private final boolean[] confirmed;
    private final int[] admissionStreak;
    private final long[] admissionSince;
    private final boolean[] heavy;
    private final int[] streak;
    private final long createdAt;
    private boolean valid;

    private Group(int[] members, long now, Group previous) {
      this.members = members;
      sources = new Source[members.length];
      available = new boolean[members.length];
      confirmed = new boolean[members.length];
      admissionStreak = new int[members.length];
      admissionSince = new long[members.length];
      Arrays.fill(admissionSince, now);
      // A membership change must not turn every healthy replica into an unconfirmed candidate,
      // otherwise the all-pending availability fallback would also admit the newly added replica.
      if (previous != null) {
        for (int i = 0; i < members.length; i++) {
          int oldIndex = Arrays.binarySearch(previous.members, members[i]);
          if (oldIndex >= 0) {
            available[i] = previous.available[oldIndex];
            confirmed[i] = previous.confirmed[oldIndex];
          }
        }
      }
      heavy = new boolean[members.length];
      streak = new int[members.length];
      createdAt = now;
    }
  }

  private static final class Source {
    private final byte protocol;
    private final long generation;
    private final long[] edgeGenerations;
    private final double[] smoothed;
    private long[] raw;
    private long sampledAt;
    private long sequence;
    private long observedSequence = -1;

    private Source(byte protocol, long generation, long[] edgeGenerations, int size) {
      this.protocol = protocol;
      this.generation = generation;
      this.edgeGenerations = edgeGenerations;
      smoothed = new double[size];
    }
  }
}
