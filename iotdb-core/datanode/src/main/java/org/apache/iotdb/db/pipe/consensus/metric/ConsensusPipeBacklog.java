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

package org.apache.iotdb.db.pipe.consensus.metric;

import org.apache.iotdb.commons.pipe.event.PipeReplicationEventToken;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Pending work for one Consensus Pipe source instance. Counts describe planned replication events,
 * not missing rows or bytes. The registry contains only running source instances; event tokens
 * retain their original owner across task restarts and never retain data resources.
 */
public final class ConsensusPipeBacklog implements AutoCloseable {

  public static final byte VALID = 0;
  public static final byte RECOVERING = 1;
  public static final byte ERROR = 3;

  public enum Kind {
    TABLET,
    FILE,
    DELETION
  }

  private static final ConcurrentMap<String, ConsensusPipeBacklog> INSTANCES =
      new ConcurrentHashMap<>();
  private static final AtomicLong NEXT_GENERATION = new AtomicLong();

  private final String pipeName;
  private final long creationTime;
  private final long sourceId;
  private final long generation = NEXT_GENERATION.incrementAndGet();

  // Unclaimed work belongs to the existing historical resource queue, not a second event index.
  private final long[] pending = new long[Kind.values().length];
  private final long[] unclaimed = new long[Kind.values().length];
  private boolean historicalReady;
  private boolean realtimeReady;
  private boolean invalid;
  private boolean closed;

  private ConsensusPipeBacklog(
      final String pipeName, final long creationTime, final long sourceId) {
    this.pipeName = pipeName;
    this.creationTime = creationTime;
    this.sourceId = sourceId;
  }

  public static ConsensusPipeBacklog create(
      final String pipeName, final long creationTime, final long sourceId) {
    final ConsensusPipeBacklog instance =
        new ConsensusPipeBacklog(pipeName, creationTime, sourceId);
    final ConsensusPipeBacklog previous = INSTANCES.put(pipeName, instance);
    if (previous != null) {
      previous.close();
    }
    return instance;
  }

  public static ConsensusPipeBacklog get(final String pipeName) {
    return INSTANCES.get(pipeName);
  }

  public static Snapshot snapshot(final String pipeName) {
    final ConsensusPipeBacklog instance = INSTANCES.get(pipeName);
    return instance == null ? null : instance.snapshot();
  }

  public static Map<String, Snapshot> getSnapshots() {
    final Map<String, Snapshot> snapshots = new HashMap<>();
    INSTANCES.forEach((name, instance) -> snapshots.put(name, instance.snapshot()));
    return snapshots;
  }

  /** Counts a newly generated item; neither retries nor queue transfers call this method. */
  public synchronized Token newToken(final Kind kind) {
    if (kind == null || closed) {
      invalid = true;
      return new Token(this, Kind.TABLET, true);
    }
    final int index = kind.ordinal();
    if (pending[index] == Long.MAX_VALUE) {
      invalid = true;
      return new Token(this, kind, true);
    }
    pending[index]++;
    return new Token(this, kind, false);
  }

  /** Reserves already-discovered historical items before the source supplies their events. */
  public synchronized boolean addPending(final Kind kind, final long count) {
    if (kind == null || count < 0 || closed) {
      invalid = true;
      return false;
    }
    final int index = kind.ordinal();
    if (count > Long.MAX_VALUE - pending[index]) {
      invalid = true;
      return false;
    }
    pending[index] += count;
    unclaimed[index] += count;
    return true;
  }

  /** Transfers one historical reservation to an event without incrementing pending work. */
  public synchronized Token takePendingToken(final Kind kind) {
    if (kind == null || closed) {
      invalid = true;
      return new Token(this, Kind.TABLET, true);
    }
    final int index = kind.ordinal();
    if (unclaimed[index] == 0) {
      invalid = true;
      return new Token(this, kind, true);
    }
    unclaimed[index]--;
    return new Token(this, kind, false);
  }

  /** Removes historical items that the actual source has filtered out before supplying. */
  public synchronized void discardPending(final Kind kind, final long count) {
    if (kind == null || count < 0) {
      invalid = true;
      return;
    }
    final int index = kind.ordinal();
    if (count > unclaimed[index]) {
      invalid = true;
      return;
    }
    unclaimed[index] -= count;
    pending[index] -= count;
  }

  public synchronized void markHistoricalReady() {
    historicalReady = true;
  }

  public synchronized void markRealtimeReady() {
    realtimeReady = true;
  }

  public synchronized void invalidate() {
    invalid = true;
  }

  public synchronized Snapshot snapshot() {
    return new Snapshot(
        generation,
        pending[Kind.TABLET.ordinal()],
        pending[Kind.FILE.ordinal()],
        pending[Kind.DELETION.ordinal()],
        invalid || closed ? ERROR : historicalReady && realtimeReady ? VALID : RECOVERING);
  }

  public long getCreationTime() {
    return creationTime;
  }

  public long getSourceId() {
    return sourceId;
  }

  public long getGeneration() {
    return generation;
  }

  @Override
  public synchronized void close() {
    closed = true;
    INSTANCES.remove(pipeName, this);
  }

  public static final class Token implements PipeReplicationEventToken {

    private final ConsensusPipeBacklog owner;
    private final Kind kind;
    // Protected by owner's monitor, shared with its counters and snapshot.
    private boolean finished;

    private Token(final ConsensusPipeBacklog owner, final Kind kind, final boolean finished) {
      this.owner = owner;
      this.kind = kind;
      this.finished = finished;
    }

    @Override
    public void finish() {
      synchronized (owner) {
        if (finished) {
          return;
        }
        finished = true;
        final int index = kind.ordinal();
        if (owner.pending[index] <= owner.unclaimed[index]) {
          owner.invalid = true;
          return;
        }
        owner.pending[index]--;
      }
    }

    @Override
    public void invalidate() {
      owner.invalidate();
    }

    public Kind getKind() {
      return kind;
    }

    public boolean isOwnerClosed() {
      synchronized (owner) {
        return owner.closed;
      }
    }
  }

  public static final class Snapshot {

    private final long generation;
    private final long tabletCount;
    private final long fileCount;
    private final long deletionCount;
    private final byte state;

    private Snapshot(
        final long generation,
        final long tabletCount,
        final long fileCount,
        final long deletionCount,
        final byte state) {
      this.generation = generation;
      this.tabletCount = tabletCount;
      this.fileCount = fileCount;
      this.deletionCount = deletionCount;
      this.state = state;
    }

    public long getGeneration() {
      return generation;
    }

    public long getTabletCount() {
      return tabletCount;
    }

    public long getFileCount() {
      return fileCount;
    }

    public long getDeletionCount() {
      return deletionCount;
    }

    public byte getState() {
      return state;
    }
  }
}
