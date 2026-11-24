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

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.consensus.iot.thrift.TLogEntry;
import org.apache.iotdb.rpc.TSStatusCode;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.function.IntFunction;
import java.util.zip.CRC32;

/** Tracks the outgoing writer's contiguous, successfully applied prefix on its replacement. */
final class MigrationTargetProgress {
  private static final long NONE = -1;
  private final Path file;
  private long migrationId = NONE;
  private int sourceId = -1;
  private long baseline = NONE;
  private String snapshot = "";
  private long confirmed = NONE;
  // A pending prefix is deliberately never restored. Its writes may still have been in memory.
  private long applied = NONE;
  private volatile int activeSource = -1;

  MigrationTargetProgress(Path file) throws IOException {
    this.file = file;
    byte[] bytes;
    try {
      bytes = Files.readAllBytes(file);
    } catch (NoSuchFileException e) {
      return;
    }
    if (bytes.length < Long.BYTES) {
      throw new IOException();
    }
    int length = bytes.length - Long.BYTES;
    CRC32 crc = new CRC32();
    crc.update(bytes, 0, length);
    if (crc.getValue() != ByteBuffer.wrap(bytes, length, Long.BYTES).getLong()) {
      throw new IOException();
    }
    try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes, 0, length))) {
      if (in.readByte() != 1) {
        throw new IOException();
      }
      migrationId = in.readLong();
      sourceId = in.readInt();
      baseline = in.readLong();
      snapshot = in.readUTF();
      confirmed = in.readLong();
      // No pre-crash in-memory prefix can be certified. Ordinary replication may continue while
      // the CN retains the source; only an already finalized receipt authorizes removal.
    }
  }

  synchronized boolean install(
      long id, int source, long index, String snapshotId, boolean inactive, BooleanSupplier load)
      throws IOException {
    if (id < 0 || source < 0 || index < 0 || id < migrationId) {
      return false;
    }
    if (id == migrationId) {
      return hasBaseline(id, source) && index == baseline && snapshot.equals(snapshotId);
    }
    if (!inactive) {
      return false;
    }
    // Reserve the identity before destructive installation. A lost response or restart must never
    // cause an already active target to reload a stale snapshot.
    persist(id, source, index, snapshotId, NONE);
    activeSource = -1;
    applied = NONE;
    if (!load.getAsBoolean()) {
      return false;
    }
    applied = index;
    activeSource = source;
    return true;
  }

  synchronized void invalidate() throws IOException {
    if (sourceId >= 0) {
      persist(migrationId, -1, NONE, "", NONE);
      activeSource = -1;
      applied = NONE;
    }
  }

  boolean tracks(int source) {
    return source >= 0 && activeSource == source;
  }

  synchronized boolean hasBaseline(long id, int source) {
    return id == migrationId
        && source >= 0
        && source == sourceId
        && (applied >= baseline || confirmed >= baseline);
  }

  synchronized TSStatus apply(int source, List<TLogEntry> entries, IntFunction<TSStatus> write) {
    if (!tracks(source)) {
      return null;
    }
    if (applied == NONE || entries.isEmpty()) {
      return rejected();
    }
    long previous = entries.get(0).getSearchIndex() - 1;
    if (previous < 0 || previous > applied) {
      return rejected();
    }
    // Check every wire index; the first/last indices alone cannot establish continuity.
    for (TLogEntry entry : entries) {
      if (entry.getSearchIndex() != ++previous) {
        return rejected();
      }
    }
    List<TSStatus> result = new ArrayList<>(entries.size());
    for (int i = 0; i < entries.size(); i++) {
      long index = entries.get(i).getSearchIndex();
      if (index > applied) {
        TSStatus status = write.apply(i);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          // Include a retryable status even for a terminal application error: the ordinary
          // dispatcher completes batches with non-retryable errors. The prefix must stop here.
          return rejected();
        }
        applied = index;
      }
      result.add(new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode()));
    }
    return new TSStatus().setSubStatus(result);
  }

  private static TSStatus rejected() {
    return new TSStatus()
        .setSubStatus(
            Collections.singletonList(
                new TSStatus(TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode())));
  }

  synchronized boolean confirm(long id, int source, long finalIndex, Flush flush)
      throws IOException {
    if (source < 0 || id != migrationId || source != sourceId || finalIndex < baseline) {
      return false;
    }
    if (confirmed != NONE) {
      return finalIndex == confirmed;
    }
    if (finalIndex != applied || !flush.run()) {
      return false;
    }
    persist(id, source, baseline, snapshot, finalIndex);
    activeSource = -1;
    return true;
  }

  @FunctionalInterface
  interface Flush {
    boolean run() throws IOException;
  }

  // Use a fixed inode and fsync invalidations before replacing region data. An incomplete write is
  // rejected by the checksum on recovery and cannot resurrect a receipt for discarded data.
  private void persist(long id, int source, long index, String snapshotId, long finalIndex)
      throws IOException {
    ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    try (DataOutputStream out = new DataOutputStream(buffer)) {
      out.writeByte(1);
      out.writeLong(id);
      out.writeInt(source);
      out.writeLong(index);
      out.writeUTF(snapshotId);
      out.writeLong(finalIndex);
    }
    byte[] record = buffer.toByteArray();
    CRC32 crc = new CRC32();
    crc.update(record);
    try (FileOutputStream fileOut = new FileOutputStream(file.toFile());
        DataOutputStream out = new DataOutputStream(fileOut)) {
      out.write(record);
      out.writeLong(crc.getValue());
      out.flush();
      fileOut.getFD().sync();
    }
    migrationId = id;
    sourceId = source;
    baseline = index;
    snapshot = snapshotId;
    confirmed = finalIndex;
  }
}
