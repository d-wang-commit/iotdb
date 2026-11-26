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

import org.apache.iotdb.consensus.i18n.IoTConsensusMessages;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.zip.CRC32;

/**
 * Source authorization for one handoff. A nonnegative final index fences local writes until the
 * handoff is completed or explicitly rolled back.
 */
final class MigrationSourceState {

  static final long PREPARED = -1;
  static final long ABORTED = -2;
  private static final int VERSION = 1;
  private static final int CONTENT_SIZE = 2 * Integer.BYTES + 2 * Long.BYTES;

  final long migrationId;
  final int targetNodeId;
  final long finalIndex;

  MigrationSourceState(long migrationId, int targetNodeId, long finalIndex) {
    this.migrationId = migrationId;
    this.targetNodeId = targetNodeId;
    this.finalIndex = finalIndex;
  }

  static MigrationSourceState load(Path path) throws IOException {
    byte[] record;
    try {
      record = Files.readAllBytes(path);
    } catch (NoSuchFileException e) {
      return null;
    }
    ByteBuffer buffer = ByteBuffer.wrap(record);
    if (buffer.remaining() != CONTENT_SIZE + Long.BYTES
        || buffer.getInt() != VERSION
        || buffer.getLong(CONTENT_SIZE) != checksum(record)) {
      throw new IOException(
          String.format(
              IoTConsensusMessages.EXCEPTION_INVALID_MIGRATION_FENCE_AT_ARG_03FF1559, path));
    }
    MigrationSourceState state =
        new MigrationSourceState(buffer.getLong(), buffer.getInt(), buffer.getLong());
    if (state.migrationId < 0 || state.targetNodeId < 0 || state.finalIndex < ABORTED) {
      throw new IOException(
          String.format(
              IoTConsensusMessages.EXCEPTION_INVALID_MIGRATION_FENCE_AT_ARG_03FF1559, path));
    }
    // A preparation from an earlier writer lifetime cannot authorize a new final index.
    return state.finalIndex == PREPARED ? null : state;
  }

  void persist(Path path) throws IOException {
    Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
    ByteBuffer buffer = ByteBuffer.allocate(CONTENT_SIZE + Long.BYTES);
    buffer.putInt(VERSION).putLong(migrationId).putInt(targetNodeId).putLong(finalIndex);
    buffer.putLong(checksum(buffer.array())).flip();
    try (FileChannel channel =
        FileChannel.open(
            tmp,
            StandardOpenOption.WRITE,
            StandardOpenOption.CREATE,
            StandardOpenOption.TRUNCATE_EXISTING)) {
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }
      channel.force(true);
    }
    Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    try (FileChannel dir = FileChannel.open(path.getParent(), StandardOpenOption.READ)) {
      dir.force(true);
    } catch (AccessDeniedException e) {
      // The Windows file provider does not allow opening directories with FileChannel.
      if (!path.getFileSystem().getSeparator().equals("\\")) {
        throw e;
      }
    }
  }

  private static long checksum(byte[] record) {
    CRC32 crc = new CRC32();
    crc.update(record, 0, CONTENT_SIZE);
    return crc.getValue();
  }
}
