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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class MigrationTargetProgressTest {

  @Rule public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void onlyContiguousSuccessfulAppliesExtendTheInstalledSnapshot() throws Exception {
    Path receipt = folder.getRoot().toPath().resolve("receipt");
    MigrationTargetProgress target = new MigrationTargetProgress(receipt);
    assertTrue(target.install(20, 7, 10, "snapshot-1", true, () -> true));
    assertTrue(target.hasBaseline(20, 7));
    assertFalse(target.hasBaseline(21, 7));
    assertFalse(target.hasBaseline(20, 8));
    List<Long> written = new ArrayList<>();

    assertNull(
        target.apply(
            8,
            logs(11),
            i -> {
              throw new AssertionError();
            }));
    assertRejected(
        target.apply(
            7,
            logs(12),
            i -> {
              throw new AssertionError();
            }));
    assertRejected(
        target.apply(
            7,
            logs(11, 13),
            i -> {
              throw new AssertionError();
            }));
    assertRejected(
        target.apply(
            7,
            logs(10, 11, 12),
            i -> {
              if (i == 2) {
                return new TSStatus(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode());
              }
              written.add(10L + i);
              return success();
            }));
    assertFalse(
        target.confirm(
            20,
            7,
            12,
            () -> {
              throw new AssertionError();
            }));

    // Replay the overlapping batch. Only index 12 still needs to be applied.
    List<TSStatus> statuses =
        target
            .apply(
                7,
                logs(10, 11, 12),
                i -> {
                  written.add(10L + i);
                  return success();
                })
            .getSubStatus();
    assertEquals(3, statuses.size());
    assertTrue(statuses.stream().allMatch(status -> status.getCode() == success().getCode()));
    assertEquals(Arrays.asList(11L, 12L), written);
    assertFalse(target.confirm(20, 8, 12, () -> true));
    assertFalse(target.confirm(19, 7, 12, () -> true));
    assertFalse(target.confirm(20, 7, 12, () -> false));
    assertThrows(
        IOException.class,
        () ->
            target.confirm(
                20,
                7,
                12,
                () -> {
                  throw new IOException();
                }));

    MigrationTargetProgress beforeConfirmation = new MigrationTargetProgress(receipt);
    assertFalse(beforeConfirmation.hasBaseline(20, 7));
    assertFalse(beforeConfirmation.tracks(7));
    assertNull(
        beforeConfirmation.apply(
            7,
            logs(13),
            i -> {
              throw new AssertionError();
            }));
    assertFalse(beforeConfirmation.confirm(20, 7, 12, () -> true));
    assertFalse(beforeConfirmation.install(20, 7, 10, "snapshot-1", true, unexpectedLoad()));
    assertTrue(target.confirm(20, 7, 12, () -> true));
    MigrationTargetProgress recovered = new MigrationTargetProgress(receipt);
    assertTrue(recovered.hasBaseline(20, 7));
    assertTrue(
        recovered.confirm(
            20,
            7,
            12,
            () -> {
              throw new AssertionError();
            }));
    assertFalse(recovered.confirm(20, 7, 13, () -> true));
    assertTrue(recovered.install(20, 7, 10, "snapshot-1", false, unexpectedLoad()));
    assertFalse(recovered.install(21, 7, 12, "snapshot-2", false, unexpectedLoad()));
    assertTrue(recovered.confirm(20, 7, 12, () -> true));
    recovered.invalidate();
    assertFalse(new MigrationTargetProgress(receipt).confirm(20, 7, 12, () -> true));
  }

  @Test
  public void failedSnapshotCannotSupplyBaselineEvenForEmptyRegion() throws Exception {
    MigrationTargetProgress target =
        new MigrationTargetProgress(folder.getRoot().toPath().resolve("receipt"));
    assertFalse(target.install(12, 7, 0, "first", true, () -> false));
    assertFalse(target.confirm(12, 7, 0, () -> true));
    assertFalse(target.install(12, 7, 0, "unrelated", true, unexpectedLoad()));
    assertTrue(target.install(13, 7, 0, "second", true, () -> true));
    assertFalse(target.install(12, 7, 0, "first", true, unexpectedLoad()));
    assertTrue(target.confirm(13, 7, 0, () -> true));
  }

  private static List<TLogEntry> logs(long... indices) {
    List<TLogEntry> entries = new ArrayList<>();
    for (long index : indices) {
      entries.add(new TLogEntry().setSearchIndex(index));
    }
    return entries;
  }

  private static BooleanSupplier unexpectedLoad() {
    return () -> {
      throw new AssertionError();
    };
  }

  private static TSStatus success() {
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  private static void assertRejected(TSStatus response) {
    assertEquals(
        TSStatusCode.WRITE_PROCESS_REJECT.getStatusCode(),
        response.getSubStatus().get(0).getCode());
  }
}
