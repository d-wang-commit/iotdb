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
package org.apache.iotdb.db.storageengine;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.exception.TsFileProcessorException;
import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.rpc.TSStatusCode;

import com.sun.nio.file.ExtendedOpenOption;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DataRegionDeletionTest {
  private static final AtomicInteger IDS = new AtomicInteger(780000);
  private static final String DATABASE = "root.deletion_retry";
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private StorageEngine engine;
  private DataRegionId regionId;
  private DataRegion region;
  private String previousProtocol;
  private Path systemFolder;
  private Path marker;
  private final List<Path> files = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    previousProtocol = config.getDataRegionConsensusProtocolClass();
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.RATIS_CONSENSUS);
    engine = newEngine();
    regionId = new DataRegionId(IDS.incrementAndGet());
    Field field = StorageEngine.class.getDeclaredField("systemDir");
    field.setAccessible(true);
    Path system = new File((String) field.get(null)).toPath();
    systemFolder = system.resolve(DATABASE).resolve(Integer.toString(regionId.getId()));
    marker =
        system
            .resolveSibling("pending-region-deletions")
            .resolve(Integer.toString(regionId.getId()));
    Files.createDirectories(systemFolder);
    Files.write(systemFolder.resolve("statistics"), new byte[] {1});
    for (String folder : TierManager.getInstance().getAllLocalFilesFolders()) {
      Path data =
          new File(folder).toPath().resolve(DATABASE).resolve(Integer.toString(regionId.getId()));
      Files.createDirectories(data);
      Path file = data.resolve("data.tsfile");
      Files.write(file, new byte[] {1, 2, 3});
      files.add(file);
    }
    region = mock(DataRegion.class);
    when(region.getDatabaseName()).thenReturn(DATABASE);
    when(region.getDataRegionIdString()).thenReturn(Integer.toString(regionId.getId()));
    regions(engine, "dataRegionMap").put(regionId, region);
    doAnswer(
            invocation -> {
              DataRegion.deleteRegionDataFiles(DATABASE, regionId);
              return null;
            })
        .when(region)
        .syncDeleteDataFiles();
  }

  @After
  public void tearDown() throws Exception {
    try {
      for (Path file : files) {
        FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(
            file.getParent().toFile(), ignored -> {});
      }
      FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(systemFolder.toFile(), ignored -> {});
      FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(marker.toFile(), ignored -> {});
      Files.deleteIfExists(marker.resolveSibling(marker.getFileName() + ".tmp"));
    } finally {
      config.setDataRegionConsensusProtocolClass(previousProtocol);
    }
  }

  @Test(timeout = 10000)
  public void testFailureRemainsRetryableUntilDataIsDeleted() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    doAnswer(
            invocation -> {
              if (attempts.getAndIncrement() < 2) {
                throw new TsFileProcessorException("injected close failure");
              }
              DataRegion.deleteRegionDataFiles(DATABASE, regionId);
              return null;
            })
        .when(region)
        .syncDeleteDataFiles();
    for (int attempt = 0; attempt < 2; attempt++) {
      assertStatus(TSStatusCode.DELETE_REGION_ERROR, engine.deleteDataRegion(regionId));
      assertTrue(StorageEngine.isDataRegionDeletionPending(regionId));
      assertNull(engine.getDataRegion(regionId));
      assertSame(region, regions(engine, "deletingDataRegionMap").get(regionId));
      for (Path file : files) {
        assertTrue(Files.exists(file));
      }
    }
    assertStatus(TSStatusCode.SUCCESS_STATUS, engine.deleteDataRegion(regionId));
    assertTrue(regions(engine, "deletingDataRegionMap").isEmpty());
    assertFalse(Files.exists(marker));
    assertFalse(Files.exists(systemFolder));
    for (Path file : files) {
      assertFalse(Files.exists(file));
    }
    assertStatus(TSStatusCode.REGION_NOT_EXIST, engine.deleteDataRegion(regionId));
    assertEquals(3, attempts.get());
  }

  @Test(timeout = 10000)
  public void testMarkerFailureDoesNotDiscardActiveRegion() throws Exception {
    Files.createDirectories(marker);
    assertStatus(TSStatusCode.DELETE_REGION_ERROR, engine.deleteDataRegion(regionId));
    assertSame(region, engine.getDataRegion(regionId));
    assertTrue(regions(engine, "deletingDataRegionMap").isEmpty());
    verify(region, never()).syncDeleteDataFiles();
    Files.delete(marker);
    assertStatus(TSStatusCode.SUCCESS_STATUS, engine.deleteDataRegion(regionId));
  }

  @Test(timeout = 10000)
  public void testPendingDeletionBlocksCreateAndSnapshotLoad() throws Exception {
    engine.prepareDataRegionDeletion(regionId);
    try {
      engine.createDataRegion(regionId, "root.replacement");
      fail("A pending replica must not be recreated");
    } catch (DataRegionException expected) {
      assertTrue(StorageEngine.isDataRegionDeletionPending(regionId));
    }
    AtomicInteger loads = new AtomicInteger();
    assertNull(
        engine.setDataRegionForSnapshotLoad(
            regionId,
            region,
            () -> {
              loads.incrementAndGet();
              return region;
            }));
    assertEquals(0, loads.get());
    assertStatus(TSStatusCode.SUCCESS_STATUS, engine.deleteDataRegion(regionId));
  }

  @Test(timeout = 10000)
  public void testRestartFindsDataAfterSystemIndexWasDeleted() throws Exception {
    engine.prepareDataRegionDeletion(regionId);
    FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(systemFolder.toFile(), ignored -> {});
    StorageEngine restarted = newEngine();
    assertNull(restarted.getDataRegion(regionId));
    assertStatus(TSStatusCode.SUCCESS_STATUS, restarted.deleteDataRegion(regionId));
    assertFalse(Files.exists(marker));
    for (Path file : files) {
      assertFalse(Files.exists(file));
    }
    assertStatus(TSStatusCode.REGION_NOT_EXIST, restarted.deleteDataRegion(regionId));
  }

  @Test(timeout = 10000)
  public void testStartupRetiresPendingPeerBeforeRegionRecovery() throws Exception {
    engine.prepareDataRegionDeletion(regionId);
    Path peer = Files.createTempDirectory("pending-peer");
    Files.write(peer.resolve("progress"), new byte[] {1});
    IConsensus original = DataRegionConsensusImpl.getInstance();
    IConsensus consensus = mock(IConsensus.class);
    when(consensus.getRegionDirFromConsensusGroupId(any())).thenReturn(peer.toString());
    DataRegionConsensusImpl.setInstance(consensus);
    try {
      StorageEngine restarted = newEngine();
      restarted.recoverDataRegionDeletions();
      assertFalse(Files.exists(peer));
      assertTrue(Files.exists(marker));
      assertFalse(restarted.getLocalDataRegionInfo().get(DATABASE).contains(regionId));
      assertStatus(TSStatusCode.SUCCESS_STATUS, restarted.deleteDataRegion(regionId));
    } finally {
      DataRegionConsensusImpl.setInstance(original);
      FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(peer.toFile(), ignored -> {});
    }
  }

  @Test(timeout = 10000)
  public void testMarkerRemovalFailureRemainsRetryable() throws Exception {
    assumeTrue(System.getProperty("os.name").startsWith("Windows"));
    engine.prepareDataRegionDeletion(regionId);
    try (FileChannel ignored =
        FileChannel.open(marker, StandardOpenOption.READ, ExtendedOpenOption.NOSHARE_DELETE)) {
      assertStatus(TSStatusCode.DELETE_REGION_ERROR, engine.deleteDataRegion(regionId));
      assertTrue(Files.exists(marker));
      assertSame(region, regions(engine, "deletingDataRegionMap").get(regionId));
      for (Path file : files) {
        assertFalse(Files.exists(file));
      }
    }
    assertStatus(TSStatusCode.SUCCESS_STATUS, engine.deleteDataRegion(regionId));
    assertFalse(Files.exists(marker));
  }

  @Test(timeout = 10000)
  public void testDeletionWaitsForSnapshotReplacement() throws Exception {
    CountDownLatch loading = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    CountDownLatch deleting = new CountDownLatch(1);
    DataRegion replacement = mock(DataRegion.class);
    when(replacement.getDatabaseName()).thenReturn(DATABASE);
    when(replacement.getDataRegionIdString()).thenReturn(Integer.toString(regionId.getId()));
    ExecutorService executor = Executors.newFixedThreadPool(2);
    try {
      Future<DataRegion> load =
          executor.submit(
              () ->
                  engine.setDataRegionForSnapshotLoad(
                      regionId,
                      region,
                      () -> {
                        loading.countDown();
                        try {
                          assertTrue(release.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                          throw new AssertionError(e);
                        }
                        return replacement;
                      }));
      assertTrue(loading.await(5, TimeUnit.SECONDS));
      Future<String> prepare =
          executor.submit(
              () -> {
                deleting.countDown();
                return engine.prepareDataRegionDeletion(regionId);
              });
      assertTrue(deleting.await(5, TimeUnit.SECONDS));
      assertFalse(prepare.isDone());
      release.countDown();
      assertSame(replacement, load.get(5, TimeUnit.SECONDS));
      assertEquals(DATABASE, prepare.get(5, TimeUnit.SECONDS));
      assertTrue(StorageEngine.isDataRegionDeletionPending(regionId));
    } finally {
      release.countDown();
      executor.shutdownNow();
    }
  }

  @Test(timeout = 10000)
  public void testOldSnapshotCannotReplaceNewReplica() throws Exception {
    assertStatus(TSStatusCode.SUCCESS_STATUS, engine.deleteDataRegion(regionId));
    DataRegion replacement = mock(DataRegion.class);
    regions(engine, "dataRegionMap").put(regionId, replacement);
    assertNull(
        engine.setDataRegionForSnapshotLoad(
            regionId,
            region,
            () -> {
              throw new AssertionError("Obsolete snapshot loaded after replica replacement");
            }));
    assertSame(replacement, engine.getDataRegion(regionId));
  }

  private static StorageEngine newEngine() throws Exception {
    Constructor<StorageEngine> constructor = StorageEngine.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    return constructor.newInstance();
  }

  @SuppressWarnings("unchecked")
  private static Map<DataRegionId, DataRegion> regions(StorageEngine storage, String name)
      throws Exception {
    Field field = StorageEngine.class.getDeclaredField(name);
    field.setAccessible(true);
    return (Map<DataRegionId, DataRegion>) field.get(storage);
  }

  private static void assertStatus(TSStatusCode expected, TSStatus result) {
    assertNotNull(result);
    assertEquals(expected.getStatusCode(), result.getCode());
  }
}
