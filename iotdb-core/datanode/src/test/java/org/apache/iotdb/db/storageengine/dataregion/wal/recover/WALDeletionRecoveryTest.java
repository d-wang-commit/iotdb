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

package org.apache.iotdb.db.storageengine.dataregion.wal.recover;

import org.apache.iotdb.commons.concurrent.ExceptionalCountDownLatch;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.runtime.StorageEngineFailureException;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;

import com.sun.nio.file.ExtendedOpenOption;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class WALDeletionRecoveryTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private static final String DATABASE = "root.wal-deletion-recovery";
  private static final DataRegionId REGION_ID = new DataRegionId(739101);
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private final WALRecoverManager recovery = WALRecoverManager.getInstance();
  private String previousConsensus;
  private String[] previousWalDirs;
  private WALMode previousMode;
  private Path walDirectory;
  private Path marker;
  private Path regionSystemDirectory;
  private boolean ownsDeletionPaths;

  @Before
  public void setUp() throws Exception {
    previousConsensus = config.getDataRegionConsensusProtocolClass();
    previousWalDirs = commonConfig.getWalDirs();
    previousMode = config.getWalMode();
    walDirectory = folder.newFolder("wal").toPath();
    commonConfig.setWalDirs(new String[] {walDirectory.toString()});
    config.setWalMode(WALMode.DISABLE);
    recovery.clear();
    recovery.setAllDataRegionScannedLatch(new ExceptionalCountDownLatch(0));

    Field field = StorageEngine.class.getDeclaredField("systemDir");
    field.setAccessible(true);
    Path systemDirectory = new File((String) field.get(null)).toPath();
    regionSystemDirectory =
        systemDirectory.resolve(DATABASE).resolve(Integer.toString(REGION_ID.getId()));
    marker =
        systemDirectory
            .resolveSibling("pending-region-deletions")
            .resolve(Integer.toString(REGION_ID.getId()));
    assertFalse(Files.exists(marker));
    assertFalse(Files.exists(regionSystemDirectory));
    ownsDeletionPaths = true;
    Files.createDirectories(regionSystemDirectory);
    StorageEngine.getInstance().prepareDataRegionDeletion(REGION_ID);
    assertTrue(StorageEngine.isDataRegionDeletionPending(REGION_ID));
    FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(regionSystemDirectory.toFile(), null);
  }

  @After
  public void tearDown() throws Exception {
    try {
      recovery.clear();
      if (ownsDeletionPaths && marker != null) {
        Files.deleteIfExists(marker);
      }
      if (ownsDeletionPaths && regionSystemDirectory != null) {
        FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(regionSystemDirectory.toFile(), null);
        Files.deleteIfExists(regionSystemDirectory.getParent());
      }
    } finally {
      config.setDataRegionConsensusProtocolClass(previousConsensus);
      commonConfig.setWalDirs(previousWalDirs);
      config.setWalMode(previousMode);
    }
  }

  @Test(timeout = 10_000)
  public void testPendingIoTConsensusWALIsNotRecovered() throws Exception {
    assertPendingDedicatedWALIsNotRecovered(ConsensusFactory.IOT_CONSENSUS);
  }

  @Test(timeout = 10_000)
  public void testPendingIoTConsensusV2WALIsNotRecovered() throws Exception {
    assertPendingDedicatedWALIsNotRecovered(ConsensusFactory.IOT_CONSENSUS_V2);
  }

  @Test(timeout = 10_000)
  public void testSharedCleanupFailureStopsStartupAndCanRetry() throws Exception {
    assumeTrue(System.getProperty("os.name").startsWith("Windows"));
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
    Path node = Files.createDirectories(walDirectory.resolve("0"));
    Path locked = Files.write(node.resolve("orphan.tmp"), new byte[] {1});
    try (FileChannel ignored =
        FileChannel.open(locked, StandardOpenOption.READ, ExtendedOpenOption.NOSHARE_DELETE)) {
      StartupException failure = assertThrows(StartupException.class, recovery::recover);
      assertEquals(StorageEngineMessages.FAIL_TO_RECOVER_WAL, failure.getMessage());
      assertTrue(Files.exists(locked));
      assertTrue(Files.exists(marker));
    }

    recovery.clear();
    recovery.setAllDataRegionScannedLatch(new ExceptionalCountDownLatch(0));
    recovery.recover();

    assertFalse(Files.exists(node));
    assertTrue(Files.exists(marker));
  }

  @Test(timeout = 10_000)
  public void testSimpleCleanupFailureWithPendingDeletionIsReported() throws Exception {
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
    assertCleanupFailureCanRetry(true);
  }

  @Test(timeout = 10_000)
  public void testSimpleCleanupFailureWithoutPendingDeletionAllowsRecovery() throws Exception {
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
    Files.delete(marker);
    assertFalse(StorageEngine.hasPendingDataRegionDeletions());
    assertCleanupFailureCanRetry(false);
  }

  @Test(timeout = 10_000)
  public void testV2CleanupFailureWithUnrelatedPendingDeletionAllowsRecovery() throws Exception {
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.IOT_CONSENSUS_V2);
    assertCleanupFailureCanRetry(false);
  }

  private void assertCleanupFailureCanRetry(boolean expectStartupFailure) throws Exception {
    File node = mock(File.class);
    when(node.getName()).thenReturn("node");
    when(node.listFiles(any(FilenameFilter.class))).thenReturn(new File[0]);
    Path realDirectory = folder.newFolder("cleanup-failure").toPath();
    Path retained = Files.write(realDirectory.resolve("orphan.tmp"), new byte[] {1});
    Path unavailable = mock(Path.class);
    FileSystem fileSystem = mock(FileSystem.class);
    FileSystemProvider provider = mock(FileSystemProvider.class);
    when(node.toPath()).thenReturn(unavailable, realDirectory);
    when(unavailable.getFileSystem()).thenReturn(fileSystem);
    when(fileSystem.provider()).thenReturn(provider);
    IOException failure = new IOException("injected cleanup directory scan failure");
    when(provider.newDirectoryStream(any(Path.class), any(DirectoryStream.Filter.class)))
        .thenThrow(failure);
    CountDownLatch first = new CountDownLatch(1);

    WALNodeRecoverTask task = new WALNodeRecoverTask(node, first);
    if (expectStartupFailure) {
      StorageEngineFailureException error =
          assertThrows(StorageEngineFailureException.class, task::run);
      assertSame(failure, error.getCause());
    } else {
      task.run();
    }
    verify(provider).newDirectoryStream(any(Path.class), any(DirectoryStream.Filter.class));
    assertEquals(0, first.getCount());
    assertTrue(Files.exists(retained));

    CountDownLatch retry = new CountDownLatch(1);
    new WALNodeRecoverTask(node, retry).run();

    assertEquals(0, retry.getCount());
    assertFalse(Files.exists(realDirectory));
  }

  private void assertPendingDedicatedWALIsNotRecovered(String protocol) throws Exception {
    config.setDataRegionConsensusProtocolClass(protocol);
    Path node = Files.createDirectories(walDirectory.resolve(DATABASE + "-" + REGION_ID.getId()));
    byte[] unfinished = new byte[] {1, 2, 3};
    Path wal = Files.write(node.resolve("_0-0-1.wal"), unfinished);

    recovery.recover();

    assertArrayEquals(unfinished, Files.readAllBytes(wal));
    assertTrue(Files.exists(marker));
  }

  @Test(timeout = 10_000)
  public void testSharedWALStillRecoversWithPendingRegion() throws Exception {
    config.setDataRegionConsensusProtocolClass(ConsensusFactory.SIMPLE_CONSENSUS);
    Path sharedNode =
        Files.createDirectories(walDirectory.resolve(Integer.toString(REGION_ID.getId())));
    Files.write(sharedNode.resolve("orphan.tmp"), new byte[] {1});

    recovery.recover();

    assertFalse(Files.exists(sharedNode));
    assertTrue(Files.exists(marker));
  }
}
