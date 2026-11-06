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

package org.apache.iotdb.commons.utils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FileUtilsTest {
  private File tmpDir;
  private File targetDir;

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load").toUri());
    targetDir = new File(Files.createTempDirectory("target").toUri());
  }

  @After
  public void tearDown() throws Exception {
    if (tmpDir != null) {
      FileUtils.deleteFileOrDirectory(tmpDir, true);
    }
    if (targetDir != null) {
      FileUtils.deleteFileOrDirectory(targetDir, true);
    }
  }

  @Test
  public void testFileUtils() throws WriteProcessException, IOException {
    File tstFile = new File(tmpDir, "1-1-0-0.tsfile");
    File tstFile2 = new File(tmpDir, "2-1-0-0.tsfile");
    generateFile(tstFile);
    FileUtils.copyFile(tstFile, tstFile2);
    FileUtils.moveFileWithMD5Check(tstFile, targetDir);
    tstFile2.renameTo(tstFile);
    FileUtils.moveFileWithMD5Check(tstFile, targetDir);
  }

  @Test
  public void testGetIllegalError4DirectoryRejectsEmptyPath() {
    Assert.assertNotNull(FileUtils.getIllegalError4Directory(null));
    Assert.assertNotNull(FileUtils.getIllegalError4Directory(""));
    Assert.assertNull(FileUtils.getIllegalError4Directory("valid_dir"));
  }

  @Test
  public void testIsFilePathAllowedRejectsAllowedDirectoryItself() {
    final File allowedDirectory = new File(tmpDir, "allowed-export-dir");
    final String[] allowedDirectories = {allowedDirectory.getAbsolutePath()};

    Assert.assertFalse(
        FileUtils.isFilePathAllowed(allowedDirectory.getAbsolutePath(), allowedDirectories));
    Assert.assertTrue(
        FileUtils.isFilePathAllowed(
            new File(allowedDirectory, "result.tsfile").getAbsolutePath(), allowedDirectories));
  }

  @Test
  public void testTruncateFile() throws IOException {
    File file = new File(tmpDir, "truncate-file");
    Files.write(file.toPath(), new byte[] {1, 2, 3, 4});

    FileUtils.truncateFile(file, 2);

    Assert.assertArrayEquals(new byte[] {1, 2}, Files.readAllBytes(file.toPath()));
  }

  @Test
  public void testDeleteFileOrDirectoryWithRateLimiter() throws IOException {
    File deleteDir = new File(tmpDir, "deleteWithRateLimiter");
    File subDir = new File(deleteDir, "subDir");
    Assert.assertTrue(subDir.mkdirs());
    Files.write(new File(deleteDir, "file1").toPath(), new byte[3]);
    Files.write(new File(subDir, "file2").toPath(), new byte[7]);
    Files.write(new File(subDir, "empty").toPath(), new byte[0]);

    AtomicLong acquiredBytes = new AtomicLong();
    AtomicInteger acquiredFiles = new AtomicInteger();
    FileUtils.deleteFileOrDirectoryWithRateLimiter(
        deleteDir,
        removeCost -> {
          acquiredFiles.incrementAndGet();
          acquiredBytes.addAndGet(removeCost);
        });

    Assert.assertFalse(deleteDir.exists());
    Assert.assertEquals(5, acquiredFiles.get());
    Assert.assertEquals(
        3 * FileUtils.estimateFileOrDirectoryRemoveCost(new File("file"))
            + 2 * FileUtils.estimateFileOrDirectoryRemoveCost(tmpDir),
        acquiredBytes.get());
  }

  @Test
  public void testStrictDeleteTreeAndRetryMissingTarget() throws IOException {
    Path root = Files.createDirectory(tmpDir.toPath().resolve("strict"));
    Path nested = Files.createDirectory(root.resolve("nested"));
    Files.write(root.resolve("file"), new byte[] {1});
    Files.write(nested.resolve("file"), new byte[] {2});
    AtomicInteger permits = new AtomicInteger();
    AtomicLong costs = new AtomicLong();

    FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(
        root.toFile(),
        cost -> {
          permits.incrementAndGet();
          costs.addAndGet(cost);
        });
    Assert.assertFalse(Files.exists(root));
    Assert.assertEquals(4, permits.get());
    Assert.assertEquals(
        2 * FileUtils.estimateFileOrDirectoryRemoveCost(new File("file"))
            + 2 * FileUtils.estimateFileOrDirectoryRemoveCost(tmpDir),
        costs.get());

    FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(
        root.toFile(), cost -> permits.incrementAndGet());
    Assert.assertEquals(4, permits.get());
  }

  @Test
  public void testStrictDeletePropagatesFailureAndCanRetry() throws IOException {
    Path root = Files.createDirectory(tmpDir.toPath().resolve("strict-failure"));
    Path retained = root.resolve("retained");
    Assert.assertThrows(
        DirectoryNotEmptyException.class,
        () ->
            FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(
                root.toFile(),
                cost -> {
                  try {
                    Files.write(retained, new byte[] {1});
                  } catch (IOException e) {
                    throw new UncheckedIOException(e);
                  }
                }));
    Assert.assertArrayEquals(new byte[] {1}, Files.readAllBytes(retained));

    FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(root.toFile(), null);
    Assert.assertFalse(Files.exists(root));
  }

  @Test
  public void testStrictDeleteDoesNotFollowSymbolicLinks() throws IOException {
    Path root = Files.createDirectory(tmpDir.toPath().resolve("strict-links"));
    Path retained = Files.write(targetDir.toPath().resolve("retained"), new byte[] {1});
    try {
      Files.createSymbolicLink(root.resolve("external"), targetDir.toPath());
    } catch (UnsupportedOperationException | FileSystemException e) {
      Assume.assumeNoException(e);
    }

    FileUtils.deleteFileOrDirectoryWithRateLimiterStrict(root.toFile(), null);
    Assert.assertFalse(Files.exists(root));
    Assert.assertArrayEquals(new byte[] {1}, Files.readAllBytes(retained));
  }

  @Test
  public void testDeleteDirectoryAndEmptyParentWithRateLimiter() throws IOException {
    File parentDir = new File(tmpDir, "parentDir");
    File deleteDir = new File(parentDir, "deleteDir");
    Assert.assertTrue(deleteDir.mkdirs());
    Files.write(new File(deleteDir, "file").toPath(), new byte[5]);

    AtomicLong acquiredBytes = new AtomicLong();
    FileUtils.deleteDirectoryAndEmptyParentWithRateLimiter(deleteDir, acquiredBytes::addAndGet);

    Assert.assertFalse(deleteDir.exists());
    Assert.assertFalse(parentDir.exists());
    Assert.assertTrue(tmpDir.exists());
    Assert.assertEquals(
        FileUtils.estimateFileOrDirectoryRemoveCost(new File("file"))
            + 2 * FileUtils.estimateFileOrDirectoryRemoveCost(tmpDir),
        acquiredBytes.get());
  }

  @Test
  public void testDeleteDirectoryAndEmptyParentWithRateLimiterAndNoParent() throws IOException {
    File deleteDir = new File("deleteDirWithoutParent-" + System.nanoTime());
    try {
      Assert.assertTrue(deleteDir.mkdirs());

      AtomicLong acquiredBytes = new AtomicLong();
      long directoryRemoveCost = FileUtils.estimateFileOrDirectoryRemoveCost(deleteDir);
      FileUtils.deleteDirectoryAndEmptyParentWithRateLimiter(deleteDir, acquiredBytes::addAndGet);

      Assert.assertFalse(deleteDir.exists());
      Assert.assertEquals(directoryRemoveCost, acquiredBytes.get());
    } finally {
      FileUtils.deleteFileOrDirectory(deleteDir, true);
    }
  }

  private void generateFile(File tsfile) throws WriteProcessException, IOException {
    try (TsFileWriter writer = new TsFileWriter(tsfile)) {
      writer.registerAlignedTimeseries(
          "root.test.d1",
          Collections.singletonList(new MeasurementSchema("s1", TSDataType.BOOLEAN)));
      Tablet tablet =
          new Tablet(
              "root.test.d1",
              Collections.singletonList(new MeasurementSchema("s1", TSDataType.BOOLEAN)));
      for (int i = 0; i < 5; i++) {
        tablet.addTimestamp(i, i);
        tablet.addValue(i, 0, true);
      }
      writer.writeTree(tablet);
    }
  }
}
