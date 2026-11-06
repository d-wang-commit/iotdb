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

package org.apache.iotdb.db.pipe.receiver.protocol.iotconsensusv2;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.consensus.pipe.consensuspipe.ConsensusPipeName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import com.sun.nio.file.ExtendedOpenOption;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class IoTConsensusV2ReceiverAgentTest {
  private static final DataRegionId REGION_ID = new DataRegionId(918273);

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String[] originalReceiverDirs;

  @Before
  public void setUp() throws Exception {
    Field receiverDirs = IoTDBConfig.class.getDeclaredField("iotConsensusV2ReceiverFileDirs");
    receiverDirs.setAccessible(true);
    originalReceiverDirs = (String[]) receiverDirs.get(config);
  }

  @After
  public void tearDown() {
    config.setIotConsensusV2ReceiverFileDirs(originalReceiverDirs);
  }

  @Test
  public void testUnregisteredReceiversAreDeletedFromAllRoots() throws Exception {
    Path firstRoot = temporaryFolder.newFolder("first").toPath();
    Path secondRoot = temporaryFolder.newFolder("second").toPath();
    config.setIotConsensusV2ReceiverFileDirs(
        new String[] {firstRoot.toString(), secondRoot.toString()});
    Path first = createReceiverFile(firstRoot, REGION_ID, 1, config.getDataNodeId());
    Path second = createReceiverFile(secondRoot, REGION_ID, 2, config.getDataNodeId());
    Path otherRegion =
        createReceiverFile(
            firstRoot, new DataRegionId(REGION_ID.getId() + 1), 1, config.getDataNodeId());
    Path otherReceiver = createReceiverFile(firstRoot, REGION_ID, 1, config.getDataNodeId() + 1);
    Path unrecognized = Files.write(firstRoot.resolve("unrecognized.bin"), new byte[] {1});
    Path malformed = Files.createDirectory(firstRoot.resolve("__consensus.DataRegion_1_2"));

    new IoTConsensusV2ReceiverAgent().releaseReceiverResource(REGION_ID);

    assertFalse(Files.exists(first.getParent()));
    assertFalse(Files.exists(second.getParent()));
    assertTrue(Files.exists(otherRegion));
    assertTrue(Files.exists(otherReceiver));
    assertTrue(Files.exists(unrecognized));
    assertTrue(Files.exists(malformed));
    // A fresh agent has no receiver registrations and still accepts the completed cleanup.
    new IoTConsensusV2ReceiverAgent().releaseReceiverResource(REGION_ID);
    assertTrue(Files.exists(otherRegion));
    assertTrue(Files.exists(otherReceiver));
  }

  @Test
  public void testMissingReceiverRootIsAlreadyClean() throws Exception {
    Path missing = temporaryFolder.getRoot().toPath().resolve("missing");
    config.setIotConsensusV2ReceiverFileDirs(new String[] {missing.toString()});

    new IoTConsensusV2ReceiverAgent().releaseReceiverResource(REGION_ID);

    assertFalse(Files.exists(missing));
  }

  @Test
  public void testUnreadableDirectoryStructurePropagatesFailure() throws Exception {
    Path regularFile = temporaryFolder.newFile("not-a-directory").toPath();
    config.setIotConsensusV2ReceiverFileDirs(new String[] {regularFile.toString()});

    assertThrows(
        IOException.class,
        () -> new IoTConsensusV2ReceiverAgent().releaseReceiverResource(REGION_ID));

    assertTrue(Files.isRegularFile(regularFile));
  }

  @Test
  public void testLockedReceiverFileCanBeRetriedByFreshAgent() throws Exception {
    Assume.assumeTrue(System.getProperty("os.name").startsWith("Windows"));
    Path root = temporaryFolder.newFolder("receiver").toPath();
    config.setIotConsensusV2ReceiverFileDirs(new String[] {root.toString()});
    Path lockedFile = createReceiverFile(root, REGION_ID, 1, config.getDataNodeId());
    try (SeekableByteChannel ignored =
        Files.newByteChannel(
            lockedFile, StandardOpenOption.READ, ExtendedOpenOption.NOSHARE_DELETE)) {
      assertThrows(
          IOException.class,
          () -> new IoTConsensusV2ReceiverAgent().releaseReceiverResource(REGION_ID));
      assertTrue(Files.exists(lockedFile));
    }

    new IoTConsensusV2ReceiverAgent().releaseReceiverResource(REGION_ID);

    assertFalse(Files.exists(lockedFile.getParent()));
  }

  private Path createReceiverFile(Path root, DataRegionId regionId, int sender, int receiver)
      throws IOException {
    Path receiverDirectory =
        Files.createDirectory(
            root.resolve(new ConsensusPipeName(regionId, sender, receiver).toString()));
    return Files.write(receiverDirectory.resolve("staging.tsfile"), new byte[] {1});
  }
}
