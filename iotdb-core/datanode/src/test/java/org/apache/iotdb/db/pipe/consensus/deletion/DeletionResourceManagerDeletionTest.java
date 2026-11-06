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

package org.apache.iotdb.db.pipe.consensus.deletion;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DeletionResourceManagerDeletionTest {
  @Rule public final TemporaryFolder folder = new TemporaryFolder();
  private final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String previousDirectory;
  private Field managersField;
  private Object previousManagers;
  private Path deletionDirectory;

  @Before
  public void setUp() throws Exception {
    previousDirectory = config.getIotConsensusV2DeletionFileDir();
    deletionDirectory = folder.newFolder("dal").toPath();
    config.setIotConsensusV2DeletionFileDir(deletionDirectory.toString());
    Class<?> holder =
        Class.forName(DeletionResourceManager.class.getName() + "$DeletionResourceManagerHolder");
    managersField = holder.getDeclaredField("CONSENSUS_GROUP_ID_2_INSTANCE_MAP");
    managersField.setAccessible(true);
    previousManagers = managersField.get(null);
    managersField.set(null, null);
  }

  @After
  public void tearDown() throws Exception {
    try {
      DeletionResourceManager.exit();
    } finally {
      managersField.set(null, previousManagers);
      config.setIotConsensusV2DeletionFileDir(previousDirectory);
    }
  }

  @Test
  public void testDeleteWithoutInitializedManager() throws Exception {
    Path target = Files.createDirectories(deletionDirectory.resolve("7"));
    Files.write(target.resolve("_0-0.deletion"), new byte[] {1});
    Path other = Files.createDirectories(deletionDirectory.resolve("8"));
    Path retainedFile = Files.write(other.resolve("_0-0.deletion"), new byte[] {2});

    DeletionResourceManager.deleteForRegion(7);

    assertFalse(Files.exists(target));
    assertTrue(Files.exists(retainedFile));
    assertNull(managersField.get(null));
  }

  @Test(timeout = 60_000)
  public void testDeleteAfterManagerClosed() throws Exception {
    DeletionResourceManager.buildForTest();
    DeletionResourceManager manager = DeletionResourceManager.getInstance(7);
    manager.close();
    Path target = deletionDirectory.resolve("7");
    Files.write(target.resolve("_0-0.deletion"), new byte[] {1});

    DeletionResourceManager.deleteForRegion(7);
    DeletionResourceManager.deleteForRegion(7);

    assertFalse(Files.exists(target));
    assertTrue(((Map<?, ?>) managersField.get(null)).isEmpty());
  }

  @Test
  public void testDeleteAbsentRegionDoesNotInitializeManager() throws Exception {
    DeletionResourceManager.deleteForRegion(7);

    assertFalse(Files.exists(deletionDirectory.resolve("7")));
    assertNull(managersField.get(null));
  }
}
