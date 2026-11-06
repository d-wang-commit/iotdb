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
package org.apache.iotdb.db.storageengine.dataregion.wal.allocation;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.write.InsertRowNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.IWALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.node.WALNode;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALFileUtils;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.db.utils.constant.TestConstant;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.spi.FileSystemProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ElasticStrategyTest {
  private static final CommonConfig commonConfig = CommonDescriptor.getInstance().getConfig();
  private final String[] walDirs =
      new String[] {
        TestConstant.BASE_OUTPUT_PATH.concat("wal_test1"),
        TestConstant.BASE_OUTPUT_PATH.concat("wal_test2"),
        TestConstant.BASE_OUTPUT_PATH.concat("wal_test3")
      };
  private String[] prevWalDirs;

  @Before
  public void setUp() throws Exception {
    prevWalDirs = commonConfig.getWalDirs();
    EnvironmentUtils.envSetUp();
    commonConfig.setWalDirs(walDirs);
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
    for (String walDir : walDirs) {
      EnvironmentUtils.cleanDir(walDir);
    }
    commonConfig.setWalDirs(prevWalDirs);
  }

  @Test
  public void testAllocateWALNode() throws Exception {
    ElasticStrategy elasticStrategy = new ElasticStrategy();
    IWALNode[] walNodes = new IWALNode[3];
    try {
      for (int i = 0; i < 12; i++) {
        IWALNode walNode = elasticStrategy.applyForWALNode(String.valueOf(i));
        if (i % ElasticStrategy.APPLICATION_NODE_RATIO == 0) {
          walNodes[i / ElasticStrategy.APPLICATION_NODE_RATIO] = walNode;
        } else {
          assertEquals(walNodes[i / ElasticStrategy.APPLICATION_NODE_RATIO], walNode);
        }
        walNode.log(i, getInsertRowNode());
      }
      for (String walDir : walDirs) {
        File walDirFile = new File(walDir);
        assertTrue(walDirFile.exists());
        File[] nodeDirs = walDirFile.listFiles(File::isDirectory);
        assertNotNull(nodeDirs);
        assertEquals(1, nodeDirs.length);
        for (File nodeDir : nodeDirs) {
          assertTrue(nodeDir.exists());
          assertNotEquals(0, WALFileUtils.listAllWALFiles(nodeDir).length);
        }
      }
      int walNodeNum = 3;
      for (int i = 0; i < 12; i++) {
        elasticStrategy.deleteUniqueIdAndMayDeleteWALNode(String.valueOf(i));
        if ((i + 1) % ElasticStrategy.APPLICATION_NODE_RATIO == 0) {
          assertEquals(--walNodeNum, elasticStrategy.getNodesNum());
          walNodes[i / ElasticStrategy.APPLICATION_NODE_RATIO] = null;
        } else {
          assertEquals(walNodeNum, elasticStrategy.getNodesNum());
        }
      }
      assertEquals(0, elasticStrategy.getNodesNum());
    } finally {
      for (IWALNode walNode : walNodes) {
        if (walNode != null) {
          walNode.close();
        }
      }
    }
  }

  @Test
  public void testLastApplicantRetriesFailedDirectoryDeletion() throws Exception {
    Path directory = new File(walDirs[0], "0").toPath();
    Files.createDirectories(directory);
    Path walFile = Files.write(directory.resolve("_0-0-1.wal"), new byte[] {1});
    File failingFile = mock(File.class);
    Path failingPath = mock(Path.class);
    FileSystem fileSystem = mock(FileSystem.class);
    FileSystemProvider provider = mock(FileSystemProvider.class);
    when(failingFile.toPath()).thenReturn(failingPath);
    when(failingPath.getFileSystem()).thenReturn(fileSystem);
    when(fileSystem.provider()).thenReturn(provider);
    when(provider.readAttributes(failingPath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS))
        .thenThrow(new IOException("injected directory read failure"));

    WALNode node = mock(WALNode.class);
    when(node.getLogDirectory()).thenReturn(failingFile, directory.toFile());
    ElasticStrategy strategy =
        new ElasticStrategy() {
          @Override
          protected IWALNode createWALNode(String identifier) {
            return node;
          }
        };
    try {
      assertSame(node, strategy.applyForWALNode("region-a"));
      assertSame(node, strategy.applyForWALNode("region-b"));
      strategy.deleteUniqueIdAndMayDeleteWALNode("region-a");
      assertTrue(Files.exists(walFile));
      verify(node, never()).close();

      assertThrows(IOException.class, () -> strategy.deleteUniqueIdAndMayDeleteWALNode("region-b"));
      assertTrue(Files.exists(walFile));
      assertEquals(0, strategy.getNodesNum());
      verify(node).close();

      strategy.deleteUniqueIdAndMayDeleteWALNode("region-b");
      assertFalse(Files.exists(directory));
      verify(node, times(1)).close();
      strategy.deleteUniqueIdAndMayDeleteWALNode("region-b");
    } finally {
      strategy.clear();
    }
  }

  private InsertRowNode getInsertRowNode() throws IllegalPathException {
    long time = 110L;
    TSDataType[] dataTypes =
        new TSDataType[] {
          TSDataType.DOUBLE,
          TSDataType.FLOAT,
          TSDataType.INT64,
          TSDataType.INT32,
          TSDataType.BOOLEAN,
          TSDataType.TEXT
        };

    Object[] columns = new Object[6];
    columns[0] = 1.0;
    columns[1] = 2.0F;
    columns[2] = 10000L;
    columns[3] = 100;
    columns[4] = false;
    columns[5] = new Binary("hh" + 0, TSFileConfig.STRING_CHARSET);

    MeasurementSchema[] schemas =
        new MeasurementSchema[] {
          new MeasurementSchema("s1", dataTypes[0]),
          new MeasurementSchema("s2", dataTypes[1]),
          new MeasurementSchema("s3", dataTypes[2]),
          new MeasurementSchema("s4", dataTypes[3]),
          new MeasurementSchema("s5", dataTypes[4]),
          new MeasurementSchema("s6", dataTypes[5]),
        };

    return new InsertRowNode(
        new PlanNodeId("0"),
        new PartialPath("root.test_sg.test_d"),
        false,
        new String[] {"s1", "s2", "s3", "s4", "s5", "s6"},
        dataTypes,
        schemas,
        time,
        columns,
        true);
  }
}
