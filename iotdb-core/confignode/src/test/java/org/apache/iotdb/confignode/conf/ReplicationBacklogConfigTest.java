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
package org.apache.iotdb.confignode.conf;

import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReplicationBacklogConfigTest {
  @Test
  public void defaultObservesWithoutInventingThresholds() throws Exception {
    ReplicationBacklogConfig conf = ReplicationBacklogConfig.defaults();
    assertEquals(ReplicationBacklogConfig.Mode.OBSERVE, conf.getMode());
    assertFalse(conf.hasThresholds((byte) 1));
    assertFalse(conf.hasThresholds((byte) 2));
    assertSame(conf, ReplicationBacklogConfig.load(new Properties(), conf));
  }

  @Test
  public void validateAllValuesBeforePublishingEnforce() throws Exception {
    ConfigNodeConfig conf = new ConfigNodeConfig();
    ReplicationBacklogConfig original = conf.getReplicationBacklogConfig();
    Properties props = new Properties();
    props.setProperty(ReplicationBacklogConfig.PREFIX + "mode", "ENFORCE");
    try {
      conf.setReplicationBacklogConfig(ReplicationBacklogConfig.load(props, original));
      fail("Expected missing thresholds to reject enforcement");
    } catch (IOException expected) {
      assertNotNull(expected.getMessage());
    }
    assertSame(original, conf.getReplicationBacklogConfig());
    for (String kind : Arrays.asList("request", "tablet", "file", "deletion")) {
      props.setProperty(ReplicationBacklogConfig.PREFIX + kind + "_gap", "100");
    }
    conf.setReplicationBacklogConfig(ReplicationBacklogConfig.load(props, original));
    assertEquals(
        ReplicationBacklogConfig.Mode.ENFORCE, conf.getReplicationBacklogConfig().getMode());
    assertTrue(conf.getReplicationBacklogConfig().hasThresholds((byte) 1));
    assertTrue(conf.getReplicationBacklogConfig().hasThresholds((byte) 2));
  }

  @Test
  public void rejectsNonFiniteCoefficientsAndInvalidDurations() throws Exception {
    String[][] invalid = {
      {"ewma_alpha", "NaN"},
      {"ewma_alpha", "0"},
      {"relative_gap", "Infinity"},
      {"ttl_ms", "100"},
      {"sample_interval_ms", "0"},
      {"balance_interval_ms", "-1"},
      {"confirmation_rounds", "0"},
      {"mode", "AUTO"},
      {"request_gap", "0"}
    };
    for (String[] entry : invalid) {
      Properties properties = new Properties();
      properties.setProperty(ReplicationBacklogConfig.PREFIX + entry[0], entry[1]);
      try {
        ReplicationBacklogConfig.load(properties, ReplicationBacklogConfig.defaults());
        fail(Arrays.toString(entry));
      } catch (IOException expected) {
        assertTrue(expected.getMessage().contains(ReplicationBacklogConfig.PREFIX + entry[0]));
      }
    }
  }
}
