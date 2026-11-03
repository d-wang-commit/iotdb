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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ReplicationBacklogConfigTest {
  @Test
  public void defaultsEnforceWithConfiguredThresholds() throws Exception {
    ReplicationBacklogConfig conf = ReplicationBacklogConfig.defaults();
    assertEquals(ReplicationBacklogConfig.Mode.ENFORCE, conf.getMode());
    assertEquals(3, conf.getConfirmationRounds());
    assertTrue(conf.hasThresholds((byte) 1));
    assertTrue(conf.hasThresholds((byte) 2));
    for (int dimension = 0; dimension < 4; dimension++) {
      assertEquals(10, conf.getAbsoluteGap(dimension));
    }
    assertSame(conf, ReplicationBacklogConfig.load(new Properties(), conf));
  }

  @Test
  public void reloadsConfirmationRoundsWithoutChangingPreviousSettings() throws Exception {
    ReplicationBacklogConfig original = ReplicationBacklogConfig.defaults();
    Properties properties = new Properties();
    properties.setProperty(ReplicationBacklogConfig.PREFIX + "confirmation_rounds", "5");
    ReplicationBacklogConfig updated = ReplicationBacklogConfig.load(properties, original);
    assertEquals(5, updated.getConfirmationRounds());
    assertEquals(3, original.getConfirmationRounds());
    assertEquals(original.getMode(), updated.getMode());
    assertEquals(original.getAlpha(), updated.getAlpha(), 0);
    assertSame(updated, ReplicationBacklogConfig.load(properties, updated));
  }

  @Test
  public void validateAllValuesBeforePublishingEnforce() throws Exception {
    ConfigNodeConfig conf = new ConfigNodeConfig();
    Properties observation = new Properties();
    observation.setProperty(ReplicationBacklogConfig.PREFIX + "mode", "OBSERVE");
    for (String kind : Arrays.asList("request", "tablet", "file", "deletion")) {
      observation.setProperty(ReplicationBacklogConfig.PREFIX + kind + "_gap", "-1");
    }
    conf.setReplicationBacklogConfig(
        ReplicationBacklogConfig.load(observation, conf.getReplicationBacklogConfig()));
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
