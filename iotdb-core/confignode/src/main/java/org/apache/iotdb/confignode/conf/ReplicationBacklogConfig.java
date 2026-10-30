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

import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/** Immutable so a reload never exposes a partly validated filtering policy. */
public final class ReplicationBacklogConfig {
  public enum Mode {
    OFF,
    OBSERVE,
    ENFORCE
  }

  public static final String PREFIX = "iot_consensus_replication_backlog_";
  private static final String[] COUNT_KEYS = {
    "request_gap", "tablet_gap", "file_gap", "deletion_gap"
  };
  private final Mode mode;
  private final long sampleIntervalMs;
  private final long ttlMs;
  private final long balanceIntervalMs;
  private final double alpha;
  private final double relativeGap;
  private final int confirmationRounds;
  private final long[] absoluteGaps;

  private ReplicationBacklogConfig(
      Mode mode,
      long sampleIntervalMs,
      long ttlMs,
      long balanceIntervalMs,
      double alpha,
      double relativeGap,
      int confirmationRounds,
      long[] absoluteGaps) {
    this.mode = mode;
    this.sampleIntervalMs = sampleIntervalMs;
    this.ttlMs = ttlMs;
    this.balanceIntervalMs = balanceIntervalMs;
    this.alpha = alpha;
    this.relativeGap = relativeGap;
    this.confirmationRounds = confirmationRounds;
    this.absoluteGaps = absoluteGaps.clone();
  }

  public static ReplicationBacklogConfig defaults() {
    return new ReplicationBacklogConfig(
        Mode.OBSERVE, 5000, 30000, 10000, 0.5, 1.0, 3, new long[] {-1, -1, -1, -1});
  }

  public static ReplicationBacklogConfig load(Properties properties, ReplicationBacklogConfig old)
      throws IOException {
    String key = PREFIX + "mode";
    try {
      Mode mode =
          Mode.valueOf(
              properties.getProperty(key, old.mode.name()).trim().toUpperCase(Locale.ROOT));
      key = PREFIX + "sample_interval_ms";
      long sample =
          Long.parseLong(properties.getProperty(key, Long.toString(old.sampleIntervalMs)));
      if (sample <= 0 || sample > TimeUnit.DAYS.toMillis(1)) {
        throw invalid(key);
      }
      key = PREFIX + "ttl_ms";
      long ttl = Long.parseLong(properties.getProperty(key, Long.toString(old.ttlMs)));
      if (ttl < sample || ttl > TimeUnit.DAYS.toMillis(7)) {
        throw invalid(key);
      }
      key = PREFIX + "balance_interval_ms";
      long balance =
          Long.parseLong(properties.getProperty(key, Long.toString(old.balanceIntervalMs)));
      if (balance <= 0 || balance > TimeUnit.DAYS.toMillis(1)) {
        throw invalid(key);
      }
      key = PREFIX + "ewma_alpha";
      double alpha = Double.parseDouble(properties.getProperty(key, Double.toString(old.alpha)));
      if (!Double.isFinite(alpha) || alpha <= 0 || alpha > 1) {
        throw invalid(key);
      }
      key = PREFIX + "relative_gap";
      double relative =
          Double.parseDouble(properties.getProperty(key, Double.toString(old.relativeGap)));
      if (!Double.isFinite(relative) || relative <= 0) {
        throw invalid(key);
      }
      key = PREFIX + "confirmation_rounds";
      int rounds =
          Integer.parseInt(properties.getProperty(key, Integer.toString(old.confirmationRounds)));
      if (rounds <= 0 || rounds > 1000) {
        throw invalid(key);
      }
      long[] gaps = new long[COUNT_KEYS.length];
      for (int i = 0; i < gaps.length; i++) {
        key = PREFIX + COUNT_KEYS[i];
        gaps[i] = Long.parseLong(properties.getProperty(key, Long.toString(old.absoluteGaps[i])));
        if ((gaps[i] != -1 && gaps[i] <= 0) || (mode == Mode.ENFORCE && gaps[i] <= 0)) {
          throw invalid(key);
        }
      }
      ReplicationBacklogConfig result =
          new ReplicationBacklogConfig(mode, sample, ttl, balance, alpha, relative, rounds, gaps);
      return result.sameSettings(old) && result.mode == old.mode ? old : result;
    } catch (IllegalArgumentException e) {
      throw new IOException(
          String.format(
              ConfigNodeMessages.EXCEPTION_INVALID_REPLICATION_BACKLOG_CONFIGURATION_ARG_283A8D2F,
              key),
          e);
    }
  }

  private static IOException invalid(String key) {
    return new IOException(
        String.format(
            ConfigNodeMessages.EXCEPTION_INVALID_REPLICATION_BACKLOG_CONFIGURATION_ARG_283A8D2F,
            key));
  }

  public boolean sameSettings(ReplicationBacklogConfig other) {
    return sampleIntervalMs == other.sampleIntervalMs
        && ttlMs == other.ttlMs
        && balanceIntervalMs == other.balanceIntervalMs
        && Double.compare(alpha, other.alpha) == 0
        && Double.compare(relativeGap, other.relativeGap) == 0
        && confirmationRounds == other.confirmationRounds
        && Arrays.equals(absoluteGaps, other.absoluteGaps);
  }

  public boolean hasThresholds(byte protocol) {
    if (protocol == 1) {
      return absoluteGaps[0] > 0;
    }
    return protocol == 2 && absoluteGaps[1] > 0 && absoluteGaps[2] > 0 && absoluteGaps[3] > 0;
  }

  public Mode getMode() {
    return mode;
  }

  public long getSampleIntervalMs() {
    return sampleIntervalMs;
  }

  public long getTtlMs() {
    return ttlMs;
  }

  public long getBalanceIntervalMs() {
    return balanceIntervalMs;
  }

  public double getAlpha() {
    return alpha;
  }

  public double getRelativeGap() {
    return relativeGap;
  }

  public int getConfirmationRounds() {
    return confirmationRounds;
  }

  public long getAbsoluteGap(int dimension) {
    return absoluteGaps[dimension];
  }
}
