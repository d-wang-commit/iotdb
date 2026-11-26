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

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.audit.UserDataTransferAuditHandler;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.disk.strategy.DirectoryStrategyType;
import org.apache.iotdb.commons.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.request.IConsensusRequest;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.KillPoint.DataNodeKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusDeleteLocalPeerKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusRemovePeerCoordinatorKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.commons.utils.RegionMigrationFileRemoveRateLimiter;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.IStateMachine.Registry;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.config.UserDataTransferAuditClassifier;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;
import org.apache.iotdb.consensus.i18n.ConsensusMessages;
import org.apache.iotdb.consensus.i18n.IoTConsensusMessages;
import org.apache.iotdb.consensus.iot.client.AsyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.client.IoTConsensusClientPool.AsyncIoTConsensusServiceClientPoolFactory;
import org.apache.iotdb.consensus.iot.client.IoTConsensusClientPool.SyncIoTConsensusServiceClientPoolFactory;
import org.apache.iotdb.consensus.iot.client.SyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.logdispatcher.IoTConsensusMemoryManager;
import org.apache.iotdb.consensus.iot.service.IoTConsensusRPCService;
import org.apache.iotdb.consensus.iot.service.IoTConsensusRPCServiceProcessor;
import org.apache.iotdb.consensus.iot.snapshot.IoTConsensusRateLimiter;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class IoTConsensus implements IConsensus {

  private static final long READER_UPDATE_INTERVAL_IN_MINUTES = 3;
  private static final Logger logger = LoggerFactory.getLogger(IoTConsensus.class);

  private final TEndPoint thisNode;
  private final int thisNodeId;
  private final File storageDir;
  private final List<String> recvSnapshotDirs;
  private final DirectoryStrategyType recvFolderStrategyType;
  private final IStateMachine.Registry registry;
  private final Map<ConsensusGroupId, IoTConsensusServerImpl> stateMachineMap =
      new ConcurrentHashMap<>();
  private final Set<ConsensusGroupId> pendingPeerCreatedCallbacks = ConcurrentHashMap.newKeySet();
  private final IoTConsensusRPCService service;
  private final RegisterManager registerManager = new RegisterManager();
  private final UserDataTransferAuditHandler userDataTransferAuditHandler;
  private final UserDataTransferAuditClassifier userDataTransferAuditClassifier;
  private IoTConsensusConfig config;

  /**
   * Optional callback invoked after a local peer is started, either during recovery or via {@link
   * #createLocalPeer}. Used by the subscription system to auto-bind prefetching queues to available
   * DataRegions.
   */
  public static volatile BiConsumer<ConsensusGroupId, IoTConsensusServerImpl> onNewPeerCreated;

  /**
   * Optional callback invoked after a recovered local peer is constructed and before it starts.
   * Used by the subscription system to restore WAL-retention guards before WAL cleanup is enabled.
   */
  public static volatile BiConsumer<ConsensusGroupId, IoTConsensusServerImpl> onPeerRecovering;

  /**
   * Optional callback invoked before a local peer is deleted via {@link #deleteLocalPeer}. Used by
   * the subscription system to unbind and clean up prefetching queues before the region is removed.
   */
  public static volatile Consumer<ConsensusGroupId> onPeerRemoved;

  private final IClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clientManager;
  private final IClientManager<TEndPoint, SyncIoTConsensusServiceClient> syncClientManager;
  private final ScheduledExecutorService backgroundTaskService;
  private Future<?> updateReaderFuture;
  private Map<ConsensusGroupId, List<Peer>> correctPeerListBeforeStart = null;

  public IoTConsensus(ConsensusConfig config, Registry registry) {
    this.thisNode = config.getThisNodeEndPoint();
    this.thisNodeId = config.getThisNodeId();
    this.storageDir = new File(config.getStorageDir());
    this.recvSnapshotDirs = config.getRecvSnapshotDirs();
    this.recvFolderStrategyType = config.getDirectoryStrategyType();
    this.config = config.getIotConsensusConfig();
    this.userDataTransferAuditHandler = config.getUserDataTransferAuditHandler();
    this.userDataTransferAuditClassifier = config.getUserDataTransferAuditClassifier();
    this.registry = registry;
    this.service =
        new IoTConsensusRPCService(
            thisNode, config.getIotConsensusConfig(), config.getTrustedChannelFailureHandler());
    this.clientManager =
        new IClientManager.Factory<TEndPoint, AsyncIoTConsensusServiceClient>()
            .createClientManager(
                new AsyncIoTConsensusServiceClientPoolFactory(config.getIotConsensusConfig()));
    this.syncClientManager =
        new IClientManager.Factory<TEndPoint, SyncIoTConsensusServiceClient>()
            .createClientManager(
                new SyncIoTConsensusServiceClientPoolFactory(config.getIotConsensusConfig()));
    this.backgroundTaskService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.IOT_CONSENSUS_BACKGROUND_TASK_EXECUTOR.getName());
    // init IoTConsensus memory manager
    IoTConsensusMemoryManager.getInstance()
        .init(
            config.getIotConsensusConfig().getReplication().getConsensusMemoryBlock(),
            config.getIotConsensusConfig().getReplication().getMaxMemoryRatioForQueue());
    // init IoTConsensus Rate Limiter
    IoTConsensusRateLimiter.getInstance()
        .init(
            config
                .getIotConsensusConfig()
                .getReplication()
                .getRegionMigrationSpeedLimitBytesPerSecond());
  }

  @Override
  public synchronized void start() throws IOException {
    initAndRecover();
    service.initSyncedServiceImpl(new IoTConsensusRPCServiceProcessor(this));
    try {
      registerManager.register(service);
    } catch (StartupException e) {
      throw new IOException(e);
    }
    if (updateReaderFuture == null) {
      updateReaderFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              backgroundTaskService,
              () ->
                  stateMachineMap
                      .values()
                      .forEach(impl -> impl.getLogDispatcher().checkAndFlushIndex()),
              READER_UPDATE_INTERVAL_IN_MINUTES,
              READER_UPDATE_INTERVAL_IN_MINUTES,
              TimeUnit.MINUTES);
    }
  }

  private void initAndRecover() throws IOException {
    if (!storageDir.exists()) {
      if (!storageDir.mkdirs()) {
        throw new IOException(
            String.format(ConsensusMessages.UNABLE_TO_CREATE_CONSENSUS_DIR_FMT, storageDir));
      }
    } else {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir.toPath())) {
        for (Path path : stream) {
          String[] items = path.getFileName().toString().split("_");
          ConsensusGroupId consensusGroupId =
              ConsensusGroupId.Factory.create(
                  Integer.parseInt(items[0]), Integer.parseInt(items[1]));
          Peer local = new Peer(consensusGroupId, thisNodeId, thisNode);
          if (correctPeerListBeforeStart != null
              && !correctPeerListBeforeStart
                  .getOrDefault(consensusGroupId, Collections.emptyList())
                  .contains(local)
              && mustRetainMigrationPeer(storageDir, consensusGroupId)) {
            // Preserve failed source cleanup and rolled-back target data outside CN membership.
            continue;
          }
          IoTConsensusServerImpl consensus =
              new IoTConsensusServerImpl(
                  path.toString(),
                  recvSnapshotDirs,
                  recvFolderStrategyType,
                  local,
                  Collections.emptyList(),
                  registry.apply(consensusGroupId),
                  backgroundTaskService,
                  clientManager,
                  syncClientManager,
                  config,
                  userDataTransferAuditHandler,
                  userDataTransferAuditClassifier);
          pendingPeerCreatedCallbacks.add(consensusGroupId);
          stateMachineMap.put(consensusGroupId, consensus);
        }
      } catch (DiskSpaceInsufficientException e) {
        throw new IOException(e);
      }
    }
    if (correctPeerListBeforeStart != null) {
      BiConsumer<ConsensusGroupId, List<Peer>> resetPeerListWithoutThrow =
          (consensusGroupId, peers) -> {
            try {
              resetPeerListImpl(consensusGroupId, peers, false);
            } catch (ConsensusGroupNotExistException ignore) {

            } catch (Exception e) {
              logger.warn(ConsensusMessages.FAILED_TO_RESET_PEER_LIST_WHILE_START, e);
            }
          };
      // make peers which are in list correct
      correctPeerListBeforeStart.forEach(resetPeerListWithoutThrow);
      // clear peers which are not in the list
      stateMachineMap.keySet().stream()
          .filter(consensusGroupId -> !correctPeerListBeforeStart.containsKey(consensusGroupId))
          // copy to a new list to avoid concurrent modification
          .collect(Collectors.toList())
          .forEach(
              consensusGroupId ->
                  resetPeerListWithoutThrow.accept(consensusGroupId, Collections.emptyList()));
    }
    stateMachineMap.forEach(
        (consensusGroupId, consensus) -> {
          try {
            final BiConsumer<ConsensusGroupId, IoTConsensusServerImpl> callback = onPeerRecovering;
            if (callback != null) {
              callback.accept(consensusGroupId, consensus);
            }
            consensus.start();
            // Subscription metadata may have been loaded before any local consensus peers existed.
            // Recovered peers must trigger the same binding as newly created peers, after startup
            // has restored their indices and installed the pre-start WAL-retention guards.
            notifyPeerCreated(consensusGroupId, consensus);
          } finally {
            pendingPeerCreatedCallbacks.remove(consensusGroupId);
          }
        });
  }

  @Override
  public synchronized void stop() {
    Optional.ofNullable(updateReaderFuture).ifPresent(future -> future.cancel(false));
    stateMachineMap.values().parallelStream().forEach(IoTConsensusServerImpl::stop);
    clientManager.close();
    syncClientManager.close();
    registerManager.deregisterAll();
    backgroundTaskService.shutdown();
    try {
      backgroundTaskService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn(ConsensusMessages.INTERRUPTED_WHEN_SHUTTING_DOWN_EXECUTOR, this, e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public TSStatus write(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (impl.isReadOnly()) {
      return StatusUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY);
    } else if (!impl.isActive()) {
      String message =
          String.format(
              ConsensusMessages.PEER_INACTIVE_NOT_READY_WRITE,
              groupId.toString(),
              impl.getThisNode().getNodeId());
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, message);
    } else {
      return impl.write(request);
    }
  }

  @Override
  public DataSet read(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    checkServingReplica(groupId, impl);
    return impl.read(request);
  }

  @SuppressWarnings("java:S2201")
  @Override
  public void createLocalPeer(ConsensusGroupId groupId, List<Peer> peers)
      throws ConsensusException {
    checkMigrationPeerBeforeCreate(groupId);
    int consensusGroupSize = peers.size();
    if (consensusGroupSize == 0) {
      throw new IllegalPeerNumException(consensusGroupSize);
    }
    if (!peers.contains(new Peer(groupId, thisNodeId, thisNode))) {
      throw new IllegalPeerEndpointException(thisNode, peers);
    }
    AtomicBoolean exist = new AtomicBoolean(true);
    Optional.ofNullable(
            stateMachineMap.computeIfAbsent(
                groupId,
                k -> {
                  exist.set(false);

                  String path = buildPeerDir(storageDir, groupId);
                  File file = new File(path);
                  if (!file.exists() && !file.mkdirs()) {
                    logger.warn(
                        ConsensusMessages.UNABLE_TO_CREATE_CONSENSUS_DIR_FOR_GROUP, groupId, path);
                    return null;
                  }

                  IoTConsensusServerImpl impl = null;
                  try {
                    impl =
                        new IoTConsensusServerImpl(
                            path,
                            recvSnapshotDirs,
                            recvFolderStrategyType,
                            new Peer(groupId, thisNodeId, thisNode),
                            peers,
                            registry.apply(groupId),
                            backgroundTaskService,
                            clientManager,
                            syncClientManager,
                            config,
                            userDataTransferAuditHandler,
                            userDataTransferAuditClassifier);
                  } catch (DiskSpaceInsufficientException | IOException e) {
                    throw new RuntimeException(e);
                  }
                  impl.start();
                  pendingPeerCreatedCallbacks.add(groupId);
                  return impl;
                }))
        .orElseThrow(
            () ->
                new ConsensusException(
                    String.format(
                        ConsensusMessages.UNABLE_TO_CREATE_CONSENSUS_DIR_FOR_GROUP_FMT, groupId)));
    KillPoint.setKillPoint(DataNodeKillPoints.DESTINATION_CREATE_LOCAL_PEER);
    if (exist.get()) {
      throw new ConsensusGroupAlreadyExistException(groupId);
    }

    try {
      notifyPeerCreated(groupId, stateMachineMap.get(groupId));
    } finally {
      pendingPeerCreatedCallbacks.remove(groupId);
    }
  }

  /** A retained replica must be reconciled before another operation reuses its storage. */
  public void checkMigrationPeerBeforeCreate(ConsensusGroupId groupId) throws ConsensusException {
    IoTConsensusServerImpl impl = stateMachineMap.get(groupId);
    if (impl != null
        ? impl.isMigrationFrozen() || impl.isUnavailableMigrationTarget()
        : mustRetainMigrationPeer(storageDir, groupId)) {
      throw new ConsensusException(
          String.format(
              IoTConsensusMessages
                  .EXCEPTION_CANNOT_CREATE_A_PEER_FOR_ARG_WHILE_ITS_PREVIOUS_MIGRATION_REPLICA_AWAITS_CLEANUP_51B5EAEF,
              groupId));
    }
  }

  private static void checkServingReplica(ConsensusGroupId groupId, IoTConsensusServerImpl impl)
      throws ConsensusGroupNotExistException {
    if (impl.isUnavailableMigrationTarget()) {
      throw unavailableTarget(groupId);
    }
  }

  private static ConsensusGroupNotExistException unavailableTarget(ConsensusGroupId groupId) {
    // Expose a retryable missing serving replica to reads using a stale route.
    return new ConsensusGroupNotExistException(
        String.format(
            IoTConsensusMessages
                .EXCEPTION_MIGRATION_TARGET_FOR_ARG_IS_UNAVAILABLE_WHILE_ITS_RETAINED_DATA_AWAITS_CLEANUP_88AD8F55,
            groupId));
  }

  public static boolean mustRetainMigrationPeer(File consensusDir, ConsensusGroupId groupId) {
    Path peerDirectory = new File(buildPeerDir(consensusDir, groupId)).toPath();
    try {
      MigrationSourceState source =
          MigrationSourceState.load(peerDirectory.resolve("migration.source"));
      return (source != null && source.finalIndex >= 0)
          || new MigrationTargetProgress(peerDirectory.resolve("migration.target")).hasSnapshot();
    } catch (IOException e) {
      logger.error(
          IoTConsensusMessages
              .LOG_FAILED_TO_READ_OR_WRITE_MIGRATION_RECEIPT_FOR_REGION_ARG_24C67DE7,
          groupId,
          e);
      return true;
    }
  }

  /** Returns whether a new or recovered local peer is still waiting for its creation callback. */
  public boolean isPeerCreationCallbackPending(final ConsensusGroupId groupId) {
    return pendingPeerCreatedCallbacks.contains(groupId);
  }

  private void notifyPeerCreated(
      final ConsensusGroupId groupId, final IoTConsensusServerImpl serverImpl) {
    final BiConsumer<ConsensusGroupId, IoTConsensusServerImpl> callback = onNewPeerCreated;
    if (callback != null) {
      try {
        callback.accept(groupId, serverImpl);
      } catch (final Exception e) {
        logger.warn(
            IoTConsensusMessages.LOG_ONNEWPEERCREATED_CALLBACK_FAILED_GROUP_ARG_2671FCDA,
            groupId,
            e);
      }
    }
  }

  @Override
  public void deleteLocalPeer(ConsensusGroupId groupId) throws ConsensusException {
    deleteLocalPeer(groupId, null, null);
  }

  private void notifyPeerRemoved(ConsensusGroupId groupId) {
    // Notify subscription system before stopping the peer, so that subscription queues can
    // properly unregister from the still-alive serverImpl.
    final Consumer<ConsensusGroupId> removeCallback = onPeerRemoved;
    if (removeCallback != null) {
      try {
        removeCallback.accept(groupId);
      } catch (final Exception e) {
        logger.warn(
            IoTConsensusMessages.LOG_ONPEERREMOVED_CALLBACK_FAILED_GROUP_ARG_9B79CBAF, groupId, e);
      }
    }
  }

  private TSStatus deleteLocalPeer(
      ConsensusGroupId groupId,
      IoTConsensusServerImpl expectedSource,
      Supplier<TSStatus> deleteStorage)
      throws ConsensusException {
    KillPoint.setKillPoint(IoTConsensusDeleteLocalPeerKillPoints.BEFORE_DELETE);
    if (expectedSource == null) {
      notifyPeerRemoved(groupId);
    }
    AtomicReference<TSStatus> cleanup = new AtomicReference<>();
    stateMachineMap.computeIfPresent(
        groupId,
        (k, v) -> {
          if (expectedSource != null) {
            // An ordinary Remove/Reconstruct may have replaced the source since its fence was read.
            if (v != expectedSource) {
              return v;
            }
            notifyPeerRemoved(groupId);
            pendingPeerCreatedCallbacks.remove(groupId);
          }
          v.stop();
          cleanup.set(deleteStorage == null ? RpcUtils.SUCCESS_STATUS : deleteStorage.get());
          // Keep the fence visible until storage has gone. A new peer must never open these
          // files or be deleted by this source's delayed task.
          if (deleteStorage != null
              && cleanup.get().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
            cleanup.set(deleteFencedPeerDirectory(groupId));
          }
          return null;
        });
    if (cleanup.get() == null) {
      throw new ConsensusGroupNotExistException(groupId);
    }
    if (expectedSource == null) {
      pendingPeerCreatedCallbacks.remove(groupId);
      deleteLocalPeerDirectory(groupId);
    }
    KillPoint.setKillPoint(IoTConsensusDeleteLocalPeerKillPoints.AFTER_DELETE);
    return cleanup.get();
  }

  private void deleteLocalPeerDirectory(ConsensusGroupId groupId) {
    FileUtils.deleteFileOrDirectoryWithRateLimiter(
        new File(buildPeerDir(storageDir, groupId)),
        RegionMigrationFileRemoveRateLimiter.getInstance()::acquire);
  }

  private TSStatus deleteFencedPeerDirectory(ConsensusGroupId groupId) {
    Path directory = new File(buildPeerDir(storageDir, groupId)).toPath();
    Path fence = directory.resolve("migration.source");
    try {
      // The rate-limited remover logs individual failures. Check each entry before releasing
      // the fence so a surviving checkpoint cannot be reused for a different replica.
      try (DirectoryStream<Path> entries = Files.newDirectoryStream(directory)) {
        for (Path entry : entries) {
          if (!entry.equals(fence)) {
            FileUtils.deleteFileOrDirectoryWithRateLimiter(
                entry.toFile(), RegionMigrationFileRemoveRateLimiter.getInstance()::acquire);
            if (!Files.notExists(entry, LinkOption.NOFOLLOW_LINKS)) {
              throw new DirectoryNotEmptyException(directory.toString());
            }
          }
        }
      }
      Files.delete(fence);
      Files.delete(directory);
      return RpcUtils.SUCCESS_STATUS;
    } catch (IOException e) {
      return RpcUtils.getStatus(TSStatusCode.MIGRATE_REGION_ERROR, e.toString());
    }
  }

  @Override
  public void addRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    addRemotePeer(groupId, peer, null);
  }

  public void addRemotePeerForMigration(ConsensusGroupId groupId, Peer peer, long migrationId)
      throws ConsensusException {
    addRemotePeer(groupId, peer, migrationId);
  }

  private void addRemotePeer(ConsensusGroupId groupId, Peer peer, Long migrationId)
      throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    synchronized (impl) {
      checkServingReplica(groupId, impl);
      if (migrationId != null) {
        try {
          impl.checkMigrationAddAllowed(migrationId, peer.getNodeId());
        } catch (ConsensusGroupModifyPeerException e) {
          throw new ConsensusException(e);
        }
      }
      if (impl.getConfiguration().contains(peer)) {
        if (migrationId != null) {
          // The same source lifetime remembers the completed Add. Across restarts only a
          // durably frozen source can prove this migration's boundary.
          if (impl.hasPreparedMigration(migrationId, peer.getNodeId())) {
            return;
          }
          throw new ConsensusException(
              String.format(
                  IoTConsensusMessages
                      .EXCEPTION_MIGRATION_ARG_CANNOT_VERIFY_THE_EXISTING_PEER_ARG_EA694D35,
                  migrationId,
                  peer));
        }
        throw new PeerAlreadyInConsensusGroupException(groupId, peer);
      }
      try {
        // step 1: inactive new Peer to prepare for following steps
        logger.info(IoTConsensusMessages.INACTIVATE_NEW_PEER, peer);
        impl.inactivatePeer(peer, false);

        // step 2: notify all the other Peers to build the sync connection to newPeer
        logger.info(IoTConsensusMessages.NOTIFY_PEERS_BUILD_SYNC_LOG);
        impl.notifyPeersToBuildSyncLogChannel(peer);

        // step 3: take snapshot
        logger.info(IoTConsensusMessages.START_TAKE_SNAPSHOT);

        long snapshotIndex = -1;
        if (migrationId == null) {
          impl.takeSnapshot();
        } else {
          snapshotIndex = impl.takeMigrationSnapshot();
        }

        // step 4: transit snapshot
        logger.info(IoTConsensusMessages.START_TRANSMIT_SNAPSHOT);
        int fileCount = impl.transmitSnapshot(peer, migrationId != null);

        // step 5: let the new peer load snapshot
        logger.info(IoTConsensusMessages.TRIGGER_LOAD_SNAPSHOT);
        impl.triggerSnapshotLoad(peer, migrationId, snapshotIndex, fileCount);
        KillPoint.setKillPoint(DataNodeKillPoints.COORDINATOR_ADD_PEER_TRANSITION);

        // step 6: active new Peer
        logger.info(IoTConsensusMessages.ACTIVATE_NEW_PEER);
        impl.activePeer(peer);
        impl.waitNewPeerUntilSyncLogCompleted(peer);
        if (migrationId != null) {
          impl.prepareMigration(migrationId, peer.getNodeId());
        }

        // step 7: notify remote peer to clean up transferred snapshot
        logger.info(IoTConsensusMessages.CLEANUP_REMOTE_SNAPSHOT);
        try {
          impl.cleanupRemoteSnapshot(peer);
        } catch (ConsensusGroupModifyPeerException e) {
          logger.warn(IoTConsensusMessages.FAILED_CLEANUP_REMOTE_SNAPSHOT, e);
        }
        KillPoint.setKillPoint(DataNodeKillPoints.COORDINATOR_ADD_PEER_DONE);

      } catch (ConsensusGroupModifyPeerException e) {
        if (migrationId == null) {
          logger.info(IoTConsensusMessages.ADD_REMOTE_PEER_FAILED_CLEANUP);
          impl.notifyPeersToRemoveSyncLogChannel(peer);
        }
        throw new ConsensusException(e);
      } finally {
        logger.info(IoTConsensusMessages.CLEANUP_LOCAL_SNAPSHOT);
        impl.cleanupLocalSnapshot();
      }
    }
  }

  public void removeRemotePeerForMigration(ConsensusGroupId groupId, Peer source, long migrationId)
      throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    synchronized (impl) {
      try {
        KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.INIT);
        // A target that restarted before confirmation cannot certify its old in-memory prefix.
        // Refuse locally while the source is still writable.
        if (!impl.hasTargetMigrationBaseline(migrationId, source.getNodeId())) {
          throw new ConsensusException(
              String.format(
                  IoTConsensusMessages
                      .EXCEPTION_MIGRATION_ARG_CANNOT_VERIFY_THE_SNAPSHOT_ON_ARG_4FB0EEC8,
                  migrationId,
                  impl.getThisNode()));
        }
        long finalIndex = impl.freezeRemoteSource(source, migrationId);
        KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.AFTER_INACTIVE_PEER);
        // Wait for the source dispatchers while all channels are still present. The destination's
        // own durable confirmation below is required even on replay after a membership change.
        impl.waitTargetPeerUntilSyncLogCompleted(source, true);
        if (!impl.confirmMigrationCatchUp(migrationId, source.getNodeId(), finalIndex)) {
          throw new ConsensusException(
              String.format(
                  IoTConsensusMessages
                      .EXCEPTION_MIGRATION_ARG_CANNOT_VERIFY_SNAPSHOT_COVERAGE_THROUGH_INDEX_ARG_99DE0F71,
                  migrationId,
                  finalIndex));
        }
        // CN removes channels when it reconciles the final peer list. A late Remove must leave
        // replication to the source intact if CN has already chosen to roll back.
        KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.AFTER_CONFIRMATION);
        impl.waitReleaseAllRegionRelatedResource(source);
        KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.FINISH);
      } catch (ConsensusGroupModifyPeerException e) {
        throw new ConsensusException(e);
      }
    }
  }

  public boolean isRegionMigrationConfirmed(
      ConsensusGroupId groupId, int sourceNodeId, long migrationId) {
    IoTConsensusServerImpl impl = stateMachineMap.get(groupId);
    return impl != null && impl.isMigrationConfirmed(migrationId, sourceNodeId);
  }

  public void rollbackRegionMigration(
      ConsensusGroupId groupId, int otherNodeId, long migrationId, boolean isSource)
      throws ConsensusException {
    IoTConsensusServerImpl impl = stateMachineMap.get(groupId);
    try {
      if (isSource) {
        if (impl == null) {
          throw new ConsensusGroupNotExistException(groupId);
        }
        impl.rollbackMigration(migrationId, otherNodeId);
      } else if (impl != null) {
        impl.cancelMigrationTarget(migrationId, otherNodeId);
      } else {
        // An unlisted target stays on disk after restart. Persist its cancellation before
        // acknowledging it, so even a later Create cannot reopen this replica as healthy.
        MigrationTargetProgress retained =
            new MigrationTargetProgress(
                new File(buildPeerDir(storageDir, groupId), "migration.target").toPath());
        if (retained.hasSnapshot()) {
          retained.abort(migrationId, otherNodeId);
        }
      }
    } catch (IOException | ConsensusGroupModifyPeerException e) {
      throw new ConsensusException(e);
    }
  }

  public TSStatus deleteLocalPeerForMigration(
      ConsensusGroupId groupId, long migrationId, Supplier<TSStatus> deleteStorage)
      throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    synchronized (impl) {
      try {
        if (!impl.hasMigrationFence(migrationId)) {
          throw new ConsensusException(
              String.format(
                  IoTConsensusMessages
                      .EXCEPTION_SOURCE_MIGRATION_FENCE_DOES_NOT_MATCH_MIGRATION_ARG_5AE240E7,
                  migrationId));
        }
      } catch (IOException e) {
        throw new ConsensusException(e);
      }
      return deleteLocalPeer(groupId, impl, deleteStorage);
    }
  }

  @Override
  public void removeRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));

    synchronized (impl) {
      if (!impl.getConfiguration().contains(peer)) {
        throw new PeerNotInConsensusGroupException(groupId, peer.toString());
      }

      KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.INIT);

      // let other peers remove the sync channel with target peer
      impl.notifyPeersToRemoveSyncLogChannel(peer);
      KillPoint.setKillPoint(
          IoTConsensusRemovePeerCoordinatorKillPoints
              .AFTER_NOTIFY_PEERS_TO_REMOVE_REPLICATE_CHANNEL);

      try {
        // let target peer reject new write
        impl.inactivatePeer(peer, true);
        KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.AFTER_INACTIVE_PEER);
        // wait its SyncLog to complete
        impl.waitTargetPeerUntilSyncLogCompleted(peer);
        // wait its region related resource to release
        impl.waitReleaseAllRegionRelatedResource(peer);
      } catch (ConsensusGroupModifyPeerException e) {
        throw new ConsensusException(e.getMessage());
      }
      KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.FINISH);
    }
  }

  @Override
  public void transferLeader(ConsensusGroupId groupId, Peer newLeader) throws ConsensusException {
    throw new ConsensusException(IoTConsensusMessages.NOT_SUPPORT_LEADER_TRANSFER);
  }

  @Override
  public void triggerSnapshot(ConsensusGroupId groupId, boolean force) throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    try {
      synchronized (impl) {
        impl.takeSnapshot();
      }
    } catch (ConsensusGroupModifyPeerException e) {
      throw new ConsensusException(e.getMessage());
    }
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    return true;
  }

  @Override
  public boolean isLeaderReady(ConsensusGroupId groupId) {
    return true;
  }

  @Override
  public long getLogicalClock(ConsensusGroupId groupId) {
    return Optional.ofNullable(stateMachineMap.get(groupId))
        .map(IoTConsensusServerImpl::getSearchIndex)
        .orElse(0L);
  }

  @Override
  public Peer getLeader(ConsensusGroupId groupId) {
    if (!stateMachineMap.containsKey(groupId)) {
      return null;
    }
    return new Peer(groupId, thisNodeId, thisNode);
  }

  @Override
  public int getReplicationNum(ConsensusGroupId groupId) {
    IoTConsensusServerImpl impl = stateMachineMap.get(groupId);
    return impl != null ? impl.getConfiguration().size() : 0;
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(stateMachineMap.keySet());
  }

  @Override
  public String getRegionDirFromConsensusGroupId(ConsensusGroupId groupId) {
    return buildPeerDir(storageDir, groupId);
  }

  @Override
  public void reloadConsensusConfig(ConsensusConfig consensusConfig) {
    config = consensusConfig.getIotConsensusConfig();

    for (IoTConsensusServerImpl impl : stateMachineMap.values()) {
      impl.reloadConsensusConfig(config);
    }

    // update region migration speed limit
    IoTConsensusRateLimiter.getInstance()
        .init(config.getReplication().getRegionMigrationSpeedLimitBytesPerSecond());
  }

  @Override
  public void recordCorrectPeerListBeforeStarting(
      Map<ConsensusGroupId, List<Peer>> correctPeerList) {
    logger.info(ConsensusMessages.RECORD_CORRECT_PEER_LIST, correctPeerList);
    this.correctPeerListBeforeStart = correctPeerList;
  }

  @Override
  public void resetPeerList(ConsensusGroupId groupId, List<Peer> correctPeers)
      throws ConsensusException {
    resetPeerListImpl(groupId, correctPeers, true);
  }

  private void resetPeerListImpl(
      ConsensusGroupId groupId, List<Peer> correctPeers, boolean startNow)
      throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));

    Peer localPeer = new Peer(groupId, thisNodeId, thisNode);
    if (!correctPeers.contains(localPeer)) {
      if (impl.isMigrationFrozen() || impl.isUnavailableMigrationTarget()) {
        // Resetting membership does not clean Region storage. Only its matching Delete may
        // discard the fence.
        return;
      }
      logger.info(ConsensusMessages.RESET_PEER_LIST_NOT_IN_CORRECT, groupId);
      deleteLocalPeer(groupId);
      return;
    }

    synchronized (impl) {
      // remove invalid peer
      ImmutableList<Peer> currentMembers = ImmutableList.copyOf(impl.getConfiguration());
      String previousPeerListStr = currentMembers.toString();
      for (Peer peer : currentMembers) {
        if (!correctPeers.contains(peer)) {
          if (!impl.removeSyncLogChannel(peer)) {
            logger.error(
                ConsensusMessages.RESET_PEER_LIST_FAILED_TO_REMOVE_SYNC_CHANNEL, groupId, peer);
          } else {
            logger.info(ConsensusMessages.RESET_PEER_LIST_REMOVE_SYNC_CHANNEL, groupId, peer);
          }
        }
      }
      // add correct peer
      for (Peer peer : correctPeers) {
        if (!impl.getConfiguration().contains(peer)) {
          impl.buildSyncLogChannel(peer, startNow);
          logger.info(ConsensusMessages.RESET_PEER_LIST_BUILD_SYNC_CHANNEL, groupId, peer);
        }
      }
      // show result
      String newPeerListStr = impl.getConfiguration().toString();
      if (!previousPeerListStr.equals(newPeerListStr)) {
        logger.info(
            ConsensusMessages.RESET_PEER_LIST_RESET_RESULT,
            groupId,
            previousPeerListStr,
            newPeerListStr);
      } else {
        logger.info(
            ConsensusMessages.RESET_PEER_LIST_NOTHING_TO_RESET, groupId, previousPeerListStr);
      }
    }
  }

  public IoTConsensusServerImpl getImpl(ConsensusGroupId groupId) {
    return stateMachineMap.get(groupId);
  }

  public static String buildPeerDir(File storageDir, ConsensusGroupId groupId) {
    return storageDir + File.separator + groupId.getType().getValue() + "_" + groupId.getId();
  }
}
