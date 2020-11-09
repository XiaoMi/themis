package org.apache.hadoop.hbase.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.avro.reflect.Nullable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.CallQueueTooBigException;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.RegionLocations;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.ClusterStatusListener.DeadServerHandler;
import org.apache.hadoop.hbase.client.ClusterStatusListener.Listener;
import org.apache.hadoop.hbase.client.Scan.ReadType;
import org.apache.hadoop.hbase.client.TableState.State;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicy;
import org.apache.hadoop.hbase.client.backoff.ClientBackoffPolicyFactory;
import org.apache.hadoop.hbase.exceptions.ClientExceptionsUtil;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.hadoop.hbase.ipc.RpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.log.HBaseMarkers;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.RequestConverter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GetUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.GrantResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.HasUserPermissionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos.RevokeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.CoprocessorServiceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AbortProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AddColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.AssignRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.BalanceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClearDeadServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ClearDeadServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.CreateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DecommissionRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DecommissionRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DeleteTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.DisableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableCatalogJanitorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableCatalogJanitorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.EnableTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ExecProcedureResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetClusterStatusResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetCompletedSnapshotsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetLocksRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetLocksResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetNamespaceDescriptorResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProcedureResultResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProceduresRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetProceduresResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetSchemaAlterStatusResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableNamesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableStateRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.GetTableStateResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.HbckService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsBalancerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCatalogJanitorEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsCleanerChoreEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsInMaintenanceModeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsMasterRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsMasterRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsNormalizerEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsProcedureDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsRpcThrottleEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotCleanupEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotCleanupEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSnapshotDoneResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.IsSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListDecommissionedRegionServersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespaceDescriptorsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespacesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListNamespacesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableDescriptorsByNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableNamesByNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ListTableNamesByNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampForRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MajorCompactionTimestampResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MergeTableRegionsResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyColumnResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyNamespaceResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ModifyTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MoveRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.NormalizeResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.OfflineRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RecommissionRegionServerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RecommissionRegionServerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RestoreSnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCatalogScanResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCleanerChoreRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.RunCleanerChoreResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SecurityCapabilitiesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetBalancerRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetCleanerChoreRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetCleanerChoreRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetNormalizerRunningResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetQuotaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSnapshotCleanupRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSnapshotCleanupResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SetSplitOrMergeEnabledResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ShutdownRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.ShutdownResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SnapshotResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SplitTableRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SplitTableRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.StopMasterResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchExceedThrottleQuotaRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchExceedThrottleQuotaResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchRpcThrottleRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.SwitchRpcThrottleResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.TruncateTableResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.UnassignRegionResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.HbckService.BlockingInterface;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetQuotaStatesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.QuotaProtos.GetSpaceQuotaRegionSizesResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.AddReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.DisableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.EnableReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.GetReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.ListReplicationPeersResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.RemoveReplicationPeerResponse;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigRequest;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ReplicationProtos.UpdateReplicationPeerConfigResponse;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ConcurrentMapUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hbase.thirdparty.com.google.common.base.Throwables;
import org.apache.hbase.thirdparty.com.google.protobuf.BlockingRpcChannel;
import org.apache.hbase.thirdparty.com.google.protobuf.RpcController;
import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionImplementation implements ClusterConnection, Closeable {
  public static final String RETRIES_BY_SERVER_KEY = "hbase.client.retries.by.server";
  private static final Logger LOG = LoggerFactory.getLogger(ConnectionImplementation.class);
  private static final String RESOLVE_HOSTNAME_ON_FAIL_KEY = "hbase.resolve.hostnames.on.failure";
  private final boolean hostnamesCanChange;
  private final long pause;
  private final long pauseForCQTBE;
  private boolean useMetaReplicas;
  private final int metaReplicaCallTimeoutScanInMicroSecond;
  private final int numTries;
  final int rpcTimeout;
  private static volatile NonceGenerator nonceGenerator = null;
  private static final Object nonceGeneratorCreateLock = new Object();
  private final AsyncProcess asyncProcess;
  private final ServerStatisticTracker stats;
  private volatile boolean closed;
  private volatile boolean aborted;
  ClusterStatusListener clusterStatusListener;
  private final Object metaRegionLock = new Object();
  private final Object masterLock = new Object();
  private volatile ThreadPoolExecutor batchPool = null;
  private volatile ThreadPoolExecutor metaLookupPool = null;
  private volatile boolean cleanupPool = false;
  private final Configuration conf;
  private final ConnectionConfiguration connectionConfig;
  private final RpcClient rpcClient;
  private final MetaCache metaCache;
  private final MetricsConnection metrics;
  protected User user;
  private final RpcRetryingCallerFactory rpcCallerFactory;
  private final RpcControllerFactory rpcControllerFactory;
  private final RetryingCallerInterceptor interceptor;
  private final ConnectionRegistry registry;
  private final ClientBackoffPolicy backoffPolicy;
  private final String alternateBufferedMutatorClassName;
  private final ReentrantLock userRegionLock = new ReentrantLock();
  private ChoreService authService;
  protected String clusterId = null;
  private final ConcurrentMap<String, Object> stubs = new ConcurrentHashMap();
  final ConnectionImplementation.MasterServiceState masterServiceState = new ConnectionImplementation.MasterServiceState(this);

  ConnectionImplementation(Configuration conf, ExecutorService pool, User user) throws IOException {
    this.conf = conf;
    this.user = user;
    if (user != null && user.isLoginFromKeytab()) {
      this.spawnRenewalChore(user.getUGI());
    }

    this.batchPool = (ThreadPoolExecutor)pool;
    this.connectionConfig = new ConnectionConfiguration(conf);
    this.closed = false;
    this.pause = conf.getLong("hbase.client.pause", 100L);
    long configuredPauseForCQTBE = conf.getLong("hbase.client.pause.cqtbe", this.pause);
    if (configuredPauseForCQTBE < this.pause) {
      LOG.warn("The hbase.client.pause.cqtbe setting: " + configuredPauseForCQTBE + " is smaller than " + "hbase.client.pause" + ", will use " + this.pause + " instead.");
      this.pauseForCQTBE = this.pause;
    } else {
      this.pauseForCQTBE = configuredPauseForCQTBE;
    }

    this.useMetaReplicas = conf.getBoolean("hbase.meta.replicas.use", false);
    this.metaReplicaCallTimeoutScanInMicroSecond = this.connectionConfig.getMetaReplicaCallTimeoutMicroSecondScan();
    this.numTries = ConnectionUtils.retries2Attempts(this.connectionConfig.getRetriesNumber());
    this.rpcTimeout = conf.getInt("hbase.rpc.timeout", 60000);
    if (conf.getBoolean("hbase.client.nonces.enabled", true)) {
      synchronized(nonceGeneratorCreateLock) {
        if (nonceGenerator == null) {
          nonceGenerator = PerClientRandomNonceGenerator.get();
        }
      }
    } else {
      nonceGenerator = ConnectionUtils.NO_NONCE_GENERATOR;
    }

    this.stats = ServerStatisticTracker.create(conf);
    this.interceptor = (new RetryingCallerInterceptorFactory(conf)).build();
    this.rpcControllerFactory = RpcControllerFactory.instantiate(conf);
    this.rpcCallerFactory = RpcRetryingCallerFactory.instantiate(conf, this.interceptor, this.stats);
    this.backoffPolicy = ClientBackoffPolicyFactory.create(conf);
    this.asyncProcess = new AsyncProcess(this, conf, this.rpcCallerFactory, this.rpcControllerFactory);
    if (conf.getBoolean("hbase.client.metrics.enable", false)) {
      this.metrics = new MetricsConnection(this.toString(), this::getBatchPool, this::getMetaLookupPool);
    } else {
      this.metrics = null;
    }

    this.metaCache = new MetaCache(this.metrics);
    boolean shouldListen = conf.getBoolean("hbase.status.published", false);
    this.hostnamesCanChange = conf.getBoolean("hbase.resolve.hostnames.on.failure", true);
    Class<? extends Listener> listenerClass = conf.getClass("hbase.status.listener.class", ClusterStatusListener.DEFAULT_STATUS_LISTENER_CLASS, Listener.class);
    this.alternateBufferedMutatorClassName = this.conf.get("hbase.client.bufferedmutator.classname");

    try {
      this.registry = ConnectionRegistryFactory.getRegistry(conf);
      this.retrieveClusterId();
      this.rpcClient = RpcClientFactory.createClient(this.conf, this.clusterId, this.metrics);
      if (shouldListen) {
        if (listenerClass == null) {
          LOG.warn("hbase.status.published is true, but hbase.status.listener.class is not set - not listening status");
        } else {
          this.clusterStatusListener = new ClusterStatusListener(new DeadServerHandler() {
            public void newDead(ServerName sn) {
              ConnectionImplementation.this.clearCaches(sn);
              ConnectionImplementation.this.rpcClient.cancelConnections(sn);
            }
          }, conf, listenerClass);
        }
      }

    } catch (Throwable var9) {
      LOG.debug("connection construction failed", var9);
      this.close();
      throw var9;
    }
  }

  private void spawnRenewalChore(UserGroupInformation user) {
    this.authService = new ChoreService("Relogin service");
    this.authService.scheduleChore(AuthUtil.getAuthRenewalChore(user));
  }

  @VisibleForTesting
  void setUseMetaReplicas(boolean useMetaReplicas) {
    this.useMetaReplicas = useMetaReplicas;
  }

  @VisibleForTesting
  static NonceGenerator injectNonceGeneratorForTesting(ClusterConnection conn, NonceGenerator cnm) {
    ConnectionImplementation connImpl = (ConnectionImplementation)conn;
    NonceGenerator ng = connImpl.getNonceGenerator();
    LOG.warn("Nonce generator is being replaced by test code for " + cnm.getClass().getName());
    nonceGenerator = cnm;
    return ng;
  }

  public Table getTable(TableName tableName) throws IOException {
    return this.getTable(tableName, this.getBatchPool());
  }

  public TableBuilder getTableBuilder(TableName tableName, final ExecutorService pool) {
    return new TableBuilderBase(tableName, this.connectionConfig) {
      public Table build() {
        return new HTable(ConnectionImplementation.this, this, ConnectionImplementation.this.rpcCallerFactory, ConnectionImplementation.this.rpcControllerFactory, pool);
      }
    };
  }

  public BufferedMutator getBufferedMutator(BufferedMutatorParams params) {
    if (params.getTableName() == null) {
      throw new IllegalArgumentException("TableName cannot be null.");
    } else {
      if (params.getPool() == null) {
        params.pool(HTable.getDefaultExecutor(this.getConfiguration()));
      }

      if (params.getWriteBufferSize() == -1L) {
        params.writeBufferSize(this.connectionConfig.getWriteBufferSize());
      }

      if (params.getWriteBufferPeriodicFlushTimeoutMs() == -1L) {
        params.setWriteBufferPeriodicFlushTimeoutMs(this.connectionConfig.getWriteBufferPeriodicFlushTimeoutMs());
      }

      if (params.getWriteBufferPeriodicFlushTimerTickMs() == -1L) {
        params.setWriteBufferPeriodicFlushTimerTickMs(this.connectionConfig.getWriteBufferPeriodicFlushTimerTickMs());
      }

      if (params.getMaxKeyValueSize() == -1) {
        params.maxKeyValueSize(this.connectionConfig.getMaxKeyValueSize());
      }

      String implementationClassName = params.getImplementationClassName();
      if (implementationClassName == null) {
        implementationClassName = this.alternateBufferedMutatorClassName;
      }

      if (implementationClassName == null) {
        return new BufferedMutatorImpl(this, this.rpcCallerFactory, this.rpcControllerFactory, params);
      } else {
        try {
          return (BufferedMutator)ReflectionUtils.newInstance(Class.forName(implementationClassName), new Object[]{this, this.rpcCallerFactory, this.rpcControllerFactory, params});
        } catch (ClassNotFoundException var4) {
          throw new RuntimeException(var4);
        }
      }
    }
  }

  public BufferedMutator getBufferedMutator(TableName tableName) {
    return this.getBufferedMutator(new BufferedMutatorParams(tableName));
  }

  public RegionLocator getRegionLocator(TableName tableName) throws IOException {
    return new HRegionLocator(tableName, this);
  }

  public Admin getAdmin() throws IOException {
    return new HBaseAdmin(this);
  }

  public Hbck getHbck() throws IOException {
    return this.getHbck((ServerName)get(this.registry.getActiveMaster()));
  }

  public Hbck getHbck(ServerName masterServer) throws IOException {
    this.checkClosed();
    if (this.isDeadServer(masterServer)) {
      throw new RegionServerStoppedException(masterServer + " is dead.");
    } else {
      String key = ConnectionUtils.getStubKey(BlockingInterface.class.getName(), masterServer, this.hostnamesCanChange);
      return new HBaseHbck((BlockingInterface)ConcurrentMapUtils.computeIfAbsentEx(this.stubs, key, () -> {
        BlockingRpcChannel channel = this.rpcClient.createBlockingRpcChannel(masterServer, this.user, this.rpcTimeout);
        return HbckService.newBlockingStub(channel);
      }), this.rpcControllerFactory);
    }
  }

  public MetricsConnection getConnectionMetrics() {
    return this.metrics;
  }

  private ThreadPoolExecutor getBatchPool() {
    if (this.batchPool == null) {
      synchronized(this) {
        if (this.batchPool == null) {
          int threads = this.conf.getInt("hbase.hconnection.threads.max", 256);
          this.batchPool = this.getThreadPool(threads, threads, "-shared", (BlockingQueue)null);
          this.cleanupPool = true;
        }
      }
    }

    return this.batchPool;
  }

  private ThreadPoolExecutor getThreadPool(int maxThreads, int coreThreads, String nameHint, BlockingQueue<Runnable> passedWorkQueue) {
    if (maxThreads == 0) {
      maxThreads = Runtime.getRuntime().availableProcessors() * 8;
    }

    if (coreThreads == 0) {
      coreThreads = Runtime.getRuntime().availableProcessors() * 8;
    }

    long keepAliveTime = this.conf.getLong("hbase.hconnection.threads.keepalivetime", 60L);
    BlockingQueue<Runnable> workQueue = passedWorkQueue;
    if (passedWorkQueue == null) {
      workQueue = new LinkedBlockingQueue(maxThreads * this.conf.getInt("hbase.client.max.total.tasks", 100));
      coreThreads = maxThreads;
    }

    ThreadPoolExecutor tpe = new ThreadPoolExecutor(coreThreads, maxThreads, keepAliveTime, TimeUnit.SECONDS, (BlockingQueue)workQueue, Threads.newDaemonThreadFactory(this.toString() + nameHint));
    tpe.allowCoreThreadTimeOut(true);
    return tpe;
  }

  private ThreadPoolExecutor getMetaLookupPool() {
    if (this.metaLookupPool == null) {
      synchronized(this) {
        if (this.metaLookupPool == null) {
          int threads = this.conf.getInt("hbase.hconnection.meta.lookup.threads.max", 128);
          this.metaLookupPool = this.getThreadPool(threads, threads, "-metaLookup-shared-", new LinkedBlockingQueue());
        }
      }
    }

    return this.metaLookupPool;
  }

  protected ExecutorService getCurrentMetaLookupPool() {
    return this.metaLookupPool;
  }

  protected ExecutorService getCurrentBatchPool() {
    return this.batchPool;
  }

  private void shutdownPools() {
    if (this.cleanupPool && this.batchPool != null && !this.batchPool.isShutdown()) {
      this.shutdownBatchPool(this.batchPool);
    }

    if (this.metaLookupPool != null && !this.metaLookupPool.isShutdown()) {
      this.shutdownBatchPool(this.metaLookupPool);
    }

  }

  private void shutdownBatchPool(ExecutorService pool) {
    pool.shutdown();

    try {
      if (!pool.awaitTermination(10L, TimeUnit.SECONDS)) {
        pool.shutdownNow();
      }
    } catch (InterruptedException var3) {
      pool.shutdownNow();
    }

  }

  @VisibleForTesting
  RpcClient getRpcClient() {
    return this.rpcClient;
  }

  public String toString() {
    return "hconnection-0x" + Integer.toHexString(this.hashCode());
  }

  protected void retrieveClusterId() {
    if (this.clusterId == null) {
      try {
        this.clusterId = (String)this.registry.getClusterId().get();
      } catch (ExecutionException | InterruptedException var2) {
        LOG.warn("Retrieve cluster id failed", var2);
      }

      if (this.clusterId == null) {
        this.clusterId = "default-cluster";
        LOG.debug("clusterid came back null, using default " + this.clusterId);
      }

    }
  }

  public Configuration getConfiguration() {
    return this.conf;
  }

  private void checkClosed() throws DoNotRetryIOException {
    if (this.closed) {
      throw new DoNotRetryIOException(this.toString() + " closed");
    }
  }

  /** @deprecated */
  @Deprecated
  public boolean isMasterRunning() throws MasterNotRunningException, ZooKeeperConnectionException {
    MasterKeepAliveConnection m;
    try {
      m = this.getKeepAliveMasterService();
    } catch (IOException var3) {
      throw new MasterNotRunningException(var3);
    }

    m.close();
    return true;
  }

  public HRegionLocation getRegionLocation(TableName tableName, byte[] row, boolean reload) throws IOException {
    return reload ? this.relocateRegion(tableName, row) : this.locateRegion(tableName, row);
  }

  public boolean isTableEnabled(TableName tableName) throws IOException {
    return this.getTableState(tableName).inStates(State.ENABLED);
  }

  public boolean isTableDisabled(TableName tableName) throws IOException {
    return this.getTableState(tableName).inStates(State.DISABLED);
  }

  public boolean isTableAvailable(TableName tableName, @Nullable byte[][] splitKeys) throws IOException {
    this.checkClosed();

    try {
      if (!this.isTableEnabled(tableName)) {
        LOG.debug("Table {} not enabled", tableName);
        return false;
      } else if (TableName.isMetaTableName(tableName)) {
        return true;
      } else {
        List<Pair<RegionInfo, ServerName>> locations = MetaTableAccessor.getTableRegionsAndLocations(this, tableName, true);
        int notDeployed = 0;
        int regionCount = 0;
        Iterator var6 = locations.iterator();

        while(true) {
          while(var6.hasNext()) {
            Pair<RegionInfo, ServerName> pair = (Pair)var6.next();
            RegionInfo info = (RegionInfo)pair.getFirst();
            if (pair.getSecond() == null) {
              LOG.debug("Table {} has not deployed region {}", tableName, ((RegionInfo)pair.getFirst()).getEncodedName());
              ++notDeployed;
            } else if (splitKeys != null && !Bytes.equals(info.getStartKey(), HConstants.EMPTY_BYTE_ARRAY)) {
              byte[][] var9 = splitKeys;
              int var10 = splitKeys.length;

              for(int var11 = 0; var11 < var10; ++var11) {
                byte[] splitKey = var9[var11];
                if (Bytes.equals(info.getStartKey(), splitKey)) {
                  ++regionCount;
                  break;
                }
              }
            } else {
              ++regionCount;
            }
          }

          if (notDeployed > 0) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Table {} has {} regions not deployed", tableName, notDeployed);
            }

            return false;
          }

          if (splitKeys != null && regionCount != splitKeys.length + 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Table {} expected to have {} regions, but only {} available", new Object[]{tableName, splitKeys.length + 1, regionCount});
            }

            return false;
          }

          LOG.trace("Table {} should be available", tableName);
          return true;
        }
      }
    } catch (TableNotFoundException var13) {
      LOG.warn("Table {} does not exist", tableName);
      return false;
    }
  }

  public HRegionLocation locateRegion(byte[] regionName) throws IOException {
    RegionLocations locations = this.locateRegion(RegionInfo.getTable(regionName), RegionInfo.getStartKey(regionName), false, true);
    return locations == null ? null : locations.getRegionLocation();
  }

  private boolean isDeadServer(ServerName sn) {
    return this.clusterStatusListener == null ? false : this.clusterStatusListener.isDeadServer(sn);
  }

  public List<HRegionLocation> locateRegions(TableName tableName) throws IOException {
    return this.locateRegions(tableName, false, true);
  }

  public List<HRegionLocation> locateRegions(TableName tableName, boolean useCache, boolean offlined) throws IOException {
    List regions;
    if (TableName.isMetaTableName(tableName)) {
      regions = Collections.singletonList(RegionInfoBuilder.FIRST_META_REGIONINFO);
    } else {
      regions = MetaTableAccessor.getTableRegions(this, tableName, !offlined);
    }

    List<HRegionLocation> locations = new ArrayList();
    Iterator var6 = regions.iterator();

    while(true) {
      RegionLocations list;
      do {
        RegionInfo regionInfo;
        do {
          if (!var6.hasNext()) {
            return locations;
          }

          regionInfo = (RegionInfo)var6.next();
        } while(!RegionReplicaUtil.isDefaultReplica(regionInfo));

        list = this.locateRegion(tableName, regionInfo.getStartKey(), useCache, true);
      } while(list == null);

      HRegionLocation[] var9 = list.getRegionLocations();
      int var10 = var9.length;

      for(int var11 = 0; var11 < var10; ++var11) {
        HRegionLocation loc = var9[var11];
        if (loc != null) {
          locations.add(loc);
        }
      }
    }
  }

  public HRegionLocation locateRegion(TableName tableName, byte[] row) throws IOException {
    RegionLocations locations = this.locateRegion(tableName, row, true, true);
    return locations == null ? null : locations.getRegionLocation();
  }

  public HRegionLocation relocateRegion(TableName tableName, byte[] row) throws IOException {
    RegionLocations locations = this.relocateRegion(tableName, row, 0);
    return locations == null ? null : locations.getRegionLocation(0);
  }

  public RegionLocations relocateRegion(TableName tableName, byte[] row, int replicaId) throws IOException {
    if (!tableName.equals(TableName.META_TABLE_NAME) && this.isTableDisabled(tableName)) {
      throw new TableNotEnabledException(tableName.getNameAsString() + " is disabled.");
    } else {
      return this.locateRegion(tableName, row, false, true, replicaId);
    }
  }

  public RegionLocations locateRegion(TableName tableName, byte[] row, boolean useCache, boolean retry) throws IOException {
    return this.locateRegion(tableName, row, useCache, retry, 0);
  }

  public RegionLocations locateRegion(TableName tableName, byte[] row, boolean useCache, boolean retry, int replicaId) throws IOException {
    this.checkClosed();
    if (tableName != null && tableName.getName().length != 0) {
      return tableName.equals(TableName.META_TABLE_NAME) ? this.locateMeta(tableName, useCache, replicaId) : this.locateRegionInMeta(tableName, row, useCache, retry, replicaId);
    } else {
      throw new IllegalArgumentException("table name cannot be null or zero length");
    }
  }

  private RegionLocations locateMeta(TableName tableName, boolean useCache, int replicaId) throws IOException {
    byte[] metaCacheKey = HConstants.EMPTY_START_ROW;
    RegionLocations locations = null;
    if (useCache) {
      locations = this.getCachedLocation(tableName, metaCacheKey);
      if (locations != null && locations.getRegionLocation(replicaId) != null) {
        return locations;
      }
    }

    synchronized(this.metaRegionLock) {
      if (useCache) {
        locations = this.getCachedLocation(tableName, metaCacheKey);
        if (locations != null && locations.getRegionLocation(replicaId) != null) {
          return locations;
        }
      }

      locations = (RegionLocations)get(this.registry.getMetaRegionLocations());
      if (locations != null) {
        this.cacheLocation(tableName, locations);
      }

      return locations;
    }
  }

  private RegionLocations locateRegionInMeta(TableName tableName, byte[] row, boolean useCache, boolean retry, int replicaId) throws IOException {
    if (useCache) {
      RegionLocations locations = this.getCachedLocation(tableName, row);
      if (locations != null && locations.getRegionLocation(replicaId) != null) {
        return locations;
      }
    }

    byte[] metaStartKey = RegionInfo.createRegionName(tableName, row, "99999999999999", false);
    byte[] metaStopKey = RegionInfo.createRegionName(tableName, HConstants.EMPTY_START_ROW, "", false);
    Scan s = (new Scan()).withStartRow(metaStartKey).withStopRow(metaStopKey, true).addFamily(HConstants.CATALOG_FAMILY).setReversed(true).setCaching(5).setReadType(ReadType.PREAD);
    if (this.useMetaReplicas) {
      s.setConsistency(Consistency.TIMELINE);
    }

    int maxAttempts = retry ? this.numTries : 1;
    boolean relocateMeta = false;

    int tries;
    for(tries = 0; tries < maxAttempts; ++tries) {
      if (useCache) {
        RegionLocations locations = this.getCachedLocation(tableName, row);
        if (locations != null && locations.getRegionLocation(replicaId) != null) {
          return locations;
        }
      } else {
        this.metaCache.clearCache(tableName, row, replicaId);
      }

      long pauseBase = this.pause;
      this.userRegionLock.lock();

      try {
        if (useCache) {
          RegionLocations locations = this.getCachedLocation(tableName, row);
          if (locations != null && locations.getRegionLocation(replicaId) != null) {
            RegionLocations var53 = locations;
            return var53;
          }
        }

        if (relocateMeta) {
          this.relocateRegion(TableName.META_TABLE_NAME, HConstants.EMPTY_START_ROW, 0);
        }

        s.resetMvccReadPoint();
        ReversedClientScanner rcs = new ReversedClientScanner(this.conf, s, TableName.META_TABLE_NAME, this, this.rpcCallerFactory, this.rpcControllerFactory, this.getMetaLookupPool(), this.metaReplicaCallTimeoutScanInMicroSecond);
        Throwable var15 = null;

        try {
          boolean tableNotFound = true;

          RegionLocations locations;
          RegionInfo regionInfo;
          do {
            Result regionInfoRow = rcs.next();
            if (regionInfoRow == null) {
              if (tableNotFound) {
                throw new TableNotFoundException(tableName);
              }

              throw new IOException("Unable to find region for " + Bytes.toStringBinary(row) + " in " + tableName);
            }

            tableNotFound = false;
            locations = MetaTableAccessor.getRegionLocations(regionInfoRow);
            if (locations == null || locations.getRegionLocation(replicaId) == null) {
              throw new IOException("RegionInfo null in " + tableName + ", row=" + regionInfoRow);
            }

            regionInfo = locations.getRegionLocation(replicaId).getRegion();
            if (regionInfo == null) {
              throw new IOException("RegionInfo null or empty in " + TableName.META_TABLE_NAME + ", row=" + regionInfoRow);
            }
          } while(regionInfo.isSplitParent());

          if (regionInfo.isOffline()) {
            throw new RegionOfflineException("Region offline; disable table call? " + regionInfo.getRegionNameAsString());
          }

          if (!regionInfo.containsRow(row)) {
            throw new IOException("Unable to find region for " + Bytes.toStringBinary(row) + " in " + tableName);
          }

          ServerName serverName = locations.getRegionLocation(replicaId).getServerName();
          if (serverName == null) {
            throw new NoServerForRegionException("No server address listed in " + TableName.META_TABLE_NAME + " for region " + regionInfo.getRegionNameAsString() + " containing row " + Bytes.toStringBinary(row));
          }

          if (this.isDeadServer(serverName)) {
            throw new RegionServerStoppedException("hbase:meta says the region " + regionInfo.getRegionNameAsString() + " is managed by the server " + serverName + ", but it is dead.");
          }

          this.cacheLocation(tableName, locations);
          RegionLocations var21 = locations;
          return var21;
        } catch (Throwable var44) {
          var15 = var44;
          throw var44;
        } finally {
          if (rcs != null) {
            if (var15 != null) {
              try {
                rcs.close();
              } catch (Throwable var43) {
                var15.addSuppressed(var43);
              }
            } else {
              rcs.close();
            }
          }

        }
      } catch (TableNotFoundException var46) {
        throw var46;
      } catch (IOException var47) {
        IOException e = var47;
        ExceptionUtil.rethrowIfInterrupt(var47);
        if (var47 instanceof RemoteException) {
          e = ((RemoteException)var47).unwrapRemoteException();
        }

        if (e instanceof CallQueueTooBigException) {
          pauseBase = this.pauseForCQTBE;
        }

        if (tries >= maxAttempts - 1) {
          throw e;
        }

        LOG.debug("locateRegionInMeta parentTable='{}', attempt={} of {} failed; retrying after sleep of {}", new Object[]{TableName.META_TABLE_NAME, tries, maxAttempts, maxAttempts, e});
        relocateMeta = !(e instanceof RegionOfflineException) && !(e instanceof NoServerForRegionException);
      } finally {
        this.userRegionLock.unlock();
      }

      try {
        Thread.sleep(ConnectionUtils.getPauseTime(pauseBase, tries));
      } catch (InterruptedException var42) {
        throw new InterruptedIOException("Giving up trying to location region in meta: thread is interrupted.");
      }
    }

    throw new NoServerForRegionException("Unable to find region for " + Bytes.toStringBinary(row) + " in " + tableName + " after " + tries + " tries.");
  }

  public void cacheLocation(TableName tableName, RegionLocations location) {
    this.metaCache.cacheLocation(tableName, location);
  }

  RegionLocations getCachedLocation(TableName tableName, byte[] row) {
    return this.metaCache.getCachedLocation(tableName, row);
  }

  public void clearRegionCache(TableName tableName, byte[] row) {
    this.metaCache.clearCache(tableName, row);
  }

  public void clearCaches(ServerName serverName) {
    this.metaCache.clearCache(serverName);
  }

  public void clearRegionLocationCache() {
    this.metaCache.clearCache();
  }

  public void clearRegionCache(TableName tableName) {
    this.metaCache.clearCache(tableName);
  }

  private void cacheLocation(TableName tableName, ServerName source, HRegionLocation location) {
    this.metaCache.cacheLocation(tableName, source, location);
  }

  public org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.BlockingInterface getAdminForMaster() throws IOException {
    return this.getAdmin((ServerName)get(this.registry.getActiveMaster()));
  }

  public org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.BlockingInterface getAdmin(ServerName serverName) throws IOException {
    this.checkClosed();
    if (this.isDeadServer(serverName)) {
      throw new RegionServerStoppedException(serverName + " is dead.");
    } else {
      String key = ConnectionUtils.getStubKey(org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.BlockingInterface.class.getName(), serverName, this.hostnamesCanChange);
      return (org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.BlockingInterface)ConcurrentMapUtils.computeIfAbsentEx(this.stubs, key, () -> {
        BlockingRpcChannel channel = this.rpcClient.createBlockingRpcChannel(serverName, this.user, this.rpcTimeout);
        return AdminService.newBlockingStub(channel);
      });
    }
  }

  public org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService.BlockingInterface getClient(ServerName serverName) throws IOException {
    this.checkClosed();
    if (this.isDeadServer(serverName)) {
      throw new RegionServerStoppedException(serverName + " is dead.");
    } else {
      String key = ConnectionUtils.getStubKey(org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService.BlockingInterface.class.getName(), serverName, this.hostnamesCanChange);
      return (org.apache.hadoop.hbase.shaded.protobuf.generated.ClientProtos.ClientService.BlockingInterface)ConcurrentMapUtils.computeIfAbsentEx(this.stubs, key, () -> {
        BlockingRpcChannel channel = this.rpcClient.createBlockingRpcChannel(serverName, this.user, this.rpcTimeout);
        return ClientService.newBlockingStub(channel);
      });
    }
  }

  public MasterKeepAliveConnection getMaster() throws IOException {
    return this.getKeepAliveMasterService();
  }

  private void resetMasterServiceState(ConnectionImplementation.MasterServiceState mss) {
    ++mss.userCount;
  }

  private MasterKeepAliveConnection getKeepAliveMasterService() throws IOException {
    synchronized(this.masterLock) {
      if (!this.isKeepAliveMasterConnectedAndRunning(this.masterServiceState)) {
        ConnectionImplementation.MasterServiceStubMaker stubMaker = new ConnectionImplementation.MasterServiceStubMaker();
        this.masterServiceState.stub = stubMaker.makeStub();
      }

      this.resetMasterServiceState(this.masterServiceState);
    }

    final org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService.BlockingInterface stub = this.masterServiceState.stub;
    return new MasterKeepAliveConnection() {
      ConnectionImplementation.MasterServiceState mss;

      {
        this.mss = ConnectionImplementation.this.masterServiceState;
      }

      public AbortProcedureResponse abortProcedure(RpcController controller, AbortProcedureRequest request) throws ServiceException {
        return stub.abortProcedure(controller, request);
      }

      public GetProceduresResponse getProcedures(RpcController controller, GetProceduresRequest request) throws ServiceException {
        return stub.getProcedures(controller, request);
      }

      public GetLocksResponse getLocks(RpcController controller, GetLocksRequest request) throws ServiceException {
        return stub.getLocks(controller, request);
      }

      public AddColumnResponse addColumn(RpcController controller, AddColumnRequest request) throws ServiceException {
        return stub.addColumn(controller, request);
      }

      public DeleteColumnResponse deleteColumn(RpcController controller, DeleteColumnRequest request) throws ServiceException {
        return stub.deleteColumn(controller, request);
      }

      public ModifyColumnResponse modifyColumn(RpcController controller, ModifyColumnRequest request) throws ServiceException {
        return stub.modifyColumn(controller, request);
      }

      public MoveRegionResponse moveRegion(RpcController controller, MoveRegionRequest request) throws ServiceException {
        return stub.moveRegion(controller, request);
      }

      public MergeTableRegionsResponse mergeTableRegions(RpcController controller, MergeTableRegionsRequest request) throws ServiceException {
        return stub.mergeTableRegions(controller, request);
      }

      public AssignRegionResponse assignRegion(RpcController controller, AssignRegionRequest request) throws ServiceException {
        return stub.assignRegion(controller, request);
      }

      public UnassignRegionResponse unassignRegion(RpcController controller, UnassignRegionRequest request) throws ServiceException {
        return stub.unassignRegion(controller, request);
      }

      public OfflineRegionResponse offlineRegion(RpcController controller, OfflineRegionRequest request) throws ServiceException {
        return stub.offlineRegion(controller, request);
      }

      public SplitTableRegionResponse splitRegion(RpcController controller, SplitTableRegionRequest request) throws ServiceException {
        return stub.splitRegion(controller, request);
      }

      public DeleteTableResponse deleteTable(RpcController controller, DeleteTableRequest request) throws ServiceException {
        return stub.deleteTable(controller, request);
      }

      public TruncateTableResponse truncateTable(RpcController controller, TruncateTableRequest request) throws ServiceException {
        return stub.truncateTable(controller, request);
      }

      public EnableTableResponse enableTable(RpcController controller, EnableTableRequest request) throws ServiceException {
        return stub.enableTable(controller, request);
      }

      public DisableTableResponse disableTable(RpcController controller, DisableTableRequest request) throws ServiceException {
        return stub.disableTable(controller, request);
      }

      public ModifyTableResponse modifyTable(RpcController controller, ModifyTableRequest request) throws ServiceException {
        return stub.modifyTable(controller, request);
      }

      public CreateTableResponse createTable(RpcController controller, CreateTableRequest request) throws ServiceException {
        return stub.createTable(controller, request);
      }

      public ShutdownResponse shutdown(RpcController controller, ShutdownRequest request) throws ServiceException {
        return stub.shutdown(controller, request);
      }

      public StopMasterResponse stopMaster(RpcController controller, StopMasterRequest request) throws ServiceException {
        return stub.stopMaster(controller, request);
      }

      public IsInMaintenanceModeResponse isMasterInMaintenanceMode(RpcController controller, IsInMaintenanceModeRequest request) throws ServiceException {
        return stub.isMasterInMaintenanceMode(controller, request);
      }

      public BalanceResponse balance(RpcController controller, BalanceRequest request) throws ServiceException {
        return stub.balance(controller, request);
      }

      public SetBalancerRunningResponse setBalancerRunning(RpcController controller, SetBalancerRunningRequest request) throws ServiceException {
        return stub.setBalancerRunning(controller, request);
      }

      public NormalizeResponse normalize(RpcController controller, NormalizeRequest request) throws ServiceException {
        return stub.normalize(controller, request);
      }

      public SetNormalizerRunningResponse setNormalizerRunning(RpcController controller, SetNormalizerRunningRequest request) throws ServiceException {
        return stub.setNormalizerRunning(controller, request);
      }

      public RunCatalogScanResponse runCatalogScan(RpcController controller, RunCatalogScanRequest request) throws ServiceException {
        return stub.runCatalogScan(controller, request);
      }

      public EnableCatalogJanitorResponse enableCatalogJanitor(RpcController controller, EnableCatalogJanitorRequest request) throws ServiceException {
        return stub.enableCatalogJanitor(controller, request);
      }

      public IsCatalogJanitorEnabledResponse isCatalogJanitorEnabled(RpcController controller, IsCatalogJanitorEnabledRequest request) throws ServiceException {
        return stub.isCatalogJanitorEnabled(controller, request);
      }

      public RunCleanerChoreResponse runCleanerChore(RpcController controller, RunCleanerChoreRequest request) throws ServiceException {
        return stub.runCleanerChore(controller, request);
      }

      public SetCleanerChoreRunningResponse setCleanerChoreRunning(RpcController controller, SetCleanerChoreRunningRequest request) throws ServiceException {
        return stub.setCleanerChoreRunning(controller, request);
      }

      public IsCleanerChoreEnabledResponse isCleanerChoreEnabled(RpcController controller, IsCleanerChoreEnabledRequest request) throws ServiceException {
        return stub.isCleanerChoreEnabled(controller, request);
      }

      public CoprocessorServiceResponse execMasterService(RpcController controller, CoprocessorServiceRequest request) throws ServiceException {
        return stub.execMasterService(controller, request);
      }

      public SnapshotResponse snapshot(RpcController controller, SnapshotRequest request) throws ServiceException {
        return stub.snapshot(controller, request);
      }

      public GetCompletedSnapshotsResponse getCompletedSnapshots(RpcController controller, GetCompletedSnapshotsRequest request) throws ServiceException {
        return stub.getCompletedSnapshots(controller, request);
      }

      public DeleteSnapshotResponse deleteSnapshot(RpcController controller, DeleteSnapshotRequest request) throws ServiceException {
        return stub.deleteSnapshot(controller, request);
      }

      public IsSnapshotDoneResponse isSnapshotDone(RpcController controller, IsSnapshotDoneRequest request) throws ServiceException {
        return stub.isSnapshotDone(controller, request);
      }

      public RestoreSnapshotResponse restoreSnapshot(RpcController controller, RestoreSnapshotRequest request) throws ServiceException {
        return stub.restoreSnapshot(controller, request);
      }

      public SetSnapshotCleanupResponse switchSnapshotCleanup(RpcController controller, SetSnapshotCleanupRequest request) throws ServiceException {
        return stub.switchSnapshotCleanup(controller, request);
      }

      public IsSnapshotCleanupEnabledResponse isSnapshotCleanupEnabled(RpcController controller, IsSnapshotCleanupEnabledRequest request) throws ServiceException {
        return stub.isSnapshotCleanupEnabled(controller, request);
      }

      public ExecProcedureResponse execProcedure(RpcController controller, ExecProcedureRequest request) throws ServiceException {
        return stub.execProcedure(controller, request);
      }

      public ExecProcedureResponse execProcedureWithRet(RpcController controller, ExecProcedureRequest request) throws ServiceException {
        return stub.execProcedureWithRet(controller, request);
      }

      public IsProcedureDoneResponse isProcedureDone(RpcController controller, IsProcedureDoneRequest request) throws ServiceException {
        return stub.isProcedureDone(controller, request);
      }

      public GetProcedureResultResponse getProcedureResult(RpcController controller, GetProcedureResultRequest request) throws ServiceException {
        return stub.getProcedureResult(controller, request);
      }

      public IsMasterRunningResponse isMasterRunning(RpcController controller, IsMasterRunningRequest request) throws ServiceException {
        return stub.isMasterRunning(controller, request);
      }

      public ModifyNamespaceResponse modifyNamespace(RpcController controller, ModifyNamespaceRequest request) throws ServiceException {
        return stub.modifyNamespace(controller, request);
      }

      public CreateNamespaceResponse createNamespace(RpcController controller, CreateNamespaceRequest request) throws ServiceException {
        return stub.createNamespace(controller, request);
      }

      public DeleteNamespaceResponse deleteNamespace(RpcController controller, DeleteNamespaceRequest request) throws ServiceException {
        return stub.deleteNamespace(controller, request);
      }

      public ListNamespacesResponse listNamespaces(RpcController controller, ListNamespacesRequest request) throws ServiceException {
        return stub.listNamespaces(controller, request);
      }

      public GetNamespaceDescriptorResponse getNamespaceDescriptor(RpcController controller, GetNamespaceDescriptorRequest request) throws ServiceException {
        return stub.getNamespaceDescriptor(controller, request);
      }

      public ListNamespaceDescriptorsResponse listNamespaceDescriptors(RpcController controller, ListNamespaceDescriptorsRequest request) throws ServiceException {
        return stub.listNamespaceDescriptors(controller, request);
      }

      public ListTableDescriptorsByNamespaceResponse listTableDescriptorsByNamespace(RpcController controller, ListTableDescriptorsByNamespaceRequest request) throws ServiceException {
        return stub.listTableDescriptorsByNamespace(controller, request);
      }

      public ListTableNamesByNamespaceResponse listTableNamesByNamespace(RpcController controller, ListTableNamesByNamespaceRequest request) throws ServiceException {
        return stub.listTableNamesByNamespace(controller, request);
      }

      public GetTableStateResponse getTableState(RpcController controller, GetTableStateRequest request) throws ServiceException {
        return stub.getTableState(controller, request);
      }

      public void close() {
        ConnectionImplementation.release(this.mss);
      }

      public GetSchemaAlterStatusResponse getSchemaAlterStatus(RpcController controller, GetSchemaAlterStatusRequest request) throws ServiceException {
        return stub.getSchemaAlterStatus(controller, request);
      }

      public GetTableDescriptorsResponse getTableDescriptors(RpcController controller, GetTableDescriptorsRequest request) throws ServiceException {
        return stub.getTableDescriptors(controller, request);
      }

      public GetTableNamesResponse getTableNames(RpcController controller, GetTableNamesRequest request) throws ServiceException {
        return stub.getTableNames(controller, request);
      }

      public GetClusterStatusResponse getClusterStatus(RpcController controller, GetClusterStatusRequest request) throws ServiceException {
        return stub.getClusterStatus(controller, request);
      }

      public SetQuotaResponse setQuota(RpcController controller, SetQuotaRequest request) throws ServiceException {
        return stub.setQuota(controller, request);
      }

      public MajorCompactionTimestampResponse getLastMajorCompactionTimestamp(RpcController controller, MajorCompactionTimestampRequest request) throws ServiceException {
        return stub.getLastMajorCompactionTimestamp(controller, request);
      }

      public MajorCompactionTimestampResponse getLastMajorCompactionTimestampForRegion(RpcController controller, MajorCompactionTimestampForRegionRequest request) throws ServiceException {
        return stub.getLastMajorCompactionTimestampForRegion(controller, request);
      }

      public IsBalancerEnabledResponse isBalancerEnabled(RpcController controller, IsBalancerEnabledRequest request) throws ServiceException {
        return stub.isBalancerEnabled(controller, request);
      }

      public SetSplitOrMergeEnabledResponse setSplitOrMergeEnabled(RpcController controller, SetSplitOrMergeEnabledRequest request) throws ServiceException {
        return stub.setSplitOrMergeEnabled(controller, request);
      }

      public IsSplitOrMergeEnabledResponse isSplitOrMergeEnabled(RpcController controller, IsSplitOrMergeEnabledRequest request) throws ServiceException {
        return stub.isSplitOrMergeEnabled(controller, request);
      }

      public IsNormalizerEnabledResponse isNormalizerEnabled(RpcController controller, IsNormalizerEnabledRequest request) throws ServiceException {
        return stub.isNormalizerEnabled(controller, request);
      }

      public SecurityCapabilitiesResponse getSecurityCapabilities(RpcController controller, SecurityCapabilitiesRequest request) throws ServiceException {
        return stub.getSecurityCapabilities(controller, request);
      }

      public AddReplicationPeerResponse addReplicationPeer(RpcController controller, AddReplicationPeerRequest request) throws ServiceException {
        return stub.addReplicationPeer(controller, request);
      }

      public RemoveReplicationPeerResponse removeReplicationPeer(RpcController controller, RemoveReplicationPeerRequest request) throws ServiceException {
        return stub.removeReplicationPeer(controller, request);
      }

      public EnableReplicationPeerResponse enableReplicationPeer(RpcController controller, EnableReplicationPeerRequest request) throws ServiceException {
        return stub.enableReplicationPeer(controller, request);
      }

      public DisableReplicationPeerResponse disableReplicationPeer(RpcController controller, DisableReplicationPeerRequest request) throws ServiceException {
        return stub.disableReplicationPeer(controller, request);
      }

      public ListDecommissionedRegionServersResponse listDecommissionedRegionServers(RpcController controller, ListDecommissionedRegionServersRequest request) throws ServiceException {
        return stub.listDecommissionedRegionServers(controller, request);
      }

      public DecommissionRegionServersResponse decommissionRegionServers(RpcController controller, DecommissionRegionServersRequest request) throws ServiceException {
        return stub.decommissionRegionServers(controller, request);
      }

      public RecommissionRegionServerResponse recommissionRegionServer(RpcController controller, RecommissionRegionServerRequest request) throws ServiceException {
        return stub.recommissionRegionServer(controller, request);
      }

      public GetReplicationPeerConfigResponse getReplicationPeerConfig(RpcController controller, GetReplicationPeerConfigRequest request) throws ServiceException {
        return stub.getReplicationPeerConfig(controller, request);
      }

      public UpdateReplicationPeerConfigResponse updateReplicationPeerConfig(RpcController controller, UpdateReplicationPeerConfigRequest request) throws ServiceException {
        return stub.updateReplicationPeerConfig(controller, request);
      }

      public ListReplicationPeersResponse listReplicationPeers(RpcController controller, ListReplicationPeersRequest request) throws ServiceException {
        return stub.listReplicationPeers(controller, request);
      }

      public GetSpaceQuotaRegionSizesResponse getSpaceQuotaRegionSizes(RpcController controller, GetSpaceQuotaRegionSizesRequest request) throws ServiceException {
        return stub.getSpaceQuotaRegionSizes(controller, request);
      }

      public GetQuotaStatesResponse getQuotaStates(RpcController controller, GetQuotaStatesRequest request) throws ServiceException {
        return stub.getQuotaStates(controller, request);
      }

      public ClearDeadServersResponse clearDeadServers(RpcController controller, ClearDeadServersRequest request) throws ServiceException {
        return stub.clearDeadServers(controller, request);
      }

      public SwitchRpcThrottleResponse switchRpcThrottle(RpcController controller, SwitchRpcThrottleRequest request) throws ServiceException {
        return stub.switchRpcThrottle(controller, request);
      }

      public IsRpcThrottleEnabledResponse isRpcThrottleEnabled(RpcController controller, IsRpcThrottleEnabledRequest request) throws ServiceException {
        return stub.isRpcThrottleEnabled(controller, request);
      }

      public SwitchExceedThrottleQuotaResponse switchExceedThrottleQuota(RpcController controller, SwitchExceedThrottleQuotaRequest request) throws ServiceException {
        return stub.switchExceedThrottleQuota(controller, request);
      }

      public GrantResponse grant(RpcController controller, GrantRequest request) throws ServiceException {
        return stub.grant(controller, request);
      }

      public RevokeResponse revoke(RpcController controller, RevokeRequest request) throws ServiceException {
        return stub.revoke(controller, request);
      }

      public GetUserPermissionsResponse getUserPermissions(RpcController controller, GetUserPermissionsRequest request) throws ServiceException {
        return stub.getUserPermissions(controller, request);
      }

      public HasUserPermissionsResponse hasUserPermissions(RpcController controller, HasUserPermissionsRequest request) throws ServiceException {
        return stub.hasUserPermissions(controller, request);
      }
    };
  }

  private static void release(ConnectionImplementation.MasterServiceState mss) {
    if (mss != null && mss.connection != null) {
      ((ConnectionImplementation)mss.connection).releaseMaster(mss);
    }

  }

  private boolean isKeepAliveMasterConnectedAndRunning(ConnectionImplementation.MasterServiceState mss) {
    if (mss.getStub() == null) {
      return false;
    } else {
      try {
        return mss.isMasterRunning();
      } catch (UndeclaredThrowableException var3) {
        LOG.info("Master connection is not running anymore", var3.getUndeclaredThrowable());
        return false;
      } catch (IOException var4) {
        LOG.warn("Checking master connection", var4);
        return false;
      }
    }
  }

  void releaseMaster(ConnectionImplementation.MasterServiceState mss) {
    if (mss.getStub() != null) {
      synchronized(this.masterLock) {
        --mss.userCount;
      }
    }
  }

  private void closeMasterService(ConnectionImplementation.MasterServiceState mss) {
    if (mss.getStub() != null) {
      LOG.info("Closing master protocol: " + mss);
      mss.clearStub();
    }

    mss.userCount = 0;
  }

  private void closeMaster() {
    synchronized(this.masterLock) {
      this.closeMasterService(this.masterServiceState);
    }
  }

  void updateCachedLocation(RegionInfo hri, ServerName source, ServerName serverName, long seqNum) {
    HRegionLocation newHrl = new HRegionLocation(hri, serverName, seqNum);
    this.cacheLocation(hri.getTable(), source, newHrl);
  }

  public void deleteCachedRegionLocation(HRegionLocation location) {
    this.metaCache.clearCache(location);
  }

  public void updateCachedLocations(TableName tableName, byte[] regionName, byte[] rowkey, Object exception, ServerName source) {
    if (rowkey != null && tableName != null) {
      if (source != null) {
        if (regionName == null) {
          if (this.metrics != null) {
            this.metrics.incrCacheDroppingExceptions(exception);
          }

          this.metaCache.clearCache(tableName, rowkey, source);
        } else {
          RegionLocations oldLocations = this.getCachedLocation(tableName, rowkey);
          HRegionLocation oldLocation = null;
          if (oldLocations != null) {
            oldLocation = oldLocations.getRegionLocationByRegionName(regionName);
          }

          if (oldLocation != null && source.equals(oldLocation.getServerName())) {
            RegionInfo regionInfo = oldLocation.getRegion();
            Throwable cause = ClientExceptionsUtil.findException(exception);
            if (cause != null) {
              if (!ClientExceptionsUtil.isMetaClearingException(cause)) {
                return;
              }

              if (cause instanceof RegionMovedException) {
                RegionMovedException rme = (RegionMovedException)cause;
                if (LOG.isTraceEnabled()) {
                  LOG.trace("Region " + regionInfo.getRegionNameAsString() + " moved to " + rme.getHostname() + ":" + rme.getPort() + " according to " + source.getAddress());
                }

                this.updateCachedLocation(regionInfo, source, rme.getServerName(), rme.getLocationSeqNum());
                return;
              }
            }

            if (this.metrics != null) {
              this.metrics.incrCacheDroppingExceptions(exception);
            }

            this.metaCache.clearCache(regionInfo);
          }
        }
      }
    } else {
      LOG.warn("Coding error, see method javadoc. row=" + (rowkey == null ? "null" : rowkey) + ", tableName=" + (tableName == null ? "null" : tableName));
    }
  }

  public AsyncProcess getAsyncProcess() {
    return this.asyncProcess;
  }

  public ServerStatisticTracker getStatisticsTracker() {
    return this.stats;
  }

  public ClientBackoffPolicy getBackoffPolicy() {
    return this.backoffPolicy;
  }

  @VisibleForTesting
  int getNumberOfCachedRegionLocations(TableName tableName) {
    return this.metaCache.getNumberOfCachedRegionLocations(tableName);
  }

  public void abort(String msg, Throwable t) {
    if (t != null) {
      LOG.error(HBaseMarkers.FATAL, msg, t);
    } else {
      LOG.error(HBaseMarkers.FATAL, msg);
    }

    this.aborted = true;
    this.close();
    this.closed = true;
  }

  public boolean isClosed() {
    return this.closed;
  }

  public boolean isAborted() {
    return this.aborted;
  }

  public void close() {
    if (!this.closed) {
      this.closeMaster();
      this.shutdownPools();
      if (this.metrics != null) {
        this.metrics.shutdown();
      }

      this.closed = true;
      this.registry.close();
      this.stubs.clear();
      if (this.clusterStatusListener != null) {
        this.clusterStatusListener.close();
      }

      if (this.rpcClient != null) {
        this.rpcClient.close();
      }

      if (this.authService != null) {
        this.authService.shutdown();
      }

    }
  }

  protected void finalize() throws Throwable {
    super.finalize();
    this.close();
  }

  public NonceGenerator getNonceGenerator() {
    return nonceGenerator;
  }

  public TableState getTableState(TableName tableName) throws IOException {
    this.checkClosed();
    TableState tableState = MetaTableAccessor.getTableState(this, tableName);
    if (tableState == null) {
      throw new TableNotFoundException(tableName);
    } else {
      return tableState;
    }
  }

  public RpcRetryingCallerFactory getNewRpcRetryingCallerFactory(Configuration conf) {
    return RpcRetryingCallerFactory.instantiate(conf, this.interceptor, this.getStatisticsTracker());
  }

  public boolean hasCellBlockSupport() {
    return this.rpcClient.hasCellBlockSupport();
  }

  public ConnectionConfiguration getConnectionConfiguration() {
    return this.connectionConfig;
  }

  public RpcRetryingCallerFactory getRpcRetryingCallerFactory() {
    return this.rpcCallerFactory;
  }

  public RpcControllerFactory getRpcControllerFactory() {
    return this.rpcControllerFactory;
  }

  private static <T> T get(CompletableFuture<T> future) throws IOException {
    try {
      return future.get();
    } catch (InterruptedException var3) {
      Thread.currentThread().interrupt();
      throw (IOException)(new InterruptedIOException()).initCause(var3);
    } catch (ExecutionException var4) {
      Throwable cause = var4.getCause();
      Throwables.propagateIfPossible(cause, IOException.class);
      throw new IOException(cause);
    }
  }

  private final class MasterServiceStubMaker {
    private MasterServiceStubMaker() {
    }

    private void isMasterRunning(org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService.BlockingInterface stub) throws IOException {
      try {
        stub.isMasterRunning((RpcController)null, RequestConverter.buildIsMasterRunningRequest());
      } catch (ServiceException var3) {
        throw ProtobufUtil.handleRemoteException(var3);
      }
    }

    private org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService.BlockingInterface makeStubNoRetries() throws IOException, KeeperException {
      ServerName sn = (ServerName)ConnectionImplementation.get(ConnectionImplementation.this.registry.getActiveMaster());
      String key;
      if (sn == null) {
        key = "ZooKeeper available but no active master location found";
        ConnectionImplementation.LOG.info(key);
        throw new MasterNotRunningException(key);
      } else if (ConnectionImplementation.this.isDeadServer(sn)) {
        throw new MasterNotRunningException(sn + " is dead.");
      } else {
        key = ConnectionUtils.getStubKey(MasterService.getDescriptor().getName(), sn, ConnectionImplementation.this.hostnamesCanChange);
        org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService.BlockingInterface stub = (org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService.BlockingInterface)ConcurrentMapUtils.computeIfAbsentEx(ConnectionImplementation.this.stubs, key, () -> {
          BlockingRpcChannel channel = ConnectionImplementation.this.rpcClient.createBlockingRpcChannel(sn, ConnectionImplementation.this.user, ConnectionImplementation.this.rpcTimeout);
          return MasterService.newBlockingStub(channel);
        });
        this.isMasterRunning(stub);
        return stub;
      }
    }

    org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService.BlockingInterface makeStub() throws IOException {
      synchronized(ConnectionImplementation.this.masterLock) {
        Exception exceptionCaught = null;
        if (!ConnectionImplementation.this.closed) {
          try {
            org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService.BlockingInterface var10000 = this.makeStubNoRetries();
            return var10000;
          } catch (IOException var5) {
            exceptionCaught = var5;
          } catch (KeeperException var6) {
            exceptionCaught = var6;
          }

          throw new MasterNotRunningException((Exception)exceptionCaught);
        } else {
          throw new DoNotRetryIOException("Connection was closed while trying to get master");
        }
      }
    }
  }

  static class ServerErrorTracker {
    private final ConcurrentMap<ServerName, ConnectionImplementation.ServerErrorTracker.ServerErrors> errorsByServer = new ConcurrentHashMap();
    private final long canRetryUntil;
    private final int maxTries;
    private final long startTrackingTime;

    public ServerErrorTracker(long timeout, int maxTries) {
      this.maxTries = maxTries;
      this.canRetryUntil = EnvironmentEdgeManager.currentTime() + timeout;
      this.startTrackingTime = (new Date()).getTime();
    }

    boolean canTryMore(int numAttempt) {
      return numAttempt < this.maxTries || this.maxTries > 1 && EnvironmentEdgeManager.currentTime() < this.canRetryUntil;
    }

    long calculateBackoffTime(ServerName server, long basePause) {
      ConnectionImplementation.ServerErrorTracker.ServerErrors errorStats = (ConnectionImplementation.ServerErrorTracker.ServerErrors)this.errorsByServer.get(server);
      long result;
      if (errorStats != null) {
        result = ConnectionUtils.getPauseTime(basePause, Math.max(0, errorStats.getCount() - 1));
      } else {
        result = 0L;
      }

      return result;
    }

    void reportServerError(ServerName server) {
      ((ConnectionImplementation.ServerErrorTracker.ServerErrors)ConcurrentMapUtils.computeIfAbsent(this.errorsByServer, server, () -> {
        return new ConnectionImplementation.ServerErrorTracker.ServerErrors();
      })).addError();
    }

    long getStartTrackingTime() {
      return this.startTrackingTime;
    }

    private static class ServerErrors {
      private final AtomicInteger retries;

      private ServerErrors() {
        this.retries = new AtomicInteger(0);
      }

      public int getCount() {
        return this.retries.get();
      }

      public void addError() {
        this.retries.incrementAndGet();
      }
    }
  }

  static class MasterServiceState {
    Connection connection;
    org.apache.hadoop.hbase.shaded.protobuf.generated.MasterProtos.MasterService.BlockingInterface stub;
    int userCount;

    MasterServiceState(Connection connection) {
      this.connection = connection;
    }

    public String toString() {
      return "MasterService";
    }

    Object getStub() {
      return this.stub;
    }

    void clearStub() {
      this.stub = null;
    }

    boolean isMasterRunning() throws IOException {
      IsMasterRunningResponse response;

      try {
        response = this.stub.isMasterRunning(null, RequestConverter.buildIsMasterRunningRequest());
      } catch (Exception var3) {
        throw ProtobufUtil.handleRemoteException(var3);
      }

      return response != null && response.getIsMasterRunning();
    }
  }
}
