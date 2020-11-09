package org.apache.hadoop.hbase.master;

import com.google.protobuf.Service;
import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ChoreService;
import org.apache.hadoop.hbase.CoordinatedStateManager;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.MasterSwitchType;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.executor.ExecutorService;
import org.apache.hadoop.hbase.favored.FavoredNodesManager;
import org.apache.hadoop.hbase.master.assignment.AssignmentManager;
import org.apache.hadoop.hbase.master.locking.LockManager;
import org.apache.hadoop.hbase.master.normalizer.RegionNormalizer;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.master.replication.ReplicationPeerManager;
import org.apache.hadoop.hbase.master.snapshot.SnapshotManager;
import org.apache.hadoop.hbase.procedure.MasterProcedureManagerHost;
import org.apache.hadoop.hbase.procedure2.LockedResource;
import org.apache.hadoop.hbase.procedure2.Procedure;
import org.apache.hadoop.hbase.procedure2.ProcedureEvent;
import org.apache.hadoop.hbase.procedure2.ProcedureExecutor;
import org.apache.hadoop.hbase.quotas.MasterQuotaManager;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerDescription;
import org.apache.hadoop.hbase.security.access.AccessChecker;
import org.apache.hadoop.hbase.security.access.ZKPermissionWatcher;
import org.apache.hadoop.hbase.zookeeper.ZKWatcher;

public class MockMasterServices implements MasterServices {

  private Configuration conf;

  private Connection conn;

  private ZKWatcher zkWatcher;

  public MockMasterServices(Configuration conf, Connection conn, ZKWatcher zkWatcher) {
    this.conf = conf;
    this.conn = conn;
    this.zkWatcher = zkWatcher;
  }

  @Override
  public Configuration getConfiguration() {
    return conf;
  }

  @Override
  public ZKWatcher getZooKeeper() {
    return zkWatcher;
  }

  @Override
  public Connection getConnection() {
    return conn;
  }

  @Override
  public Connection createConnection(Configuration conf) throws IOException {
    return null;
  }

  @Override
  public ClusterConnection getClusterConnection() {
    return null;
  }

  @Override
  public ServerName getServerName() {
    return null;
  }

  @Override
  public CoordinatedStateManager getCoordinatedStateManager() {
    return null;
  }

  @Override
  public ChoreService getChoreService() {
    return null;
  }

  @Override
  public void abort(String why, Throwable e) {
  }

  @Override
  public boolean isAborted() {
    return false;
  }

  @Override
  public void stop(String why) {
  }

  @Override
  public boolean isStopped() {
    return false;
  }

  @Override
  public SnapshotManager getSnapshotManager() {
    return null;
  }

  @Override
  public MasterProcedureManagerHost getMasterProcedureManagerHost() {
    return null;
  }

  @Override
  public ClusterSchema getClusterSchema() {
    return null;
  }

  @Override
  public AssignmentManager getAssignmentManager() {
    return null;
  }

  @Override
  public MasterFileSystem getMasterFileSystem() {
    return null;
  }

  @Override
  public MasterWalManager getMasterWalManager() {
    return null;
  }

  @Override
  public ServerManager getServerManager() {
    return null;
  }

  @Override
  public ExecutorService getExecutorService() {
    return null;
  }

  @Override
  public TableStateManager getTableStateManager() {
    return null;
  }

  @Override
  public MasterCoprocessorHost getMasterCoprocessorHost() {
    return null;
  }

  @Override
  public MasterQuotaManager getMasterQuotaManager() {
    return null;
  }

  @Override
  public RegionNormalizer getRegionNormalizer() {
    return null;
  }

  @Override
  public CatalogJanitor getCatalogJanitor() {
    return null;
  }

  @Override
  public ProcedureExecutor<MasterProcedureEnv> getMasterProcedureExecutor() {
    return null;
  }

  @Override
  public ProcedureEvent<?> getInitializedEvent() {
    return null;
  }

  @Override
  public MetricsMaster getMasterMetrics() {
    return null;
  }

  @Override
  public void checkTableModifiable(TableName tableName)
      throws IOException, TableNotFoundException, TableNotDisabledException {
  }

  @Override
  public long createTable(TableDescriptor desc, byte[][] splitKeys, long nonceGroup, long nonce)
      throws IOException {
    return 0;
  }

  @Override
  public long createSystemTable(TableDescriptor tableDescriptor) throws IOException {
    return 0;
  }

  @Override
  public long deleteTable(TableName tableName, long nonceGroup, long nonce) throws IOException {
    return 0;
  }

  @Override
  public long truncateTable(TableName tableName, boolean preserveSplits, long nonceGroup,
      long nonce) throws IOException {
    return 0;
  }

  @Override
  public long modifyTable(TableName tableName, TableDescriptor descriptor, long nonceGroup,
      long nonce) throws IOException {
    return 0;
  }

  @Override
  public long enableTable(TableName tableName, long nonceGroup, long nonce) throws IOException {
    return 0;
  }

  @Override
  public long disableTable(TableName tableName, long nonceGroup, long nonce) throws IOException {
    return 0;
  }

  @Override
  public long addColumn(TableName tableName, ColumnFamilyDescriptor column, long nonceGroup,
      long nonce) throws IOException {
    return 0;
  }

  @Override
  public long modifyColumn(TableName tableName, ColumnFamilyDescriptor descriptor, long nonceGroup,
      long nonce) throws IOException {
    return 0;
  }

  /*
   * (non-Javadoc)
   * @see
   * org.apache.hadoop.hbase.master.MasterServices#deleteColumn(org.apache.hadoop.hbase.TableName,
   * byte[], long, long)
   */
  @Override
  public long deleteColumn(TableName tableName, byte[] columnName, long nonceGroup, long nonce)
      throws IOException {

    return 0;
  }

  @Override
  public long mergeRegions(RegionInfo[] regionsToMerge, boolean forcible, long nonceGroup,
      long nonce) throws IOException {
    return 0;
  }

  @Override
  public long splitRegion(RegionInfo regionInfo, byte[] splitRow, long nonceGroup, long nonce)
      throws IOException {
    return 0;
  }

  @Override
  public TableDescriptors getTableDescriptors() {
    return null;
  }

  @Override
  public boolean registerService(Service instance) {
    return false;
  }

  @Override
  public boolean isActiveMaster() {
    return false;
  }

  @Override
  public boolean isInitialized() {
    return false;
  }

  @Override
  public boolean isInMaintenanceMode() {
    return false;
  }

  @Override
  public boolean abortProcedure(long procId, boolean mayInterruptIfRunning) throws IOException {
    return false;
  }

  @Override
  public List<Procedure<?>> getProcedures() throws IOException {
    return null;
  }

  @Override
  public List<LockedResource> getLocks() throws IOException {
    return null;
  }

  @Override
  public List<TableDescriptor> listTableDescriptorsByNamespace(String name) throws IOException {
    return null;
  }

  @Override
  public List<TableName> listTableNamesByNamespace(String name) throws IOException {
    return null;
  }

  @Override
  public long getLastMajorCompactionTimestamp(TableName table) throws IOException {
    return 0;
  }

  @Override
  public long getLastMajorCompactionTimestampForRegion(byte[] regionName) throws IOException {
    return 0;
  }

  @Override
  public LoadBalancer getLoadBalancer() {
    return null;
  }

  @Override
  public boolean isSplitOrMergeEnabled(MasterSwitchType switchType) {
    return false;
  }

  @Override
  public FavoredNodesManager getFavoredNodesManager() {
    return null;
  }

  @Override
  public long addReplicationPeer(String peerId, ReplicationPeerConfig peerConfig, boolean enabled)
      throws ReplicationException, IOException {
    return 0;
  }

  @Override
  public long removeReplicationPeer(String peerId) throws ReplicationException, IOException {
    return 0;
  }

  @Override
  public long enableReplicationPeer(String peerId) throws ReplicationException, IOException {
    return 0;
  }

  @Override
  public long disableReplicationPeer(String peerId) throws ReplicationException, IOException {
    return 0;
  }

  @Override
  public ReplicationPeerConfig getReplicationPeerConfig(String peerId)
      throws ReplicationException, IOException {
    return null;
  }

  @Override
  public ReplicationPeerManager getReplicationPeerManager() {
    return null;
  }

  @Override
  public long updateReplicationPeerConfig(String peerId, ReplicationPeerConfig peerConfig)
      throws ReplicationException, IOException {
    return 0;
  }

  @Override
  public List<ReplicationPeerDescription> listReplicationPeers(String regex)
      throws ReplicationException, IOException {
    return null;
  }

  @Override
  public LockManager getLockManager() {
    return null;
  }

  @Override
  public String getRegionServerVersion(ServerName sn) {
    return null;
  }

  @Override
  public void checkIfShouldMoveSystemRegionAsync() {

  }

  @Override
  public String getClientIdAuditPrefix() {

    return null;
  }

  @Override
  public boolean isClusterUp() {
    return false;
  }

    @Override
    public AccessChecker getAccessChecker() {
        return null;
    }

    @Override
    public ZKPermissionWatcher getZKPermissionWatcher() {
        return null;
    }

    @Override
    public List<RegionPlan> executeRegionPlansWithThrottling(List<RegionPlan> list) {
        return null;
    }

    @Override
    public void runReplicationBarrierCleaner() {

    }
}
