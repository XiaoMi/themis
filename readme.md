# Themis 

## Introduction

Themis provides cross-row/cross-table transaction on HBase based on [google's percolator](http://research.google.com/pubs/pub36726.html).

Themis guarantees the ACID characteristics of cross-row transaction by two-phase commit and conflict resolution, which is based on the single-row transaction of HBase. Themis depends on [chronos](https://github.com/XiaoMi/chronos) to provide global strictly incremental timestamp, which defines the global order for transactions and makes themis could read database snapshot before given timestamp. Themis adopts HBase coprocessor framework, which could serve after loading themis coprocessors without changing source code of HBase. The APIs of themis are similar with HBase's, including themisPut/themisDelete/themisGet/themisScan. We validate the correctness of themis for a few months, and try to optimize the [percolator](http://research.google.com/pubs/pub36726.html) algorithm to achieve better performance.

## Example of Themis API

### themisPut 

    Configuration conf = HBaseConfiguration.create();
    HConnection connection = HConnectionManager.createConnection(conf);
    Transaction transaction = new Transaction(conf, connection);
    ThemisPut put = new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE);
    transaction.put(TABLENAME, put);
    put = new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, VALUE);
    transaction.put(TABLENAME, put);
    transaction.commit();

### themisDelete 

     Transaction transaction = new Transaction(conf, connection);
     ThemisDelete delete = new ThemisDelete(ROW);
     delete.deleteColumn(FAMILY, QUALIFIER);
     transaction.delete(TABLENAME, delete);
     delete = new ThemisDelete(ANOTHER_ROW);
     delete.deleteColumn(FAMILY, QUALIFIER);
     transaction.delete(TABLENAME, delete);
     transaction.commit();

In above code, the mutations of two rows will both be applied to HBase and visiable to read after transaction.commit() finished. If transaction.commit() failed, neither of the two mutations will be visiable to read.

### themisGet 

    Transaction transaction = new Transaction(conf, connection);
    ThemisGet get = new ThemisGet(ROW).addColumn(FAMILY, QUALIFIER);
    Result resultA = transaction.get(TABLENAME, get);
    get = new ThemisGet(ANOTHER_ROW).addColumn(FAMILY, QUALIFIER);
    Result resultB = transaction.get(TABLENAME, get);
    // themisGet will return consistent results from ROW and ANOTHER_ROW 

### themisScan

    Transaction transaction = new Transaction(conf, connection);
    ThemisScan scan = new ThemisScan();
    scan.addColumn(FAMILY, QUALIFIER);
    ThemisScanner scanner = transaction.getScanner(TABLENAME, scan);
    Result result = null;
    while ((result = scanner.next()) != null) {
      // themisScan will return consistent state of database
      int value = Bytes.toInt(result.getValue(FAMILY, QUALIFIER));
    }
    scanner.close();

Themis will get a timestamp from chronos before transaction starts, and will promise to read the database snapshot before the timestamp.

For more example code, please see [Example.java](https://github.com/XiaoMi/themis/blob/master/themis-client/src/main/java/org/apache/hadoop/hbase/themis/example/Example.java)

## Principal and Implementation 

### Principal

**Themis Write:**

1. Select one column as primaryColumn and others as secondaryColumns from mutations of users. Themis will construct persistent lock for each column.
2. Prewrite-Phase: get timestamp from chronos(denoted as prewriteTs), write data and persistent lock to HBase with timestamp=prewriteTs when no write/write conflicts discovered.
3. After prewrite-phase finished, get timestamp from chronos(denoted as commitTs) and commit primaryColumn: erase the persistent lock and write the commit information with timestamp=commitTs if the persistent lock of primaryColumn is not deleted.
4. After primaryColumn committed, commit secondaryColumns: erase the persistent lock and write the commit information with timestamp=commitTs for each secondaryColumns.

Themis applies transaction mutations by two-phase write(prewrite/commit). The transaction will success and be visiable to read if primaryColumn is committed succesfully; otherwise, the transaction will fail and can't be read.

**Themis Read:**

1. Get timestamp from chronos(named startTs), check the read/write conflicts.
2. Read the snapshot of database before startTs when there are no read/write conflicts.

Themis provides the guarantee to read all transactions with commitTs smaller than startTs, which is the snapshot of database before startTs.

**Conflict Resolution:**

There might be write/write and read/write conflicts as described. Themis will use the timestamp saved in persistent lock to judge whether the conflict transaction is expired. If the conflict transaction is expired, the current transaction will do rollback or commit for the conflict transaction according to whether the primaryColumn of conflict transcation is committed; otherwise, the current transaction will fail.

Please see [google's percolator](http://research.google.com/pubs/pub36726.html) for more details.

### Themis Implementation

The implementation of Themis adopts the HBase coprocessor framework, the following picture gives the modules of Themis:

![themis_architecture](https://raw.githubusercontent.com/XiaoMi/themis/master/themis_architecture.png)

**Themis Client:**

1. Transaction: provides APIs of themis, including themisPut/themisGet/themisDelete/themisScan/commit.
2. MutationCache: index the mutations of users by rows in client side.
3. ThemisCoprocessorClient: the client to access themis coprocessor.
4. TimestampOracle: the client to query [chronos](https://github.com/XiaoMi/chronos), which will cache the timestamp requests and issue batch request to chronos in one rpc.
5. LockCleaner: resovle write/write conflict and read/write conflict.

Themis client will manage the users's mutations by row and invoke methods of ThemisCoprocessorClient to do prewrite/commit for each row.

**Themis Coprocessor:**

1. ThemisProtocol/ThemisCoprocessorImpl: definition and implementation of the themis coprocessor interfaces. The major interfaces are prewrite/commit/themisGet.
2. ThemisServerScanner/ThemisScanObserver: implement themis scan logic.
3. ThemisRegionObserver: single-row write optimization, add data clean filter to the scanner of flush and compaction.
4. ThemisMasterObserver: automatically add lock family when users creating table, set timestamp for data clean periodly.

## Usage 

### Loads themis coprocessor in server side 

1. add themis-coprocessor dependency in the pom of HBase:

     ```
     <dependency>
       <groupId>com.xiaomi.infra</groupId>
       <artifactId>themis-coprocessor</artifactId>
       <version>1.0-SNAPSHOT</version>
     </dependency>
     ```
                                          
2. add configurations for themis coprocessor in hbase-site.xml:

     ```
     <property>
       <name>hbase.coprocessor.user.region.classes</name>
       <value>org.apache.hadoop.hbase.themis.cp.ThemisEndpoint,org.apache.hadoop.hbase.themis.cp.ThemisScanObserver,org.apache.hadoop.hbase.regionserver.ThemisRegionObserver</value>
     </property>
     <property>
       <name>hbase.coprocessor.master.classes</name>
       <value>org.apache.hadoop.hbase.master.ThemisMasterObserver</value>
     </property>
               
     ```

3. For familiy needs themis, set THEMIS_ENABLE to 'true' by adding "METADATA => {'THEMIS_ENABLE', 'true'}" to the family descriptor when creating table. 

### Depends themis-client

add the themis-client dependency to pom of project which needs cross-row transactions.

     <dependency>
       <groupId>com.xiaomi.infra</groupId>
       <artifactId>themis-client</artifactId>
       <version>1.0-SNAPSHOT</version>
     </dependency>

### Run example code

1. this branch depends on hbase 0.98.5 with hadoop.version=2.2.0. We need download source code of hbase 0.98.5 and install in maven local repository by(in the directory of hbase 0.98.5):
   
     mvn clean install -DskipTests -P hadoop-2.0

2. install themis in maven local repository(in the directory of themis):

     mvn clean install -DskipTests

3. start a standalone HBase cluster(0.98.5 with hadoop.version=2.2.0) and make sure themis-coprocessor is loaded as above steps.

4. run "org.apache.hadoop.hbase.themis.example.Example.java" by:
     
     mvn exec:java -Dexec.mainClass="org.apache.hadoop.hbase.themis.example.Example"
  
The result of themisPut/themisGet/themisDelete/themisScan will output to screen.

### Themis Options

**Use Chronos As Timestamp Oracle**

Themis will use a LocalTimestampOracle class to provide incremental timestamp for threads in the same process. To use the global incremental timestamp from Chronos, we need the following steps and config:

1. config and start a Chronos cluster, please see : https://github.com/XiaoMi/chronos/.

2. set themis.timestamp.oracle.class to "org.apache.hadoop.hbase.themis.timestamp.RemoteTimestampOracleProxy" in the config of cliet-side. With this config, themis will connect Chronos cluster in local machine.

3. The Chronos cluster address could be configed by 'themis.remote.timestamp.server.zk.quorum' and cluster name could be configed by 'themis.remote.timestamp.server.clustername'.

**Data Clean Options**

1. Timeout of transaction. The timeout of read/write transaction could be set by 'themis.read.transaction.ttl' and 'themis.write.transaction.ttl' respectively.

2. Data clean. Old data which could not be read any more will be cleaned periodly if 'themis.expired.data.clean.enable' is enable, and the clean period could be specified by 'themis.expired.timestamp.calculator.period'.

These settings could be set in hbase-site.xml of server-side.

**Conflict Resolution Options**

1. As mentioned in [percolator](http://research.google.com/pubs/pub36726.html), failed clients could be detected quickly if "Running workers write a token into the Chubby lockservice" which could help resolve conflict lock more efficiently. In themis, we can set themis.worker.register.class to "org.apache.hadoop.hbase.themis.lockcleaner.ZookeeperWorkerRegister" to enable this function, then, each alive clients will create a emphemeral node in the zookeeper cluster depends by hbase.

2. Themis will retry conflict resolution where retry count could be specified by "themis.retry.count" and the pause between retries could be specified by "themis.pause".

**Customer Filter**

1. Themis use timestamp and version attribute of HBase internally. Therefore, users should not set filters using timestamp or version attribute.

2. For filters only use rowkey, such as PrefixFilter defined in HBase, users could make them implement "RowLevelFilter" interface(defined in themis) which could help do themis read more efficiently. 

### MapReduce Support

Themis implement InputFormat and OutputFormat interface in MapReduce framework:

1. ThemisTableInputFormat is implemented to read data from themis-enable table in Mapper. To read data from multi-tables, please use MultiThemisTableInputFormat.
 
2. ThemisTableOutputFormat is implemented to write data by themis to themis-enable table in Reducer. To write data across multi-tables, please use MultiThemisTableOutputFormat.

3. ThemisTableMapReduceUtil provides utility methods to start a MapReduce job.

### Global Secondary Index Support

Based on the cross table data consistency guaranteed by themis transaction, we build an expiremental sub-project "themis-index" to support global secondary index, including:

1. Secondary index could be defined on the column under themis-enable family by setting family attribute as : CONFIG => {'SECONDARY_INDEX_NAMES', 'index_name:qualifier;...'}, and users need the following configuration to support this schema.
 
      ```
      <property>
        <name>hbase.coprocessor.master.classes</name>
        <value>org.apache.hadoop.hbase.master.ThemisMasterObserver,org.apache.hadoop.hbase.themis.index.cp.IndexMasterObserver</value>
      </property>

      ```
 
2. When mutating columns which contain secondary index definitions, mutations corresponding to the index table will be applied automatically.

3. Users could read data by secondary index with IndexGet / IndexScan.

Themis-index is also expiremental, we will improve this sub-project after studying the demands better.
 

## Test 

### Correctness Validation

We design an AccountTransfer simulation program to validate the correctness of implementation. This program will distribute initial values in different tables, rows and columns in HBase. Each column represents an account. Then, configured client threads will be concurrently started to read out a number of account values from different tables and rows by themisGet. After this, clients will randomly transfer values among these accounts while keeping the sum unchanged, which simulates concurrent cross-table/cross-row transactions. To check the correctness of transactions, a checker thread will periodically scan account values from all columns, make sure the current total value is the same as the initial total value. We run this validation program for a period when releasing a new version for themis.

### Performance Test 

The Performance Test is copyied from [readme.md](https://github.com/XiaoMi/themis/) from master branch of Themis which is based on HBase 0.94. Because the core algorithm of themis in this branch is the same as master branch, we believe the test will get similar results for this branch. We will do the same tests on HBase 0.98 and update this part. 

**Percolator Result:**

[percolator](http://research.google.com/pubs/pub36726.html) tests the read/write performance for single-column transaction(represents the worst case of percolator) and gives the relative drop compared to BigTable as follow table.

|             | BigTable  | Percolator       | Relative            |
|-------------|-----------|------------------|---------------------|
| Read/s      | 15513     | 14590            | 0.94                |
| Write/s     | 31003     | 7232             | 0.23                |

**Themis Result:**
We evaluate the performance of themis under similar test conditions with percolator's and give the relative drop compared to HBase.

Evaluation of themisGet. Load 30g data into HBase before testing themisGet by reading loaded rows. We set the heap size of region server to 10g and hfile.block.cache.size=0.45.

| Client Thread | GetCount  | Themis AvgLatency(us) | HBase AvgLatency(us) | Relative |
|-------------  |---------- |-----------------------|----------------------|----------|
| 5             | 10000000  | 1029.88               | 1191.21              | 0.86     | 
| 10            | 20000000  | 1230.44               | 1407.93              | 0.87     | 
| 20            | 20000000  | 1848.05               | 2190.00              | 0.84     | 
| 50            | 30000000  | 4529.80               | 5382.87              | 0.84     | 

Evaluation of themisPut. Load 3,000,000 rows data into HBase before testing themisPut. We config 256M cache size to keep locks in memory for write transaction. 

| Client Thread | PutCount  | Themis AvgLatency(us) | HBase AvgLatency(us) | Relative |
|-------------  |---------- |-----------------------|----------------------|----------|
| 1             | 3000000   | 1620.69               | 818.62               | 0.51     |
| 5             | 10000000  | 1695.89               | 1074.13              | 0.63     |
| 10            | 20000000  | 2057.55               | 1309.12              | 0.64     |
| 20            | 20000000  | 2761.66               | 1902.79              | 0.69     |
| 50            | 30000000  | 5441.48               | 3702.04              | 0.68     |

The above tests are all done in a single region server. From the results, we can see the performance of themisGet is 85% of HBase's get and the performance of themisPut is about 60% of HBase's put. For themisGet, the result is about 10% lower to that reported in [percolator](http://research.google.com/pubs/pub36726.html) paper. The themisPut performance is much better compared to that reported in [percolator](http://research.google.com/pubs/pub36726.html) paper. We optimize the performance of single-column transaction by the following skills:

1. In prewrite phase, we only write the lock to MemStore;  

2. In commit phase, we erase corresponding lock if it exist, write data and commit information at the same time.

The aboving skills make prewrite phase not write HLog, so that improving the write performance a lot for single-column transaction(also for single-row transaction). After applying the skills, if region server restarts after prewrite phase, the commit phase can't read the persistent lock and the transaction will fail, this won't break correctness of the algorithm.

**ConcurrentThemis Result:**
The prewrite and commit of secondary rows could be implemented concurrently, which could do cross-row transaction more efficiently. We use 'ConcurrentThemis' to represent the concurrent way and 'RawThemis' to represent the original way, then get the efficiency comparsion(we don't pre-load data before this comparsion because we focus on the relative improvement):

| TransactionSize | PutCount | RawThemis AvgTime(us) | ConcurrentThemis AvgTime(us) | Relative Improve |
|-----------------|--------- |-----------------------|------------------------------|------------------|
| 2               | 1000000  | 1654.14               | 995.98                       | 1.66             |
| 4               | 1000000  | 3233.11               | 1297.49                      | 2.50             |
| 8               | 1000000  | 6470.30               | 1963.47                      | 3.30             |
| 16              | 1000000  | 13301.50              | 2941.81                      | 4.52             |
| 32              | 600000   | 28151.37              | 3384.17                      | 7.25             |
| 64              | 400000   | 51658.58              | 5765.08                      | 8.96             |
| 128             | 200000   | 103289.95             | 11282.95                     | 9.15             |

TransactionSize is number of rows in one transaction. The 'Relative Improve' is 'RawThemis AvgTime(us)' / 'ConcurrentThemis AvgTime(us)'. We can see ConcurrentThemis performs much better as the TransactionSize increases, however, there is slowdown of improvement when the TransactionSize is bigger than 32.

## Future Works

1. Optimize the memory usage of RegionServer. Persistent locks of committed transactions should be removed from memory so that only keeping persistent locks of un-committed transactions in memory.
2. When reading from a transaction, merge the the local mutation of the transaction with committed transactions from server-side.
3. Support different ioslation levels.
4. Use a different family to save commit information and compare the performance with the current way(currently, we save commit information under the same family with data column).
5. Commit secondary rows in background to improve latency.
6. Release the correctness validate program AccountTransfer.
7. Test the performance of themis for HBase 0.98.
