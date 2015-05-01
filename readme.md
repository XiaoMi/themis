# Themis 

## Introduction

Themis provides cross-row/cross-table transaction on HBase based on [google's percolator](http://research.google.com/pubs/pub36726.html).

Themis guarantees the ACID characteristics of cross-row transaction by two-phase commit and conflict resolution, which is based on the single-row transaction of HBase. Themis depends on [chronos](https://github.com/XiaoMi/chronos) to provide global strictly incremental timestamp, which defines the global order for transactions and makes themis could read database snapshot before given timestamp. Themis adopts HBase coprocessor framework, which could be applied without changing source code of HBase. We validate the correctness of themis for a few months, and optimize the algorithm to achieve better performance.

## Implementation 

Themis contains three components: timestamp server, client library, themis coprocessor.

![themis_architecture](https://raw.githubusercontent.com/XiaoMi/themis/master/themis_architecture.png)

**Timestamp Server**

Themis uses the timestamp of HBase's KeyValue internally, and the timestamp must be global strictly incremental. Themis depends on [chronos](https://github.com/XiaoMi/chronos) to provide such timestamp service.

**Client Library**

1. Provide transaction APIs.
2. Fetch timestamp from chronos.
3. Issue requests to themis coprocessor in server-side.
4. Resolve conflict for concurrent mutations of other clients.

**Themis Coprocessor:**

1. Provide RPC methods for two-phase commit and read.
2. Create auxiliary families and set family attributes for the algorithm automatically.
3. Periodically clean the data of the aborted and expired transactions.

## Usage 

### Build 

1. get the latest source code of Themis:

     ```
     git clone git@github.com:XiaoMi/themis.git 
     ```

2. the master branch of Themis depends on hbase 0.94.21 with hadoop.version=2.0.0-alpha. We can download source code of hbase 0.94.21 and install in maven local repository by:
   
     ```
     (in the directory of hbase 0.94.21)
     mvn clean install -DskipTests -Dhadoop.profile=2.0
     ```

3. build Themis:

     ```
     cd themis
     mvn clean package -DskipTests
     ```

### Loads themis coprocessor in HBase: 

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
       <value>org.apache.hadoop.hbase.themis.cp.ThemisProtocolImpl,org.apache.hadoop.hbase.themis.cp.ThemisScanObserver,org.apache.hadoop.hbase.regionserver.ThemisRegionObserver</value>
     </property>
     <property>
        <name>hbase.coprocessor.master.classes</name>
        <value>org.apache.hadoop.hbase.master.ThemisMasterObserver</value>
     </property>

     ```

### Depends themis-client:

add the themis-client dependency to pom of project which needs cross-row transactions.

     <dependency>
       <groupId>com.xiaomi.infra</groupId>
       <artifactId>themis-client</artifactId>
       <version>1.0-SNAPSHOT</version>
     </dependency>

### Run the example code

1. start a standalone HBase cluster(0.94.21 with hadoop.version=2.0.0-alpha) and make sure themis-coprocessor is loaded as above steps.

2. after building Themis, run "org.apache.hadoop.hbase.themis.example.Example.java" by:
     
     mvn exec:java -Dexec.mainClass="org.apache.hadoop.hbase.themis.example.Example"
  
The screen will output the result of read and write transactions.

## Example of Themis API

3. For familiy needs themis, set THEMIS_ENABLE to 'true' by adding "CONFIG => {'THEMIS_ENABLE', 'true'}" to the family descriptor when creating table. 

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
