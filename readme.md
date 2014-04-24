# Themis 

## 简介

Themis是基于google提出的[percolator](http://research.google.com/pubs/pub36726.html)算法，在HBase上实现跨行、跨表事务。
Themis以HBase行级别事务为基础，通过client端的协同完成跨行事务。Themis依赖[chronos](https://github.com/XiaoMi/chronos)提供的全局单调递增时钟服务，为事务全局定序，确保事务的ACID特性。Themis的实现利用了HBase coprocessor框架，不需要对HBase的代码做修改，在server端加载Themis coprocessor后即可服务。Themis提供与HBase类似的数据读写接口：put/delete/get/scan，经过了几个月的正确性验证和性能测试，性能与[percolator](http://research.google.com/pubs/pub36726.html)论文中报告的结果相近。

## Themis API使用示例
Themis API与HBase原生API相近，我们首先给出示例代码需要的常量：

    private static final byte[] TABLENAME = Bytes.toBytes("ThemisTable");
    private static final byte[] ROW = Bytes.toBytes("Row");
    private static final byte[] ANOTHER_ROW = Bytes.toBytes("AnotherRow");
    private static final byte[] FAMILY = Bytes.toBytes("ThemisCF");
    private static final byte[] QUALIFIER = Bytes.toBytes("Qualifier");
    private static final byte[] VALUE = Bytes.toBytes(10);
    private static Configuration conf;

### 跨行写

    HConnection connection = HConnectionManager.createConnection(conf);
    Transaction transaction = new Transaction(conf, connection);
    ThemisPut put = new ThemisPut(ROW).add(FAMILY, QUALIFIER, VALUE);
    transaction.put(TABLENAME, put);
    put = new ThemisPut(ANOTHER_ROW).add(FAMILY, QUALIFIER, VALUE);
    transaction.put(TABLENAME, put);
    transaction.commit();

如果transaction.commit()成功，会保证对于ROW和ANOTHER_ROW的修改同时成功，并且对于读同时可见；如果commit失败，会保证ROW和ANOTHER_ROW的修改都失败，对于读都不可见。
### Themis读

    Transaction transaction = new Transaction(conf, connection);
    ThemisGet get = new ThemisGet(ROW).addColumn(FAMILY, QUALIFIER);
    Result resultA = transaction.get(TABLENAME, get);
    get = new ThemisGet(ANOTHER_ROW).addColumn(FAMILY, QUALIFIER);
    Result resultB = transaction.get(TABLENAME, get);
    // ... 

对于跨行读，themis可以确保读取完整的事务。
更多示例代码参见：org.apache.hadoop.hbase.themis.example.Example.java

## 原理和实现
### Themis原理

Themis实现了[percolator](http://research.google.com/pubs/pub36726.html)算法，依赖全局时钟服务[chronos](https://github.com/XiaoMi/chronos)为事务定序。

Themis的写步骤：

1. 在用户写中选取一个column做为primaryColumn，其余的column为secondaryColumns。Themis会为primaryColumn和scondaryColumn构建对应的持久化锁(persistentLock)信息。
2. 从chronos取全局时间prewriteTs，进行prewrite。对于每一个column，themis会使用对应的lockColumn保留persistentLock；prewrite阶段在没有写冲突的情况下，将数据和持久化锁分别写入对应的column和lockColumn。
3. prewrite成功后，从chronos取全局时间commitTs，对primaryColumn进行commit。对于每一个column，themis会使用对应的writeColumn存储提交信息，内容是事务的prewriteTs。primaryColumn的提交需要确保其persistentLock没有被删除的情况下删除persistentLock并写入commit信息。
4. primaryColumn提交成功后，开始提交secondaryColumn，动作是删除persistentLock并写入commit信息。

Themis是通过prewrite/commit两阶段写来完成事务。primaryColumn的commit成功后，事务整体成功，对读可见；否则事务整体失败，对读不可见。

Themis读步骤：

1. 从chronos取一个startTs，首先从lockColumn读取数据判断是否有读写冲突。
2. 如果没有读写冲突，读取timestamp < startTs的最新提交的事务。

Themis可以确保读取commitTs < startTs的所有提交事物，即数据库在startTs之前的snapshot。

Themis冲突解决：

Themis可能会遇到写写冲突和读写冲突。解决冲突的关键是利用存储在persistentLock中的时间戳，判断冲突事务是否过期。如果过期，根据冲突事务的primaryColumn是否提交，回滚或提交事务；否则，当前事务失败。

更多原理细节参考[percolator](http://research.google.com/pubs/pub36726.html)

### Themis实现

Themis的实现利用了HBase的coprocessor框架，其架构为：
[在gitlab中，图片貌似没法显示，先给出链接](http://git.n.xiaomi.com/yehangjun/themis/blob/master/themis_architecture.jpg)

ThemisClient组件为：
1. Transaction。提供Themis的API：themisPut/themisGet/themisDelete/themisScan。
2. ThemisPut/PercolatorGet/PercolatorDelete/PercolatorScan是HBase的put/get/delete/scan的封装，屏蔽了timestamp的设置接口。
3. ColumnMutationCache。将用户的修改按照row索引起来。
4. TimestampOracle。访问[chronos](https://github.com/XiaoMi/chronos)的客户端，可以将客户端对chronos的请求做batch，然后批量访问。
5. LockCleaner。负责解决写写冲突和读写冲突。

对于写事务，Themis将用户的mutations按照row进行索引，然后利用ThemisCoprocessorClient的接口进行prewrite/commit和读操作。

ThemisCoprocessor组件为：
1. ThemisProtocol/ThemisCoprocessorImpl。定义和实现Themis coprocessor，主要接口是prewrite/commit/themisGet。
2. ThemisServerScanner/ThemisScanObserver。实现themisScan逻辑。

## Themis使用

### Themis服务端
1. 需要在hbase的pom中引入对themis-coprocessor的依赖：
    \<dependency\>
    \<groupId\>com.xiaomi.infra\</groupId\>
    \<artifactId\>percolator-coprocessor\</artifactId\>
    \<version\>1.0-SNAPSHOT\</version\>
    \</dependency\>
                                          
2. hbase的配置文件hbase-site.xml中加入themis-coprocessor的配置项：
    \<property\>
    \<name\>hbase.coprocessor.user.region.classes\</name\>
    \<value\>org.apache.hadoop.hbase.coprocessor.AggregateImplementation,org.apache.hadoop.hbase.coprocessor.example.BulkDeleteEndpoint,
             org.apache.hadoop.hbase.themis.cp.ThemisProtocolImpl\</value\>
    \</property\>
    \<property\>
    \<name\>hbase.coprocessor.region.classes\</name\>
    \<value\>org.apache.hadoop.hbase.themis.cp.ThemisScanObserver\</value\>
    \</property\>
3. 对于需要使用themis的表，创建一个额外的family='L'，用来存储persistentLock，IN_MEMORY属性设置为true。

### Themis客户端
需要在使用Themis的项目的pom中引入themis-client的依赖即可：
    \<dependency\>
    \<groupId\>com.xiaomi.infra\</groupId\>
    \<artifactId\>percolator-client\</artifactId\>
    \<version\>1.0-SNAPSHOT\</version\>
    \</dependency\>

## 测试

### 正确性验证

我们设计了一个AccountTransfer程序对themis进行正确性验证。AccountTransfer模拟多个用户，每个用户在HBase的某个column下初始一个value，记录验证开始前的initTotal。验证开始后，会启动多个线程在选定的column之间进行value transfter，修改value的值，但逻辑上保持total value不变，用以模拟事物的并发运行。同时，会有一个TotalChecker线程，不断读出当前所有column的currentTotal，检查currentTotal=initTotal，否则验证失败。另外，会在themis的主要步骤上随机抛出异常，使事务失败，测试themis解决冲突的逻辑。每次更新themis的实现后，都会运行AccountTransfer一段时间，确保逻辑正确。

### 性能测试

[percolator](http://research.google.com/pubs/pub36726.html)测试了percolator在单column情况的读写性能相对于BigTable的降低百分比：

| | BigTable | Percolator | Relative |
|-------------|---------|------------------|---------------------|
| Read/s      | 15513    | 14590            | 0.94               |
| Write/s     | 31003     | 7232            | 0.23               |

与percolator类似，themis也对比了单column情况下读写性能相对于HBase的降低，我们结论如下：
1. themisGet对比。预写入10g数据。

| Client Thread | GetCount | Themis AvgLatency(us) | HBase AvgLatency(us) | Relative |
|-------------  |--------- |-----------------------|----------------------|----------|
| 1             | 1000000  | 846.08                | 783.08               | 0.90     |
| 5             | 5000000  | 1125.95               | 1016.54              | 0.90     |
| 10            | 5000000  | 1513.61               | 1348.58              | 0.89     |
| 20            | 5000000  | 2639.60               | 2427.78              | 0.92     |
| 50            | 5000000  | 6295.83               | 5935.88              | 0.94     |

2. themisPut对比。预写入10g数据，然后对其中的row进行更新，对比写性能。

| Client Thread | PutCount | Themis AvgLatency(us) | HBase AvgLatency(us) | Relative |
|-------------  |--------- |-----------------------|----------------------|----------|
| 1             | 1000000  | 3658.28               | 882.63               | 0.24     |
| 5             | 1000000  | 4005.77               | 1096.53              | 0.27     |
| 10            | 1000000  | 1096.5                | 1376.60              | 0.24     |
| 20            | 1000000  | 8486.28               | 1891.47              | 0.22     |
| 50            | 1000000  | 18356.76              | 3384.32              | 0.18     |

## 将来的工作
