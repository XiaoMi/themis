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

## 原理

## Themis实现

## Themis使用

### Themis服务端

### Themis客户端

## 测试

### 正确性验证
### 性能测试

## 将来的工作
