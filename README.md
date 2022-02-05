# Clickhouse storage backend for Janusgraph

## Overview

Clickhouse implementation of Janusgraph storage backend.

## Features

* New version 0.6.1 of JanusGraph compatibility.
* ClickHouse 20.7 or above supported.
* Use [official clickhouse jdbc](https://github.com/ClickHouse/clickhouse-jdbc) and [HikariCP](https://github.com/brettwooldridge/HikariCP) to manage the datasource.


## Getting Started
1. Add dependency to your maven project.
```
<dependency>
    <groupId>com.github.artjourney.janusgraph</groupId>
    <artifactId>janusgraph-clickhouse</artifactId>
    <version>${janusgraph-clickhouse.version}</version>
</dependency>
```

2. Config the Janusgraph.
```
ModifiableConfiguration config = GraphDatabaseConfiguration.buildGraphConfiguration();
config.set(GraphDatabaseConfiguration.STORAGE_BACKEND, "io.github.artjourney.janusgraph.ClickhouseStoreManager");
config.set(ClickhouseConfiguration.JDBC_URL, "jdbc:ch://127.0.0.1:8123/default?connect_timeout=60000&http_keep_alive=true");
config.set(ClickhouseConfiguration.JDBC_USERNAME, "");
config.set(ClickhouseConfiguration.JDBC_PASSWORD, "");
config.set(ClickhouseConfiguration.AUTO_CREATE_TABLE, true);
JanusGraph graph = JanusGraphFactory.open(config);
```

3. Clickhouse Configuration.

| Configuration           | Property Name                               | Description                                                                                                                                       | Data Type | Default Value            | Mutability |
| ----------------------- | ------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------- | --------- | ------------------------ | ---------- |
| AUTO_CREATE_TABLE       | storage.clickhouse.auto-create-table        | When this is true, JanusGraph will attempt to auto create tables if not exists. This is useful when running JanusGraph in first running instance. | Boolean   | true                     | MASKABLE   |
| JDBC_URL                | storage.clickhouse.jdbc.url                 | The jdbc url of the clickhouse.                                                                                                                   | String    | jdbc:ch://127.0.0.1:8123 | LOCAL      |
| JDBC_USERNAME           | storage.clickhouse.jdbc.username            | The jdbc username of the clickhouse.                                                                                                              | String    | (no default value)       | LOCAL      |
| JDBC_PASSWORD           | storage.clickhouse.jdbc.password            | The jdbc password of the clickhouse.                                                                                                              | String    | (no default value)       | LOCAL      |
| JDBC_MAXIMUM_POOL_SIZE  | storage.clickhouse.jdbc.maximum-pool-size   | The jdbc maximum pool size of the clickhouse.                                                                                                     | Integer   | 10                       | LOCAL      |
| JDBC_MINIMUM_IDLE       | storage.clickhouse.jdbc.minimum-idle        | The jdbc minimum idle of the clickhouse.                                                                                                          | Integer   | 5                        | LOCAL      |
| JDBC_MAX_LIFETIME       | storage.clickhouse.jdbc.max-lifetime        | The jdbc max lifetime of the clickhouse.                                                                                                          | Integer   | 1800000                  | LOCAL      |
| JDBC_IDLE_TIMEOUT       | storage.clickhouse.jdbc.idle-timeout        | The jdbc idle timeout of the clickhouse.                                                                                                          | Integer   | 600000                   | LOCAL      |
| JDBC_CONNECTION_TIMEOUT | storage.clickhouse.jdbc.connection-timeout  | The jdbc connection timeout of the clickhouse.                                                                                                    | Integer   | 60000                    | LOCAL      |
| JDBC_TCP_KEEPALIVE      | storage.clickhouse.jdbc.tcp-keepalive       | The jdbc if tcp keep alive enabled of the clickhouse.                                                                                             | Boolean   | true                     | LOCAL      |
| JDBC_KEEPALIVE_TIME     | storage.clickhouse.jdbc.keepalive-time      | The jdbc keep alive time of the clickhouse.                                                                                                       | Integer   | 60000                    | LOCAL      |
| BATCH_MUTATION_SIZE     | storage.clickhouse.jdbc.batch-mutation-size | The batch mutation(insert/delete) size of the clickhouse.                                                                                         | Integer   | 1000                     | LOCAL      |


Example of configuration:

```
storage.clickhouse.auto-create-table=true
storage.clickhouse.jdbc.url=jdbc:ch://127.0.0.1:8123/default
storage.clickhouse.jdbc.username=
storage.clickhouse.jdbc.password=
storage.clickhouse.jdbc.maximum-pool-size=10
storage.clickhouse.jdbc.minimum-idle=5
storage.clickhouse.jdbc.max-lifetime=1800000
storage.clickhouse.jdbc.idle-timeout=600000
storage.clickhouse.jdbc.connection-timeout=60000
storage.clickhouse.jdbc.tcp-keepalive=true
storage.clickhouse.jdbc.keepalive-time=60000
storage.clickhouse.jdbc.batch-mutation-size=1000
```

* More clickhouse JDBC usage see  [clickhouse-jdbc](https://github.com/ClickHouse/clickhouse-jdbc).


## Known limitations
1. ClickHouse® is a column-oriented database management system (DBMS) for online analytical processing of queries (OLAP). It's not transactions. Multiple instances write may case dirty reads that may lead to some unwanted side effect like ghost vertices.
2. It will create  [MergeTree](https://clickhouse.com/docs/en/engines/table-engines/mergetree-family/mergetree/) Engine of tables defaults. If you want to use ReplicatedMergeTree or Distributed, you can manually create tables. Table contains k,c,v columns, which types are Array(Int8).
    ```
    CREATE TABLE table_name (k Array(Int8), c Array(Int8), v Array(Int8)) ENGINE = ReplicatedMergeTree(...) ORDER BY (k,c)
    ```
    * You should create 9 tables as follows:
      * system_properties
      * system_properties_lock_
      * graphindex
      * graphindex_lock_
      * edgestore
      * edgestore_lock_
      * janusgraph_ids
      * systemlog
      * txlog

## TODO
1. Supports TTL.
2. Directly bulk load tools for janusgraph clickhouse backend.