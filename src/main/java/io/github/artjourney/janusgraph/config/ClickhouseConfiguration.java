/*
 * Copyright Jianting Mao, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.artjourney.janusgraph.config;

import org.janusgraph.diskstorage.configuration.ConfigNamespace;
import org.janusgraph.diskstorage.configuration.ConfigOption;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import java.util.Objects;

/**
 * The configuration option of clickhouse storage backend.
 *
 * @author Jianting Mao &lt;maojianting@gmail.com&gt;
 */
public class ClickhouseConfiguration {

    public static final ConfigNamespace CLICKHOUSE_NS = new ConfigNamespace(GraphDatabaseConfiguration.STORAGE_NS, "clickhouse", "Clickhouse storage options");
    public static final ConfigNamespace CLICKHOUSE_JDBC = new ConfigNamespace(CLICKHOUSE_NS, "jdbc", "General configuration options for clickhouse jdbc.");

    public static final ConfigOption<Boolean> AUTO_CREATE_TABLE =
        new ConfigOption<>(CLICKHOUSE_NS, "auto-create-table",
            "When this is true, JanusGraph will attempt to auto create tables if not exists. " +
                "This is useful when running JanusGraph in first running instance.",
            ConfigOption.Type.MASKABLE, true);

    public static final ConfigOption<String> JDBC_URL =
        new ConfigOption<>(CLICKHOUSE_JDBC, "url", "The jdbc url of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, "jdbc:ch://127.0.0.1:8123");

    public static final ConfigOption<String> JDBC_USERNAME =
        new ConfigOption<String>(CLICKHOUSE_JDBC, "username", "The jdbc username of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, String.class, Objects::nonNull);

    public static final ConfigOption<String> JDBC_PASSWORD =
        new ConfigOption<String>(CLICKHOUSE_JDBC, "password", "The jdbc password of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, String.class, Objects::nonNull);

    public static final ConfigOption<Integer> JDBC_MAXIMUM_POOL_SIZE =
        new ConfigOption<>(CLICKHOUSE_JDBC, "maximum-pool-size", "The jdbc maximum pool size of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, 10);

    public static final ConfigOption<Integer> JDBC_MINIMUM_IDLE =
        new ConfigOption<>(CLICKHOUSE_JDBC, "minimum-idle", "The jdbc minimum idle of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, 5);

    public static final ConfigOption<Integer> JDBC_MAX_LIFETIME =
        new ConfigOption<>(CLICKHOUSE_JDBC, "max-lifetime", "The jdbc max lifetime of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, 1800000);

    public static final ConfigOption<Integer> JDBC_IDLE_TIMEOUT =
        new ConfigOption<>(CLICKHOUSE_JDBC, "idle-timeout", "The jdbc idle timeout of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, 600000);

    public static final ConfigOption<Integer> JDBC_CONNECTION_TIMEOUT =
        new ConfigOption<>(CLICKHOUSE_JDBC, "connection-timeout", "The jdbc connection timeout of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, 60000);

    public static final ConfigOption<Boolean> JDBC_TCP_KEEPALIVE =
        new ConfigOption<>(CLICKHOUSE_JDBC, "tcp-keepalive", "The jdbc if tcp keep alive enabled of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, true);

    public static final ConfigOption<Integer> JDBC_KEEPALIVE_TIME =
        new ConfigOption<>(CLICKHOUSE_JDBC, "keepalive-time", "The jdbc keep alive time of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, 60000);

    public static final ConfigOption<Integer> BATCH_MUTATION_SIZE =
        new ConfigOption<>(CLICKHOUSE_JDBC, "batch-mutation-size", "The batch mutation(insert/delete) size of the clickhouse JanusGraph will use.",
            ConfigOption.Type.LOCAL, 1000);

}
