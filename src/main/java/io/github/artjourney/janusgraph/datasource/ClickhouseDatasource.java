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

package io.github.artjourney.janusgraph.datasource;

import com.clickhouse.jdbc.ClickHouseDataSource;
import io.github.artjourney.janusgraph.config.ClickhouseConfiguration;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.janusgraph.diskstorage.configuration.Configuration;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import static com.clickhouse.client.config.ClickHouseDefaults.PASSWORD;
import static com.clickhouse.client.config.ClickHouseDefaults.USER;

/**
 * Clickhouse datasource, use HikariCP(<a href="https://github.com/brettwooldridge/HikariCP</a>) to manage the datasource.
 *
 * @author Jianting Mao &lt;maojianting@gmail.com&gt;
 */
public class ClickhouseDatasource {

    private final HikariDataSource dataSource;

    public ClickhouseDatasource(Configuration config) throws SQLException {
        Properties properties = new Properties();
        properties.setProperty(USER.getKey(), config.get(ClickhouseConfiguration.JDBC_USERNAME));
        properties.setProperty(PASSWORD.getKey(), config.get(ClickhouseConfiguration.JDBC_PASSWORD));
        ClickHouseDataSource dataSource = new ClickHouseDataSource(config.get(ClickhouseConfiguration.JDBC_URL), properties);

        HikariConfig conf = new HikariConfig();
        conf.setDataSource(dataSource);
        conf.setMaximumPoolSize(config.get(ClickhouseConfiguration.JDBC_MAXIMUM_POOL_SIZE));
        conf.setMinimumIdle(config.get(ClickhouseConfiguration.JDBC_MINIMUM_IDLE));
        conf.setMaxLifetime(config.get(ClickhouseConfiguration.JDBC_MAX_LIFETIME));
        conf.setIdleTimeout(config.get(ClickhouseConfiguration.JDBC_IDLE_TIMEOUT));
        conf.setConnectionTimeout(config.get(ClickhouseConfiguration.JDBC_CONNECTION_TIMEOUT));
        if (config.get(ClickhouseConfiguration.JDBC_TCP_KEEPALIVE) != null && config.get(ClickhouseConfiguration.JDBC_TCP_KEEPALIVE)) {
            conf.setKeepaliveTime(config.get(ClickhouseConfiguration.JDBC_KEEPALIVE_TIME));
        }
        this.dataSource = new HikariDataSource(conf);
    }

    /**
     * Get a connection from connection pool.
     *
     * @return a connection of clickhouse.
     * @throws SQLException throws exception when database error occurs.
     */
    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    /**
     * Close the datasource.
     */
    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
        }
    }

    /**
     * Evict a connection when finish using.
     *
     * @param connection database connection.
     */
    public void evict(Connection connection) {
        dataSource.evictConnection(connection);
    }
}
