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

import org.apache.commons.collections.CollectionUtils;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.configuration.Configuration;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.janusgraph.diskstorage.Backend.EDGESTORE_NAME;
import static org.janusgraph.diskstorage.Backend.INDEXSTORE_NAME;
import static org.janusgraph.diskstorage.Backend.LOCK_STORE_SUFFIX;
import static org.janusgraph.diskstorage.Backend.SYSTEM_MGMT_LOG_NAME;
import static org.janusgraph.diskstorage.Backend.SYSTEM_TX_LOG_NAME;
import static io.github.artjourney.janusgraph.config.ClickhouseConfiguration.BATCH_MUTATION_SIZE;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.IDS_STORE_NAME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_PROPERTIES_STORE_NAME;

/**
 * All the operations of database.
 *
 * @author Jianting Mao &lt;maojianting@gmail.com&gt;
 */
public class QueryHelper {

    private final ClickhouseDatasource datasource;
    private final Configuration configuration;
    private final int batchMutationSize;

    /**
     * Create table query. The tables contain with k(key), c(column), v(value) columns, and the types are Array(Int8) stores bytes data.
     * Use MergeTree Engine default.
     */
    private static final String CREATE_TABLE_SQL = "CREATE TABLE IF NOT EXISTS %s (k Array(Int8), c Array(Int8), v Array(Int8)) ENGINE = MergeTree() ORDER BY (k,c)";
    /**
     * Slice query with key and column range.
     */
    private static final String SLICE_QUERY = "SELECT c,v FROM %s WHERE k=? AND c>=? AND c<? ORDER BY c LIMIT ?";
    /**
     * Query all keys with key range.
     */
    private static final String KEY_QUERY = "SELECT DISTINCT k FROM %s WHERE k>=? AND k<? ORDER BY k";
    /**
     * Insert data to clickhouse storage.
     */
    private static final String INSET_QUERY = "INSERT INTO %s(k, c, v) VALUES (?, ?, ?)";
    /**
     * Delete data from clickhouse storage.
     */
    private static final String DELETE_QUERY = "ALTER TABLE %s DELETE WHERE k=? AND c=?";
    /**
     * Drop tables when clear storage.
     */
    private static final String DROP_TABLE = "DROP TABLE %s";
    /**
     * Test if table exists.
     */
    private static final String CHECK_EXISTS_TABLE = "EXISTS TABLE %s";

    public QueryHelper(Configuration config) throws PermanentBackendException {
        this.configuration = config;
        try {
            this.datasource = new ClickhouseDatasource(config);
            this.batchMutationSize = config.get(BATCH_MUTATION_SIZE);
        } catch (SQLException e) {
            throw new PermanentBackendException("clickhouse backend datasource initialize error.", e);
        }
    }

    /**
     * Get all the tables' name want to create or use.
     *
     * @param config janusgraph diskstorage configuration
     * @return all the tables' name
     */
    public static List<String> getTableNames(Configuration config) {
        List<String> list = new ArrayList<>(9);
        list.add(SYSTEM_TX_LOG_NAME);
        list.add(SYSTEM_MGMT_LOG_NAME);
        list.add(SYSTEM_PROPERTIES_STORE_NAME);
        list.add(SYSTEM_PROPERTIES_STORE_NAME + LOCK_STORE_SUFFIX);
        list.add(INDEXSTORE_NAME);
        list.add(INDEXSTORE_NAME + LOCK_STORE_SUFFIX);
        list.add(EDGESTORE_NAME);
        list.add(EDGESTORE_NAME + LOCK_STORE_SUFFIX);
        list.add(config.get(IDS_STORE_NAME));
        return list;
    }

    /**
     * Check tables if exists, return tables if table not exists.
     *
     * @return not exists tables.
     * @throws SQLException throws exception when database error occurs.
     */
    public List<String> checkExists() throws SQLException {
        List<String> tables = getTableNames(configuration);
        List<String> notExists = new ArrayList<>();
        for (String table : tables) {
            List<Map<String, Object>> list = query(String.format(CHECK_EXISTS_TABLE, table));
            if ((Short) (list.get(0).get("result")) == 0) {
                notExists.add(table);
            }
        }
        return notExists;
    }

    /**
     * Create tables with list names.
     *
     * @param tables tables want to create.
     * @throws SQLException throws exception when database error occurs.
     */
    public void createTables(List<String> tables) throws SQLException {
        if (CollectionUtils.isEmpty(tables)) {
            return;
        }
        List<String> queries = tables.stream().map(t -> String.format(CREATE_TABLE_SQL, t)).collect(Collectors.toList());
        execute(queries, queries.size());
    }

    /**
     * Slice query with key and column range.
     *
     * @param name        table name.
     * @param key         table key.
     * @param columnStart range start inclusive.
     * @param columnEnd   range end exclusive.
     * @param limit       query limit.
     * @return the byte array of the slice data.
     * @throws SQLException throws exception when database error occurs.
     */
    public List<byte[][]> getSlice(String name, byte[] key, byte[] columnStart, byte[] columnEnd, int limit) throws SQLException {
        String query = String.format(SLICE_QUERY, name);
        return queryByteArray(query, key, columnStart, columnEnd, limit);
    }

    /**
     * Get all keys with name and key range.
     *
     * @param name     table name.
     * @param keyStart key range start inclusive.
     * @param keyEnd   key range end exclusive.
     * @return the byte array of the keys.
     * @throws SQLException throws exception when database error occurs.
     */
    public List<byte[]> getKeys(String name, byte[] keyStart, byte[] keyEnd) throws SQLException {
        String query = String.format(KEY_QUERY, name);
        List<byte[][]> results = queryByteArray(query, keyStart, keyEnd);
        return results.stream().map(t -> t[0]).collect(Collectors.toList());
    }

    /**
     * Insert data to database.
     *
     * @param name   table name.
     * @param params params of insert data.
     * @throws SQLException throws exception when database error occurs.
     */
    public void insert(String name, List<byte[][]> params) throws SQLException {
        String query = String.format(INSET_QUERY, name);
        execute(query, params, batchMutationSize);
    }

    /**
     * Delete data from database.
     *
     * @param name   table name.
     * @param params params of delete data.
     * @throws SQLException throws exception when database error occurs.
     */
    public void delete(String name, List<byte[][]> params) throws SQLException {
        String query = String.format(DELETE_QUERY, name);
        execute(query, params, batchMutationSize);
    }

    /**
     * Drop table.
     *
     * @param table table name.
     * @throws SQLException throws exception when database error occurs.
     */
    public void dropTable(String table) throws SQLException {
        String query = String.format(DROP_TABLE, table);
        execute(query);
    }

    /**
     * close the datasource.
     */
    public void closeDatasource() {
        datasource.close();
    }

    /**
     * Execute single query.
     *
     * @param query sql query.
     * @throws SQLException throws exception when database error occurs.
     */
    private void execute(String query) throws SQLException {
        Connection connection = datasource.getConnection();
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(query);
        } finally {
            datasource.evict(connection);
        }
    }

    /**
     * Execute multiply queries.
     *
     * @param queries   sql query list.
     * @param batchSize query batch size.
     * @throws SQLException throws exception when database error occurs.
     */
    private void execute(List<String> queries, int batchSize) throws SQLException {
        Connection connection = datasource.getConnection();
        try (Statement stmt = connection.createStatement()) {
            for (int i = 0; i < queries.size(); i++) {
                stmt.addBatch(queries.get(i));
                if ((i + 1) % batchSize == 0 || i == queries.size() - 1) {
                    stmt.executeBatch();
                }
            }
        } finally {
            datasource.evict(connection);
        }
    }

    /**
     * Execute single query with params.
     *
     * @param query     sql query.
     * @param params    query params.
     * @param batchSize query batch size.
     * @throws SQLException throws exception when database error occurs.
     */
    private void execute(String query, List<byte[][]> params, int batchSize) throws SQLException {
        Connection connection = datasource.getConnection();
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            for (int i = 0; i < params.size(); i++) {
                byte[][] ps = params.get(i);
                for (int j = 0; j < ps.length; j++) {
                    preparedStatement.setObject(j + 1, ps[j]);
                }
                preparedStatement.addBatch();
                if ((i + 1) % batchSize == 0 || i == params.size() - 1) {
                    preparedStatement.executeBatch();
                }
            }
        } finally {
            datasource.evict(connection);
        }
    }

    /**
     * Query byte array with single query and params.
     *
     * @param query  sql query.
     * @param params query params.
     * @return byte arrays of data.
     * @throws SQLException throws exception when database error occurs.
     */
    private List<byte[][]> queryByteArray(String query, Object... params) throws SQLException {
        Connection connection = datasource.getConnection();
        try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            for (int i = 0; i < params.length; i++) {
                preparedStatement.setObject(i + 1, params[i]);
            }
            try (ResultSet rs = preparedStatement.executeQuery()) {
                return ResultSetParserUtil.convertToBytes(rs);
            }
        } finally {
            datasource.evict(connection);
        }
    }

    /**
     * Single query and return map data.
     *
     * @param query sql query.
     * @return the map data of query.
     * @throws SQLException throws exception when database error occurs.
     */
    private List<Map<String, Object>> query(String query) throws SQLException {
        Connection connection = datasource.getConnection();
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery(query);
            return ResultSetParserUtil.convertToMapList(rs);
        } finally {
            datasource.evict(connection);
        }
    }
}
