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

package io.github.artjourney.janusgraph;

import io.github.artjourney.janusgraph.config.ClickhouseConfiguration;
import io.github.artjourney.janusgraph.datasource.QueryHelper;
import com.google.common.base.Preconditions;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * It's provides the persistence context to the graph database clickhouse storage backend.
 *
 * @author Jianting Mao &lt;maojianting@gmail.com&gt;
 */
public class ClickhouseStoreManager implements KeyColumnValueStoreManager {

    private final ConcurrentHashMap<String, ClickhouseKeyColumnValueStore> stores;
    private final StoreFeatures features;
    private final QueryHelper queryHelper;
    private final Configuration configuration;

    public ClickhouseStoreManager(final Configuration configuration) throws BackendException {
        this.configuration = configuration;
        stores = new ConcurrentHashMap<>();
        features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .unorderedScan(false)
            .keyOrdered(true)
            .batchMutation(true)
            .multiQuery(false)
            .persists(true)
            .optimisticLocking(true)
            .transactional(false)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .build();
        this.queryHelper = new QueryHelper(configuration);
        try {
            ensureTableExists();
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, StoreMetaData.Container metaData) throws BackendException {
        if (!stores.containsKey(name)) {
            stores.putIfAbsent(name, new ClickhouseKeyColumnValueStore(name, queryHelper));
        }
        KeyColumnValueStore store = stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeMut : mutations.entrySet()) {
            KeyColumnValueStore store = stores.get(storeMut.getKey());
            Preconditions.checkNotNull(store);
            for (Map.Entry<StaticBuffer, KCVMutation> keyMut : storeMut.getValue().entrySet()) {
                store.mutate(keyMut.getKey(), keyMut.getValue().getAdditions(), keyMut.getValue().getDeletions(), txh);
            }
        }
    }

    @Override
    public StoreTransaction beginTransaction(BaseTransactionConfig config) throws BackendException {
        return new ClickhouseTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        for (ClickhouseKeyColumnValueStore store : stores.values()) {
            store.close();
        }
        stores.clear();
        queryHelper.closeDatasource();
    }

    @Override
    public void clearStorage() throws BackendException {
        for (ClickhouseKeyColumnValueStore store : stores.values()) {
            store.clear();
        }
        stores.clear();
        queryHelper.closeDatasource();
    }

    @Override
    public boolean exists() throws BackendException {
        return checkExists();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public String getName() {
        return toString();
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    /**
     * Check table exists when initialize. If tables are not exists, and {@link ClickhouseConfiguration#AUTO_CREATE_TABLE} is true,
     * will automatic create tables if absent. Else throws {@link PermanentBackendException}.
     *
     * @return if all table exists.
     * @throws PermanentBackendException
     */
    private boolean ensureTableExists() throws PermanentBackendException {
        boolean autoCreateTable = configuration.get(ClickhouseConfiguration.AUTO_CREATE_TABLE);
        try {
            List<String> tableNotExists = queryHelper.checkExists();
            if (CollectionUtils.isNotEmpty(tableNotExists)) {
                if (!autoCreateTable) {
                    throw new PermanentBackendException("tables " + StringUtils.join(tableNotExists, ",") + " not exists.");
                }
                queryHelper.createTables(tableNotExists);
            }
        } catch (SQLException e) {
            throw new PermanentBackendException(e);
        }
        return true;
    }

    /**
     * Check all tables are exists.
     *
     * @return all tables are exists.
     * @throws BackendException  throws exception when database error occurs.
     */
    private boolean checkExists() throws BackendException {
        try {
            return CollectionUtils.isEmpty(queryHelper.checkExists());
        } catch (SQLException e) {
            throw new PermanentBackendException(e);
        }
    }
}
