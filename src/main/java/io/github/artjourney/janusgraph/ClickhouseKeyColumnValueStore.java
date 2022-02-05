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

import io.github.artjourney.janusgraph.datasource.QueryHelper;
import com.google.common.collect.Maps;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.PermanentBackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyIterator;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRangeQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.KeySlicesIterator;
import org.janusgraph.diskstorage.keycolumnvalue.MultiSlicesQuery;
import org.janusgraph.diskstorage.keycolumnvalue.SliceQuery;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.diskstorage.util.EntryArrayList;
import org.janusgraph.diskstorage.util.NoLock;
import org.janusgraph.diskstorage.util.RecordIterator;
import org.janusgraph.diskstorage.util.StaticArrayBuffer;
import org.janusgraph.diskstorage.util.StaticArrayEntry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.janusgraph.diskstorage.StaticBuffer.ARRAY_FACTORY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.STORAGE_TRANSACTIONAL;

/**
 * Implementation of {@link KeyColumnValueStore} of clickhouse storage backend.
 *
 * @author Jianting Mao &lt;maojianting@gmail.com&gt;
 */
public class ClickhouseKeyColumnValueStore implements KeyColumnValueStore {

    private volatile ReentrantLock lock = null;
    private final String name;
    private final QueryHelper queryHelper;

    public ClickhouseKeyColumnValueStore(final String name, QueryHelper queryHelper) {
        this.name = name;
        this.queryHelper = queryHelper;
    }

    /**
     * Retrieves the list of entries (column-value pairs) for a specified query.
     *
     * @param query Query to get results for
     * @param txh   Transaction
     * @return List of entries up to a maximum of "limit" entries
     * @throws org.janusgraph.diskstorage.BackendException when columnEnd &lt; columnStart
     * @see KeySliceQuery
     */
    @Override
    public EntryList getSlice(KeySliceQuery query, StoreTransaction txh) throws BackendException {
        Lock lock = getLock(txh);
        lock.lock();
        try {
            byte[] key = shift(query.getKey());
            byte[] start = shift(query.getSliceStart());
            byte[] end = shift(query.getSliceEnd());

            List<byte[][]> list = queryHelper.getSlice(name, key, start, end, query.getLimit());
            final EntryArrayList result = new EntryArrayList();
            list.forEach(t -> result.add(StaticArrayEntry.of(StaticArrayBuffer.of(shift(t[0])), StaticArrayBuffer.of(shift(t[1])))));
            return result;
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retrieves the list of entries (column-value pairs) as specified by the given {@link SliceQuery} for all
     * of the given keys together.
     *
     * @param keys  List of keys
     * @param query Slicequery specifying matching entries
     * @param txh   Transaction
     * @return The result of the query for each of the given keys as a map from the key to the list of result entries.
     * @throws org.janusgraph.diskstorage.BackendException
     */
    @Override
    public Map<StaticBuffer, EntryList> getSlice(List<StaticBuffer> keys, SliceQuery query, StoreTransaction txh) throws BackendException {
        Map<StaticBuffer, EntryList> result = Maps.newHashMap();
        for (StaticBuffer key : keys) result.put(key, getSlice(new KeySliceQuery(key, query), txh));
        return result;
    }

    /**
     * Verifies acquisition of locks {@code txh} from previous calls to
     * {@link #acquireLock(StaticBuffer, StaticBuffer, StaticBuffer, StoreTransaction)}
     * , then writes supplied {@code additions} and/or {@code deletions} to
     * {@code key} in the underlying data store. Deletions are applied strictly
     * before additions. In other words, if both an addition and deletion are
     * supplied for the same column, then the column will first be deleted and
     * then the supplied Entry for the column will be added.
     *
     * @param key       the key under which the columns in {@code additions} and
     *                  {@code deletions} will be written
     * @param additions the list of Entry instances representing column-value pairs to
     *                  create under {@code key}, or null to add no column-value pairs
     * @param deletions the list of columns to delete from {@code key}, or null to
     *                  delete no columns
     * @param txh       the transaction to use
     * @throws org.janusgraph.diskstorage.BackendException
     */
    @Override
    public void mutate(StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions, StoreTransaction txh) throws BackendException {
        Lock lock = getLock(txh);
        lock.lock();
        try {
            int addSize = additions.size();
            int delSize = deletions.size();

            List<byte[][]> adds = new ArrayList<>(addSize);
            List<byte[][]> deletes = new ArrayList<>(delSize + addSize);

            if (delSize > 0) {
                for (StaticBuffer delete : deletions) {
                    byte[] k = shift(key);
                    byte[] c = shift(delete);
                    deletes.add(new byte[][]{k, c});
                }
            }
            if (addSize > 0) {
                for (Entry entry : additions) {
                    byte[] k = shift(key);
                    byte[] c = shift(entry.getColumn());
                    byte[] v = shift(entry.getValue());
                    adds.add(new byte[][]{k, c, v});
                    deletes.add(new byte[][]{k, c});
                }
            }

            if (delSize + addSize > 0) {
                queryHelper.delete(name, deletes);
            }
            if (addSize > 0) {
                queryHelper.insert(name, adds);
            }
        } catch (Exception e) {
            throw new PermanentBackendException("clickhouse mutation error.", e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns a {@link KeyIterator} over all keys that fall within the key-range specified by the given query and have one or more columns matching the column-range.
     * Calling {@link KeyIterator#getEntries()} returns the list of all entries that match the column-range specified by the given query.
     * <p>
     * This method is only supported by stores which keep keys in byte-order.
     *
     * @param query key range query
     * @param txh   StoreTransaction
     * @return KeyIterator
     * @throws org.janusgraph.diskstorage.BackendException
     */
    @Override
    public KeyIterator getKeys(KeyRangeQuery query, StoreTransaction txh) throws BackendException {
        return new RowIterator(queryHelper, name, query);
    }

    @Override
    public void close() throws BackendException {
    }

    public void clear() throws BackendException {
        try {
            queryHelper.dropTable(name);
        } catch (Exception e) {
            throw new PermanentBackendException(e);
        }
    }

    @Override
    public String getName() {
        return name;
    }


    @Override
    public void acquireLock(StaticBuffer key, StaticBuffer column, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public KeyIterator getKeys(SliceQuery query, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException("unordered scan not supported");
    }

    @Override
    public KeySlicesIterator getKeys(MultiSlicesQuery queries, StoreTransaction txh) throws BackendException {
        throw new UnsupportedOperationException();
    }

    private Lock getLock(StoreTransaction txh) {
        Boolean txOn = txh.getConfiguration().getCustomOption(STORAGE_TRANSACTIONAL);
        if (null != txOn && txOn) {
            ReentrantLock result = lock;
            if (result == null) {
                synchronized (this) {
                    result = lock;
                    if (result == null) {
                        lock = result = new ReentrantLock();
                    }
                }
            }
            return result;
        } else return NoLock.INSTANCE;
    }

    /**
     * key iterator implementation.
     */
    private static class RowIterator implements KeyIterator {

        private final Iterator<byte[]> rows;

        private byte[] nextRow;
        private byte[] currentRow;

        private boolean isClosed;
        private final QueryHelper queryHelper;
        private final String name;

        private final KeyRangeQuery query;

        public RowIterator(QueryHelper queryHelper, String name, KeyRangeQuery query) throws PermanentBackendException {
            this.queryHelper = queryHelper;
            this.name = name;
            this.query = query;

            byte[] ks = shift(query.getKeyStart());
            byte[] ke = shift(query.getKeyEnd());

            try {
                List<byte[]> keys = queryHelper.getKeys(name, ks, ke);
                this.rows = keys.iterator();
            } catch (Exception e) {
                throw new PermanentBackendException(e);
            }
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            if (null != nextRow) {
                return true;
            }
            while (rows.hasNext()) {
                nextRow = rows.next();
                if (nextRow != null) {
                    break;
                }
            }
            return null != nextRow;
        }

        @Override
        public StaticBuffer next() {
            ensureOpen();
            currentRow = nextRow;
            nextRow = null;
            return StaticArrayBuffer.of(shift(currentRow));
        }

        @Override
        public RecordIterator<Entry> getEntries() {
            ensureOpen();
            try {
                return new RecordIterator<Entry>() {
                    final Iterator<byte[][]> items;

                    {
                        try {
                            byte[] ss = shift(query.getSliceStart());
                            byte[] se = shift(query.getSliceEnd());
                            items = queryHelper.getSlice(name, currentRow, ss, se, query.getLimit()).iterator();
                        } catch (Exception e) {
                            throw new PermanentBackendException(e);
                        }
                    }

                    @Override
                    public void close() throws IOException {
                        isClosed = true;
                    }

                    @Override
                    public boolean hasNext() {
                        ensureOpen();
                        return items.hasNext();
                    }

                    @Override
                    public Entry next() {
                        ensureOpen();
                        byte[][] next = items.next();
                        return StaticArrayEntry.of(StaticArrayBuffer.of(shift(next[0])), StaticArrayBuffer.of(shift(next[1])));
                    }
                };
            } catch (PermanentBackendException e) {
                throw new JanusGraphException(e);
            }
        }

        @Override
        public void close() {
            isClosed = true;
        }

        private void ensureOpen() {
            if (isClosed) {
                throw new IllegalStateException("Iterator has been closed.");
            }
        }
    }

    /**
     * change the sign of the StaticBuffer.
     *
     * @param buffer StaticBuffer
     * @return changed byte array.
     */
    private static byte[] shift(StaticBuffer buffer) {
        return shift(buffer.as(ARRAY_FACTORY));
    }

    /**
     * Change the sign of the bytes in the array.
     * Janusgraph ordered data with 0x00(min) to 0xFF(max), but raw byte ordered by original byte value from -128(0x80 min) to 127(0x7F max).
     * So change the sign of the byte in order to fit the order rule.
     *
     * @param bytes byte array.
     * @return changed byte array.
     */
    private static byte[] shift(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        byte[] newBytes = new byte[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            newBytes[i] = (byte) (bytes[i] ^ 0x80);
        }
        return newBytes;
    }

}
