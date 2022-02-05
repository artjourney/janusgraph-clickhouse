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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Convert ResultSet to different types.
 *
 * @author Jianting Mao &lt;maojianting@gmail.com&gt;
 */
public class ResultSetParserUtil {

    /**
     * Convert ResultSet to byte array list.
     *
     * @param rs ResultSet.
     * @return byte array data.
     * @throws SQLException throws exception when database error occurs.
     */
    public static List<byte[][]> convertToBytes(ResultSet rs) throws SQLException {
        List<byte[][]> list = new ArrayList<>();
        while (rs.next()) {
            int columnCount = rs.getMetaData().getColumnCount();
            byte[][] bytes = new byte[columnCount][];
            for (int i = 0; i < columnCount; i++) {
                bytes[i] = (byte[]) rs.getArray(i + 1).getArray();
            }
            list.add(bytes);
        }
        return list;
    }

    /**
     * Convert ResultSet to map list.
     *
     * @param rs ResultSet.
     * @return map result.
     * @throws SQLException throws exception when database error occurs.l
     */
    public static List<Map<String, Object>> convertToMapList(ResultSet rs) throws SQLException {
        List<Map<String, Object>> list = new ArrayList<>();
        while (rs.next()) {
            ResultSetMetaData metaData = rs.getMetaData();
            int totalRows = metaData.getColumnCount();
            Map<String, Object> map = new LinkedHashMap<>();
            for (int i = 0; i < totalRows; i++) {
                String key = metaData.getColumnLabel(i + 1);
                Object value = rs.getObject(i + 1);
                map.put(key, value);
            }
            list.add(map);
        }
        return list;
    }

}
